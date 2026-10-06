// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Bounded native publisher work with ordered asynchronous confirmations.

use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{StreamExt, future::BoxFuture, stream::FuturesOrdered};
use nautilus_common::msgbus::{BusMessage, MessageBusConfig, switchboard::CLOSE_TOPIC};
use redis::{Pipeline, streams::StreamMaxlen};

use super::{msgbus::RedisMessageBusConfig, stream_fields::bus_message_fields};

#[derive(Debug, Default)]
struct Outstanding {
    messages: usize,
    bytes: usize,
    failed: bool,
}

/// Credits cover the original channel, pending buffer and unconfirmed batches.
/// The close marker needs no credit, so saturation cannot prevent shutdown.
#[derive(Debug)]
pub(super) struct PublishBudget {
    maximum_messages: usize,
    maximum_bytes: usize,
    outstanding: Mutex<Outstanding>,
    failure: tokio::sync::Notify,
}

impl PublishBudget {
    pub(super) fn new(maximum_messages: usize, maximum_bytes: usize) -> Self {
        Self {
            maximum_messages,
            maximum_bytes,
            outstanding: Mutex::default(),
            failure: tokio::sync::Notify::new(),
        }
    }

    pub(super) fn reserve(&self, message: &BusMessage) -> anyhow::Result<()> {
        let mut work = self.outstanding.lock().expect("Publisher budget poisoned");
        let bytes = message_bytes(message);
        if work.failed
            || work.messages >= self.maximum_messages
            || bytes > self.maximum_bytes.saturating_sub(work.bytes)
        {
            work.failed = true;
            self.failure.notify_one();
            anyhow::bail!("Native publisher outstanding-work limit exceeded");
        }
        work.messages += 1;
        work.bytes += bytes;
        Ok(())
    }

    pub(super) fn release(&self, messages: usize, bytes: usize) {
        let mut work = self.outstanding.lock().expect("Publisher budget poisoned");
        work.messages = work
            .messages
            .checked_sub(messages)
            .expect("Publisher message credit");
        work.bytes = work
            .bytes
            .checked_sub(bytes)
            .expect("Publisher byte credit");
    }

    pub(super) fn is_failed(&self) -> bool {
        self.outstanding
            .lock()
            .expect("Publisher budget poisoned")
            .failed
    }

    async fn failed(&self) {
        if !self.is_failed() {
            self.failure.notified().await;
        }
    }
}

fn message_bytes(message: &BusMessage) -> usize {
    message
        .payload
        .len()
        .saturating_add(message.topic.as_str().len())
}

pub(super) fn validate(config: &RedisMessageBusConfig) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..=64).contains(&config.publish_max_inflight_batches)
            && (1..=4096).contains(&config.publish_batch_max_messages),
        "Native publisher batch limits are invalid"
    );

    if let Some(maximum) = config.publish_max_outstanding_messages {
        anyhow::ensure!(
            maximum > 0 && config.publish_max_outstanding_bytes > 0,
            "Native publisher outstanding-work limits must be positive"
        );
    }
    anyhow::ensure!(
        config.publish_max_inflight_batches == 1
            || config.publish_max_outstanding_messages.is_some(),
        "Asynchronous publication requires bounded outstanding work"
    );
    anyhow::ensure!(
        config.publish_coalesce_topics.is_empty()
            || config.publish_max_outstanding_messages.is_some(),
        "Quote coalescing requires bounded outstanding work"
    );
    // Age trimming is an independent awaited query in the original publisher.
    // Keep that path unchanged rather than change its transaction/error semantics.
    anyhow::ensure!(
        config
            .publish_coalesce_topics
            .iter()
            .all(|topic| { topic.starts_with("data.") && !topic.contains(['*', '?']) }),
        "Coalescing requires exact data topics"
    );
    Ok(())
}

fn release_message(budget: Option<&PublishBudget>, message: &BusMessage) {
    if let Some(budget) = budget {
        budget.release(1, message_bytes(message));
    }
}

fn buffer_message(
    buffer: &mut VecDeque<BusMessage>,
    message: BusMessage,
    latest_topics: &[String],
    budget: Option<&PublishBudget>,
) {
    if latest_topics
        .iter()
        .any(|topic| topic == message.topic.as_str())
        && let Some(index) = buffer.iter().position(|old| old.topic == message.topic)
    {
        let old = buffer.remove(index).expect("Buffered quote index");
        release_message(budget, &old);
    }
    buffer.push_back(message);
}

fn batch_pipeline(
    buffer: &mut VecDeque<BusMessage>,
    stream: &str,
    config: &MessageBusConfig,
    maximum_messages: usize,
) -> (Pipeline, usize, usize) {
    let mut pipe = redis::pipe();
    pipe.atomic();
    let count = maximum_messages.min(buffer.len());
    let mut bytes = 0;
    for message in buffer.drain(..count) {
        bytes += message_bytes(&message);
        let key = if config.stream_per_topic {
            format!("{stream}:{}", message.topic)
        } else {
            stream.to_owned()
        };
        let encoding = message.encoding.to_string();
        let fields = bus_message_fields(&message, &encoding);
        if let Some(maximum) = config.autotrim_maxlen.filter(|maximum| *maximum > 0) {
            pipe.xadd_maxlen(
                &key,
                StreamMaxlen::Approx(maximum as usize),
                "*",
                fields.as_slice(),
            );
        } else {
            pipe.xadd(&key, "*", fields.as_slice());
        }
    }
    (pipe, count, bytes)
}

/// Uses one original multiplexed connection, polling submissions in FIFO order.
/// The callback owns a clone of that connection, not a new socket or driver.
pub(super) async fn run<F, R>(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<BusMessage>,
    stream: String,
    config: MessageBusConfig,
    backing: RedisMessageBusConfig,
    budget: Option<Arc<PublishBudget>>,
    mut publish: F,
) -> anyhow::Result<()>
where
    F: FnMut(Pipeline) -> R,
    R: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    validate(&backing)?;
    anyhow::ensure!(
        config.autotrim_mins.is_none_or(|minutes| minutes == 0),
        "Bounded publication does not support age trimming; use native MAXLEN"
    );
    let mut buffer = VecDeque::new();
    let mut pending: FuturesOrdered<BoxFuture<'static, anyhow::Result<()>>> = FuturesOrdered::new();
    let interval = Duration::from_millis(u64::from(config.buffer_interval_ms.unwrap_or(0)));
    let timer = tokio::time::sleep(interval);
    tokio::pin!(timer);
    let mut flush = interval.is_zero();
    let mut closing = false;
    let mut saturation_seen = false;

    loop {
        if (closing || flush || buffer.len() >= backing.publish_batch_max_messages)
            && !buffer.is_empty()
            && pending.len() < backing.publish_max_inflight_batches
        {
            let (pipe, messages, bytes) = batch_pipeline(
                &mut buffer,
                &stream,
                &config,
                backing.publish_batch_max_messages,
            );
            let confirmation = publish(pipe);
            let credit = budget.clone();
            pending.push_back(Box::pin(async move {
                let result = confirmation.await;
                if let Some(credit) = credit {
                    credit.release(messages, bytes);
                }
                result
            }));
            flush = interval.is_zero();
        }

        if closing && buffer.is_empty() && pending.is_empty() {
            anyhow::ensure!(
                !saturation_seen,
                "Native publisher rejected work at its bounded limit"
            );
            return Ok(());
        }

        tokio::select! {
            // Poll ordered request futures before accepting the next batch.
            // This submits each MULTI/EXEC before its successor on the same driver.
            biased;
            confirmation = pending.next(), if !pending.is_empty() => {
                confirmation.expect("Pending publisher confirmation")?;
            }
            () = async {
                match budget.as_ref() {
                    Some(budget) => budget.failed().await,
                    None => std::future::pending().await,
                }
            }, if !saturation_seen => {
                saturation_seen = true;
                receiver.close();
            }
            message = receiver.recv(), if !closing && (
                buffer.len() < backing.publish_batch_max_messages
                    || !backing.publish_coalesce_topics.is_empty()
            ) => {
                match message {
                    Some(message) if message.topic == CLOSE_TOPIC => {
                        receiver.close();
                        closing = true;
                    }
                    Some(message) => {
                        if buffer.is_empty() {
                            timer.as_mut().reset(tokio::time::Instant::now() + interval);
                        }
                        buffer_message(&mut buffer, message, &backing.publish_coalesce_topics,
                                       budget.as_deref());
                    }
                    None => closing = true,
                }
            }
            () = &mut timer, if !interval.is_zero() && !flush && !buffer.is_empty() => {
                flush = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use nautilus_common::{enums::SerializationEncoding, msgbus::BusPayloadType};
    use rstest::rstest;
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, BufReader},
        net::TcpStream,
        sync::{mpsc, oneshot},
    };

    use super::*;

    fn message(topic: &str, payload: &'static [u8]) -> BusMessage {
        BusMessage::with_str_topic(
            topic,
            BusPayloadType::QuoteTick,
            Bytes::from_static(payload),
            SerializationEncoding::Json,
        )
    }

    fn limits(concurrent: usize) -> RedisMessageBusConfig {
        RedisMessageBusConfig {
            publish_max_inflight_batches: concurrent,
            publish_batch_max_messages: 1,
            publish_max_outstanding_messages: Some(4),
            publish_max_outstanding_bytes: 4096,
            ..Default::default()
        }
    }

    #[rstest]
    fn outstanding_bound_includes_unconfirmed_work_and_latches_failure() {
        let budget = PublishBudget::new(2, 128);
        let fact = message("events.order", b"fact");
        budget.reserve(&fact).unwrap();
        budget.reserve(&fact).unwrap();
        assert!(budget.reserve(&fact).is_err());
        budget.release(2, 2 * message_bytes(&fact));
        assert!(budget.is_failed());
        assert!(budget.reserve(&fact).is_err());
    }

    #[rstest]
    fn oversized_payload_latches_failure_without_retaining_it() {
        let budget = PublishBudget::new(10, 1);
        assert!(
            budget
                .reserve(&message("data.quote", b"oversized"))
                .is_err()
        );
        let outstanding = budget.outstanding.lock().unwrap();
        assert_eq!((outstanding.messages, outstanding.bytes), (0, 0));
        assert!(outstanding.failed);
    }

    #[rstest]
    fn only_selected_unsent_quotes_are_superseded_and_facts_stay_ordered() {
        let budget = PublishBudget::new(10, 1024);
        let mut buffer = VecDeque::new();
        let selected = vec!["data.quote".to_owned()];
        for msg in [
            message("data.quote", b"old"),
            message("events.order", b"first"),
            message("data.other", b"keep"),
            message("data.quote", b"new"),
            message("events.order", b"second"),
        ] {
            budget.reserve(&msg).unwrap();
            buffer_message(&mut buffer, msg, &selected, Some(&budget));
        }
        assert_eq!(
            buffer
                .iter()
                .map(|m| m.payload.as_ref())
                .collect::<Vec<_>>(),
            [b"first".as_slice(), b"keep", b"new", b"second"]
        );
        assert_eq!(budget.outstanding.lock().unwrap().messages, 4);
    }

    #[rstest]
    fn asynchronous_publication_requires_finite_work_limits() {
        let mut config = limits(2);
        config.publish_max_outstanding_messages = None;
        assert!(validate(&config).is_err());
        config.publish_max_outstanding_messages = Some(4);
        config.publish_coalesce_topics = vec!["events.order".into()];
        assert!(validate(&config).is_err());
        config.publish_coalesce_topics = vec!["data.*".into()];
        assert!(validate(&config).is_err());
    }

    #[tokio::test]
    async fn next_batch_is_submitted_before_first_ack_but_outstanding_is_bounded() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (sent, mut sends) = mpsc::unbounded_channel();
        let mut confirmations = VecDeque::new();
        let mut acknowledgments = Vec::new();
        for _ in 0..3 {
            let (ack, confirmation) = oneshot::channel();
            acknowledgments.push(Some(ack));
            confirmations.push_back(confirmation);
        }
        let config = limits(2);
        let budget = Arc::new(PublishBudget::new(4, 4096));
        for payload in [b"first".as_slice(), b"second", b"third"] {
            let fact = message("events.order", payload);
            budget.reserve(&fact).unwrap();
            tx.send(fact).unwrap();
        }
        tx.send(BusMessage::new_close()).unwrap();
        let handle = tokio::spawn(run(
            rx,
            "facts".into(),
            MessageBusConfig::default(),
            config,
            Some(budget.clone()),
            move |pipeline| {
                let sent = sent.clone();
                let confirmation = confirmations.pop_front().unwrap();
                async move {
                    sent.send(pipeline.get_packed_pipeline()).unwrap();
                    confirmation.await.map_err(anyhow::Error::from)?;
                    Ok(())
                }
            },
        ));
        for expected in [b"first".as_slice(), b"second"] {
            let packed = tokio::time::timeout(Duration::from_secs(1), sends.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(packed.windows(expected.len()).any(|part| part == expected));
        }
        assert_eq!(budget.outstanding.lock().unwrap().messages, 3);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sends.recv())
                .await
                .is_err()
        );
        acknowledgments[0].take().unwrap().send(()).unwrap();
        let third = tokio::time::timeout(Duration::from_secs(1), sends.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(third.windows(5).any(|part| part == b"third"));
        assert!(!handle.is_finished());
        acknowledgments[2].take().unwrap().send(()).unwrap();
        assert!(!handle.is_finished());
        acknowledgments[1].take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let work = budget.outstanding.lock().unwrap();
        assert_eq!((work.messages, work.bytes), (0, 0));
    }

    #[tokio::test]
    async fn sequential_positive_control_waits_for_first_ack() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (sent, mut sends) = mpsc::unbounded_channel();
        let (ack, confirmation) = oneshot::channel();
        let mut gate = Some(confirmation);
        tx.send(message("events.order", b"first")).unwrap();
        tx.send(message("events.order", b"second")).unwrap();
        drop(tx);
        let handle = tokio::spawn(run(
            rx,
            "facts".into(),
            MessageBusConfig::default(),
            limits(1),
            None,
            move |_| {
                let gate = gate.take();
                let sent = sent.clone();
                async move {
                    sent.send(()).unwrap();
                    if let Some(gate) = gate {
                        gate.await?;
                    }
                    Ok(())
                }
            },
        ));
        tokio::time::timeout(Duration::from_secs(1), sends.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sends.recv())
                .await
                .is_err()
        );
        ack.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), sends.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn failed_confirmation_is_reported_and_receiver_closes() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(message("events.order", b"fact")).unwrap();
        let result = run(
            rx,
            "facts".into(),
            MessageBusConfig::default(),
            limits(2),
            None,
            |_| async { anyhow::bail!("controlled failed confirmation") },
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("controlled failed confirmation")
        );
        assert!(tx.is_closed());
    }

    #[tokio::test]
    async fn saturation_drains_accepted_facts_and_returns_failure() {
        let (tx, rx) = mpsc::unbounded_channel();
        let budget = Arc::new(PublishBudget::new(1, 1024));
        let accepted = message("events.order", b"accepted");
        budget.reserve(&accepted).unwrap();
        tx.send(accepted).unwrap();
        assert!(
            budget
                .reserve(&message("events.order", b"rejected"))
                .is_err()
        );
        let published = Arc::new(Mutex::new(Vec::new()));
        let captured = published.clone();
        let result = run(
            rx,
            "facts".into(),
            MessageBusConfig::default(),
            limits(2),
            Some(budget.clone()),
            move |pipeline| {
                captured
                    .lock()
                    .unwrap()
                    .push(pipeline.get_packed_pipeline());
                async { Ok(()) }
            },
        )
        .await;
        assert!(result.is_err());
        assert!(tx.is_closed());
        let published = published.lock().unwrap();
        assert_eq!(published.len(), 1);
        assert!(published[0].windows(8).any(|part| part == b"accepted"));
        assert_eq!(budget.outstanding.lock().unwrap().messages, 0);
    }
    async fn resp_command(reader: &mut BufReader<TcpStream>) -> Vec<Vec<u8>> {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('*'));
        let count: usize = line[1..].trim().parse().unwrap();
        let mut arguments = Vec::new();
        for _ in 0..count {
            line.clear();
            reader.read_line(&mut line).await.unwrap();
            assert!(line.starts_with('$'));
            let length: usize = line[1..].trim().parse().unwrap();
            let mut value = vec![0; length + 2];
            reader.read_exact(&mut value).await.unwrap();
            assert_eq!(&value[length..], b"\r\n");
            value.truncate(length);
            arguments.push(value);
        }
        arguments
    }

    // Exercises the canonical Redis multiplexed driver on one real TCP socket.
    // The server withholds the first EXEC reply until it checks for batch two.
    #[tokio::test]
    async fn canonical_driver_pipelines_both_roles_before_ack_and_preserves_fact_order() {
        use tokio::{io::AsyncWriteExt, net::TcpListener};

        for role in ["coordinator", "worker"] {
            for inflight in [1, 2] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server = tokio::spawn(async move {
                    let (socket, _) = listener.accept().await.unwrap();
                    let mut reader = BufReader::new(socket);
                    loop {
                        let args = resp_command(&mut reader).await;
                        if args[0] == b"MULTI" {
                            break;
                        }
                        assert_eq!(args[0], b"CLIENT");
                        reader.get_mut().write_all(b"+OK\r\n").await.unwrap();
                    }
                    let first = resp_command(&mut reader).await;
                    assert_eq!(first[0], b"XADD");
                    assert!(first.iter().any(|value| value == b"first"));
                    assert_eq!(resp_command(&mut reader).await[0], b"EXEC");
                    let before_ack = if reader.buffer().is_empty() {
                        let mut byte = [0];
                        tokio::time::timeout(
                            Duration::from_millis(50),
                            reader.get_ref().peek(&mut byte),
                        )
                        .await
                        .is_ok()
                    } else {
                        true
                    };
                    reader
                        .get_mut()
                        .write_all(b"+OK\r\n+QUEUED\r\n*1\r\n$3\r\n1-0\r\n")
                        .await
                        .unwrap();
                    assert_eq!(resp_command(&mut reader).await[0], b"MULTI");
                    let second = resp_command(&mut reader).await;
                    assert_eq!(second[0], b"XADD");
                    assert!(second.iter().any(|value| value == b"second"));
                    assert_eq!(resp_command(&mut reader).await[0], b"EXEC");
                    reader
                        .get_mut()
                        .write_all(b"+OK\r\n+QUEUED\r\n*1\r\n$3\r\n2-0\r\n")
                        .await
                        .unwrap();
                    before_ack
                });
                let client = redis::Client::open(format!("redis://{address}/")).unwrap();
                let connection = redis::aio::ConnectionManager::new(client).await.unwrap();
                let (tx, rx) = mpsc::unbounded_channel();
                let budget = Arc::new(PublishBudget::new(4, 4096));
                for payload in [b"first".as_slice(), b"second"] {
                    let fact = message("events.order", payload);
                    budget.reserve(&fact).unwrap();
                    tx.send(fact).unwrap();
                }
                tx.send(BusMessage::new_close()).unwrap();
                tokio::time::timeout(
                    Duration::from_secs(2),
                    run(
                        rx,
                        role.into(),
                        MessageBusConfig::default(),
                        limits(inflight),
                        Some(budget.clone()),
                        move |pipeline| {
                            let mut connection = connection.clone();
                            async move {
                                pipeline.query_async::<()>(&mut connection).await?;
                                Ok(())
                            }
                        },
                    ),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(server.await.unwrap(), inflight == 2);
                assert_eq!(budget.outstanding.lock().unwrap().messages, 0);
            }
        }
    }
}
