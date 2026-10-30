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

use std::sync::{Arc, mpsc};

use nautilus_common::{
    actor::registry::get_actor_unchecked,
    runner::{TimeEventMessage, TimeEventSender, get_time_event_sender, replace_time_event_sender},
    timer::TimeEventCallback,
};
use nautilus_live::runner::AsyncRunner;

use super::*;

const OWNER: &str = "QUEUED-TIMER-OWNER";
const TIMER: &str = "queued-native-poll";

#[derive(Debug)]
struct QueuedTimer {
    core: DataActorCore,
    calls: Rc<RefCell<Vec<TimeEvent>>>,
}

impl DataActor for QueuedTimer {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.clock().set_timer_ns(
            TIMER,
            DurationNanos::from_millis(10),
            None,
            None,
            None,
            Some(false),
            Some(false),
        )
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if self.calls.borrow().is_empty() {
            self.clock().cancel_timer(TIMER);
        }

        self.calls.borrow_mut().push(event.clone());
        Ok(())
    }
}
nautilus_actor!(QueuedTimer);

#[derive(Debug)]
struct QueuedSender(mpsc::Sender<TimeEventMessage>);

impl TimeEventSender for QueuedSender {
    fn send(&self, message: TimeEventMessage) {
        let _ = self.0.send(message);
    }
}

fn compose_queued_timer(
    instance: UUID4,
    virtual_clock: bool,
    calls: Rc<RefCell<Vec<TimeEvent>>>,
    sender: Option<Arc<dyn TimeEventSender>>,
) -> anyhow::Result<LiveNode> {
    let config = LiveNodeConfig {
        trader_id: "QUEUED-001".into(),
        instance_id: Some(instance),
        environment: Environment::Sandbox,
        load_state: false,
        save_state: false,
        ..Default::default()
    };
    let mut builder = LiveNodeBuilder::from_config(config)?.with_delay_post_stop_secs(0u64);

    if virtual_clock {
        builder =
            builder.with_native_clock_factory(ClockFactory::for_environment(Environment::Backtest));
    }

    builder = builder.add_simulated_exec_client(
        Some("XNAS".into()),
        Box::new(SandboxExecutionClientFactory::with_fee_model(
            FeeModelHandle::new(FixedFeeModel::new(Money::from("0.05 USD"), Some(true))?),
        )),
        Box::new(SandboxExecutionClientConfig {
            venue: "XNAS".into(),
            account_id: "XNAS-QUEUED-001".into(),
            account_type: AccountType::Cash,
            starting_balances: vec![Money::from("100.00 USD")],
            base_currency: Some(Currency::USD()),
            ..Default::default()
        }),
    )?;
    let mut node = builder.build()?;
    let previous_sender = get_time_event_sender();

    if let Some(sender) = sender {
        replace_time_event_sender(sender);
    }

    let registered = node.add_actor(QueuedTimer {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some(OWNER.into()),
            ..Default::default()
        }),
        calls,
    });
    replace_time_event_sender(previous_sender);
    registered?;
    Ok(node)
}

#[rstest]
#[case(None)]
#[case(Some("timer"))]
#[case(Some("callback"))]
#[case(Some("owner"))]
#[case(Some("unknown"))]
fn recorded_runner_resolves_actual_queued_poll_after_cancel(
    #[case] mismatch: Option<&str>,
) -> anyhow::Result<()> {
    let instance = UUID4::new();

    let (startup, frames, original_calls) = std::thread::spawn(move || {
        runtime().block_on(async move {
            let calls = Rc::new(RefCell::new(Vec::new()));
            let trace = Rc::new(RefCell::new(Trace::default()));
            let (sender, receiver) = mpsc::channel();
            let mut node = compose_queued_timer(
                instance,
                false,
                calls.clone(),
                Some(Arc::new(QueuedSender(sender))),
            )?;
            msgbus::set_bus_tap(Rc::new(Tap {
                trace: trace.clone(),
                evidence: Rc::new(RefCell::new(Evidence::default())),
            }));
            node.enable_runtime_dispatch_capture()?;
            node.start().await?;
            let startup = trace.borrow().frames.clone();

            // Both real worker messages hold their native leases before the first poll cancels
            let first = receiver.recv_timeout(Duration::from_secs(2))?;
            let second = receiver.recv_timeout(Duration::from_secs(2))?;
            anyhow::ensure!(calls.borrow().is_empty());
            anyhow::ensure!(AsyncRunner::handle_time_event(first.into()));
            let actor = get_actor_unchecked::<QueuedTimer>(&OWNER.into());
            anyhow::ensure!(!actor.clock().timer_exists(TIMER));
            anyhow::ensure!(AsyncRunner::handle_time_event(second.into()));
            let frames = trace.borrow().frames[startup.len()..].to_vec();
            anyhow::ensure!(
                frames.len() == 2
                    && frames.iter().all(|frame| frame.complete
                        && frame
                            .timer
                            .as_ref()
                            .is_some_and(|timer| timer.has_current_native_owner())),
                "Actual queued callback capture is incomplete"
            );
            let original_calls = calls.borrow().clone();
            anyhow::ensure!(original_calls.len() == 2);
            msgbus::clear_bus_tap();
            node.stop().await?;
            node.dispose();
            Ok::<_, anyhow::Error>((startup, frames, original_calls))
        })
    })
    .join()
    .expect("queued source thread")?;
    let mismatch = mismatch.map(str::to_owned);

    std::thread::spawn(move || {
        runtime().block_on(async move {
            let calls = Rc::new(RefCell::new(Vec::new()));
            let mut node = compose_queued_timer(instance, true, calls.clone(), None)?;
            node.begin_recorded(
                startup[0].observed_at,
                RecordedRunnerConfig {
                    startup_dispatches: startup.iter().map(|frame| frame.witness.clone()).collect(),
                    excluded_actors: Vec::new(),
                },
            )
            .await?;

            for (index, mut frame) in frames.into_iter().enumerate() {
                let mut timer = frame.timer.context("Actual queued timer witness absent")?;

                if index == 1 {
                    let actor = get_actor_unchecked::<QueuedTimer>(&OWNER.into());
                    anyhow::ensure!(!actor.clock().timer_exists(TIMER));

                    match mismatch.as_deref() {
                        Some("timer") => actor.clock().set_timer_ns(
                            TIMER,
                            DurationNanos::from_millis(10),
                            None,
                            None,
                            None,
                            Some(false),
                            Some(false),
                        )?,
                        Some("callback") => actor.clock_rc().borrow_mut().register_default_handler(
                            TimeEventCallback::RustLocal(Rc::new(|_| {}))
                                .with_native_target(OWNER.into()),
                        ),
                        Some("owner") => {
                            timer.binding.owner.as_mut().unwrap().generation += 1;
                            timer.actual_owner = timer.binding.owner;
                        }
                        Some("unknown") => {
                            timer.event.name = "never-armed".into();
                            frame.witness.semantic["name"] = json!("never-armed");
                        }
                        None => {}
                        _ => unreachable!(),
                    }
                }

                let result = node.process_recorded(RecordedRunnerInput {
                    sequence: startup.len() as u64 + index as u64 + 1,
                    observed_at: frame.observed_at,
                    witness: frame.witness,
                    kind: RecordedRunnerInputKind::Timer(timer),
                });

                if index == 1 && mismatch.is_some() {
                    let e = result.expect_err("Changed native registration was admitted");
                    let expected = if mismatch.as_deref() == Some("owner") {
                        "Recorded native callback clock is absent"
                    } else {
                        "Recorded callback or timer registration changed"
                    };
                    anyhow::ensure!(
                        e.to_string().contains(expected),
                        "Unexpected rejection: {e:#}"
                    );
                    anyhow::ensure!(calls.borrow().as_slice() == &original_calls[..1]);
                    node.end_recorded().await?;
                    node.dispose();
                    return Ok(());
                }

                result?;
            }

            anyhow::ensure!(*calls.borrow() == original_calls);
            let actor = get_actor_unchecked::<QueuedTimer>(&OWNER.into());
            anyhow::ensure!(!actor.clock().timer_exists(TIMER));
            node.end_recorded().await?;
            node.dispose();
            Ok::<_, anyhow::Error>(())
        })
    })
    .join()
    .expect("queued recorded thread")
}
