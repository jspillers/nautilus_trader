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

//! Controlled native channel ordering, with no wall-clock scheduling race.

use nautilus_common::{
    actor::registry::get_actor_unchecked,
    clock::VirtualClock,
    runner::{TimeEventMessage, get_time_event_sender, recorded},
};
use nautilus_live::runner::AsyncRunner;

use super::*;

const QUEUE_ACTOR: &str = "RECORDED-QUEUE";
const QUEUE_TIMER: &str = "recorded-queue-timer";
const QUEUE_SIGNAL: &str = "recorded_queued_data";

#[derive(Debug, Default)]
struct QueueEvidence {
    enqueued: Vec<QuoteTick>,
    delivered: Vec<QuoteTick>,
    timer_quotes: Vec<Option<QuoteTick>>,
}

#[derive(Debug)]
struct QueueProbe {
    core: DataActorCore,
    evidence: Rc<RefCell<QueueEvidence>>,
}

impl DataActor for QueueProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_signal(QUEUE_SIGNAL, None);
        self.subscribe_quotes(IDS[0].into(), None, None);
        self.clock()
            .set_time_alert_ns(QUEUE_TIMER, 10.into(), None, Some(false))
    }

    fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
        let quote = quote(signal.value.as_str())?;
        get_data_event_sender().send(DataEvent::Data(Data::Quote(quote)))?;
        self.evidence.borrow_mut().enqueued.push(quote);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        anyhow::ensure!(self.cache_ref().quote(&quote.instrument_id) == Some(quote));
        self.evidence.borrow_mut().delivered.push(*quote);
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        anyhow::ensure!(event.name.as_str() == QUEUE_TIMER);
        self.evidence
            .borrow_mut()
            .timer_quotes
            .push(self.cache_ref().quote(&IDS[0].into()).copied());
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.clock().cancel_timers();
        Ok(())
    }
}
nautilus_actor!(QueueProbe);

fn quote(value: &str) -> anyhow::Result<QuoteTick> {
    let ask = match value {
        "first" => "10.00",
        "second" => "11.00",
        _ => anyhow::bail!("Unsupported controlled native input"),
    };
    Ok(QuoteTick::new(
        IDS[0].into(),
        "9.00".into(),
        ask.into(),
        "100".into(),
        "100".into(),
        1.into(),
        1.into(),
    ))
}

fn external(value: &str) -> anyhow::Result<BusMessage> {
    let custom = CustomData::new(
        std::sync::Arc::new(Signal::new(
            QUEUE_SIGNAL.into(),
            value.into(),
            1.into(),
            1.into(),
        )),
        DataType::new("Signal", None, None),
    );
    Ok(BusMessage::new(
        *msgbus::switchboard::get_signal_topic(QUEUE_SIGNAL),
        BusPayloadType::Custom("Signal".into()),
        serde_json::to_vec(&custom)?.into(),
        SerializationEncoding::Json,
    ))
}

fn compose_queue(
    instance: UUID4,
    evidence: Rc<RefCell<QueueEvidence>>,
) -> anyhow::Result<LiveNode> {
    let instrument = Equity::builder()
        .instrument_id(IDS[0].into())
        .raw_symbol("STABLE".into())
        .currency(Currency::USD())
        .price_precision(2)
        .price_increment(Price::from("0.01"))
        .ts_event(0.into())
        .ts_init(0.into())
        .build()?;
    let mut node = LiveNodeBuilder::from_config(LiveNodeConfig {
        trader_id: "RECORDED-QUEUE-001".into(),
        instance_id: Some(instance),
        environment: Environment::Sandbox,
        exec_engine: LiveExecutionEngineConfig {
            reconciliation: true,
            reconciliation_startup_delay_secs: 0.0,
            ..Default::default()
        },
        risk_engine: LiveRiskEngineConfig {
            bypass: false,
            ..Default::default()
        },
        ..Default::default()
    })?
    .with_native_clock_factory(ClockFactory::test_default())
    .with_delay_post_stop_secs(0u64)
    .add_data_client(
        Some("RECORDED-DATA".into()),
        Box::new(InputFactory),
        Box::new(InputConfig {
            instruments: vec![InstrumentAny::Equity(instrument)],
            recorded_supported: true,
            evidence: Rc::new(RefCell::new(Evidence::default())),
        }),
    )?
    .add_simulated_exec_client(
        Some("XNAS".into()),
        Box::new(SandboxExecutionClientFactory::with_fee_model(
            FeeModelHandle::new(FixedFeeModel::new(Money::from("0.00 USD"), Some(true))?),
        )),
        Box::new(SandboxExecutionClientConfig {
            venue: "XNAS".into(),
            account_id: "XNAS-RECORDED-QUEUE".into(),
            account_type: AccountType::Cash,
            starting_balances: vec![Money::from("100.00 USD")],
            base_currency: Some(Currency::USD()),
            book_type: BookType::L1_MBP,
            ..Default::default()
        }),
    )?
    .build()?;
    nautilus_model::data::registry::ensure_json_deserializer_registered(
        "Signal",
        Box::new(|value| {
            Ok(std::sync::Arc::new(serde_json::from_value::<Signal>(
                value,
            )?))
        }),
    )?;
    msgbus::get_message_bus()
        .borrow_mut()
        .add_streaming_type(BusPayloadType::Custom("Signal".into()));
    node.add_actor(QueueProbe {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some(QUEUE_ACTOR.into()),
            ..Default::default()
        }),
        evidence,
    })?;
    Ok(node)
}

fn actual_timer_message() -> anyhow::Result<TimeEventMessage> {
    let mut actor = get_actor_unchecked::<QueueProbe>(&QUEUE_ACTOR.into());
    let mut clock = actor.clock_mut();
    let clock = clock
        .as_any_mut()
        .downcast_mut::<VirtualClock>()
        .context("Native queue probe clock is not virtual")?;
    let events = clock.advance_time(10.into(), true);
    let mut handlers = clock.match_handlers(events);
    anyhow::ensure!(handlers.len() == 1, "Expected one native timer callback");
    let handler = handlers.pop().unwrap();
    Ok(TimeEventMessage::new(handler.event, handler.callback))
}

fn original_timer(instance: UUID4) -> anyhow::Result<(Vec<RecordedRunnerDispatch>, Frame)> {
    std::thread::spawn(move || {
        runtime().block_on(async move {
            let trace = Rc::new(RefCell::new(Trace::default()));
            let mut node =
                compose_queue(instance, Rc::new(RefCell::new(QueueEvidence::default())))?;
            msgbus::set_bus_tap(Rc::new(Tap {
                trace: trace.clone(),
                evidence: Rc::new(RefCell::new(Evidence::default())),
            }));
            node.enable_runtime_dispatch_capture()?;
            node.start().await?;
            let startup = trace
                .borrow()
                .frames
                .iter()
                .map(|frame| frame.witness.clone())
                .collect();
            anyhow::ensure!(AsyncRunner::handle_time_event(
                actual_timer_message()?.into()
            ));
            let timer = trace.borrow().frames.last().unwrap().clone();
            anyhow::ensure!(
                timer.complete
                    && timer.witness.channel == RecordedChannel::TimeEvents
                    && timer.timer.is_some()
            );
            msgbus::clear_bus_tap();
            node.stop().await?;
            node.dispose();
            Ok::<_, anyhow::Error>((startup, timer))
        })
    })
    .join()
    .expect("native timer capture thread")
}

fn input(
    node: &mut LiveNode,
    sequence: u64,
    at: u64,
    witness: RecordedRunnerDispatch,
    kind: RecordedRunnerInputKind,
) -> anyhow::Result<()> {
    node.process_recorded(RecordedRunnerInput {
        sequence,
        observed_at: at.into(),
        witness,
        kind,
    })
}

fn next_witness(mut witness: RecordedRunnerDispatch) -> RecordedRunnerDispatch {
    witness.ordinal = recorded::next_ordinal(witness.channel);
    witness
}

fn data_witness(quote: QuoteTick) -> RecordedRunnerDispatch {
    next_witness(recorded::describe_data_event(&DataEvent::Data(
        Data::Quote(quote),
    )))
}

#[derive(Clone, Copy, Debug)]
enum QueueCase {
    Legal,
    WrongFront,
    MissingFront,
    ReorderedFront,
    UnexpectedTimer,
    DirectData,
    StaleTimer,
    WrongTimerOrdinal,
    FutureTimer,
}

#[rstest]
#[case::legal(QueueCase::Legal)]
#[case::wrong_front(QueueCase::WrongFront)]
#[case::missing_front(QueueCase::MissingFront)]
#[case::reordered_front(QueueCase::ReorderedFront)]
#[case::unexpected_timer(QueueCase::UnexpectedTimer)]
#[case::direct_data(QueueCase::DirectData)]
#[case::stale_timer(QueueCase::StaleTimer)]
#[case::wrong_timer_ordinal(QueueCase::WrongTimerOrdinal)]
#[case::future_timer(QueueCase::FutureTimer)]
fn recorded_timer_preserves_recomputed_data_and_strict_native_guards(
    #[case] case: QueueCase,
) -> anyhow::Result<()> {
    let instance = UUID4::new();
    let (startup, timer) = original_timer(instance)?;

    std::thread::spawn(move || {
        runtime().block_on(async move {
            let evidence = Rc::new(RefCell::new(QueueEvidence::default()));
            let mut node = compose_queue(instance, evidence.clone())?;
            node.begin_recorded(
                0.into(),
                RecordedRunnerConfig {
                    startup_dispatches: startup,
                    excluded_actors: Vec::new(),
                },
            )
            .await?;

            if !matches!(case, QueueCase::MissingFront) {
                let message = external("first")?;
                input(
                    &mut node,
                    1,
                    2,
                    next_witness(recorded::describe_external(&message)),
                    RecordedRunnerInputKind::External(message),
                )?;
                anyhow::ensure!(evidence.borrow().enqueued == vec![quote("first")?]);
            }

            if matches!(case, QueueCase::Legal | QueueCase::ReorderedFront) {
                let message = external("second")?;
                input(
                    &mut node,
                    2,
                    3,
                    next_witness(recorded::describe_external(&message)),
                    RecordedRunnerInputKind::External(message),
                )?;
            }
            anyhow::ensure!(evidence.borrow().delivered.is_empty());
            anyhow::ensure!(node.kernel().cache.borrow().quote(&IDS[0].into()).is_none());

            match case {
                QueueCase::DirectData => {
                    let original = quote("first")?;
                    let e = input(
                        &mut node,
                        3,
                        10,
                        data_witness(original),
                        RecordedRunnerInputKind::Data(Data::Quote(original)),
                    )
                    .unwrap_err();
                    anyhow::ensure!(e.to_string().contains("Unexpected live input is queued"));
                }
                _ => {
                    if matches!(case, QueueCase::UnexpectedTimer) {
                        get_time_event_sender().send(actual_timer_message()?);
                    }
                    let mut dispatch = timer.timer.context("Original native timer lease absent")?;
                    if matches!(case, QueueCase::StaleTimer) {
                        dispatch.binding.timer_generation = Some(999);
                    }

                    if matches!(case, QueueCase::FutureTimer) {
                        dispatch.event.ts_event = 11.into();
                    }
                    let mut witness = timer.witness;
                    if matches!(case, QueueCase::WrongTimerOrdinal) {
                        witness.ordinal += 1;
                    }
                    let result = input(
                        &mut node,
                        3,
                        10,
                        witness,
                        RecordedRunnerInputKind::Timer(dispatch),
                    );

                    if matches!(
                        case,
                        QueueCase::UnexpectedTimer
                            | QueueCase::StaleTimer
                            | QueueCase::WrongTimerOrdinal
                            | QueueCase::FutureTimer
                    ) {
                        let e = result.unwrap_err();
                        let expected = match case {
                            QueueCase::UnexpectedTimer => "Unexpected live timer is queued",
                            QueueCase::StaleTimer => "timer registration changed",
                            QueueCase::WrongTimerOrdinal => "ordinal or capability is unsupported",
                            QueueCase::FutureTimer => "dispatch clock is invalid",
                            _ => unreachable!(),
                        };
                        anyhow::ensure!(e.to_string().contains(expected));
                        anyhow::ensure!(evidence.borrow().timer_quotes.is_empty());
                    } else {
                        result?;
                        anyhow::ensure!(evidence.borrow().timer_quotes == vec![None]);
                        anyhow::ensure!(evidence.borrow().delivered.is_empty());
                        let mut expected = if matches!(case, QueueCase::ReorderedFront) {
                            quote("second")?
                        } else {
                            quote("first")?
                        };

                        if matches!(case, QueueCase::WrongFront) {
                            expected.ask_price = Price::from("12.00");
                        }
                        let result = input(
                            &mut node,
                            4,
                            11,
                            data_witness(expected),
                            RecordedRunnerInputKind::Dispatch,
                        );

                        if matches!(case, QueueCase::Legal) {
                            result?;
                            anyhow::ensure!(evidence.borrow().delivered == vec![quote("first")?]);
                            input(
                                &mut node,
                                5,
                                12,
                                data_witness(quote("second")?),
                                RecordedRunnerInputKind::Dispatch,
                            )?;
                            anyhow::ensure!(
                                evidence.borrow().delivered
                                    == vec![quote("first")?, quote("second")?]
                            );
                        } else {
                            let e = result.unwrap_err();
                            anyhow::ensure!(if matches!(case, QueueCase::MissingFront) {
                                e.to_string().contains("queue front is absent")
                            } else {
                                e.to_string().contains("queue front differs")
                            });
                        }
                    }
                }
            }

            if !matches!(case, QueueCase::Legal) {
                anyhow::ensure!(evidence.borrow().delivered.is_empty());
                anyhow::ensure!(node.kernel().cache.borrow().quote(&IDS[0].into()).is_none());
            }
            {
                let cache = node.kernel().cache.borrow();
                let account = cache
                    .account(&"XNAS-RECORDED-QUEUE".into())
                    .context("Native Sandbox account absent")?;
                anyhow::ensure!(account.calculated_account_state());
                anyhow::ensure!(
                    account.balance_total(Some(Currency::USD())) == Some(Money::from("100.00 USD"))
                );
                anyhow::ensure!(cache.orders(None, None, None, None, None).is_empty());
            }
            node.end_recorded().await?;
            node.dispose();
            Ok::<_, anyhow::Error>(())
        })
    })
    .join()
    .expect("recorded queue ordering thread")
}
