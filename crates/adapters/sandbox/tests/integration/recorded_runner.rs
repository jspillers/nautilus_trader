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

//! Recorded inputs use actual LiveNode queues, Sandbox, native risk and accounts.

mod queued_data;
mod queued_timer;

use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore, DataActorNative},
    cache::{
        CacheView,
        bootstrap::{BootstrapLoadReadKind, BootstrapLoadWitness},
    },
    clients::DataClient,
    clock::Clock,
    enums::{Environment, SerializationEncoding},
    factories::{ClientConfig, DataClientFactory},
    live::runner::get_data_event_sender,
    messages::DataEvent,
    msgbus::{
        self, BusMessage, BusPayloadType, BusTap, Endpoint, MStr, MessageBusExternalEgress,
        MessageBusExternalIngress, Topic,
    },
    nautilus_actor,
    runner::recorded::{RecordedChannel, RecordedRunnerCompletion, RecordedRunnerDispatch},
    signal::Signal,
    timer::{RecordedTimeEventDispatch, TimeEvent},
};
use nautilus_core::{DurationNanos, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_execution::models::{
    fee::{FeeModelHandle, FixedFeeModel},
    latency::{LatencyModelAny, StaticLatencyModel},
};
use nautilus_live::node::{
    LiveNode, LiveNodeHandle, NodeState, RecordedRunnerConfig, RecordedRunnerInput,
    RecordedRunnerInputKind,
    builder::LiveNodeBuilder,
    config::{LiveExecutionEngineConfig, LiveNodeConfig, LiveRiskEngineConfig},
};
use nautilus_model::{
    accounts::Account,
    data::{CustomData, Data, DataType, QuoteTick},
    enums::{AccountType, BookType, OrderSide, OrderStatus, TimeInForce},
    events::{OrderEventAny, OrderFilled},
    identifiers::{ClientId, ClientOrderId, StrategyId, Venue},
    instruments::{Equity, InstrumentAny},
    orders::Order,
    types::{Currency, Money, Price},
};
use nautilus_sandbox::{SandboxExecutionClientConfig, SandboxExecutionClientFactory};
use nautilus_system::clock_factory::ClockFactory;
use nautilus_testkit::cache::TestCacheDatabaseControl;
use nautilus_trading::{
    Strategy, nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;
use serde_json::{Value, json};

const ACTOR: &str = "RECORDED-SOURCE";
const IDS: [&str; 3] = ["STABLE.XNAS", "BEFORE.XNAS", "AFTER.XNAS"];
const OWNERS: [&str; 3] = ["Recorded-001", "Recorded-002", "Recorded-003"];

#[derive(Clone, Debug)]
struct Frame {
    witness: RecordedRunnerDispatch,
    observed_at: UnixNanos,
    timer: Option<RecordedTimeEventDispatch>,
    data: Option<Data>,
    complete: bool,
}
#[derive(Debug, Default)]
struct Trace {
    frames: Vec<Frame>,
    stack: Vec<usize>,
    closed: bool,
    peer_ticks_at_close: Option<u64>,
    bootstrap: Option<BootstrapLoadWitness>,
}
#[derive(Debug)]
struct Tap {
    trace: Rc<RefCell<Trace>>,
    evidence: Rc<RefCell<Evidence>>,
}
impl BusTap for Tap {
    fn on_publish(&self, _: MStr<Topic>, message: &dyn Any) {
        let mut trace = self.trace.borrow_mut();
        if trace.closed {
            return;
        }

        if let Some(witness) = message.downcast_ref::<BootstrapLoadWitness>() {
            trace.bootstrap = Some(witness.clone());
        } else if let Some(witness) = message.downcast_ref::<RecordedRunnerDispatch>() {
            let index = trace.frames.len();
            trace.frames.push(Frame {
                witness: witness.clone(),
                observed_at: get_atomic_clock_realtime().get_time_ns(),
                timer: None,
                data: None,
                complete: false,
            });
            trace.stack.push(index);
        } else if let Some(dispatch) = message.downcast_ref::<RecordedTimeEventDispatch>() {
            if let Some(index) = trace.stack.last().copied() {
                trace.frames[index].timer = Some(dispatch.clone());
            }
        } else if let Some(completion) = message.downcast_ref::<RecordedRunnerCompletion>() {
            let index = trace.stack.pop().expect("native frame is open");
            assert_eq!(trace.frames[index].witness.channel, completion.channel);
            assert_eq!(trace.frames[index].witness.ordinal, completion.ordinal);
            trace.frames[index].complete = true;
            let evidence = self.evidence.borrow();
            if evidence.done {
                trace.closed = true;
                trace.peer_ticks_at_close = Some(evidence.peer_ticks);
            }
        }
    }
    fn on_send(&self, _: MStr<Endpoint>, message: &dyn Any) {
        if let Some(data) = message.downcast_ref::<Data>() {
            let mut trace = self.trace.borrow_mut();
            if let Some(index) = trace.stack.last().copied()
                && trace.frames[index].witness.channel == RecordedChannel::DataEvents
            {
                trace.frames[index].data = Some(data.clone());
            }
        }
    }
}

#[derive(Debug)]
struct InputConfig {
    instruments: Vec<InstrumentAny>,
    recorded_supported: bool,
    evidence: Rc<RefCell<Evidence>>,
}
impl ClientConfig for InputConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
#[derive(Debug)]
struct InputFactory;
impl DataClientFactory for InputFactory {
    fn name(&self) -> &'static str {
        "RECORDED-INPUT"
    }
    fn config_type(&self) -> &'static str {
        "InputConfig"
    }
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _: CacheView,
        _: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<InputConfig>()
            .context("wrong input configuration")?;
        Ok(Box::new(Input {
            id: name.into(),
            instruments: config.instruments.clone(),
            connected: false,
            recorded_supported: config.recorded_supported,
            evidence: config.evidence.clone(),
        }))
    }
}
#[derive(Debug)]
struct Input {
    id: ClientId,
    instruments: Vec<InstrumentAny>,
    connected: bool,
    recorded_supported: bool,
    evidence: Rc<RefCell<Evidence>>,
}
#[async_trait(?Send)]
impl DataClient for Input {
    fn supports_recorded_inputs(&self) -> bool {
        self.recorded_supported
    }
    fn client_id(&self) -> ClientId {
        self.id
    }
    fn venue(&self) -> Option<Venue> {
        Some("XNAS".into())
    }
    fn is_connected(&self) -> bool {
        self.connected
    }
    fn is_disconnected(&self) -> bool {
        !self.connected
    }
    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn dispose(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn connect(&mut self) -> anyhow::Result<()> {
        self.evidence.borrow_mut().connects += 1;

        for instrument in &self.instruments {
            get_data_event_sender().send(DataEvent::Instrument(instrument.clone()))?;
        }
        self.connected = true;
        Ok(())
    }
    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Evidence {
    connects: usize,
    external: Vec<Signal>,
    entries: [Option<ClientOrderId>; 3],
    denied: [Option<ClientOrderId>; 3],
    filled: [bool; 3],
    cancelled: bool,
    peer_ticks: u64,
    descendant: bool,
    done: bool,
}
#[derive(Debug)]
struct Feed {
    core: DataActorCore,
    handle: LiveNodeHandle,
    evidence: Rc<RefCell<Evidence>>,
    started: Option<UnixNanos>,
}
impl DataActor for Feed {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_signal("recorded_control", None);
        let now = self.clock().timestamp_ns();
        self.started = Some(now);
        self.clock().set_timer_ns(
            "recorded-source",
            DurationNanos::from_millis(5),
            None,
            None,
            None,
            Some(false),
            Some(false),
        )
    }
    fn on_time_event(&mut self, _: &TimeEvent) -> anyhow::Result<()> {
        let now = self.clock().timestamp_ns();
        let elapsed = now.as_u64() - self.started.unwrap().as_u64();
        let evidence = self.evidence.borrow();
        let done = evidence.filled[0]
            && evidence.filled[2]
            && evidence.cancelled
            && evidence.descendant
            && evidence.peer_ticks > 10
            && elapsed >= 2_300_000_000;
        let after = evidence.filled[2];
        drop(evidence);
        if done {
            self.evidence.borrow_mut().done = true;
            self.handle.stop();
            return Ok(());
        }

        for (index, id) in IDS.iter().enumerate() {
            let ask = if (index == 1 && elapsed > 100_000_000) || (index == 2 && after) {
                "11.00"
            } else {
                "10.00"
            };
            get_data_event_sender().send(DataEvent::Data(Data::Quote(QuoteTick::new(
                (*id).into(),
                "9.00".into(),
                ask.into(),
                "100".into(),
                "100".into(),
                now,
                now,
            ))))?;
        }
        Ok(())
    }
    fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
        self.evidence.borrow_mut().external.push(signal.clone());
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.clock().cancel_timers();
        Ok(())
    }
}
nautilus_actor!(Feed);
#[derive(Debug)]
struct Probe {
    core: StrategyCore,
    index: usize,
    evidence: Rc<RefCell<Evidence>>,
    entry: Option<ClientOrderId>,
}
impl Probe {
    fn limit(&mut self, quantity: &str) -> anyhow::Result<ClientOrderId> {
        let order = self.core.order().limit(
            IDS[self.index].into(),
            OrderSide::Buy,
            quantity.into(),
            "10.00".into(),
            Some(TimeInForce::Gtc),
            None,
            Some(false),
            Some(false),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let id = order.client_order_id();
        self.submit_order(order, None, None, None)?;
        Ok(id)
    }
}
impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_quotes(IDS[self.index].into(), None, None);
        Ok(())
    }
    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.cache_ref().quote(&quote.instrument_id) == Some(quote),
            "DataEngine did not update native Cache before Strategy"
        );

        if self.entry.is_none() {
            let entry = self.limit("1")?;
            let denied = self.limit("6")?;
            self.entry = Some(entry);
            let mut evidence = self.evidence.borrow_mut();
            evidence.entries[self.index] = Some(entry);
            evidence.denied[self.index] = Some(denied);
        } else if self.index == 1 && quote.ask_price == Price::from("11.00") {
            let order = self
                .cache_ref()
                .order(&self.entry.unwrap())
                .unwrap()
                .clone();

            if order.status() == OrderStatus::Accepted {
                self.cancel_order(order.client_order_id(), None, None)?;
            }
        }
        Ok(())
    }
}
nautilus_strategy!(Probe, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        if self.entry == Some(event.client_order_id) {
            self.evidence.borrow_mut().filled[self.index] = true;
            if self.index == 0 {
                self.query_account("XNAS-RECORDED-001".into(), None, None)
                    .unwrap();
                self.evidence.borrow_mut().descendant = true;
            }
        }
    }
    fn on_order_event(&mut self, event: OrderEventAny) {
        if matches!(event, OrderEventAny::Canceled(_)) && self.index == 1 {
            self.evidence.borrow_mut().cancelled = true;
        }
    }
});
#[derive(Debug)]
struct Peer {
    core: StrategyCore,
    evidence: Rc<RefCell<Evidence>>,
}
impl DataActor for Peer {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.clock().set_timer_ns(
            "recorded-peer",
            DurationNanos::from_millis(5),
            None,
            None,
            None,
            Some(false),
            Some(false),
        )
    }
    fn on_time_event(&mut self, _: &TimeEvent) -> anyhow::Result<()> {
        self.evidence.borrow_mut().peer_ticks += 1;
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.clock().cancel_timers();
        Ok(())
    }
}
nautilus_strategy!(Peer);

fn compose(
    instance: UUID4,
    virtual_clock: bool,
    evidence: Rc<RefCell<Evidence>>,
    recorded_supported: bool,
) -> anyhow::Result<LiveNode> {
    compose_with_persistence(instance, virtual_clock, evidence, recorded_supported, false)
}

fn compose_with_persistence(
    instance: UUID4,
    virtual_clock: bool,
    evidence: Rc<RefCell<Evidence>>,
    recorded_supported: bool,
    persistence: bool,
) -> anyhow::Result<LiveNode> {
    compose_with_ingress(
        instance,
        virtual_clock,
        evidence,
        recorded_supported,
        persistence,
        None,
    )
}

fn compose_with_ingress(
    instance: UUID4,
    virtual_clock: bool,
    evidence: Rc<RefCell<Evidence>>,
    recorded_supported: bool,
    persistence: bool,
    ingress: Option<Box<dyn MessageBusExternalIngress>>,
) -> anyhow::Result<LiveNode> {
    let instruments = IDS
        .iter()
        .map(|id| -> anyhow::Result<InstrumentAny> {
            Ok(InstrumentAny::Equity(
                Equity::builder()
                    .instrument_id((*id).into())
                    .raw_symbol(id.split('.').next().unwrap().into())
                    .currency(Currency::USD())
                    .price_precision(2)
                    .price_increment(Price::from("0.01"))
                    .ts_event(0.into())
                    .ts_init(0.into())
                    .build()?,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let config = LiveNodeConfig {
        trader_id: "RECORDED-001".into(),
        instance_id: Some(instance),
        environment: Environment::Sandbox,
        load_state: persistence,
        save_state: persistence,
        exec_engine: LiveExecutionEngineConfig {
            reconciliation: true,
            reconciliation_startup_delay_secs: 0.0,
            inflight_check_interval_ms: 1000,
            open_check_interval_secs: Some(1.0),
            position_check_interval_secs: Some(1.0),
            ..Default::default()
        },
        risk_engine: LiveRiskEngineConfig {
            bypass: false,
            max_notional_per_order: IDS
                .iter()
                .map(|id| ((*id).into(), "50".into()))
                .collect::<HashMap<_, _>>(),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut builder = LiveNodeBuilder::from_config(config)?.with_delay_post_stop_secs(0u64);
    if virtual_clock {
        builder =
            builder.with_native_clock_factory(ClockFactory::for_environment(Environment::Backtest));
    }
    builder = builder.add_data_client(
        Some("RECORDED-DATA".into()),
        Box::new(InputFactory),
        Box::new(InputConfig {
            instruments,
            recorded_supported,
            evidence: evidence.clone(),
        }),
    )?;
    builder = builder.add_simulated_exec_client(
        Some("XNAS".into()),
        Box::new(SandboxExecutionClientFactory::with_fee_model(
            FeeModelHandle::new(FixedFeeModel::new(Money::from("0.05 USD"), Some(true))?),
        )),
        Box::new(SandboxExecutionClientConfig {
            venue: "XNAS".into(),
            account_id: "XNAS-RECORDED-001".into(),
            account_type: AccountType::Cash,
            starting_balances: vec![Money::from("100.00 USD")],
            base_currency: Some(Currency::USD()),
            book_type: BookType::L1_MBP,
            latency_model: Some(LatencyModelAny::Static(StaticLatencyModel::new(
                DurationNanos::ZERO,
                DurationNanos::from_millis(400),
                DurationNanos::from_millis(200),
                DurationNanos::from_millis(50),
            ))),
            ..Default::default()
        }),
    )?;

    if let Some(ingress) = ingress {
        builder = builder.with_external_ingress(ingress);
    }
    let mut node = builder.build()?;
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
    node.add_actor(Feed {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some(ACTOR.into()),
            ..Default::default()
        }),
        handle: node.handle(),
        evidence: evidence.clone(),
        started: None,
    })?;

    for (index, owner) in OWNERS.iter().enumerate() {
        node.add_strategy(Probe {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some((*owner).into()),
                order_id_tag: Some(format!("{:03}", index + 1)),
                ..Default::default()
            }),
            index,
            evidence: evidence.clone(),
            entry: None,
        })?;
    }
    node.add_strategy(Peer {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some("RecordedPeer-004".into()),
            order_id_tag: Some("004".into()),
            ..Default::default()
        }),
        evidence,
    })?;
    Ok(node)
}
fn snapshot(node: &LiveNode) -> anyhow::Result<Value> {
    let cache = node.kernel().cache();
    let cache = cache.borrow();
    let account = cache
        .account(&"XNAS-RECORDED-001".into())
        .context("native account absent")?;
    anyhow::ensure!(account.calculated_account_state());
    anyhow::ensure!(account.balance_total(Some(Currency::USD())) == Some(Money::from("79.90 USD")));
    let mut orders=cache.orders(None,None,None,None,None).iter().map(|order| json!({"owner":order.strategy_id(),"status":format!("{:?}",order.status()),"filled":order.filled_qty().to_string(),"instrument":order.instrument_id()})).collect::<Vec<_>>();
    orders.sort_by_key(Value::to_string);

    for owner in OWNERS {
        let owner: StrategyId = owner.into();
        let denied = cache.orders(None, None, Some(&owner), None, None);
        anyhow::ensure!(denied.iter().any(|order|order.events().iter().any(|event|matches!(event,OrderEventAny::Denied(value) if value.reason.as_str().to_ascii_lowercase().contains("notional")))));
    }
    let mut positions=cache.positions_open(None,None,None,None,None).iter().map(|position|json!({"owner":position.strategy_id,"instrument":position.instrument_id,"quantity":position.quantity.to_string()})).collect::<Vec<_>>();
    positions.sort_by_key(Value::to_string);
    Ok(
        json!({"balance":account.balance_total(Some(Currency::USD())).unwrap().to_string(),"orders":orders,"positions":positions}),
    )
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[rstest]
fn recorded_runner_regenerates_native_changing_price_arrivals_and_queued_descendants()
-> anyhow::Result<()> {
    let instance = UUID4::new();

    let (frames, expected, peer_ticks, expected_external) = std::thread::spawn(move || {
        runtime().block_on(async move {
            let evidence = Rc::new(RefCell::new(Evidence::default()));
            let trace = Rc::new(RefCell::new(Trace::default()));
            let signal = Signal::new(
                "recorded_control".into(),
                "original signed body remains opaque".into(),
                123.into(),
                124.into(),
            );
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            let mut node = compose_with_ingress(
                instance,
                false,
                evidence.clone(),
                true,
                false,
                Some(Box::new(RecordedIngress(Some(receiver)))),
            )?;
            let custom = CustomData::new(
                std::sync::Arc::new(signal.clone()),
                DataType::new("Signal", None, None),
            );
            let message = BusMessage::new(
                *msgbus::switchboard::get_signal_topic("recorded_control"),
                BusPayloadType::Custom("Signal".into()),
                serde_json::to_vec(&custom)?.into(),
                SerializationEncoding::Json,
            );
            sender.try_send(message)?;
            msgbus::set_bus_tap(Rc::new(Tap {
                trace: trace.clone(),
                evidence: evidence.clone(),
            }));
            node.enable_runtime_dispatch_capture()?;
            tokio::time::timeout(Duration::from_secs(5), node.run()).await??;
            anyhow::ensure!(
                evidence.borrow().done,
                "source incomplete: {:?}",
                evidence.borrow()
            );
            anyhow::ensure!(
                evidence.borrow().external == vec![signal],
                "actual external native Signal delivery differs"
            );
            let expected = snapshot(&node)?;
            let expected_external = evidence.borrow().external.clone();
            let frames = trace.borrow().frames.clone();
            anyhow::ensure!(frames.iter().all(|frame| frame.complete));
            anyhow::ensure!(
                frames.iter().any(|frame| frame
                    .timer
                    .as_ref()
                    .is_some_and(|timer| timer.binding.execution_owner.is_some())),
                "native Sandbox arrival clock was not witnessed"
            );
            anyhow::ensure!(
                frames.iter().any(|frame| frame
                    .witness
                    .semantic
                    .get("operation")
                    .and_then(Value::as_str)
                    == Some("open_report")),
                "enabled native open-order check did not complete"
            );
            anyhow::ensure!(
                frames.iter().any(|frame| frame
                    .witness
                    .semantic
                    .get("operation")
                    .and_then(Value::as_str)
                    == Some("position_report")),
                "enabled native position check did not complete"
            );
            let peer = trace
                .borrow()
                .peer_ticks_at_close
                .context("source completion cut absent")?;
            let shutdown_peer = evidence.borrow().peer_ticks;
            anyhow::ensure!(shutdown_peer >= peer);
            println!("native peer source prefix={peer}; after ordinary shutdown={shutdown_peer}");
            msgbus::clear_bus_tap();
            node.dispose();
            Ok::<_, anyhow::Error>((frames, expected, peer, expected_external))
        })
    })
    .join()
    .expect("source thread")?;
    std::thread::spawn(move || {
        runtime().block_on(async move {
            let evidence = Rc::new(RefCell::new(Evidence::default()));
            let mut node = compose(instance, true, evidence.clone(), true)?;
            let running = frames
                .iter()
                .position(|frame| frame.witness.channel == RecordedChannel::Lifecycle)
                .context("source Running boundary absent")?;
            let start = frames[0].observed_at;
            node.begin_recorded(
                start,
                RecordedRunnerConfig {
                    startup_dispatches: frames[..=running]
                        .iter()
                        .map(|frame| frame.witness.clone())
                        .collect(),
                    excluded_actors: vec![ACTOR.into()],
                },
            )
            .await?;

            for (index, frame) in frames.into_iter().enumerate().skip(running + 1) {
                let kind = match frame.witness.channel {
                    RecordedChannel::TimeEvents => {
                        let timer = frame
                            .timer
                            .context("native dispatched callback lease absent")?;

                        if timer
                            .binding
                            .owner
                            .is_some_and(|owner| owner.actor_id.as_str() == ACTOR)
                        {
                            RecordedRunnerInputKind::ExcludedSourceTimer(timer)
                        } else {
                            RecordedRunnerInputKind::Timer(timer)
                        }
                    }
                    RecordedChannel::ExternalMessages => RecordedRunnerInputKind::External(serde_json::from_value(frame.witness.semantic.clone())?),
                    RecordedChannel::DataEvents => RecordedRunnerInputKind::Data(
                        frame.data.context("original normalized data absent")?,
                    ),
                    RecordedChannel::ExecutionCommands
                    | RecordedChannel::ExecutionEvents
                    | RecordedChannel::DataCommands => RecordedRunnerInputKind::Dispatch,
                    RecordedChannel::Callbacks => RecordedRunnerInputKind::Callbacks,
                    RecordedChannel::Maintenance => RecordedRunnerInputKind::Maintenance,
                    channel => anyhow::bail!("unsupported source channel {channel:?}"),
                };
                node.process_recorded(RecordedRunnerInput {
                    sequence: index as u64 + 1,
                    observed_at: frame.observed_at,
                    witness: frame.witness,
                    kind,
                })
                .with_context(|| format!("native recorded input {}", index + 1))?;
            }
            anyhow::ensure!(evidence.borrow().external == expected_external, "original external native Signal was not replayed exactly once");
            anyhow::ensure!(
                snapshot(&node)? == expected,
                "native economic/ownership outputs diverged"
            );
            anyhow::ensure!(
                evidence.borrow().peer_ticks == peer_ticks,
                "native peer callback count differs at the same complete source prefix: replay={} source={peer_ticks}",
                evidence.borrow().peer_ticks
            );
            anyhow::ensure!(
                evidence.borrow().descendant,
                "native fill callback descendant was not regenerated"
            );
            node.end_recorded().await?;
            node.dispose();
            Ok::<_, anyhow::Error>(())
        })
    })
    .join()
    .expect("recorded thread")
}

#[rstest]
fn recorded_runner_rejects_unqualified_data_client_before_connection() -> anyhow::Result<()> {
    std::thread::spawn(|| {
        runtime().block_on(async {
            let evidence = Rc::new(RefCell::new(Evidence::default()));
            let mut node = compose(UUID4::new(), true, evidence.clone(), false)?;
            let error = node
                .begin_recorded(10.into(), RecordedRunnerConfig::default())
                .await
                .unwrap_err();
            anyhow::ensure!(
                error
                    .to_string()
                    .contains("does not support exclusively recorded native inputs")
            );
            anyhow::ensure!(
                evidence.borrow().connects == 0,
                "unsupported live data client connected"
            );
            anyhow::ensure!(
                node.state() == NodeState::Idle,
                "failed source admission started native engines"
            );
            node.dispose();
            Ok::<_, anyhow::Error>(())
        })
    })
    .join()
    .expect("unsupported client thread")
}

#[rstest]
fn wrong_recorded_startup_inventory_is_rejected_before_native_connection() -> anyhow::Result<()> {
    let instance = UUID4::new();

    let startup = std::thread::spawn(move || {
        runtime().block_on(async move {
            let evidence = Rc::new(RefCell::new(Evidence::default()));
            let trace = Rc::new(RefCell::new(Trace::default()));
            let mut node = compose(instance, false, evidence.clone(), true)?;
            msgbus::set_bus_tap(Rc::new(Tap {
                trace: trace.clone(),
                evidence,
            }));
            node.enable_runtime_dispatch_capture()?;
            node.start().await?;
            let startup = trace
                .borrow()
                .frames
                .iter()
                .map(|frame| frame.witness.clone())
                .collect::<Vec<_>>();
            anyhow::ensure!(
                startup
                    .last()
                    .is_some_and(|frame| frame.channel == RecordedChannel::Lifecycle)
            );
            msgbus::clear_bus_tap();
            node.stop().await?;
            node.dispose();
            Ok::<_, anyhow::Error>(startup)
        })
    })
    .join()
    .expect("startup source thread")?;
    std::thread::spawn(move || {
        runtime().block_on(async move {
            let evidence = Rc::new(RefCell::new(Evidence::default()));
            let mut node = compose(instance, true, evidence.clone(), true)?;
            let mut startup = startup;
            startup.last_mut().unwrap().semantic["actors"] = json!(["UNDECLARED-ACTOR"]);
            let error = node
                .begin_recorded(
                    10.into(),
                    RecordedRunnerConfig {
                        startup_dispatches: startup,
                        excluded_actors: vec![ACTOR.into()],
                    },
                )
                .await
                .unwrap_err();
            anyhow::ensure!(
                error
                    .to_string()
                    .contains("component inventory differs before connection")
            );
            anyhow::ensure!(evidence.borrow().connects == 0 && evidence.borrow().peer_ticks == 0);
            anyhow::ensure!(node.state() == NodeState::Idle);
            let cache = node.kernel().cache();
            anyhow::ensure!(
                cache
                    .borrow()
                    .orders(None, None, None, None, None)
                    .is_empty()
            );
            anyhow::ensure!(
                cache
                    .borrow()
                    .account(&"XNAS-RECORDED-001".into())
                    .is_none()
            );
            node.dispose();
            Ok::<_, anyhow::Error>(())
        })
    })
    .join()
    .expect("startup rejected thread")
}

#[rstest]
fn recorded_runner_rejects_installed_cache_backing_before_load_or_connection() -> anyhow::Result<()>
{
    std::thread::spawn(|| {
        runtime().block_on(async {
            let evidence = Rc::new(RefCell::new(Evidence::default()));
            let mut node = compose(UUID4::new(), true, evidence.clone(), true)?;
            let (database, control) = TestCacheDatabaseControl::create();
            node.set_cache_database(Box::new(database))?;
            let error = node
                .begin_recorded(10.into(), RecordedRunnerConfig::default())
                .await
                .unwrap_err();
            anyhow::ensure!(
                error
                    .to_string()
                    .contains("cold genesis without cache restoration")
            );
            anyhow::ensure!(node.state() == NodeState::Idle && evidence.borrow().connects == 0);
            anyhow::ensure!(
                control.events().is_empty(),
                "offline admission consumed cache backing"
            );
            node.dispose();
            Ok::<_, anyhow::Error>(())
        })
    })
    .join()
    .expect("cache backing negative thread")
}

struct RecordedEgress(Rc<Cell<usize>>);
impl MessageBusExternalEgress for RecordedEgress {
    fn is_closed(&self) -> bool {
        false
    }
    fn publish(&self, _message: BusMessage) {
        self.0.set(self.0.get() + 1);
    }
    fn close(&mut self) {}
}

#[rstest]
fn recorded_runner_rejects_external_egress_before_connection_or_publication() -> anyhow::Result<()>
{
    std::thread::spawn(|| {
        runtime().block_on(async {
            let evidence = Rc::new(RefCell::new(Evidence::default()));
            let mut node = compose(UUID4::new(), true, evidence.clone(), true)?;
            let publications = Rc::new(Cell::new(0));
            msgbus::get_message_bus().borrow_mut().set_external_egress(
                Box::new(RecordedEgress(publications.clone())),
                SerializationEncoding::Json,
            );
            let error = node
                .begin_recorded(10.into(), RecordedRunnerConfig::default())
                .await
                .unwrap_err();
            anyhow::ensure!(
                error
                    .to_string()
                    .contains("cannot publish to an external message bus")
            );
            anyhow::ensure!(node.state() == NodeState::Idle && evidence.borrow().connects == 0);
            anyhow::ensure!(
                publications.get() == 0,
                "offline admission published to external backing"
            );
            node.dispose();
            Ok::<_, anyhow::Error>(())
        })
    })
    .join()
    .expect("external egress negative thread")
}

#[rstest]
#[case(false)]
#[case(true)]
fn bootstrap_witness_counts_actual_native_consumed_reads(
    #[case] nonempty: bool,
) -> anyhow::Result<()> {
    std::thread::spawn(move || {
        runtime().block_on(async move {
            let evidence = Rc::new(RefCell::new(Evidence::default()));
            let trace = Rc::new(RefCell::new(Trace::default()));
            let instance = UUID4::new();
            let mut node = compose_with_persistence(instance, false, evidence.clone(), true, true)?;
            let (database, control) = TestCacheDatabaseControl::create();
            if nonempty {
                control.set_actor_state(
                    ACTOR.into(),
                    &[("native-state".to_string(), vec![1])]
                        .into_iter()
                        .collect(),
                );
            }
            node.set_cache_database(Box::new(database))?;
            msgbus::set_bus_tap(Rc::new(Tap {
                trace: trace.clone(),
                evidence: evidence.clone(),
            }));
            node.enable_bootstrap_load_capture()?;
            node.start().await?;
            anyhow::ensure!(node.state() == NodeState::Running);
            {
                let cache_handle = node.kernel().cache();
                let cache = cache_handle.borrow();
                let account = cache
                    .account(&"XNAS-RECORDED-001".into())
                    .context("native calculated account absent")?;
                anyhow::ensure!(account.calculated_account_state());
                anyhow::ensure!(
                    account.balance_total(Some(Currency::USD())) == Some(Money::from("100.00 USD"))
                );
            }
            let witness = trace
                .borrow()
                .bootstrap
                .clone()
                .context("actual Running bootstrap witness absent")?;
            anyhow::ensure!(witness.native_instance_id == instance && witness.completed);
            anyhow::ensure!(witness.is_complete_empty() != nonempty);
            anyhow::ensure!(
                witness.loads.reads.len()
                    == 5 + witness.actor_ids.len() + witness.strategy_ids.len()
            );
            let cache_all = witness
                .loads
                .reads
                .iter()
                .find(|read| read.kind == BootstrapLoadReadKind::CacheAll)
                .context("actual load_all absent")?;
            anyhow::ensure!(
                cache_all.counts.len() == 9 && cache_all.counts.values().all(|count| *count == 0)
            );

            if nonempty {
                anyhow::ensure!(
                    witness
                        .loads
                        .reads
                        .iter()
                        .any(|read| read.component_id.as_deref() == Some(ACTOR)
                            && read.counts.get("entries") == Some(&1))
                );
            }
            anyhow::ensure!(
                control
                    .events()
                    .iter()
                    .any(|event| event == &format!("actor.load:{ACTOR}"))
            );
            msgbus::clear_bus_tap();
            node.stop().await?;
            node.dispose();
            Ok::<_, anyhow::Error>(())
        })
    })
    .join()
    .expect("native bootstrap source thread")
}

#[rstest]
fn failed_native_component_load_never_emits_completed_bootstrap() -> anyhow::Result<()> {
    std::thread::spawn(|| {
        runtime().block_on(async {
            let evidence = Rc::new(RefCell::new(Evidence::default()));
            let trace = Rc::new(RefCell::new(Trace::default()));
            let mut node =
                compose_with_persistence(UUID4::new(), false, evidence.clone(), true, true)?;
            let (database, control) = TestCacheDatabaseControl::create();
            control.set_fail_load_actor(true);
            node.set_cache_database(Box::new(database))?;
            msgbus::set_bus_tap(Rc::new(Tap {
                trace: trace.clone(),
                evidence,
            }));
            node.enable_bootstrap_load_capture()?;
            let error = node.start().await.unwrap_err();
            anyhow::ensure!(format!("{error:#}").contains("Failed to load actor"));
            anyhow::ensure!(trace.borrow().bootstrap.is_none());
            let loads = node
                .kernel()
                .cache()
                .borrow()
                .bootstrap_loads()
                .context("native failed read observations absent")?;
            anyhow::ensure!(!loads.is_complete_empty() && !loads.component_states_loaded);
            anyhow::ensure!(loads.reads.iter().any(|read| !read.succeeded));
            msgbus::clear_bus_tap();
            node.dispose();
            Ok::<_, anyhow::Error>(())
        })
    })
    .join()
    .expect("failed native bootstrap thread")
}

struct RecordedIngress(Option<tokio::sync::mpsc::Receiver<BusMessage>>);
impl MessageBusExternalIngress for RecordedIngress {
    fn is_closed(&self) -> bool {
        self.0.is_none()
    }
    fn take_receiver(&mut self) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
        self.0
            .take()
            .ok_or_else(|| anyhow::anyhow!("native ingress already taken"))
    }
    fn close(&mut self) {
        self.0 = None;
    }
}
