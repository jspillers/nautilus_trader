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

//! Online settlement through actual native DataEngine, risk, matching and accounts.

use std::{any::Any, cell::RefCell, collections::HashMap, rc::Rc, time::Duration};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    enums::Environment,
    factories::{ClientConfig, DataClientFactory},
    live::runner::get_data_event_sender,
    messages::{
        DataEvent,
        execution::{
            ExecutionClientSettlement, QueryStrategySettlement, StrategySettlementResult,
            TradingCommand,
        },
    },
    msgbus::{self, MessagingSwitchboard, ShareableMessageHandler},
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{DurationNanos, UUID4};
use nautilus_execution::models::{
    fee::{FeeModelHandle, FixedFeeModel},
    latency::{LatencyModelAny, StaticLatencyModel},
};
use nautilus_live::node::{
    LiveNode, LiveNodeHandle,
    builder::LiveNodeBuilder,
    config::{LiveNodeConfig, LiveRiskEngineConfig},
};
use nautilus_model::{
    accounts::Account,
    data::{Data, QuoteTick},
    enums::{AccountType, BookType, OrderSide, OrderStatus, TimeInForce},
    events::{OrderEventAny, OrderFilled, OrderUpdated},
    identifiers::{ClientId, ClientOrderId, StrategyId, Venue},
    instruments::{Equity, InstrumentAny},
    orders::Order,
    types::{Currency, Money, Price},
};
use nautilus_sandbox::{SandboxExecutionClientConfig, SandboxExecutionClientFactory};
use nautilus_trading::{
    Strategy, nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

const TARGET: &str = "SettlementTarget-001";

#[derive(Debug, Default)]
struct Evidence {
    feed: u64,
    peer_ticks: u64,
    peer_orders: u64,
    peer_modify_submitted: bool,
    pending_results: u64,
    stale_rejected: bool,
    controls: bool,
    descendant_fenced: bool,
    done: bool,
    error: Option<String>,
}

#[derive(Debug)]
struct InputConfig {
    instrument: InstrumentAny,
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
        "SETTLEMENT-INPUT"
    }
    fn config_type(&self) -> &'static str {
        "InputConfig"
    }
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _: CacheView,
        clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<InputConfig>()
            .context("Wrong native test data config")?;
        Ok(Box::new(Input {
            id: name.into(),
            instrument: config.instrument.clone(),
            evidence: config.evidence.clone(),
            clock,
            connected: false,
        }))
    }
}
#[derive(Debug)]
struct Input {
    id: ClientId,
    instrument: InstrumentAny,
    evidence: Rc<RefCell<Evidence>>,
    clock: Rc<RefCell<dyn Clock>>,
    connected: bool,
}
#[async_trait(?Send)]
impl DataClient for Input {
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
        self.clock.borrow_mut().cancel_timer("settlement-input");
        Ok(())
    }
    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }
    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }
    async fn connect(&mut self) -> anyhow::Result<()> {
        let sender = get_data_event_sender();
        sender.send(DataEvent::Instrument(self.instrument.clone()))?;
        let evidence = self.evidence.clone();
        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |event| {
            evidence.borrow_mut().feed += 1;
            sender
                .send(DataEvent::Data(Data::Quote(QuoteTick::new(
                    "AAPL.XNAS".into(),
                    "9.00".into(),
                    "10.00".into(),
                    "100".into(),
                    "100".into(),
                    event.ts_event,
                    event.ts_event,
                ))))
                .unwrap();
        });
        self.clock.borrow_mut().set_timer_ns(
            "settlement-input",
            DurationNanos::from_millis(2),
            None,
            None,
            Some(TimeEventCallback::from(callback)),
            Some(false),
            Some(false),
        )?;
        self.connected = true;
        Ok(())
    }
    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        self.stop()
    }
}

#[derive(Debug)]
struct Target {
    core: StrategyCore,
    handle: LiveNodeHandle,
    evidence: Rc<RefCell<Evidence>>,
    response: Rc<RefCell<Option<StrategySettlementResult>>>,
    receiver: ShareableMessageHandler,
    denied: Option<ClientOrderId>,
    filled: Option<ClientOrderId>,
    exit: Option<ClientOrderId>,
    resting: Option<ClientOrderId>,
    query: Option<UUID4>,
    phase: u8,
    peers_at_flat: u64,
    orders_at_flat: u64,
    feed_at_flat: u64,
}
impl Target {
    fn limit(
        &mut self,
        side: OrderSide,
        quantity: &str,
        price: &str,
        tif: TimeInForce,
    ) -> anyhow::Result<ClientOrderId> {
        let order = self.core.order().limit(
            "AAPL.XNAS".into(),
            side,
            quantity.into(),
            price.into(),
            Some(tif),
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
    fn status(&self, id: Option<ClientOrderId>) -> Option<OrderStatus> {
        id.and_then(|id| self.cache_ref().order(&id).map(|order| order.status()))
    }
    fn flat(&self) -> bool {
        let cache = self.cache_ref();
        let owner = StrategyId::from(TARGET);
        cache
            .orders(None, None, Some(&owner), None, None)
            .iter()
            .all(|order| order.is_closed())
            && cache
                .positions_open(None, None, Some(&owner), None, None)
                .is_empty()
    }
    fn step(&mut self) -> anyhow::Result<()> {
        match self.phase {
            0 => {
                if self.cache_ref().quote(&"AAPL.XNAS".into()).is_none() {
                    return Ok(());
                }

                if !self.evidence.borrow().stale_rejected {
                    let command_id = UUID4::new();
                    self.query = Some(command_id);
                    msgbus::send_trading_command(
                        MessagingSwitchboard::exec_engine_queue_execute(),
                        TradingCommand::QueryStrategySettlement(QueryStrategySettlement {
                            trader_id: "SETTLEMENT-001".into(),
                            expected_instance_id: UUID4::new(),
                            strategy_id: TARGET.into(),
                            command_id,
                            ts_init: self.clock().timestamp_ns(),
                        }),
                    );
                    self.phase = 10;
                    return Ok(());
                }
                self.denied = Some(self.limit(OrderSide::Buy, "6", "10.00", TimeInForce::Ioc)?);
                self.filled = Some(self.limit(OrderSide::Buy, "1", "10.00", TimeInForce::Ioc)?);
                self.phase = 1;
            }
            10 => {
                let Some(response) = self.response.borrow_mut().take() else {
                    return Ok(());
                };
                anyhow::ensure!(
                    Some(response.query.command_id) == self.query
                        && response.state == ExecutionClientSettlement::Unsupported
                        && !response.native_boundary,
                    "Stale instance query was not rejected: {response:?}"
                );
                self.evidence.borrow_mut().stale_rejected = true;
                self.phase = 0;
            }
            1 if self.status(self.denied) == Some(OrderStatus::Denied)
                && self.status(self.filled) == Some(OrderStatus::Filled) =>
            {
                let cache = self.cache_ref();
                let denied = cache.order(&self.denied.unwrap()).unwrap();
                anyhow::ensure!(denied.events().iter().any(|event| matches!(event, OrderEventAny::Denied(value) if value.reason.as_str().to_ascii_lowercase().contains("notional"))));
                let account = cache
                    .account(&"XNAS-SETTLEMENT-001".into())
                    .context("Missing native account")?;
                anyhow::ensure!(
                    account.calculated_account_state()
                        && account.balance_total(Some(Currency::USD()))
                            == Some(Money::from("90.00 USD"))
                );
                anyhow::ensure!(
                    cache
                        .positions_open(None, None, Some(&StrategyId::from(TARGET)), None, None)
                        .len()
                        == 1
                );
                drop(account);
                drop(denied);
                drop(cache);
                self.evidence.borrow_mut().controls = true;
                self.exit = Some(self.limit(OrderSide::Sell, "1", "9.00", TimeInForce::Ioc)?);
                self.phase = 2;
            }
            20 => {
                let Some(response) = self.response.borrow_mut().take() else {
                    return Ok(());
                };
                anyhow::ensure!(Some(response.query.command_id) == self.query);
                anyhow::ensure!(
                    response.state == ExecutionClientSettlement::Pending
                        && !response.native_boundary
                );
                anyhow::ensure!(
                    !response.clients.is_empty()
                        && response
                            .clients
                            .iter()
                            .all(|client| client.state == ExecutionClientSettlement::Quiescent),
                    "Client work confounded the descendant-command boundary"
                );
                anyhow::ensure!(self.flat());
                self.evidence.borrow_mut().descendant_fenced = true;
                self.resting = Some(self.limit(OrderSide::Buy, "1", "8.00", TimeInForce::Gtc)?);
                self.phase = 3;
            }
            3 if self.status(self.resting) == Some(OrderStatus::Accepted)
                && self.evidence.borrow().peer_modify_submitted =>
            {
                self.modify_order(
                    self.resting.unwrap(),
                    None,
                    Some("7.00".into()),
                    None,
                    None,
                    None,
                )?;
                // The facade marks pending state before Risk drains commands.
                // Defer cancellation to another native owner turn so Risk can
                // first admit this modify into Sandbox's delayed command queue.
                self.phase = 30;
            }
            30 => {
                anyhow::ensure!(self.status(self.resting) == Some(OrderStatus::PendingUpdate));
                self.cancel_order(self.resting.unwrap(), None, None)?;
                self.phase = 4;
            }
            4 if self.flat() => {
                let evidence = self.evidence.borrow();
                self.peers_at_flat = evidence.peer_ticks;
                self.orders_at_flat = evidence.peer_orders;
                self.feed_at_flat = evidence.feed;
                drop(evidence);
                self.query = Some(self.query_strategy_settlement()?);
                self.phase = 5;
            }
            5 => {
                let response = self.response.borrow_mut().take();
                let Some(response) = response else {
                    return Ok(());
                };
                anyhow::ensure!(
                    Some(response.query.command_id) == self.query
                        && response.query.strategy_id == StrategyId::from(TARGET)
                );
                anyhow::ensure!(
                    response.native_instance_id == msgbus::get_message_bus().borrow().instance_id
                );
                anyhow::ensure!(
                    response.state != ExecutionClientSettlement::Unsupported,
                    "Native capability unavailable: {:?}",
                    response.error
                );

                if response.state == ExecutionClientSettlement::Pending {
                    self.evidence.borrow_mut().pending_results += 1;
                    self.query = Some(self.query_strategy_settlement()?);
                } else {
                    anyhow::ensure!(response.native_boundary && self.flat());
                    let cache = self.cache_ref();
                    let account = cache.account(&"XNAS-SETTLEMENT-001".into()).unwrap();
                    anyhow::ensure!(
                        account.balance_total(Some(Currency::USD()))
                            == Some(Money::from("99.00 USD"))
                    );
                    let mut evidence = self.evidence.borrow_mut();
                    anyhow::ensure!(
                        evidence.controls
                            && evidence.stale_rejected
                            && evidence.descendant_fenced
                            && evidence.pending_results > 0
                            && evidence.peer_ticks > self.peers_at_flat
                            && evidence.peer_orders > self.orders_at_flat
                            && evidence.feed > self.feed_at_flat,
                        "Settlement evidence incomplete: {evidence:?}; at_flat=({}, {}, {}); result={response:?}",
                        self.peers_at_flat,
                        self.orders_at_flat,
                        self.feed_at_flat,
                    );
                    evidence.done = true;
                    self.clock().cancel_timers();
                    self.handle.stop();
                }
            }
            _ => {}
        }
        Ok(())
    }
}
impl DataActor for Target {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_quotes("AAPL.XNAS".into(), None, None);
        msgbus::subscribe_any(
            format!("events.execution.strategy_settlement.{TARGET}").into(),
            self.receiver.clone(),
            None,
        );
        self.clock().set_timer_ns(
            "settlement-target",
            DurationNanos::from_millis(5),
            None,
            None,
            None,
            Some(false),
            Some(false),
        )
    }
    fn on_time_event(&mut self, _: &TimeEvent) -> anyhow::Result<()> {
        if self.evidence.borrow().done {
            return Ok(());
        }

        if let Err(e) = self.step() {
            self.evidence.borrow_mut().error = Some(format!("{e:#}"));
            self.handle.stop();
        }
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.clock().cancel_timers();
        msgbus::unsubscribe_any(
            format!("events.execution.strategy_settlement.{TARGET}").into(),
            &self.receiver,
        );
        Ok(())
    }
}
nautilus_strategy!(Target, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        if self.exit != Some(event.client_order_id) {
            return;
        }
        let result = (|| -> anyhow::Result<()> {
            self.query = Some(self.query_strategy_settlement()?);
            // This descendant is queued after the fence by the earlier fill's
            // real native callback. A client-only snapshot must not certify it.
            self.query_account("XNAS-SETTLEMENT-001".into(), None, None)?;
            self.phase = 20;
            Ok(())
        })();

        if let Err(e) = result {
            self.evidence.borrow_mut().error = Some(format!("{e:#}"));
            self.handle.stop();
        }
    }
});

#[derive(Debug)]
struct Peer {
    core: StrategyCore,
    evidence: Rc<RefCell<Evidence>>,
    order: Option<ClientOrderId>,
    alternate: bool,
}
impl DataActor for Peer {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_quotes("AAPL.XNAS".into(), None, None);
        self.clock().set_timer_ns(
            "settlement-peer",
            DurationNanos::from_millis(10),
            None,
            None,
            None,
            Some(false),
            Some(false),
        )
    }
    fn on_time_event(&mut self, _: &TimeEvent) -> anyhow::Result<()> {
        self.evidence.borrow_mut().peer_ticks += 1;

        if let Some(id) = self.order {
            if self
                .cache_ref()
                .order(&id)
                .is_some_and(|order| order.status() == OrderStatus::Accepted)
            {
                self.alternate = !self.alternate;
                self.modify_order(
                    id,
                    None,
                    Some(if self.alternate { "4.00" } else { "5.00" }.into()),
                    None,
                    None,
                    None,
                )?;
                self.evidence.borrow_mut().peer_modify_submitted = true;
            }
        } else if self.cache_ref().quote(&"AAPL.XNAS".into()).is_some() {
            let order = self.core.order().limit(
                "AAPL.XNAS".into(),
                OrderSide::Buy,
                "1".into(),
                "5.00".into(),
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
            self.order = Some(order.client_order_id());
            self.submit_order(order, None, None, None)?;
        }
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.clock().cancel_timers();
        Ok(())
    }
}
nautilus_strategy!(Peer, {
    fn on_order_updated(&mut self, event: OrderUpdated) {
        if self.order == Some(event.client_order_id) {
            self.evidence.borrow_mut().peer_orders += 1;
        }
    }
});

#[tokio::test(flavor = "current_thread")]
async fn native_strategy_settlement_waits_for_delayed_work_while_peer_orders_continue()
-> anyhow::Result<()> {
    let evidence = Rc::new(RefCell::new(Evidence::default()));
    let instrument = InstrumentAny::Equity(
        Equity::builder()
            .instrument_id("AAPL.XNAS".into())
            .raw_symbol("AAPL".into())
            .currency(Currency::USD())
            .price_precision(2)
            .price_increment(Price::from("0.01"))
            .ts_event(0.into())
            .ts_init(0.into())
            .build()?,
    );
    let builder = LiveNodeBuilder::from_config(LiveNodeConfig {
        trader_id: "SETTLEMENT-001".into(),
        environment: Environment::Sandbox,
        load_state: false,
        save_state: false,
        risk_engine: LiveRiskEngineConfig {
            bypass: false,
            max_notional_per_order: HashMap::from([("AAPL.XNAS".into(), "50".into())]),
            ..Default::default()
        },
        ..Default::default()
    })?
    .with_delay_post_stop_secs(0u64);
    let builder = builder.add_data_client(
        Some("SETTLEMENT-DATA".into()),
        Box::new(InputFactory),
        Box::new(InputConfig {
            instrument,
            evidence: evidence.clone(),
        }),
    )?;
    let builder = builder.add_simulated_exec_client(
        Some("SETTLEMENT-SANDBOX".into()),
        Box::new(SandboxExecutionClientFactory::with_fee_model(
            FeeModelHandle::new(FixedFeeModel::new(Money::from("0.00 USD"), Some(true))?),
        )),
        Box::new(SandboxExecutionClientConfig {
            venue: "XNAS".into(),
            account_id: "XNAS-SETTLEMENT-001".into(),
            account_type: AccountType::Cash,
            starting_balances: vec![Money::from("100.00 USD")],
            base_currency: Some(Currency::USD()),
            book_type: BookType::L1_MBP,
            latency_model: Some(LatencyModelAny::Static(StaticLatencyModel::new(
                DurationNanos::ZERO,
                DurationNanos::from_millis(1),
                DurationNanos::from_millis(150),
                DurationNanos::from_millis(1),
            ))),
            ..Default::default()
        }),
    )?;
    let mut node: LiveNode = builder.build()?;
    let response = Rc::new(RefCell::new(None));
    let slot = response.clone();
    let receiver = ShareableMessageHandler::from_typed(move |result: &StrategySettlementResult| {
        *slot.borrow_mut() = Some(result.clone());
    });
    node.add_strategy(Target {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(TARGET.into()),
            order_id_tag: Some("001".into()),
            ..Default::default()
        }),
        handle: node.handle(),
        evidence: evidence.clone(),
        response,
        receiver,
        denied: None,
        filled: None,
        exit: None,
        resting: None,
        query: None,
        phase: 0,
        peers_at_flat: 0,
        orders_at_flat: 0,
        feed_at_flat: 0,
    })?;
    node.add_strategy(Peer {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some("SettlementPeer-002".into()),
            order_id_tag: Some("002".into()),
            ..Default::default()
        }),
        evidence: evidence.clone(),
        order: None,
        alternate: false,
    })?;
    tokio::time::timeout(Duration::from_secs(10), node.run()).await??;
    node.dispose();
    anyhow::ensure!(
        evidence.borrow().done,
        "Native settlement proof failed: {:?}",
        *evidence.borrow()
    );
    Ok(())
}
