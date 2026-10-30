// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Native construction, registration and immediately-ready connection order.

use std::{any::Any, cell::RefCell, rc::Rc};

use async_trait::async_trait;
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    enums::Environment,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
    live::runner::get_data_event_sender,
    messages::DataEvent,
    msgbus::{self, stubs::get_typed_message_saving_handler},
};
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    accounts::AccountAny,
    enums::OmsType,
    identifiers::{AccountId, ClientId, TraderId, Venue},
    instruments::{
        Instrument, InstrumentAny,
        stubs::{audusd_sim, equity_aapl},
    },
    types::{AccountBalance, MarginBalance},
};
use rstest::rstest;

use super::LiveNodeBuilder;

type Trace = Rc<RefCell<Vec<String>>>;

#[derive(Debug)]
struct OrderedConfig;

impl ClientConfig for OrderedConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
struct OrderedFactory {
    instrument: InstrumentAny,
    trace: Trace,
}

impl OrderedFactory {
    fn create_client(&self, role: &str, name: &str) -> OrderedClient {
        self.trace
            .borrow_mut()
            .push(format!("{role}:create:{name}"));
        OrderedClient {
            id: name.into(),
            instrument: self.instrument.clone(),
            trace: self.trace.clone(),
            connected: false,
        }
    }
}

impl DataClientFactory for OrderedFactory {
    fn create(
        &self,
        name: &str,
        _: &dyn ClientConfig,
        _: CacheView,
        _: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        Ok(Box::new(self.create_client("data", name)))
    }

    fn name(&self) -> &'static str {
        "ordered-data"
    }

    fn config_type(&self) -> &'static str {
        "OrderedConfig"
    }
}

impl ExecutionClientFactory for OrderedFactory {
    fn create(
        &self,
        _: TraderId,
        name: &str,
        _: &dyn ClientConfig,
        _: CacheView,
        _: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        Ok(Box::new(self.create_client("execution", name)))
    }

    fn name(&self) -> &'static str {
        "ordered-execution"
    }

    fn config_type(&self) -> &'static str {
        "OrderedConfig"
    }
}

#[derive(Debug)]
struct OrderedClient {
    id: ClientId,
    instrument: InstrumentAny,
    trace: Trace,
    connected: bool,
}

#[async_trait(?Send)]
impl DataClient for OrderedClient {
    fn client_id(&self) -> ClientId {
        self.id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.instrument.id().venue)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn is_disconnected(&self) -> bool {
        !self.connected
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.trace
            .borrow_mut()
            .push(format!("data:connect:{}", self.id));
        get_data_event_sender().send(DataEvent::Instrument(self.instrument.clone()))?;
        self.connected = true;
        Ok(())
    }
}

#[async_trait(?Send)]
impl ExecutionClient for OrderedClient {
    fn is_connected(&self) -> bool {
        self.connected
    }

    fn client_id(&self) -> ClientId {
        self.id
    }

    fn account_id(&self) -> AccountId {
        AccountId::new(format!("{}-001", self.id))
    }

    fn venue(&self) -> Venue {
        self.instrument.id().venue
    }

    fn oms_type(&self) -> OmsType {
        OmsType::Netting
    }

    fn get_account(&self) -> Option<AccountAny> {
        None
    }

    fn generate_account_state(
        &self,
        _: Vec<AccountBalance>,
        _: Vec<MarginBalance>,
        _: bool,
        _: UnixNanos,
        _: Option<Params>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.trace
            .borrow_mut()
            .push(format!("execution:connect:{}", self.id));
        self.connected = true;
        Ok(())
    }
}

fn factories(reverse: bool, trace: &Trace) -> Vec<(&'static str, OrderedFactory)> {
    let mut factories = vec![
        ("Z-FIRST", InstrumentAny::CurrencyPair(audusd_sim())),
        ("A-SECOND", InstrumentAny::Equity(equity_aapl())),
    ];

    if reverse {
        factories.reverse();
    }
    factories
        .into_iter()
        .map(|(name, instrument)| {
            (
                name,
                OrderedFactory {
                    instrument,
                    trace: trace.clone(),
                },
            )
        })
        .collect()
}

#[rstest]
#[tokio::test]
async fn client_factory_registration_and_ready_connection_preserve_add_order(
    #[values(false, true)] reverse: bool,
) {
    let trace = Trace::default();
    let factories = factories(reverse, &trace);
    let names = factories.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    let mut builder = LiveNodeBuilder::new("ORDERED-001".into(), Environment::Sandbox).unwrap();
    for (name, factory) in &factories {
        builder = builder
            .add_data_client(
                Some((*name).into()),
                Box::new(factory.clone()),
                Box::new(OrderedConfig),
            )
            .unwrap()
            .add_exec_client(
                Some((*name).into()),
                Box::new(factory.clone()),
                Box::new(OrderedConfig),
            )
            .unwrap();
    }
    let mut node = builder.build().unwrap();
    let mut expected = ["data", "execution"]
        .into_iter()
        .flat_map(|role| {
            names
                .iter()
                .map(move |name| format!("{role}:create:{name}"))
        })
        .collect::<Vec<_>>();
    assert_eq!(*trace.borrow(), expected);
    let expected_ids = names
        .iter()
        .map(|name| ClientId::new(*name))
        .collect::<Vec<_>>();
    assert_eq!(
        node.kernel
            .data_engine
            .borrow()
            .get_clients()
            .iter()
            .map(|client| client.client_id)
            .collect::<Vec<_>>(),
        expected_ids
    );
    assert_eq!(node.kernel.exec_engine.borrow().client_ids(), expected_ids);
    let (handler, observed) = get_typed_message_saving_handler::<InstrumentAny>(None);
    msgbus::subscribe_instruments("data.instrument.*".into(), handler, None);
    node.kernel.connect_data_clients().await;
    node.runner.as_mut().unwrap().flush_pending_data();
    node.kernel.connect_exec_clients().await;
    expected.extend(["data", "execution"].into_iter().flat_map(|role| {
        names
            .iter()
            .map(move |name| format!("{role}:connect:{name}"))
    }));
    assert_eq!(*trace.borrow(), expected);
    let instruments = factories
        .iter()
        .map(|(_, factory)| factory.instrument.clone())
        .collect::<Vec<_>>();
    assert_eq!(observed.get_messages(), instruments);
    for instrument in instruments {
        assert_eq!(
            node.kernel.cache.borrow().instrument(&instrument.id()),
            Some(&instrument)
        );
    }
    node.dispose();
}

#[rstest]
#[case::data(true)]
#[case::execution(false)]
fn client_factory_duplicate_names_are_rejected_before_construction(#[case] data: bool) {
    let trace = Trace::default();
    let factory = factories(false, &trace).remove(0).1;
    let builder = LiveNodeBuilder::new("DUPLICATE-001".into(), Environment::Sandbox).unwrap();
    let result = if data {
        builder
            .add_data_client(
                Some("SAME".into()),
                Box::new(factory.clone()),
                Box::new(OrderedConfig),
            )
            .unwrap()
            .add_data_client(
                Some("SAME".into()),
                Box::new(factory),
                Box::new(OrderedConfig),
            )
    } else {
        builder
            .add_exec_client(
                Some("SAME".into()),
                Box::new(factory.clone()),
                Box::new(OrderedConfig),
            )
            .unwrap()
            .add_exec_client(
                Some("SAME".into()),
                Box::new(factory),
                Box::new(OrderedConfig),
            )
    };
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("already registered")
    );
    assert!(trace.borrow().is_empty());
}
