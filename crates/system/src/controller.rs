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

use std::{cell::RefCell, collections::HashMap, fmt::Debug, rc::Rc};

use nautilus_common::{
    actor::{
        DataActor, DataActorCore, DataActorNative,
        data_actor::{DataActorConfig, ImportableActorConfig},
        registry::try_get_actor_unchecked,
    },
    component::Component,
    enums::ComponentState,
    msgbus::{Endpoint, MStr, TypedHandler, get_message_bus},
    nautilus_actor,
};
use nautilus_execution::engine::ExecutionEngine;
use nautilus_model::identifiers::{ActorId, StrategyId};
use nautilus_trading::{ImportableStrategyConfig, Strategy, StrategyNative};

use crate::{
    messages::{ControllerCommand, ControllerCommandResult},
    registration::StrategyRegistration,
    strategy_factory::StrategyFactory,
    trader::Trader,
};

#[derive(Debug)]
pub struct Controller {
    core: DataActorCore,
    trader: Rc<RefCell<Trader>>,
    execution_engine: Option<Rc<RefCell<ExecutionEngine>>>,
    strategy_factories: HashMap<String, StrategyFactory>,
    strategy_factories_sealed: bool,
}

impl Controller {
    pub const EXECUTE_ENDPOINT: &str = "Controller.execute";

    #[must_use]
    pub fn new(trader: Rc<RefCell<Trader>>, config: Option<DataActorConfig>) -> Self {
        Self {
            core: DataActorCore::new(config.unwrap_or_default()),
            trader,
            execution_engine: None,
            strategy_factories: HashMap::new(),
            strategy_factories_sealed: false,
        }
    }

    /// Supplies the native execution context for strategy OMS configuration.
    #[must_use]
    pub fn with_execution_engine(mut self, execution_engine: Rc<RefCell<ExecutionEngine>>) -> Self {
        self.execution_engine = Some(execution_engine);
        self
    }

    /// Registers a compiled native strategy factory before the controller starts.
    ///
    /// Factories are installed with the executable; importable configuration selects
    /// one of these constructors without dynamically loading strategy code.
    ///
    /// # Errors
    ///
    /// Returns an error after initial activation or if the strategy path is already registered.
    pub fn register_strategy_factory(&mut self, factory: StrategyFactory) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.strategy_factories_sealed
                && matches!(
                    self.state(),
                    ComponentState::PreInitialized | ComponentState::Ready
                ),
            "Cannot register a strategy factory after initial controller activation"
        );
        let path = factory.strategy_path().to_string();
        anyhow::ensure!(
            !self.strategy_factories.contains_key(&path),
            "Native strategy factory '{path}' is already registered"
        );
        self.strategy_factories.insert(path, factory);
        Ok(())
    }

    /// Sends a controller command to the registered controller endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the controller execute endpoint is not registered.
    pub fn send(command: &ControllerCommand) -> anyhow::Result<()> {
        let endpoint = Self::execute_endpoint();
        let handler = {
            let msgbus = get_message_bus();
            msgbus
                .borrow_mut()
                .endpoint_map::<ControllerCommand>()
                .get(endpoint)
                .cloned()
        };

        let Some(handler) = handler else {
            anyhow::bail!(
                "Controller execute endpoint '{}' not registered",
                endpoint.as_str()
            );
        };

        handler.handle(command);
        Ok(())
    }

    /// Executes a controller command against the underlying trader.
    ///
    /// # Errors
    ///
    /// Returns an error if the requested lifecycle operation fails.
    pub fn execute(&mut self, command: ControllerCommand) -> anyhow::Result<()> {
        self.execute_with_result(command).map(|_| ())
    }

    /// Executes a command and returns the actual native component identity.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration, identity validation, registration or lifecycle fails.
    pub fn execute_with_result(
        &mut self,
        command: ControllerCommand,
    ) -> anyhow::Result<ControllerCommandResult> {
        match command {
            ControllerCommand::CreateActor(command) => self
                .create_actor_from_config(&command.actor_config, command.start)
                .map(ControllerCommandResult::Actor),
            ControllerCommand::StartActor(command) => self
                .start_actor(&command.actor_id)
                .map(|()| ControllerCommandResult::Actor(command.actor_id)),
            ControllerCommand::StopActor(command) => self
                .stop_actor(&command.actor_id)
                .map(|()| ControllerCommandResult::Actor(command.actor_id)),
            ControllerCommand::RemoveActor(command) => self
                .remove_actor(&command.actor_id)
                .map(|()| ControllerCommandResult::Actor(command.actor_id)),
            ControllerCommand::CreateStrategy(command) => self
                .create_strategy_from_config_checked(
                    &command.strategy_config,
                    command.expected_strategy_id,
                    command.start,
                )
                .map(ControllerCommandResult::Strategy),
            ControllerCommand::StartStrategy(command) => self
                .start_strategy(&command.strategy_id)
                .map(|()| ControllerCommandResult::Strategy(command.strategy_id)),
            ControllerCommand::StopStrategy(command) => self
                .stop_strategy(&command.strategy_id)
                .map(|()| ControllerCommandResult::Strategy(command.strategy_id)),
            ControllerCommand::ExitMarket(strategy_id) => self
                .exit_market(&strategy_id)
                .map(|()| ControllerCommandResult::Strategy(strategy_id)),
            ControllerCommand::RemoveStrategy(command) => self
                .remove_strategy(&command.strategy_id)
                .map(|()| ControllerCommandResult::Strategy(command.strategy_id)),
        }
    }

    /// Creates a new actor and optionally starts it.
    ///
    /// # Errors
    ///
    /// Returns an error if actor registration or startup fails.
    pub fn create_actor<T>(&self, actor: T, start: bool) -> anyhow::Result<ActorId>
    where
        T: DataActor + DataActorNative + Component + Debug + 'static,
    {
        let actor_id = actor.actor_id();
        self.trader.borrow_mut().add_actor(actor)?;

        self.start_created_actor(actor_id, start)?;

        Ok(actor_id)
    }

    /// Creates a new actor from a factory and optionally starts it.
    ///
    /// # Errors
    ///
    /// Returns an error if the factory, actor registration, or startup fails.
    pub fn create_actor_from_factory<F, T>(
        &self,
        factory: F,
        start: bool,
    ) -> anyhow::Result<ActorId>
    where
        F: FnOnce() -> anyhow::Result<T>,
        T: DataActor + DataActorNative + Component + Debug + 'static,
    {
        let actor = factory()?;
        self.create_actor(actor, start)
    }

    /// Creates a new actor from an importable config and optionally starts it.
    ///
    /// # Errors
    ///
    /// Returns an error if the import, actor registration, or startup fails.
    #[cfg(feature = "python")]
    pub fn create_actor_from_config(
        &self,
        actor_config: &ImportableActorConfig,
        start: bool,
    ) -> anyhow::Result<ActorId> {
        let actor_id = self
            .trader
            .borrow_mut()
            .add_actor_from_importable_config(actor_config)?;

        self.start_created_actor(actor_id, start)?;

        Ok(actor_id)
    }

    /// Rejects importable actor creation when Python support is not compiled in.
    ///
    /// # Errors
    ///
    /// Always returns an unsupported command error.
    #[cfg(not(feature = "python"))]
    pub fn create_actor_from_config(
        &self,
        actor_config: &ImportableActorConfig,
        _start: bool,
    ) -> anyhow::Result<ActorId> {
        Self::unsupported_create_actor_config(actor_config)
    }

    /// Creates a new strategy and optionally starts it.
    ///
    /// # Errors
    ///
    /// Returns an error if strategy registration or startup fails.
    pub fn create_strategy<T>(&self, strategy: T, start: bool) -> anyhow::Result<StrategyId>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        self.create_strategy_checked(strategy, None, start)
    }

    pub(crate) fn create_strategy_checked<T>(
        &self,
        strategy: T,
        expected_id: Option<StrategyId>,
        start: bool,
    ) -> anyhow::Result<StrategyId>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let registration = self.trader.borrow_mut().add_strategy_with_context(
            strategy,
            expected_id,
            self.execution_engine.as_ref(),
        )?;
        self.start_created_strategy(&registration, start)?;
        Ok(registration.strategy_id)
    }

    /// Creates a new strategy from a factory and optionally starts it.
    ///
    /// # Errors
    ///
    /// Returns an error if the factory, strategy registration, or startup fails.
    pub fn create_strategy_from_factory<F, T>(
        &self,
        factory: F,
        start: bool,
    ) -> anyhow::Result<StrategyId>
    where
        F: FnOnce() -> anyhow::Result<T>,
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let strategy = factory()?;
        self.create_strategy(strategy, start)
    }

    /// Creates a new strategy from an importable config and optionally starts it.
    ///
    /// # Errors
    ///
    /// Returns an error if the import, strategy registration, or startup fails.
    pub fn create_strategy_from_config(
        &self,
        strategy_config: &ImportableStrategyConfig,
        start: bool,
    ) -> anyhow::Result<StrategyId> {
        self.create_strategy_from_config_checked(strategy_config, None, start)
    }

    fn create_strategy_from_config_checked(
        &self,
        strategy_config: &ImportableStrategyConfig,
        expected_id: Option<StrategyId>,
        start: bool,
    ) -> anyhow::Result<StrategyId> {
        if let Some(factory) = self.strategy_factories.get(&strategy_config.strategy_path) {
            return factory.create(self, strategy_config, expected_id, start);
        }

        #[cfg(feature = "python")]
        {
            pyo3::Python::try_attach(|_| {
                let strategy_id = self
                    .trader
                    .borrow_mut()
                    .add_strategy_from_importable_config_checked(
                        strategy_config,
                        expected_id,
                        self.execution_engine.as_ref(),
                    )?;

                self.start_created_strategy(&strategy_id, start)?;

                Ok(strategy_id.strategy_id)
            })
            .unwrap_or_else(|| {
                anyhow::bail!(
                    "No compiled strategy factory for '{}'; the Python runtime is not available",
                    strategy_config.strategy_path
                )
            })
        }
        #[cfg(not(feature = "python"))]
        Self::unsupported_create_strategy_config(strategy_config)
    }

    /// Starts the registered actor with the given identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor is not registered or cannot be started.
    pub fn start_actor(&self, actor_id: &ActorId) -> anyhow::Result<()> {
        self.trader.borrow().start_actor(actor_id)
    }

    /// Stops the registered actor with the given identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor is not registered or cannot be stopped.
    pub fn stop_actor(&self, actor_id: &ActorId) -> anyhow::Result<()> {
        self.trader.borrow().stop_actor(actor_id)
    }

    /// Removes the registered actor with the given identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor cannot be removed.
    pub fn remove_actor(&self, actor_id: &ActorId) -> anyhow::Result<()> {
        if actor_id.inner() == self.core.actor_id().inner() {
            return Ok(());
        }

        self.trader.borrow_mut().remove_actor(actor_id)
    }

    /// Starts the registered strategy with the given identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy is not registered or cannot be started.
    pub fn start_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<()> {
        self.trader.borrow().start_strategy(strategy_id)
    }

    /// Stops the registered strategy with the given identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy is not registered or cannot be stopped.
    pub fn stop_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<()> {
        self.trader.borrow_mut().stop_strategy(strategy_id)
    }

    /// Sends an exit-market command to the registered strategy.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy is not registered or its control endpoint is missing.
    pub fn exit_market(&self, strategy_id: &StrategyId) -> anyhow::Result<()> {
        Trader::market_exit_strategy(&self.trader, strategy_id)
    }

    /// Removes the registered strategy with the given identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy cannot be removed.
    pub fn remove_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<()> {
        if let Some(engine) = &self.execution_engine {
            // Validate access before retiring the component; callbacks run without an engine borrow.
            let _ = engine
                .try_borrow_mut()
                .map_err(|e| anyhow::anyhow!("Cannot deregister OMS type: {e}"))?;
        }
        self.trader.borrow_mut().remove_strategy(strategy_id)?;

        if let Some(engine) = &self.execution_engine {
            engine
                .try_borrow_mut()
                .map_err(|e| anyhow::anyhow!("Cannot deregister OMS type after retirement: {e}"))?
                .deregister_oms_type(*strategy_id);
        }
        Ok(())
    }

    fn start_created_actor(&self, actor_id: ActorId, start: bool) -> anyhow::Result<()> {
        if !start {
            return Ok(());
        }

        if let Err(start_err) = self.start_actor(&actor_id) {
            return Err(self.rollback_actor_start_failure(actor_id, start_err));
        }

        Ok(())
    }

    fn start_created_strategy(
        &self,
        registration: &StrategyRegistration,
        start: bool,
    ) -> anyhow::Result<()> {
        if !start {
            return Ok(());
        }

        if let Err(e) = self.start_strategy(&registration.strategy_id) {
            return Err(self.rollback_strategy_start_failure(registration, e));
        }
        Ok(())
    }

    fn rollback_actor_start_failure(
        &self,
        actor_id: ActorId,
        start_err: anyhow::Error,
    ) -> anyhow::Error {
        match self.remove_actor(&actor_id) {
            Ok(()) => start_err,
            Err(rollback_err) => anyhow::anyhow!(
                "Failed to start actor {actor_id}: {start_err}; rollback failed: {rollback_err}"
            ),
        }
    }

    fn rollback_strategy_start_failure(
        &self,
        registration: &StrategyRegistration,
        start_err: anyhow::Error,
    ) -> anyhow::Error {
        let strategy_id = registration.strategy_id;
        if let Err(e) = self.remove_strategy(&strategy_id) {
            return anyhow::anyhow!(
                "Failed to start strategy {strategy_id}: {start_err}; rollback failed: {e}"
            );
        }

        let mut rollback_errors = Vec::new();
        let cache = self.trader.borrow().cache.clone();
        let restore_claims = cache
            .try_borrow_mut()
            .map_err(|e| anyhow::anyhow!("Cannot restore prior external order claims: {e}"))
            .and_then(|mut cache| {
                cache.set_external_order_claims(strategy_id, &registration.prior_claims)
            });

        if let Err(e) = restore_claims {
            rollback_errors.push(e.to_string());
        }

        if let Some(engine) = &self.execution_engine
            && let Some(previous) = registration.previous_oms_type
        {
            match engine.try_borrow_mut() {
                Ok(mut engine) => engine.register_oms_type(strategy_id, previous),
                Err(e) => rollback_errors.push(format!("Cannot restore prior OMS override: {e}")),
            }
        }

        if !rollback_errors.is_empty() {
            return anyhow::anyhow!(
                "Failed to start strategy {strategy_id}: {start_err}; rollback failed: {}",
                rollback_errors.join("; ")
            );
        }
        start_err
    }

    fn register_execute_endpoint(&self) {
        let controller_id = self.core.actor_id().inner();
        let handler = TypedHandler::from(move |command: &ControllerCommand| {
            if let Some(mut controller) = try_get_actor_unchecked::<Self>(&controller_id) {
                if let Err(e) = controller.execute(command.clone()) {
                    log::error!("Controller command failed for {controller_id}: {e}");
                }
            } else {
                log::error!("Controller {controller_id} not found for command handling");
            }
        });

        get_message_bus()
            .borrow_mut()
            .endpoint_map::<ControllerCommand>()
            .register(Self::execute_endpoint(), handler);
    }

    fn deregister_execute_endpoint() {
        get_message_bus()
            .borrow_mut()
            .endpoint_map::<ControllerCommand>()
            .deregister(Self::execute_endpoint());
    }

    fn execute_endpoint() -> MStr<Endpoint> {
        Self::EXECUTE_ENDPOINT.into()
    }

    #[cfg(not(feature = "python"))]
    fn unsupported_create_actor_config(
        actor_config: &ImportableActorConfig,
    ) -> anyhow::Result<ActorId> {
        anyhow::bail!(
            "CreateActor command for importable actor '{}' is not supported by the Rust controller",
            actor_config.actor_path
        );
    }

    #[cfg(not(feature = "python"))]
    fn unsupported_create_strategy_config(
        strategy_config: &ImportableStrategyConfig,
    ) -> anyhow::Result<StrategyId> {
        anyhow::bail!(
            "CreateStrategy command for importable strategy '{}' is not supported by the Rust controller",
            strategy_config.strategy_path
        );
    }
}

impl DataActor for Controller {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.strategy_factories_sealed = true;
        self.register_execute_endpoint();
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        Self::deregister_execute_endpoint();
        Ok(())
    }

    fn on_resume(&mut self) -> anyhow::Result<()> {
        self.register_execute_endpoint();
        Ok(())
    }

    fn on_dispose(&mut self) -> anyhow::Result<()> {
        Self::deregister_execute_endpoint();
        Ok(())
    }
}

nautilus_actor!(Controller);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    #[cfg(feature = "python")]
    use std::ffi::CString;

    #[cfg(feature = "python")]
    use nautilus_common::python::actor::{PyDataActor, PyDataActorInner};
    use nautilus_common::{
        actor::data_actor::ImportableActorConfig,
        cache::Cache,
        clock::VirtualClock,
        enums::{ComponentState, Environment},
        msgbus::{MessageBus, set_message_bus},
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        enums::OmsType,
        identifiers::{InstrumentId, TraderId},
        stubs::TestDefault,
    };
    use nautilus_portfolio::portfolio::Portfolio;
    #[cfg(feature = "python")]
    use nautilus_trading::python::strategy::{PyStrategy, PyStrategyInner};
    use nautilus_trading::{
        ImportableStrategyConfig, nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };
    #[cfg(feature = "python")]
    use pyo3::{
        prelude::*,
        types::{PyDict, PyModule},
    };
    use rstest::rstest;
    use serde::Deserialize;

    use super::*;
    use crate::{
        clock_factory::ClockFactory,
        messages::{
            CreateActor, CreateStrategy, RemoveActor, RemoveStrategy, StartActor, StartStrategy,
            StopActor, StopStrategy,
        },
    };

    fn start_actor_command(actor_id: ActorId) -> ControllerCommand {
        StartActor::new(actor_id, UUID4::new(), UnixNanos::default()).into()
    }

    fn stop_actor_command(actor_id: ActorId) -> ControllerCommand {
        StopActor::new(actor_id, UUID4::new(), UnixNanos::default()).into()
    }

    fn remove_actor_command(actor_id: ActorId) -> ControllerCommand {
        RemoveActor::new(actor_id, UUID4::new(), UnixNanos::default()).into()
    }

    fn start_strategy_command(strategy_id: StrategyId) -> ControllerCommand {
        StartStrategy::new(strategy_id, UUID4::new(), UnixNanos::default()).into()
    }

    fn stop_strategy_command(strategy_id: StrategyId) -> ControllerCommand {
        StopStrategy::new(strategy_id, UUID4::new(), UnixNanos::default()).into()
    }

    fn remove_strategy_command(strategy_id: StrategyId) -> ControllerCommand {
        RemoveStrategy::new(strategy_id, UUID4::new(), UnixNanos::default()).into()
    }

    #[derive(Debug)]
    struct TestDataActor {
        core: DataActorCore,
    }

    impl TestDataActor {
        fn new(config: DataActorConfig) -> Self {
            Self {
                core: DataActorCore::new(config),
            }
        }
    }

    impl DataActor for TestDataActor {}

    nautilus_actor!(TestDataActor);

    #[derive(Debug)]
    struct TestStrategy {
        core: StrategyCore,
        interval_ms: Option<u64>,
    }

    impl TestStrategy {
        fn new(config: StrategyConfig) -> Self {
            Self {
                core: StrategyCore::new(config),
                interval_ms: None,
            }
        }
    }

    impl DataActor for TestStrategy {}

    nautilus_strategy!(TestStrategy, {
        fn external_order_instrument_ids(&self) -> Option<Vec<InstrumentId>> {
            self.core.config.external_order_instrument_ids.clone()
        }
    });

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct NativeStrategyConfig {
        base: StrategyConfig,
        interval_ms: u64,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct NamedStrategyConfig {
        base: StrategyConfig,
        label: String,
    }

    #[derive(Debug)]
    struct NamedStrategy {
        core: StrategyCore,
        label: String,
    }

    impl DataActor for NamedStrategy {
        fn on_start(&mut self) -> anyhow::Result<()> {
            anyhow::ensure!(self.label == "unrelated", "Invalid native fixture label");
            Ok(())
        }
    }

    nautilus_strategy!(NamedStrategy);

    #[derive(Debug)]
    struct FailingStartActor {
        core: DataActorCore,
    }

    impl FailingStartActor {
        fn new(config: DataActorConfig) -> Self {
            Self {
                core: DataActorCore::new(config),
            }
        }
    }

    impl DataActor for FailingStartActor {
        fn on_start(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("Simulated actor start failure")
        }
    }

    nautilus_actor!(FailingStartActor);

    #[derive(Debug)]
    struct FailingStartStrategy {
        core: StrategyCore,
    }

    impl FailingStartStrategy {
        fn new(config: StrategyConfig) -> Self {
            Self {
                core: StrategyCore::new(config),
            }
        }
    }

    impl DataActor for FailingStartStrategy {
        fn on_start(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("Simulated strategy start failure")
        }
    }

    nautilus_strategy!(FailingStartStrategy, {
        fn external_order_instrument_ids(&self) -> Option<Vec<InstrumentId>> {
            self.core.config.external_order_instrument_ids.clone()
        }
    });

    #[derive(Debug)]
    struct FailingRetirementStrategy {
        core: StrategyCore,
    }
    impl DataActor for FailingRetirementStrategy {
        fn on_start(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("Failed strategy start")
        }
        fn on_dispose(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("Failed strategy disposal")
        }
    }
    nautilus_strategy!(FailingRetirementStrategy, {
        fn external_order_instrument_ids(&self) -> Option<Vec<InstrumentId>> {
            self.core.config.external_order_instrument_ids.clone()
        }
    });

    #[derive(Debug)]
    struct ReentrantExitStrategy {
        core: StrategyCore,
        actor_to_stop: ActorId,
    }

    impl ReentrantExitStrategy {
        fn new(config: StrategyConfig, actor_to_stop: ActorId) -> Self {
            Self {
                core: StrategyCore::new(config),
                actor_to_stop,
            }
        }
    }

    impl DataActor for ReentrantExitStrategy {}

    nautilus_strategy!(ReentrantExitStrategy, {
        fn on_market_exit(&mut self) {
            Controller::send(&stop_actor_command(self.actor_to_stop)).unwrap();
        }
    });

    fn create_running_controller() -> (Rc<RefCell<Trader>>, ActorId) {
        create_running_controller_with_factories(Vec::new())
    }

    fn create_running_controller_with_factories(
        factories: Vec<StrategyFactory>,
    ) -> (Rc<RefCell<Trader>>, ActorId) {
        let trader_id = TraderId::test_default();
        let instance_id = UUID4::new();
        let clock_factory = ClockFactory::test_default();
        let clock = clock_factory.clock();
        let mut clock_ref = clock.borrow_mut();
        let test_clock = clock_ref
            .as_any_mut()
            .downcast_mut::<VirtualClock>()
            .expect("test default clock must be VirtualClock");
        test_clock.set_time(1_000_000_000u64.into());
        drop(clock_ref);

        let msgbus = Rc::new(RefCell::new(MessageBus::new(
            trader_id,
            instance_id,
            Some("test".to_string()),
            None,
        )));
        set_message_bus(msgbus);

        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            clock.clone(),
            cache.clone(),
            None,
        )));

        let trader = Rc::new(RefCell::new(Trader::new(
            trader_id,
            instance_id,
            Environment::Backtest,
            clock_factory,
            cache,
            portfolio,
        )));
        trader.borrow_mut().initialize().unwrap();

        let mut controller = Controller::new(
            trader.clone(),
            Some(DataActorConfig {
                actor_id: Some(ActorId::from("Controller-001")),
                ..Default::default()
            }),
        );

        for factory in factories {
            controller.register_strategy_factory(factory).unwrap();
        }
        let controller_id = controller.core.actor_id();

        trader.borrow_mut().add_actor(controller).unwrap();
        trader.borrow_mut().start().unwrap();

        (trader, controller_id)
    }

    #[cfg(feature = "python")]
    fn install_controller_importables_module(py: Python<'_>, module_name: &str) {
        let module = PyModule::new(py, module_name).expect("test module should create");
        module
            .setattr("DataActor", py.get_type::<PyDataActor>())
            .expect("DataActor type should bind");
        module
            .setattr("Strategy", py.get_type::<PyStrategy>())
            .expect("Strategy type should bind");
        module
            .setattr("RESULTS", PyDict::new(py))
            .expect("RESULTS should bind");

        let code = CString::new(
            r#"
RESULTS["actor_start"] = 0
RESULTS["strategy_start"] = 0
RESULTS["fallback_post_init"] = 0
RESULTS["fallback_post_init_seen"] = False
RESULTS["fallback_actor_id"] = ""

class CommandActorConfig:
    def __init__(self, actor_id=None, log_events=True, log_commands=True):
        self.actor_id = actor_id
        self.log_events = log_events
        self.log_commands = log_commands

class CommandActor(DataActor):
    def __init__(self, config):
        super().__init__(config)

    def on_start(self):
        RESULTS["actor_start"] += 1

class FailingActor(CommandActor):
    def on_start(self):
        raise RuntimeError("simulated actor start failure")

class FallbackActorConfig:
    def __init__(self):
        self.actor_id = None
        self.log_events = True
        self.log_commands = True
        self.post_init_called = False

    def __post_init__(self):
        self.post_init_called = True
        RESULTS["fallback_post_init"] += 1

class FallbackActor(DataActor):
    def __init__(self, config):
        super().__init__(config)
        RESULTS["fallback_post_init_seen"] = config.post_init_called
        RESULTS["fallback_actor_id"] = str(config.actor_id)

class CommandStrategyConfig:
    def __init__(self, strategy_id=None, log_events=True, log_commands=True, oms_type=None, external_order_instrument_ids=None):
        self.strategy_id = strategy_id
        self.log_events = log_events
        self.log_commands = log_commands
        self.oms_type = oms_type
        self.external_order_instrument_ids = external_order_instrument_ids

class CommandStrategy(Strategy):
    def __init__(self, config):
        super().__init__(config)

    def on_start(self):
        RESULTS["strategy_start"] += 1

class FailingStrategy(CommandStrategy):
    def on_start(self):
        raise RuntimeError("simulated strategy start failure")
"#,
        )
        .expect("python test code should be valid CString");

        py.run(code.as_c_str(), Some(&module.dict()), None)
            .expect("test importables module should load");

        let sys_modules = py
            .import("sys")
            .expect("sys should import")
            .getattr("modules")
            .expect("sys.modules should exist");
        sys_modules
            .set_item(module_name, module)
            .expect("test module should register");
    }

    #[rstest]
    #[cfg(not(feature = "python"))]
    fn test_controller_rejects_importable_create_commands() {
        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();

        let mut controller = try_get_actor_unchecked::<Controller>(&controller_actor_id).unwrap();
        let actor_config = ImportableActorConfig {
            actor_path: "tests.actors:Actor".to_string(),
            config_path: "tests.actors:ActorConfig".to_string(),
            config: HashMap::new(),
        };
        let strategy_config = ImportableStrategyConfig {
            strategy_path: "tests.strategies:Strategy".to_string(),
            config_path: "tests.strategies:StrategyConfig".to_string(),
            config: HashMap::new(),
        };

        let actor_result = controller.execute(
            CreateActor::new(actor_config, true, UUID4::new(), UnixNanos::default()).into(),
        );
        let strategy_result = controller.execute(
            CreateStrategy::new(strategy_config, true, UUID4::new(), UnixNanos::default()).into(),
        );

        assert_eq!(
            actor_result.unwrap_err().to_string(),
            "CreateActor command for importable actor 'tests.actors:Actor' is not supported by the Rust controller"
        );
        assert_eq!(
            strategy_result.unwrap_err().to_string(),
            "CreateStrategy command for importable strategy 'tests.strategies:Strategy' is not supported by the Rust controller"
        );

        drop(controller);
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    #[cfg(feature = "python")]
    fn test_controller_creates_importable_actor_and_strategy_commands() {
        Python::initialize();

        let module_name = "test_system_controller_importables";
        Python::attach(|py| install_controller_importables_module(py, module_name));

        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();
        let actor_id = ActorId::from("CommandActor-001");
        let strategy_id = StrategyId::from("CommandStrategy-001");

        {
            let mut controller = try_get_actor_unchecked::<Controller>(&controller_actor_id)
                .expect("controller should be registered");
            let actor_config = ImportableActorConfig {
                actor_path: format!("{module_name}:CommandActor"),
                config_path: format!("{module_name}:CommandActorConfig"),
                config: HashMap::from([(
                    "actor_id".to_string(),
                    serde_json::Value::String("CommandActor-001".to_string()),
                )]),
            };
            let strategy_config = ImportableStrategyConfig {
                strategy_path: format!("{module_name}:CommandStrategy"),
                config_path: format!("{module_name}:CommandStrategyConfig"),
                config: HashMap::from([(
                    "strategy_id".to_string(),
                    serde_json::Value::String("CommandStrategy-001".to_string()),
                )]),
            };

            controller
                .execute(
                    CreateActor::new(actor_config, false, UUID4::new(), UnixNanos::default())
                        .into(),
                )
                .unwrap();
            controller
                .execute(
                    CreateStrategy::new(strategy_config, true, UUID4::new(), UnixNanos::default())
                        .into(),
                )
                .unwrap();
        }

        assert!(trader.borrow().actor_ids().contains(&actor_id));
        assert!(trader.borrow().strategy_ids().contains(&strategy_id));

        assert_eq!(
            try_get_actor_unchecked::<PyDataActorInner>(&actor_id.inner())
                .unwrap()
                .state(),
            ComponentState::Ready
        );
        assert_eq!(
            try_get_actor_unchecked::<PyStrategyInner>(&strategy_id.inner())
                .unwrap()
                .state(),
            ComponentState::Running
        );

        Python::attach(|py| {
            let module = py.import(module_name).expect("test module should import");
            let results_obj = module.getattr("RESULTS").expect("RESULTS should exist");
            let results = results_obj
                .cast::<PyDict>()
                .expect("RESULTS should be a dict");
            assert_eq!(
                results
                    .get_item("actor_start")
                    .expect("actor_start lookup should not error")
                    .expect("actor_start should exist")
                    .extract::<usize>()
                    .expect("actor_start should extract"),
                0
            );
            assert_eq!(
                results
                    .get_item("strategy_start")
                    .expect("strategy_start lookup should not error")
                    .expect("strategy_start should exist")
                    .extract::<usize>()
                    .expect("strategy_start should extract"),
                1
            );
        });

        trader.borrow_mut().remove_actor(&actor_id).unwrap();
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    #[cfg(feature = "python")]
    fn test_controller_stop_skips_unstarted_importable_components() {
        Python::initialize();

        let module_name = "test_system_controller_unstarted_components";
        Python::attach(|py| install_controller_importables_module(py, module_name));

        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();
        let actor_id = ActorId::from("CommandActor-001");
        let strategy_id = StrategyId::from("CommandStrategy-001");

        {
            let mut controller = try_get_actor_unchecked::<Controller>(&controller_actor_id)
                .expect("controller should be registered");
            let actor_config = ImportableActorConfig {
                actor_path: format!("{module_name}:CommandActor"),
                config_path: format!("{module_name}:CommandActorConfig"),
                config: HashMap::from([(
                    "actor_id".to_string(),
                    serde_json::Value::String("CommandActor-001".to_string()),
                )]),
            };
            let strategy_config = ImportableStrategyConfig {
                strategy_path: format!("{module_name}:CommandStrategy"),
                config_path: format!("{module_name}:CommandStrategyConfig"),
                config: HashMap::from([(
                    "strategy_id".to_string(),
                    serde_json::Value::String("CommandStrategy-001".to_string()),
                )]),
            };

            controller
                .execute(
                    CreateActor::new(actor_config, false, UUID4::new(), UnixNanos::default())
                        .into(),
                )
                .unwrap();
            controller
                .execute(
                    CreateStrategy::new(strategy_config, false, UUID4::new(), UnixNanos::default())
                        .into(),
                )
                .unwrap();
        }

        assert_eq!(
            try_get_actor_unchecked::<PyDataActorInner>(&actor_id.inner())
                .unwrap()
                .state(),
            ComponentState::Ready
        );
        assert_eq!(
            try_get_actor_unchecked::<PyStrategyInner>(&strategy_id.inner())
                .unwrap()
                .state(),
            ComponentState::Ready
        );

        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    #[cfg(feature = "python")]
    fn test_controller_importable_strategy_tag_collision_does_not_register_orphan() {
        Python::initialize();

        let module_name = "test_system_controller_tag_collision";
        Python::attach(|py| install_controller_importables_module(py, module_name));

        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();
        let existing_strategy_id = StrategyId::from("ExistingStrategy-001");
        let colliding_strategy_id = StrategyId::from("CommandStrategy-001");

        {
            let controller = try_get_actor_unchecked::<Controller>(&controller_actor_id)
                .expect("controller should be registered");
            controller
                .create_strategy(
                    TestStrategy::new(StrategyConfig {
                        strategy_id: Some(existing_strategy_id),
                        order_id_tag: Some("001".to_string()),
                        ..Default::default()
                    }),
                    false,
                )
                .unwrap();
        }

        let clock_count_before = trader.borrow().get_component_clocks().len();

        let result = {
            let mut controller = try_get_actor_unchecked::<Controller>(&controller_actor_id)
                .expect("controller should be registered");
            let strategy_config = ImportableStrategyConfig {
                strategy_path: format!("{module_name}:CommandStrategy"),
                config_path: format!("{module_name}:CommandStrategyConfig"),
                config: HashMap::from([(
                    "strategy_id".to_string(),
                    serde_json::Value::String("CommandStrategy-001".to_string()),
                )]),
            };

            controller.execute(
                CreateStrategy::new(strategy_config, false, UUID4::new(), UnixNanos::default())
                    .into(),
            )
        };

        assert_eq!(
            result.unwrap_err().to_string(),
            "Strategy order_id_tag conflict for '001', explicitly define unique order_id_tag values"
        );
        assert!(
            !trader
                .borrow()
                .strategy_ids()
                .contains(&colliding_strategy_id)
        );
        assert!(
            try_get_actor_unchecked::<PyStrategyInner>(&colliding_strategy_id.inner()).is_none()
        );
        assert_eq!(
            trader.borrow().get_component_clocks().len(),
            clock_count_before
        );

        trader
            .borrow_mut()
            .remove_strategy(&existing_strategy_id)
            .unwrap();
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    #[cfg(feature = "python")]
    fn test_python_factory_with_context_registers_native_oms_and_claims() {
        Python::initialize();
        let module_name = "test_system_python_registration_context";
        Python::attach(|py| install_controller_importables_module(py, module_name));
        let controller_id = ActorId::from("PYTHON-CONTEXT-CONTROLLER");
        let mut kernel = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig {
                actor_id: Some(controller_id),
                ..Default::default()
            })
            .build()
            .unwrap();
        let id = StrategyId::from("Python-001");
        let instrument = InstrumentId::from("AUDUSD.SIM");
        let config = ImportableStrategyConfig {
            strategy_path: format!("{module_name}:CommandStrategy"),
            config_path: format!("{module_name}:CommandStrategyConfig"),
            config: HashMap::from([
                ("strategy_id".into(), serde_json::json!("Python-001")),
                ("oms_type".into(), serde_json::json!("HEDGING")),
                (
                    "external_order_instrument_ids".into(),
                    serde_json::json!(["AUDUSD.SIM"]),
                ),
            ]),
        };
        let mut controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let wrong = CreateStrategy::new(config.clone(), true, UUID4::new(), UnixNanos::default())
            .with_expected_strategy_id("Wrong-001".into());
        assert!(controller.execute_with_result(wrong.into()).is_err());
        assert_eq!(kernel.exec_engine.borrow().oms_type_override(id), None);
        assert_eq!(
            kernel.cache.borrow().external_order_claim(&instrument),
            None
        );
        assert_eq!(
            controller
                .create_strategy_from_config(&config, false)
                .unwrap(),
            id
        );
        assert_eq!(
            kernel.exec_engine.borrow().oms_type_override(id),
            Some(OmsType::Hedging)
        );
        assert_eq!(
            kernel
                .exec_engine
                .borrow()
                .get_external_order_claim(&instrument),
            Some(id)
        );
        assert_eq!(
            nautilus_common::component::component_state(&id.inner()).unwrap(),
            ComponentState::Ready
        );
        controller.remove_strategy(&id).unwrap();
        assert_eq!(kernel.exec_engine.borrow().oms_type_override(id), None);
        assert_eq!(
            kernel.cache.borrow().external_order_claim(&instrument),
            None
        );
        drop(controller);
        kernel.dispose();
    }

    #[rstest]
    #[cfg(feature = "python")]
    fn test_expected_python_strategy_identity_precedes_registration() {
        Python::initialize();
        let module_name = "test_system_expected_strategy_identity";
        Python::attach(|py| install_controller_importables_module(py, module_name));
        let (trader, controller_id) = create_running_controller();
        let clocks = trader.borrow().get_component_clocks().len();
        let config = ImportableStrategyConfig {
            strategy_path: format!("{module_name}:CommandStrategy"),
            config_path: format!("{module_name}:CommandStrategyConfig"),
            config: HashMap::from([(
                "strategy_id".into(),
                serde_json::json!("CommandStrategy-001"),
            )]),
        };
        let mut controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let command = CreateStrategy::new(config.clone(), true, UUID4::new(), UnixNanos::default())
            .with_expected_strategy_id("Other-001".into());
        assert!(
            controller
                .execute_with_result(command.into())
                .unwrap_err()
                .to_string()
                .contains("identity mismatch")
        );
        assert!(trader.borrow().strategy_ids().is_empty());
        assert_eq!(trader.borrow().get_component_clocks().len(), clocks);
        let actual = StrategyId::from("CommandStrategy-001");
        assert!(try_get_actor_unchecked::<PyStrategyInner>(&actual.inner()).is_none());
        let command = CreateStrategy::new(config, false, UUID4::new(), UnixNanos::default())
            .with_expected_strategy_id(actual);
        assert_eq!(
            controller.execute_with_result(command.into()).unwrap(),
            ControllerCommandResult::Strategy(actual)
        );
        assert_eq!(
            try_get_actor_unchecked::<PyStrategyInner>(&actual.inner())
                .unwrap()
                .state(),
            ComponentState::Ready
        );
        drop(controller);
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    #[cfg(feature = "python")]
    fn test_controller_importable_start_failure_rolls_back_registration() {
        Python::initialize();

        let module_name = "test_system_controller_start_failure";
        Python::attach(|py| install_controller_importables_module(py, module_name));

        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();
        let actor_id = ActorId::from("FailingPyActor-001");
        let strategy_id = StrategyId::from("FailingPyStrategy-001");
        let clock_count_before = trader.borrow().get_component_clocks().len();

        let actor_result = {
            let mut controller = try_get_actor_unchecked::<Controller>(&controller_actor_id)
                .expect("controller should be registered");
            let actor_config = ImportableActorConfig {
                actor_path: format!("{module_name}:FailingActor"),
                config_path: format!("{module_name}:CommandActorConfig"),
                config: HashMap::from([(
                    "actor_id".to_string(),
                    serde_json::Value::String("FailingPyActor-001".to_string()),
                )]),
            };

            controller.execute(
                CreateActor::new(actor_config, true, UUID4::new(), UnixNanos::default()).into(),
            )
        };

        assert!(
            actor_result
                .unwrap_err()
                .to_string()
                .contains("simulated actor start failure")
        );
        assert!(!trader.borrow().actor_ids().contains(&actor_id));
        if let Some(actor) = try_get_actor_unchecked::<PyDataActorInner>(&actor_id.inner()) {
            assert_eq!(actor.state(), ComponentState::Disposed);
        }
        assert_eq!(
            trader.borrow().get_component_clocks().len(),
            clock_count_before
        );

        let strategy_result = {
            let mut controller = try_get_actor_unchecked::<Controller>(&controller_actor_id)
                .expect("controller should be registered");
            let strategy_config = ImportableStrategyConfig {
                strategy_path: format!("{module_name}:FailingStrategy"),
                config_path: format!("{module_name}:CommandStrategyConfig"),
                config: HashMap::from([(
                    "strategy_id".to_string(),
                    serde_json::Value::String("FailingPyStrategy-001".to_string()),
                )]),
            };

            controller.execute(
                CreateStrategy::new(strategy_config, true, UUID4::new(), UnixNanos::default())
                    .into(),
            )
        };

        assert!(
            strategy_result
                .unwrap_err()
                .to_string()
                .contains("simulated strategy start failure")
        );
        assert!(!trader.borrow().strategy_ids().contains(&strategy_id));
        if let Some(strategy) = try_get_actor_unchecked::<PyStrategyInner>(&strategy_id.inner()) {
            assert_eq!(strategy.state(), ComponentState::Disposed);
        }
        assert_eq!(
            trader.borrow().get_component_clocks().len(),
            clock_count_before
        );

        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    #[cfg(feature = "python")]
    fn test_controller_importable_malformed_paths_fail_without_mutation() {
        Python::initialize();

        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();
        let clock_count_before = trader.borrow().get_component_clocks().len();

        let actor_result = {
            let mut controller = try_get_actor_unchecked::<Controller>(&controller_actor_id)
                .expect("controller should be registered");
            let actor_config = ImportableActorConfig {
                actor_path: "no_colon_here".to_string(),
                config_path: String::new(),
                config: HashMap::new(),
            };

            controller.execute(
                CreateActor::new(actor_config, false, UUID4::new(), UnixNanos::default()).into(),
            )
        };
        let strategy_result = {
            let mut controller = try_get_actor_unchecked::<Controller>(&controller_actor_id)
                .expect("controller should be registered");
            let strategy_config = ImportableStrategyConfig {
                strategy_path: "module:Class:Extra".to_string(),
                config_path: String::new(),
                config: HashMap::new(),
            };

            controller.execute(
                CreateStrategy::new(strategy_config, false, UUID4::new(), UnixNanos::default())
                    .into(),
            )
        };

        assert_eq!(
            actor_result.unwrap_err().to_string(),
            "actor_path must be in format 'module.path:ClassName'"
        );
        assert_eq!(
            strategy_result.unwrap_err().to_string(),
            "strategy_path must be in format 'module.path:ClassName'"
        );
        assert_eq!(trader.borrow().actor_ids(), vec![controller_id]);
        assert!(trader.borrow().strategy_ids().is_empty());
        assert_eq!(
            trader.borrow().get_component_clocks().len(),
            clock_count_before
        );

        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    #[cfg(feature = "python")]
    fn test_controller_importable_config_fallback_registers_actor() {
        Python::initialize();

        let module_name = "test_system_controller_config_fallback";
        Python::attach(|py| install_controller_importables_module(py, module_name));

        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();
        let actor_id = ActorId::from("FallbackActor-001");

        {
            let mut controller = try_get_actor_unchecked::<Controller>(&controller_actor_id)
                .expect("controller should be registered");
            let actor_config = ImportableActorConfig {
                actor_path: format!("{module_name}:FallbackActor"),
                config_path: format!("{module_name}:FallbackActorConfig"),
                config: HashMap::from([(
                    "actor_id".to_string(),
                    serde_json::Value::String("FallbackActor-001".to_string()),
                )]),
            };

            controller
                .execute(
                    CreateActor::new(actor_config, false, UUID4::new(), UnixNanos::default())
                        .into(),
                )
                .unwrap();
        }

        assert!(trader.borrow().actor_ids().contains(&actor_id));
        assert_eq!(
            try_get_actor_unchecked::<PyDataActorInner>(&actor_id.inner())
                .unwrap()
                .state(),
            ComponentState::Ready
        );

        Python::attach(|py| {
            let module = py.import(module_name).expect("test module should import");
            let results_obj = module.getattr("RESULTS").expect("RESULTS should exist");
            let results = results_obj
                .cast::<PyDict>()
                .expect("RESULTS should be a dict");
            assert_eq!(
                results
                    .get_item("fallback_post_init")
                    .expect("fallback_post_init lookup should not error")
                    .expect("fallback_post_init should exist")
                    .extract::<usize>()
                    .expect("fallback_post_init should extract"),
                1
            );
            assert!(
                results
                    .get_item("fallback_post_init_seen")
                    .expect("fallback_post_init_seen lookup should not error")
                    .expect("fallback_post_init_seen should exist")
                    .extract::<bool>()
                    .expect("fallback_post_init_seen should extract")
            );
            assert_eq!(
                results
                    .get_item("fallback_actor_id")
                    .expect("fallback_actor_id lookup should not error")
                    .expect("fallback_actor_id should exist")
                    .extract::<String>()
                    .expect("fallback_actor_id should extract"),
                "FallbackActor-001"
            );
        });

        trader.borrow_mut().remove_actor(&actor_id).unwrap();
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    fn native_strategy_factory() -> StrategyFactory {
        StrategyFactory::new(
            "native:TestStrategy",
            "native:NativeStrategyConfig",
            |config: NativeStrategyConfig| {
                anyhow::ensure!(config.interval_ms > 0, "Invalid native fixture interval");
                let mut strategy = TestStrategy::new(config.base);
                strategy.interval_ms = Some(config.interval_ms);
                Ok(strategy)
            },
        )
        .unwrap()
    }

    fn native_strategy_config(id: &str, interval_ms: u64) -> ImportableStrategyConfig {
        ImportableStrategyConfig {
            strategy_path: "native:TestStrategy".to_string(),
            config_path: "native:NativeStrategyConfig".to_string(),
            config: HashMap::from([
                (
                    "base".to_string(),
                    serde_json::to_value(StrategyConfig {
                        strategy_id: Some(StrategyId::from(id)),
                        ..Default::default()
                    })
                    .unwrap(),
                ),
                ("interval_ms".to_string(), serde_json::json!(interval_ms)),
            ]),
        }
    }

    fn named_strategy_factory() -> StrategyFactory {
        StrategyFactory::new(
            "native:NamedStrategy",
            "native:NamedStrategyConfig",
            |config: NamedStrategyConfig| {
                Ok(NamedStrategy {
                    core: StrategyCore::new(config.base),
                    label: config.label,
                })
            },
        )
        .unwrap()
    }

    fn named_strategy_config(id: &str) -> ImportableStrategyConfig {
        ImportableStrategyConfig {
            strategy_path: "native:NamedStrategy".into(),
            config_path: "native:NamedStrategyConfig".into(),
            config: HashMap::from([
                (
                    "base".into(),
                    serde_json::to_value(StrategyConfig {
                        strategy_id: Some(id.into()),
                        ..Default::default()
                    })
                    .unwrap(),
                ),
                ("label".into(), serde_json::json!("unrelated")),
            ]),
        }
    }

    fn contextual_strategy_config(
        id: &str,
        oms_type: Option<OmsType>,
        claims: Vec<InstrumentId>,
    ) -> ImportableStrategyConfig {
        let mut config = native_strategy_config(id, 10);
        config.config.insert(
            "base".into(),
            serde_json::to_value(StrategyConfig {
                strategy_id: Some(id.into()),
                oms_type,
                external_order_instrument_ids: Some(claims),
                ..Default::default()
            })
            .unwrap(),
        );
        config
    }

    #[rstest]
    fn test_contextual_factory_preserves_oms_claims_and_clears_removed_override() {
        let controller_id = ActorId::from("OMS-CONTROLLER");
        let id = StrategyId::from("Configured-001");
        let peer = StrategyId::from("Peer-002");
        let instrument = InstrumentId::from("AUDUSD.SIM");
        let peer_instrument = InstrumentId::from("EURUSD.SIM");
        let mut kernel = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig {
                actor_id: Some(controller_id),
                ..Default::default()
            })
            .with_strategy_factory(native_strategy_factory())
            .with_strategies(vec![
                contextual_strategy_config(
                    "Configured-001",
                    Some(OmsType::Hedging),
                    vec![instrument],
                ),
                contextual_strategy_config(
                    "Peer-002",
                    Some(OmsType::Netting),
                    vec![peer_instrument],
                ),
            ])
            .build()
            .unwrap();
        assert_eq!(
            kernel.exec_engine.borrow().oms_type_override(id),
            Some(OmsType::Hedging)
        );
        assert_eq!(
            kernel
                .exec_engine
                .borrow()
                .get_external_order_claim(&instrument),
            Some(id)
        );
        assert_eq!(
            nautilus_common::component::component_state(&id.inner()).unwrap(),
            ComponentState::Ready
        );
        let controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        controller.remove_strategy(&id).unwrap();
        assert_eq!(kernel.exec_engine.borrow().oms_type_override(id), None);
        assert_eq!(
            kernel.cache.borrow().external_order_claim(&instrument),
            None
        );
        controller
            .create_strategy_from_config(&native_strategy_config("Configured-001", 10), false)
            .unwrap();
        assert_eq!(kernel.exec_engine.borrow().oms_type_override(id), None);
        assert_eq!(
            kernel.exec_engine.borrow().oms_type_override(peer),
            Some(OmsType::Netting)
        );
        assert_eq!(
            kernel.cache.borrow().external_order_claim(&peer_instrument),
            Some(peer)
        );
        drop(controller);
        kernel.dispose();
    }

    #[rstest]
    fn test_contextual_factory_preflight_failures_leave_peer_metadata_unchanged() {
        let controller_id = ActorId::from("PREFLIGHT-CONTROLLER");
        let peer = StrategyId::from("Peer-001");
        let target = StrategyId::from("Rejected-002");
        let instrument = InstrumentId::from("AUDUSD.SIM");
        let fresh = InstrumentId::from("EURUSD.SIM");
        let mut kernel = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig {
                actor_id: Some(controller_id),
                ..Default::default()
            })
            .with_strategy_factory(native_strategy_factory())
            .with_strategies(vec![contextual_strategy_config(
                "Peer-001",
                Some(OmsType::Netting),
                vec![instrument],
            )])
            .build()
            .unwrap();
        let clocks = kernel.trader.borrow().get_component_clocks().len();
        let mut controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let conflict = contextual_strategy_config(
            "Rejected-002",
            Some(OmsType::Hedging),
            vec![fresh, instrument],
        );
        assert!(
            controller
                .create_strategy_from_config(&conflict, false)
                .is_err()
        );
        let available =
            contextual_strategy_config("Rejected-002", Some(OmsType::Hedging), vec![fresh]);
        let engine = kernel.exec_engine.borrow_mut();
        assert!(
            controller
                .create_strategy_from_config(&available, false)
                .unwrap_err()
                .to_string()
                .contains("Cannot register OMS")
        );
        let mismatch = CreateStrategy::new(available, false, UUID4::new(), UnixNanos::default())
            .with_expected_strategy_id("Other-002".into());
        assert!(
            controller
                .execute_with_result(mismatch.into())
                .unwrap_err()
                .to_string()
                .contains("identity mismatch")
        );
        drop(engine);
        assert_eq!(kernel.trader.borrow().strategy_ids(), vec![peer]);
        assert_eq!(kernel.trader.borrow().get_component_clocks().len(), clocks);
        assert_eq!(kernel.cache.borrow().external_order_claim(&fresh), None);
        assert_eq!(kernel.exec_engine.borrow().oms_type_override(target), None);
        assert_eq!(
            kernel.exec_engine.borrow().oms_type_override(peer),
            Some(OmsType::Netting)
        );
        assert_eq!(
            kernel.cache.borrow().external_order_claim(&instrument),
            Some(peer)
        );
        drop(controller);
        kernel.dispose();
    }

    #[rstest]
    #[case::new_metadata(Some(OmsType::Hedging), true)]
    #[case::no_new_metadata(None, false)]
    fn test_contextual_failed_start_restores_prior_oms_and_retires_new_claims(
        #[case] oms_type: Option<OmsType>,
        #[case] new_claims: bool,
    ) {
        let controller_id = ActorId::from("ROLLBACK-CONTROLLER");
        let target = StrategyId::from("Failure-002");
        let peer = StrategyId::from("Peer-001");
        let instrument = InstrumentId::from("AUDUSD.SIM");
        let fresh = InstrumentId::from("EURUSD.SIM");
        let prior = InstrumentId::from("GBPUSD.SIM");
        let failing = StrategyFactory::new(
            "native:FailingStrategy",
            "native:NativeStrategyConfig",
            |config: NativeStrategyConfig| Ok(FailingStartStrategy::new(config.base)),
        )
        .unwrap();
        let mut kernel = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig {
                actor_id: Some(controller_id),
                ..Default::default()
            })
            .with_strategy_factory(native_strategy_factory())
            .with_strategy_factory(failing)
            .with_strategies(vec![contextual_strategy_config(
                "Peer-001",
                Some(OmsType::Hedging),
                vec![instrument],
            )])
            .build()
            .unwrap();
        kernel.start();
        kernel.start_trader().unwrap();
        kernel
            .exec_engine
            .borrow_mut()
            .register_oms_type(target, OmsType::Netting);
        kernel
            .cache
            .borrow_mut()
            .register_external_order_claims(target, &[prior])
            .unwrap();
        let clocks = kernel.trader.borrow().get_component_clocks().len();
        let controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let mut failure = contextual_strategy_config(
            "Failure-002",
            oms_type,
            if new_claims { vec![fresh] } else { vec![] },
        );
        failure.strategy_path = "native:FailingStrategy".into();
        assert!(
            controller
                .create_strategy_from_config(&failure, true)
                .unwrap_err()
                .to_string()
                .contains("Simulated strategy start failure")
        );
        assert_eq!(kernel.trader.borrow().strategy_ids(), vec![peer]);
        assert_eq!(kernel.trader.borrow().get_component_clocks().len(), clocks);
        assert_eq!(kernel.cache.borrow().external_order_claim(&fresh), None);
        assert_eq!(
            kernel.cache.borrow().external_order_claim(&prior),
            Some(target)
        );
        assert_eq!(
            kernel.exec_engine.borrow().oms_type_override(target),
            Some(OmsType::Netting)
        );
        assert_eq!(
            kernel.exec_engine.borrow().oms_type_override(peer),
            Some(OmsType::Hedging)
        );
        assert_eq!(
            kernel.cache.borrow().external_order_claim(&instrument),
            Some(peer)
        );
        drop(controller);
        kernel.dispose();
    }

    #[rstest]
    fn test_unclean_start_rollback_preserves_native_ownership_and_oms() {
        let controller_id = ActorId::from("UNCLEAN-ROLLBACK-CONTROLLER");
        let target = StrategyId::from("Failure-001");
        let instrument = InstrumentId::from("AUDUSD.SIM");
        let failing = StrategyFactory::new(
            "native:Unclean",
            "native:NativeStrategyConfig",
            |config: NativeStrategyConfig| {
                Ok(FailingRetirementStrategy {
                    core: StrategyCore::new(config.base),
                })
            },
        )
        .unwrap();
        let mut kernel = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig {
                actor_id: Some(controller_id),
                ..Default::default()
            })
            .with_strategy_factory(failing)
            .build()
            .unwrap();
        let controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let mut config =
            contextual_strategy_config("Failure-001", Some(OmsType::Hedging), vec![instrument]);
        config.strategy_path = "native:Unclean".into();
        assert!(
            controller
                .create_strategy_from_config(&config, true)
                .unwrap_err()
                .to_string()
                .contains("rollback failed")
        );
        assert_eq!(kernel.trader.borrow().strategy_ids(), vec![target]);
        assert_eq!(
            nautilus_common::component::component_state(&target.inner()).unwrap(),
            ComponentState::Faulted
        );
        assert_eq!(
            kernel.exec_engine.borrow().oms_type_override(target),
            Some(OmsType::Hedging)
        );
        assert_eq!(
            kernel.cache.borrow().external_order_claim(&instrument),
            Some(target)
        );
        controller.remove_strategy(&target).unwrap();
        assert_eq!(kernel.exec_engine.borrow().oms_type_override(target), None);
        assert_eq!(
            kernel.cache.borrow().external_order_claim(&instrument),
            None
        );
        drop(controller);
        kernel.dispose();
    }

    #[rstest]
    fn test_failed_registration_restores_prior_claims_and_releases_partial_clock() {
        let controller_id = ActorId::from("REGISTRATION-ROLLBACK-CONTROLLER");
        let target = StrategyId::from("Invalid-001");
        let prior = InstrumentId::from("AUDUSD.SIM");
        let added = InstrumentId::from("EURUSD.SIM");
        let invalid = StrategyFactory::new(
            "native:AlreadyInitialized",
            "native:NativeStrategyConfig",
            |config: NativeStrategyConfig| {
                let mut strategy = TestStrategy::new(config.base);
                strategy.initialize()?;
                Ok(strategy)
            },
        )
        .unwrap();
        let mut kernel = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig {
                actor_id: Some(controller_id),
                ..Default::default()
            })
            .with_strategy_factory(invalid)
            .build()
            .unwrap();
        kernel
            .cache
            .borrow_mut()
            .register_external_order_claims(target, &[prior])
            .unwrap();
        kernel
            .exec_engine
            .borrow_mut()
            .register_oms_type(target, OmsType::Netting);
        let clocks = kernel.trader.borrow().get_component_clocks().len();
        let controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let mut config =
            contextual_strategy_config("Invalid-001", Some(OmsType::Hedging), vec![added]);
        config.strategy_path = "native:AlreadyInitialized".into();
        assert!(
            controller
                .create_strategy_from_config(&config, false)
                .is_err()
        );
        assert!(kernel.trader.borrow().strategy_ids().is_empty());
        assert_eq!(kernel.trader.borrow().get_component_clocks().len(), clocks);
        assert_eq!(kernel.cache.borrow().external_order_claim(&added), None);
        assert_eq!(
            kernel.cache.borrow().external_order_claim(&prior),
            Some(target)
        );
        assert_eq!(
            kernel.exec_engine.borrow().oms_type_override(target),
            Some(OmsType::Netting)
        );
        assert!(try_get_actor_unchecked::<TestStrategy>(&target.inner()).is_none());
        drop(controller);
        kernel.dispose();
    }

    #[rstest]
    fn test_contextless_controller_rejects_explicit_oms_before_registration() {
        let (trader, controller_id) =
            create_running_controller_with_factories(vec![native_strategy_factory()]);
        let controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let config = contextual_strategy_config(
            "NoEngine-001",
            Some(OmsType::Hedging),
            vec!["AUDUSD.SIM".into()],
        );
        assert!(
            controller
                .create_strategy_from_config(&config, true)
                .unwrap_err()
                .to_string()
                .contains("execution engine is required")
        );
        assert!(trader.borrow().strategy_ids().is_empty());
        assert!(
            trader
                .borrow()
                .cache
                .borrow()
                .external_order_claim_instrument_ids(None)
                .is_empty()
        );
        drop(controller);
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_expected_native_strategy_identity_precedes_registration(#[case] start: bool) {
        let (trader, controller_id) =
            create_running_controller_with_factories(vec![native_strategy_factory()]);
        let clocks = trader.borrow().get_component_clocks().len();
        let actual = StrategyId::from("Actual-001");
        let mut controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let command = CreateStrategy::new(
            native_strategy_config("Actual-001", 10),
            start,
            UUID4::new(),
            UnixNanos::default(),
        )
        .with_expected_strategy_id("Expected-001".into());
        assert!(
            controller
                .execute_with_result(command.into())
                .unwrap_err()
                .to_string()
                .contains("identity mismatch")
        );
        assert!(trader.borrow().strategy_ids().is_empty());
        assert_eq!(trader.borrow().get_component_clocks().len(), clocks);
        assert!(try_get_actor_unchecked::<TestStrategy>(&actual.inner()).is_none());
        assert!(nautilus_common::component::get_component(&actual.inner()).is_none());
        let command = CreateStrategy::new(
            native_strategy_config("Actual-001", 10),
            false,
            UUID4::new(),
            UnixNanos::default(),
        )
        .with_expected_strategy_id(actual);
        assert_eq!(
            controller.execute_with_result(command.into()).unwrap(),
            ControllerCommandResult::Strategy(actual)
        );
        assert_eq!(
            try_get_actor_unchecked::<TestStrategy>(&actual.inner())
                .unwrap()
                .state(),
            ComponentState::Ready
        );
        assert_eq!(
            controller
                .execute_with_result(start_strategy_command(actual))
                .unwrap(),
            ControllerCommandResult::Strategy(actual)
        );
        drop(controller);
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_expected_identity_uses_native_normalization_without_consuming_a_tag() {
        let (trader, controller_id) =
            create_running_controller_with_factories(vec![native_strategy_factory()]);
        let mut config = native_strategy_config("Unused-999", 10);
        config.config.insert(
            "base".into(),
            serde_json::to_value(StrategyConfig::default()).unwrap(),
        );
        let mut controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let command = CreateStrategy::new(config.clone(), true, UUID4::new(), UnixNanos::default())
            .with_expected_strategy_id("Wrong-000".into());
        assert!(controller.execute_with_result(command.into()).is_err());
        assert!(trader.borrow().strategy_ids().is_empty());
        let actual = StrategyId::from("TestStrategy-000");
        let command = CreateStrategy::new(config, false, UUID4::new(), UnixNanos::default())
            .with_expected_strategy_id(actual);
        assert_eq!(
            controller.execute_with_result(command.into()).unwrap(),
            ControllerCommandResult::Strategy(actual)
        );
        assert_eq!(trader.borrow().strategy_ids(), vec![actual]);
        drop(controller);
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_native_kernel_starts_with_only_a_configured_controller() {
        let controller_id = ActorId::from("IDLE-NATIVE-CONTROLLER");
        let mut kernel = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig {
                actor_id: Some(controller_id),
                ..Default::default()
            })
            .with_strategy_factory(native_strategy_factory())
            .build()
            .unwrap();
        assert!(kernel.trader.borrow().strategy_ids().is_empty());
        assert_eq!(kernel.trader.borrow().actor_ids(), vec![controller_id]);
        kernel.start();
        kernel.start_trader().unwrap();
        assert!(
            try_get_actor_unchecked::<Controller>(&controller_id.inner())
                .unwrap()
                .is_running()
        );
        kernel.dispose();
    }

    #[rstest]
    fn test_native_kernel_constructs_configured_types_and_keeps_factories_for_runtime() {
        let controller_id = ActorId::from("NATIVE-KERNEL-CONTROLLER");
        let mut kernel = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig {
                actor_id: Some(controller_id),
                ..Default::default()
            })
            .with_strategy_factory(native_strategy_factory())
            .with_strategy_factory(named_strategy_factory())
            .with_strategies(vec![
                native_strategy_config("Kernel-001", 10),
                native_strategy_config("Kernel-002", 20),
                named_strategy_config("Peer-003"),
            ])
            .build()
            .unwrap();

        for id in ["Kernel-001", "Kernel-002", "Peer-003"] {
            assert_eq!(
                nautilus_common::component::component_state(&StrategyId::from(id).inner()).unwrap(),
                ComponentState::Ready
            );
        }
        assert_eq!(
            try_get_actor_unchecked::<TestStrategy>(&StrategyId::from("Kernel-002").inner())
                .unwrap()
                .interval_ms,
            Some(20)
        );
        kernel.start();
        kernel.start_trader().unwrap();
        assert!(
            try_get_actor_unchecked::<NamedStrategy>(&StrategyId::from("Peer-003").inner())
                .unwrap()
                .is_running()
        );
        let mut controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let actual = StrategyId::from("Runtime-004");
        let command = CreateStrategy::new(
            native_strategy_config("Runtime-004", 30),
            true,
            UUID4::new(),
            UnixNanos::default(),
        )
        .with_expected_strategy_id(actual);
        assert_eq!(
            controller.execute_with_result(command.into()).unwrap(),
            ControllerCommandResult::Strategy(actual)
        );
        assert!(
            try_get_actor_unchecked::<TestStrategy>(&actual.inner())
                .unwrap()
                .is_running()
        );
        assert!(
            controller
                .register_strategy_factory(native_strategy_factory())
                .is_err()
        );
        drop(controller);
        kernel.dispose();
    }

    #[rstest]
    #[case::unknown_path("unknown")]
    #[case::duplicate_tag("tag")]
    #[case::invalid_parameters("parameters")]
    fn test_native_kernel_failed_configuration_retires_staged_components(#[case] invalid: &str) {
        let controller_id = ActorId::from("INVALID-KERNEL-CONTROLLER");
        let mut second = native_strategy_config("Invalid-002", 20);
        match invalid {
            "unknown" => second.strategy_path = "unknown:Strategy".into(),
            "tag" => second = native_strategy_config("Other-001", 20),
            "parameters" => {
                second
                    .config
                    .insert("interval_ms".into(), serde_json::json!("bad"));
            }
            _ => unreachable!(),
        }
        let result = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig {
                actor_id: Some(controller_id),
                ..Default::default()
            })
            .with_strategy_factory(native_strategy_factory())
            .with_strategies(vec![native_strategy_config("Staged-001", 10), second])
            .build();
        assert!(result.is_err());
        assert!(nautilus_common::component::get_component(&controller_id.inner()).is_none());
        assert!(try_get_actor_unchecked::<Controller>(&controller_id.inner()).is_none());
        assert!(
            try_get_actor_unchecked::<TestStrategy>(&StrategyId::from("Staged-001").inner())
                .is_none()
        );
        assert!(
            nautilus_common::component::get_component(&StrategyId::from("Staged-001").inner())
                .is_none()
        );
    }

    #[rstest]
    #[cfg(feature = "python")]
    fn test_unknown_native_path_preserves_an_uninitialized_python_runtime() {
        const CHILD: &str = "NAUTILUS_NATIVE_FACTORY_NO_PYTHON_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "controller::tests::test_unknown_native_path_preserves_an_uninitialized_python_runtime",
                    "--exact",
                    "--test-threads=1",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        assert!(Python::try_attach(|_| ()).is_none());
        let controller_id = ActorId::from("NO-PYTHON-NATIVE-CONTROLLER");
        let mut unknown = native_strategy_config("Unknown-002", 10);
        unknown.strategy_path = "unknown:Strategy".into();
        let result = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig {
                actor_id: Some(controller_id),
                ..Default::default()
            })
            .with_strategy_factory(native_strategy_factory())
            .with_strategies(vec![native_strategy_config("Staged-001", 10), unknown])
            .build();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Python runtime is not available")
        );
        assert!(Python::try_attach(|_| ()).is_none());
        assert!(nautilus_common::component::get_component(&controller_id.inner()).is_none());
        assert!(
            nautilus_common::component::get_component(&StrategyId::from("Staged-001").inner())
                .is_none()
        );
        let mut kernel = crate::builder::NautilusKernelBuilder::default()
            .with_native_controller(DataActorConfig::default())
            .with_strategy_factory(native_strategy_factory())
            .with_strategies(vec![native_strategy_config("Native-001", 10)])
            .build()
            .unwrap();
        assert_eq!(
            kernel.trader.borrow().strategy_ids(),
            vec![StrategyId::from("Native-001")]
        );
        assert!(Python::try_attach(|_| ()).is_none());
        kernel.dispose();
    }

    #[rstest]
    fn test_native_kernel_requires_explicit_controller_for_factories_and_strategies() {
        assert!(
            crate::builder::NautilusKernelBuilder::default()
                .with_strategy_factory(native_strategy_factory())
                .build()
                .unwrap_err()
                .to_string()
                .contains("requires a native controller")
        );
        assert!(
            crate::builder::NautilusKernelBuilder::default()
                .with_strategies(vec![native_strategy_config("Missing-001", 10)])
                .build()
                .unwrap_err()
                .to_string()
                .contains("requires a native controller")
        );
        let mut kernel = crate::builder::NautilusKernelBuilder::default()
            .build()
            .unwrap();
        assert!(kernel.trader.borrow().actor_ids().is_empty());
        kernel.dispose();
    }

    #[rstest]
    fn test_native_strategy_factory_creates_multiple_types_and_instances_while_running() {
        let named_factory = StrategyFactory::new(
            "native:NamedStrategy",
            "native:NamedStrategyConfig",
            |config: NamedStrategyConfig| {
                Ok(NamedStrategy {
                    core: StrategyCore::new(config.base),
                    label: config.label,
                })
            },
        )
        .unwrap();
        let (trader, controller_id) = create_running_controller_with_factories(vec![
            native_strategy_factory(),
            named_factory,
        ]);
        let first = StrategyId::from("Native-001");
        let second = StrategyId::from("Native-002");
        let peer = StrategyId::from("Named-003");

        {
            let mut controller =
                try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
            assert_eq!(
                controller
                    .create_strategy_from_config(&native_strategy_config("Native-001", 10), true)
                    .unwrap(),
                first
            );
            controller
                .execute(
                    CreateStrategy::new(
                        native_strategy_config("Native-002", 20),
                        true,
                        UUID4::new(),
                        UnixNanos::default(),
                    )
                    .into(),
                )
                .unwrap();
            let config = ImportableStrategyConfig {
                strategy_path: "native:NamedStrategy".to_string(),
                config_path: "native:NamedStrategyConfig".to_string(),
                config: HashMap::from([
                    (
                        "base".to_string(),
                        serde_json::to_value(StrategyConfig {
                            strategy_id: Some(peer),
                            ..Default::default()
                        })
                        .unwrap(),
                    ),
                    ("label".to_string(), serde_json::json!("unrelated")),
                ]),
            };
            assert_eq!(
                controller
                    .create_strategy_from_config(&config, true)
                    .unwrap(),
                peer
            );
            assert_eq!(trader.borrow().strategy_ids(), vec![first, second, peer]);
            assert_eq!(
                try_get_actor_unchecked::<TestStrategy>(&first.inner())
                    .unwrap()
                    .interval_ms,
                Some(10)
            );
            assert_eq!(
                try_get_actor_unchecked::<TestStrategy>(&second.inner())
                    .unwrap()
                    .interval_ms,
                Some(20)
            );
            assert!(
                try_get_actor_unchecked::<TestStrategy>(&first.inner())
                    .unwrap()
                    .is_running()
            );
            assert!(
                try_get_actor_unchecked::<NamedStrategy>(&peer.inner())
                    .unwrap()
                    .is_running()
            );
            controller.stop_strategy(&first).unwrap();
            controller.remove_strategy(&first).unwrap();
            assert!(
                try_get_actor_unchecked::<NamedStrategy>(&peer.inner())
                    .unwrap()
                    .is_running()
            );
            assert_eq!(
                controller
                    .create_strategy_from_config(&native_strategy_config("Native-004", 30), true)
                    .unwrap(),
                StrategyId::from("Native-004")
            );
        }

        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_native_factory_rejects_unexposed_configured_claims_without_changing_legacy_getter() {
        let (trader, controller_id) =
            create_running_controller_with_factories(vec![named_strategy_factory()]);
        let id = StrategyId::from("Named-001");
        let mut config = named_strategy_config("Named-001");
        config.config.insert(
            "base".into(),
            serde_json::to_value(StrategyConfig {
                strategy_id: Some(id),
                external_order_instrument_ids: Some(vec!["AUDUSD.SIM".into()]),
                ..Default::default()
            })
            .unwrap(),
        );
        let clocks = trader.borrow().get_component_clocks().len();
        let controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        assert!(
            controller
                .create_strategy_from_config(&config, false)
                .unwrap_err()
                .to_string()
                .contains("does not expose configured external order claims")
        );
        assert!(trader.borrow().strategy_ids().is_empty());
        assert_eq!(trader.borrow().get_component_clocks().len(), clocks);
        assert!(
            trader
                .borrow()
                .cache
                .borrow()
                .external_order_claim_instrument_ids(None)
                .is_empty()
        );
        let strategy = NamedStrategy {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(id),
                external_order_instrument_ids: Some(vec!["AUDUSD.SIM".into()]),
                ..Default::default()
            }),
            label: "unrelated".into(),
        };
        assert!(strategy.external_order_instrument_ids().is_none());
        assert_eq!(controller.create_strategy(strategy, false).unwrap(), id);
        assert!(
            trader
                .borrow()
                .cache
                .borrow()
                .external_order_claim_instrument_ids(None)
                .is_empty()
        );
        drop(controller);
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_native_strategy_factory_rejects_invalid_input_without_native_registration() {
        let (trader, controller_id) =
            create_running_controller_with_factories(vec![native_strategy_factory()]);
        let clocks_before = trader.borrow().get_component_clocks().len();
        let controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        let mut wrong_path = native_strategy_config("Invalid-001", 10);
        wrong_path.config_path = "native:OtherConfig".to_string();
        assert!(
            controller
                .create_strategy_from_config(&wrong_path, true)
                .unwrap_err()
                .to_string()
                .contains("requires config path")
        );
        let mut malformed = native_strategy_config("Invalid-001", 10);
        malformed
            .config
            .insert("interval_ms".to_string(), serde_json::json!("invalid"));
        assert!(
            controller
                .create_strategy_from_config(&malformed, true)
                .unwrap_err()
                .to_string()
                .contains("Invalid native strategy configuration")
        );
        assert!(
            controller
                .create_strategy_from_config(&native_strategy_config("Invalid-001", 0), true)
                .unwrap_err()
                .to_string()
                .contains("Invalid native fixture interval")
        );
        assert!(trader.borrow().strategy_ids().is_empty());
        assert_eq!(trader.borrow().get_component_clocks().len(), clocks_before);
        drop(controller);
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_native_strategy_factory_preserves_native_tag_checks_and_start_rollback() {
        let failing_factory = StrategyFactory::new(
            "native:FailingStrategy",
            "native:NativeStrategyConfig",
            |config: NativeStrategyConfig| Ok(FailingStartStrategy::new(config.base)),
        )
        .unwrap();
        let (trader, controller_id) = create_running_controller_with_factories(vec![
            native_strategy_factory(),
            failing_factory,
        ]);
        let controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        controller
            .create_strategy_from_config(&native_strategy_config("Native-001", 10), true)
            .unwrap();
        let clocks_before = trader.borrow().get_component_clocks().len();
        assert!(
            controller
                .create_strategy_from_config(&native_strategy_config("Other-001", 20), true)
                .unwrap_err()
                .to_string()
                .contains("order_id_tag conflict")
        );
        let mut failing = native_strategy_config("Failure-002", 10);
        failing.strategy_path = "native:FailingStrategy".to_string();
        assert!(
            controller
                .create_strategy_from_config(&failing, true)
                .unwrap_err()
                .to_string()
                .contains("Simulated strategy start failure")
        );
        assert_eq!(
            trader.borrow().strategy_ids(),
            vec![StrategyId::from("Native-001")]
        );
        assert_eq!(trader.borrow().get_component_clocks().len(), clocks_before);
        drop(controller);
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_native_strategy_factory_registration_is_fixed_before_activation() {
        let (trader, controller_id) =
            create_running_controller_with_factories(vec![native_strategy_factory()]);
        let mut controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        assert!(
            controller
                .register_strategy_factory(native_strategy_factory())
                .unwrap_err()
                .to_string()
                .contains("after initial controller activation")
        );
        drop(controller);
        trader.borrow_mut().stop().unwrap();
        let mut controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        assert!(
            controller
                .register_strategy_factory(native_strategy_factory())
                .unwrap_err()
                .to_string()
                .contains("after initial controller activation")
        );
        controller.reset().unwrap();
        assert_eq!(controller.state(), ComponentState::Ready);
        assert!(
            controller
                .register_strategy_factory(native_strategy_factory())
                .unwrap_err()
                .to_string()
                .contains("after initial controller activation")
        );
        drop(controller);
        let mut unstarted = Controller::new(trader.clone(), None);
        unstarted
            .register_strategy_factory(native_strategy_factory())
            .unwrap();
        assert!(
            unstarted
                .register_strategy_factory(native_strategy_factory())
                .unwrap_err()
                .to_string()
                .contains("already registered")
        );
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_execute_with_result_returns_native_actor_identity() {
        let (trader, controller_id) = create_running_controller();
        let actor_id = ActorId::from("ResultActor-001");
        let mut controller = try_get_actor_unchecked::<Controller>(&controller_id.inner()).unwrap();
        controller
            .create_actor(
                TestDataActor::new(DataActorConfig {
                    actor_id: Some(actor_id),
                    ..Default::default()
                }),
                false,
            )
            .unwrap();

        for command in [
            start_actor_command(actor_id),
            stop_actor_command(actor_id),
            remove_actor_command(actor_id),
        ] {
            assert_eq!(
                controller.execute_with_result(command).unwrap(),
                ControllerCommandResult::Actor(actor_id)
            );
        }
        assert!(!trader.borrow().actor_ids().contains(&actor_id));
        drop(controller);
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_controller_manages_actor_lifecycle_by_message() {
        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();

        let actor_id = {
            let controller = try_get_actor_unchecked::<Controller>(&controller_actor_id).unwrap();
            controller
                .create_actor(
                    TestDataActor::new(DataActorConfig {
                        actor_id: Some(ActorId::from("TestActor-001")),
                        ..Default::default()
                    }),
                    false,
                )
                .unwrap()
        };

        assert!(trader.borrow().actor_ids().contains(&actor_id));

        Controller::send(&start_actor_command(actor_id)).unwrap();
        let actor_registry_id = actor_id.inner();
        assert_eq!(
            try_get_actor_unchecked::<TestDataActor>(&actor_registry_id)
                .unwrap()
                .state(),
            ComponentState::Running
        );

        Controller::send(&stop_actor_command(actor_id)).unwrap();
        assert_eq!(
            try_get_actor_unchecked::<TestDataActor>(&actor_registry_id)
                .unwrap()
                .state(),
            ComponentState::Stopped
        );

        Controller::send(&remove_actor_command(actor_id)).unwrap();
        assert!(!trader.borrow().actor_ids().contains(&actor_id));

        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_controller_manages_strategy_lifecycle_and_exit_market() {
        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();

        let strategy_id = {
            let controller = try_get_actor_unchecked::<Controller>(&controller_actor_id).unwrap();
            controller
                .create_strategy(
                    TestStrategy::new(StrategyConfig {
                        strategy_id: Some(StrategyId::from("TestStrategy-001")),
                        order_id_tag: Some("001".to_string()),
                        ..Default::default()
                    }),
                    false,
                )
                .unwrap()
        };

        assert!(trader.borrow().strategy_ids().contains(&strategy_id));

        Controller::send(&start_strategy_command(strategy_id)).unwrap();
        let strategy_registry_id = strategy_id.inner();
        assert_eq!(
            try_get_actor_unchecked::<TestStrategy>(&strategy_registry_id)
                .unwrap()
                .state(),
            ComponentState::Running
        );

        Controller::send(&ControllerCommand::ExitMarket(strategy_id)).unwrap();
        assert!(
            try_get_actor_unchecked::<TestStrategy>(&strategy_registry_id)
                .unwrap()
                .is_exiting()
        );

        Controller::send(&stop_strategy_command(strategy_id)).unwrap();
        let strategy = try_get_actor_unchecked::<TestStrategy>(&strategy_registry_id).unwrap();
        assert_eq!(strategy.state(), ComponentState::Stopped);
        assert!(!strategy.is_exiting());
        drop(strategy);

        Controller::send(&remove_strategy_command(strategy_id)).unwrap();
        assert!(!trader.borrow().strategy_ids().contains(&strategy_id));

        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_controller_create_actor_rolls_back_on_start_failure() {
        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();
        let actor_id = ActorId::from("FailingActor-001");

        let result = {
            let controller = try_get_actor_unchecked::<Controller>(&controller_actor_id).unwrap();
            controller.create_actor(
                FailingStartActor::new(DataActorConfig {
                    actor_id: Some(actor_id),
                    ..Default::default()
                }),
                true,
            )
        };

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Simulated actor start failure")
        );
        assert!(!trader.borrow().actor_ids().contains(&actor_id));
        if let Some(actor) = try_get_actor_unchecked::<FailingStartActor>(&actor_id.inner()) {
            assert_eq!(actor.state(), ComponentState::Disposed);
        }

        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_controller_create_strategy_rolls_back_on_start_failure() {
        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();
        let strategy_id = StrategyId::from("FailingStrategy-001");

        let result = {
            let controller = try_get_actor_unchecked::<Controller>(&controller_actor_id).unwrap();
            controller.create_strategy(
                FailingStartStrategy::new(StrategyConfig {
                    strategy_id: Some(strategy_id),
                    order_id_tag: Some("001".to_string()),
                    ..Default::default()
                }),
                true,
            )
        };

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Simulated strategy start failure")
        );
        assert!(!trader.borrow().strategy_ids().contains(&strategy_id));

        if let Some(strategy) =
            try_get_actor_unchecked::<FailingStartStrategy>(&strategy_id.inner())
        {
            assert_eq!(strategy.state(), ComponentState::Disposed);
        }

        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_controller_exit_market_allows_reentrant_controller_commands() {
        let (trader, controller_id) = create_running_controller();
        let controller_actor_id = controller_id.inner();

        let target_actor_id = {
            let controller = try_get_actor_unchecked::<Controller>(&controller_actor_id).unwrap();
            controller
                .create_actor(
                    TestDataActor::new(DataActorConfig {
                        actor_id: Some(ActorId::from("TargetActor-001")),
                        ..Default::default()
                    }),
                    true,
                )
                .unwrap()
        };

        let strategy_id = {
            let controller = try_get_actor_unchecked::<Controller>(&controller_actor_id).unwrap();
            controller
                .create_strategy(
                    ReentrantExitStrategy::new(
                        StrategyConfig {
                            strategy_id: Some(StrategyId::from("ReentrantStrategy-001")),
                            order_id_tag: Some("001".to_string()),
                            ..Default::default()
                        },
                        target_actor_id,
                    ),
                    false,
                )
                .unwrap()
        };

        Controller::send(&start_strategy_command(strategy_id)).unwrap();
        Controller::send(&ControllerCommand::ExitMarket(strategy_id)).unwrap();

        let target_actor =
            try_get_actor_unchecked::<TestDataActor>(&target_actor_id.inner()).unwrap();
        assert_eq!(target_actor.state(), ComponentState::Stopped);
        drop(target_actor);
        assert!(
            try_get_actor_unchecked::<ReentrantExitStrategy>(&strategy_id.inner())
                .unwrap()
                .is_exiting()
        );

        Controller::send(&stop_strategy_command(strategy_id)).unwrap();
        Controller::send(&remove_strategy_command(strategy_id)).unwrap();
        Controller::send(&remove_actor_command(target_actor_id)).unwrap();
        trader.borrow_mut().stop().unwrap();
        trader.borrow_mut().dispose_components().unwrap();
    }

    #[rstest]
    fn test_controller_send_fails_after_controller_stop() {
        let (trader, _) = create_running_controller();

        trader.borrow_mut().stop().unwrap();

        let result = Controller::send(&stop_actor_command(ActorId::from("AnyActor-001")));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Controller execute endpoint 'Controller.execute' not registered"
        );

        trader.borrow_mut().dispose_components().unwrap();
    }
}
