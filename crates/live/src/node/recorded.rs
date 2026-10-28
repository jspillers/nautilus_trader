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

//! Recorded native runner execution for complete cold-genesis PAPER inputs.

pub(super) mod maintenance;

use std::{cell::RefCell, fmt::Debug, rc::Rc};

use nautilus_common::{
    actor::{self, registry::actor_registration},
    cache::bootstrap::BootstrapLoadWitness,
    clock::{Clock, VirtualClock},
    enums::Environment,
    messages::{DataEvent, ExecutionEvent},
    msgbus::{self, BusMessage},
    runner::{
        TimeEventMessage,
        recorded::{self, RecordedChannel, RecordedRunnerDispatch, RuntimeDispatchScope},
    },
    timer::RecordedTimeEventDispatch,
};
use nautilus_core::UnixNanos;
use nautilus_model::{data::Data, identifiers::ActorId};
use nautilus_system::trader::{ComponentStateLimits, RegisteredComponentState};

use super::{LiveNode, NodeState};
use crate::runner::{AsyncRunner, PendingRunnerEvent};

/// Original startup witnesses and explicit nonfinancial source-only exclusions.
#[derive(Clone, Debug, Default)]
pub struct RecordedRunnerConfig {
    /// Original consumed native dispatches through the Running transition.
    pub startup_dispatches: Vec<RecordedRunnerDispatch>,
    /// Recreated nonfinancial input producers or measurement observers.
    pub excluded_actors: Vec<ActorId>,
}
/// One original observation at the native owner-thread dispatch boundary.
#[derive(Debug)]
pub struct RecordedRunnerInput {
    /// Original event-store sequence; strictly increasing within this run.
    pub sequence: u64,
    /// Original owner-thread observation, separate from payload timestamps.
    pub observed_at: UnixNanos,
    /// Expected native queue channel, ordinal and semantic front.
    pub witness: RecordedRunnerDispatch,
    /// Original input or a request to consume recomputed native work.
    pub kind: RecordedRunnerInputKind,
}
/// Inputs retain original payload timestamps; queued financial events are never inputs.
#[derive(Debug)]
pub enum RecordedRunnerInputKind {
    /// Original normalized `DataEngine` input.
    Data(Data),
    /// Original native external bus input, decoded by the ordinary live-node handler.
    External(BusMessage),
    /// Original callback lease dispatched by the native clock.
    Timer(RecordedTimeEventDispatch),
    /// Consume an already generated native queue front.
    Dispatch,
    /// Drain the standard bounded native callback queue.
    Callbacks,
    /// Execute original native due decisions or a recomputed report completion.
    Maintenance,
    /// Retain an inventoried nonfinancial source-only callback without executing it.
    ExcludedSourceTimer(RecordedTimeEventDispatch),
}
#[derive(Debug)]
pub(super) struct RecordedRunnerState {
    config: RecordedRunnerConfig,
    last_sequence: Option<u64>,
    observed_at: UnixNanos,
    callbacks_pending: bool,
    failed: bool,
    reports: maintenance::ReportTasks,
}

impl LiveNode {
    /// Restores an original bounded component callback payload before its first start.
    ///
    /// This is an owner-thread operation between recorded inputs, after native creation
    /// registered the target at Ready and before an original Start/probe input. The opened
    /// native recorded origin and cold runtime guards remain mandatory. The fresh native
    /// registration UUID is preserved. A rejected or failed restore fails this offline run.
    ///
    /// # Errors
    ///
    /// Returns an error for an absent/failed recorded runtime, unsupported origin, changed
    /// registration or lifecycle, invalid bounds or an actual standard callback failure.
    pub fn restore_recorded_component_state(
        &mut self,
        state: RegisteredComponentState,
        limits: ComponentStateLimits,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state() == NodeState::Running
                && self.recorded.as_ref().is_some_and(|state| !state.failed),
            "Component state restore requires a healthy active recorded node"
        );
        let result = self.kernel.restore_recorded_component_state(state, limits);
        if result.is_err()
            && let Some(state) = self.recorded.as_mut()
        {
            state.failed = true;
        }
        result
    }

    /// Enables native queue dispatch witnesses before startup without changing scheduling.
    ///
    /// # Errors
    ///
    /// Returns an error after node startup or if a recorded runner already owns the node.
    pub fn enable_runtime_dispatch_capture(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state() == NodeState::Idle && self.recorded.is_none(),
            "Runtime capture must be selected before startup"
        );
        recorded::enable_capture();
        Ok(())
    }

    /// Enables bounded evidence of actual native bootstrap reads before startup.
    ///
    /// Registered components must already be installed. This diagnostic issues no database
    /// reads and never changes loading, callbacks or persistence.
    ///
    /// # Errors
    ///
    /// Returns an error after startup or when recorded execution owns the node.
    pub fn enable_bootstrap_load_capture(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state() == NodeState::Idle && self.recorded.is_none(),
            "Bootstrap capture must be selected before startup"
        );
        let trader = self.kernel.trader.borrow();
        self.kernel.cache.borrow().begin_bootstrap_load_capture(
            trader.actor_ids().iter().map(ToString::to_string).collect(),
            trader
                .strategy_ids()
                .iter()
                .map(ToString::to_string)
                .collect(),
        );
        Ok(())
    }

    fn recorded_started_description(&self) -> RecordedRunnerDispatch {
        let mut clients = self.kernel.exec_engine.borrow().client_ids();
        clients.sort();
        recorded::describe_started(serde_json::json!({
            "phase":"running","native_instance_id":self.instance_id(),"trader_id":self.config.trader_id,
            "actors":self.kernel.trader.borrow().actor_ids(),"strategies":self.kernel.trader.borrow().strategy_ids(),
            "execution_clients":clients,
        }))
    }

    pub(super) fn capture_recorded_started(&self) {
        let _scope = RuntimeDispatchScope::enter(|| self.recorded_started_description());

        if let Some(loads) = self.kernel.cache.borrow().finish_bootstrap_load_capture() {
            let mut clients = self.kernel.exec_engine.borrow().client_ids();
            clients.sort();
            let witness = BootstrapLoadWitness {
                native_instance_id: self.instance_id(),
                trader_id: self.config.trader_id,
                observed_at: self.kernel.clock.borrow().timestamp_ns(),
                completed: loads.is_complete(),
                cache_backing: self.kernel.cache.borrow().has_backing(),
                load_cache: self.config.exec_engine.load_cache,
                load_state: self.config.load_state,
                save_state: self.config.save_state,
                flush_on_start: self
                    .config
                    .cache
                    .as_ref()
                    .is_some_and(|config| config.flush_on_start),
                parent_run_id: self
                    .kernel
                    .event_store()
                    .and_then(|store| store.parent_run_id())
                    .map(str::to_owned),
                actor_ids: self
                    .kernel
                    .trader
                    .borrow()
                    .actor_ids()
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                strategy_ids: self
                    .kernel
                    .trader
                    .borrow()
                    .strategy_ids()
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                execution_client_ids: clients.iter().map(ToString::to_string).collect(),
                loads,
            };
            msgbus::publish_any("events.runtime.bootstrap_load".into(), &witness);
        }
    }

    fn recorded_clocks(&self) -> anyhow::Result<Vec<Rc<RefCell<dyn Clock>>>> {
        let mut clocks = vec![self.kernel.clock.clone()];
        clocks.extend(self.kernel.trader.borrow().get_component_clocks());
        let engine = self.kernel.exec_engine.borrow();
        for client in engine.get_all_clients() {
            let clock = client.recorded_clock().ok_or_else(|| {
                anyhow::anyhow!("Execution client does not support recorded native clocks")
            })?;
            let lease = clock.borrow().execution_registration().ok_or_else(|| {
                anyhow::anyhow!("Execution client clock has no native registration")
            })?;
            anyhow::ensure!(
                lease.is_active()
                    && engine.client_clock_registration(client.client_id())
                        == Some(lease.registration()),
                "Execution client clock registration changed"
            );
            clocks.push(clock);
        }
        anyhow::ensure!(
            !engine.get_all_clients().is_empty(),
            "Recorded PAPER execution requires a supported execution client"
        );
        anyhow::ensure!(
            clocks
                .iter()
                .all(|clock| clock.borrow().as_any().is::<VirtualClock>()),
            "Recorded execution requires exclusively native VirtualClocks"
        );
        Ok(clocks)
    }

    fn set_recorded_time(&self, time: UnixNanos) -> anyhow::Result<()> {
        for clock in self.recorded_clocks()? {
            clock
                .borrow()
                .as_any()
                .downcast_ref::<VirtualClock>()
                .expect("validated VirtualClock")
                .set_time(time);
        }
        Ok(())
    }

    /// Starts standard native PAPER startup under explicit recorded `VirtualClock` execution.
    ///
    /// Original startup dispatches must end at the native Running boundary. Strategies,
    /// accounts, risk, reconciliation and Sandbox startup use the ordinary native path.
    /// No background clock firing or live external bus ingress is permitted.
    ///
    /// # Errors
    ///
    /// Returns an error for a nonvirgin node, unsupported client/clock, restored cache,
    /// missing startup witnesses, source mismatch, or native startup failure.
    ///
    /// # Panics
    ///
    /// Panics if the validated startup witness or installed recorded state is lost during
    /// native startup, violating the node's internal ownership invariant.
    pub async fn begin_recorded(
        &mut self,
        start: UnixNanos,
        config: RecordedRunnerConfig,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.recorded.is_none() && self.state() == NodeState::Idle && self.runner.is_some(),
            "Recorded execution requires a virgin native node"
        );
        anyhow::ensure!(
            self.config.environment == Environment::Sandbox && !self.config.risk_engine.bypass,
            "Recorded execution requires PAPER and enabled native risk"
        );
        anyhow::ensure!(
            !self.config.load_state
                && self.cache_database_factory.is_none()
                && !self.kernel.cache.borrow().has_backing()
                && !self.kernel.is_event_store_replay_configured(),
            "Recorded execution requires cold genesis without cache restoration"
        );
        anyhow::ensure!(
            self.external_msgbus.is_none(),
            "Recorded execution cannot consume a live external message bus"
        );

        anyhow::ensure!(
            !msgbus::get_message_bus().borrow().has_backing,
            "Recorded execution cannot publish to an external message bus"
        );
        {
            let cache = self.kernel.cache.borrow();
            anyhow::ensure!(
                cache.accounts_all_owned().is_empty()
                    && cache.orders(None, None, None, None, None).is_empty()
                    && cache.positions(None, None, None, None, None).is_empty(),
                "Recorded execution requires an empty native financial cache"
            );
        }

        for client in self.kernel.data_engine.borrow().get_clients() {
            anyhow::ensure!(
                client.supports_recorded_inputs(),
                "Data client {} does not support exclusively recorded native inputs",
                client.client_id
            );
        }
        anyhow::ensure!(
            config
                .startup_dispatches
                .last()
                .is_some_and(
                    |witness| witness.channel == RecordedChannel::Lifecycle && witness.supported
                ),
            "Original startup must include the native Running boundary"
        );
        let expected_running = config
            .startup_dispatches
            .last()
            .expect("validated Running boundary");
        let mut actual_running = self.recorded_started_description();
        actual_running.ordinal = 1;
        anyhow::ensure!(
            *expected_running == actual_running,
            "Recorded startup component inventory differs before connection"
        );
        self.set_recorded_time(start)?;
        recorded::begin_startup_validation(config.startup_dispatches.clone());
        self.recorded = Some(RecordedRunnerState {
            config,
            last_sequence: None,
            observed_at: start,
            callbacks_pending: false,
            failed: false,
            reports: maintenance::ReportTasks::default(),
        });
        let startup = self
            .start()
            .await
            .and_then(|()| recorded::finish_startup_validation());
        if let Err(e) = startup {
            self.recorded.as_mut().expect("recorded state").failed = true;
            recorded::disable_capture();
            return Err(e);
        }
        anyhow::ensure!(
            self.state() == NodeState::Running,
            "Recorded native startup did not enter Running"
        );
        Ok(())
    }

    /// Processes one witnessed native input without advancing other timers or queue fronts.
    ///
    /// Source financial outputs are comparison-only. Dispatch consumes the already generated
    /// native FIFO front after semantic and ordinal correlation. A source-only producer timer
    /// can be excluded only by the explicit inventory, never for a registered Strategy/client.
    /// An admitted failure permanently fails this offline run.
    ///
    /// # Errors
    ///
    /// Returns an error on stale ordering, unsupported forms, missing/replaced callback leases,
    /// a mismatched native queue front, or a native callback/financial failure.
    ///
    /// # Panics
    ///
    /// Panics if admitted recorded state or its native runner disappears during synchronous
    /// owner-thread processing, violating the node's internal ownership invariant.
    pub fn process_recorded(&mut self, input: RecordedRunnerInput) -> anyhow::Result<()> {
        let state = self
            .recorded
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Recorded runner is not active"))?;
        anyhow::ensure!(
            !state.failed && self.state() == NodeState::Running && !self.handle.should_stop(),
            "Recorded runner is failed or stopping"
        );
        anyhow::ensure!(
            state.last_sequence.is_none_or(|last| input.sequence > last)
                && input.observed_at >= state.observed_at,
            "Recorded source ordering is not monotonic"
        );
        anyhow::ensure!(
            input.witness.supported
                && input.witness.ordinal == recorded::next_ordinal(input.witness.channel),
            "Recorded dispatch ordinal or capability is unsupported"
        );
        let sequence = input.sequence;
        let observed_at = input.observed_at;
        if let Err(e) = self.process_recorded_impl(input) {
            self.recorded.as_mut().expect("recorded state").failed = true;
            return Err(e);
        }
        let state = self.recorded.as_mut().expect("recorded state");
        state.last_sequence = Some(sequence);
        state.observed_at = observed_at;
        Ok(())
    }

    fn require_recorded_front(
        expected: &RecordedRunnerDispatch,
        mut actual: RecordedRunnerDispatch,
    ) -> anyhow::Result<()> {
        actual.ordinal = recorded::next_ordinal(actual.channel);
        anyhow::ensure!(
            *expected == actual,
            "Recorded native queue front differs: expected {expected:?}; actual {actual:?}"
        );
        Ok(())
    }

    fn process_recorded_impl(&mut self, input: RecordedRunnerInput) -> anyhow::Result<()> {
        self.set_recorded_time(input.observed_at)?;
        match input.kind {
            RecordedRunnerInputKind::External(message) => {
                Self::require_recorded_front(
                    &input.witness,
                    recorded::describe_external(&message),
                )?;
                self.process_external_msgbus_message(&message);
            }
            RecordedRunnerInputKind::Data(data) => {
                anyhow::ensure!(
                    self.runner
                        .as_ref()
                        .expect("recorded runner")
                        .recorded_external_empty(),
                    "Unexpected live input is queued during recorded execution"
                );
                let event = DataEvent::Data(data);
                Self::require_recorded_front(
                    &input.witness,
                    recorded::describe_data_event(&event),
                )?;
                AsyncRunner::dispatch_data_event(event.into());
            }
            RecordedRunnerInputKind::Timer(dispatch) => {
                // Native time work can precede data generated by an earlier recorded input,
                // that data remains queued until its own exact witnessed FIFO dispatch.
                anyhow::ensure!(
                    self.runner
                        .as_ref()
                        .expect("recorded runner")
                        .recorded_time_empty(),
                    "Unexpected live timer is queued during recorded timer execution"
                );

                // Scheduled deadlines are payload facts, not prior native clock observations
                anyhow::ensure!(
                    dispatch.has_current_native_owner()
                        && dispatch.event.ts_init <= input.observed_at,
                    "Source callback lease or dispatch clock is invalid"
                );
                let clocks = self.recorded_clocks()?;
                let clock = clocks
                    .into_iter()
                    .find(|clock| {
                        let clock = clock.borrow();
                        if let Some(source) = dispatch.binding.owner {
                            clock.actor_registration().is_some_and(|runtime| {
                                runtime.actor_id == source.actor_id
                                    && runtime.generation == source.generation
                            })
                        } else {
                            dispatch
                                .binding
                                .execution_owner
                                .zip(clock.execution_registration())
                                .is_some_and(|(source, runtime)| {
                                    source.client_id == runtime.registration().client_id
                                        && source.generation == runtime.registration().generation
                                        && runtime.is_active()
                                })
                        }
                    })
                    .ok_or_else(|| anyhow::anyhow!("Recorded native callback clock is absent"))?;
                Self::require_recorded_front(
                    &input.witness,
                    RecordedRunnerDispatch {
                        channel: RecordedChannel::TimeEvents,
                        ordinal: 0,
                        semantic: serde_json::json!({"name":dispatch.event.name}),
                        supported: true,
                    },
                )?;
                let handler = clock.borrow_mut().recorded_time_event(&dispatch)?;
                let message = TimeEventMessage::new(handler.event, handler.callback);
                anyhow::ensure!(
                    AsyncRunner::handle_time_event(message.into()),
                    "Native callback lease was not dispatched"
                );
            }
            RecordedRunnerInputKind::Dispatch => {
                let event = self
                    .runner
                    .as_mut()
                    .expect("recorded runner")
                    .take_recorded(&input.witness)?;

                match event {
                    PendingRunnerEvent::ExecEvent(event) => {
                        let eligible = self
                            .runner
                            .as_ref()
                            .expect("recorded runner")
                            .recorded_execution_empty()
                            && !self
                                .recorded
                                .as_ref()
                                .expect("recorded state")
                                .callbacks_pending
                            && self
                                .recorded
                                .as_ref()
                                .expect("recorded state")
                                .reports
                                .is_empty();
                        event.dispatch(|event| {
                            if let ExecutionEvent::StrategySettlement(marker) = event {
                                self.finish_strategy_settlement(marker, eligible);
                            } else {
                                self.process_exec_event(event);
                            }
                        });
                    }
                    event => self.process_runner_event(event),
                }
            }
            RecordedRunnerInputKind::Callbacks => {
                Self::require_recorded_front(&input.witness, recorded::describe_callbacks(64))?;
                let pending = {
                    let _scope = RuntimeDispatchScope::enter(|| recorded::describe_callbacks(64));
                    actor::drain_callbacks(64)?
                };
                self.recorded
                    .as_mut()
                    .expect("recorded state")
                    .callbacks_pending = pending;
            }
            RecordedRunnerInputKind::Maintenance => {
                self.process_recorded_maintenance(&input.witness)?;
            }
            RecordedRunnerInputKind::ExcludedSourceTimer(dispatch) => {
                anyhow::ensure!(
                    input.witness.channel == RecordedChannel::TimeEvents
                        && dispatch.has_current_native_owner()
                        && dispatch.binding.execution_owner.is_none(),
                    "Only declared source Actor timers can be excluded"
                );
                let owner = dispatch
                    .binding
                    .owner
                    .ok_or_else(|| anyhow::anyhow!("Source-only Actor owner is absent"))?;
                let id = ActorId::from(owner.actor_id.as_str());
                anyhow::ensure!(
                    self.recorded
                        .as_ref()
                        .expect("recorded state")
                        .config
                        .excluded_actors
                        .contains(&id)
                        && self.kernel.trader.borrow().actor_ids().contains(&id)
                        && !self
                            .kernel
                            .trader
                            .borrow()
                            .strategy_ids()
                            .iter()
                            .any(|strategy| strategy.inner() == owner.actor_id),
                    "Source-only Actor is outside the declared nonfinancial inventory"
                );
                anyhow::ensure!(
                    self.config
                        .native_controller
                        .as_ref()
                        .and_then(|controller| controller.actor_id)
                        .is_none_or(|id| id.inner() != owner.actor_id),
                    "Native Controller cannot be excluded"
                );
                anyhow::ensure!(
                    actor_registration(&owner.actor_id)
                        .is_some_and(|runtime| runtime.generation == owner.generation),
                    "Source-only Actor registration changed"
                );
                Self::require_recorded_front(
                    &input.witness,
                    RecordedRunnerDispatch {
                        channel: RecordedChannel::TimeEvents,
                        ordinal: 0,
                        semantic: serde_json::json!({"name":dispatch.event.name}),
                        supported: true,
                    },
                )?;
                recorded::observe_excluded(&input.witness)?;
            }
        }

        if let Some(e) = actor::callback_failure() {
            return Err(e.into());
        }
        Ok(())
    }

    /// Ends the offline run using native shutdown at its last supplied clock boundary.
    ///
    /// Read H0/H1 state before teardown: shutdown hooks may emit their normal residual work.
    /// No virtual clock is advanced and no background timer is introduced.
    ///
    /// # Errors
    ///
    /// Returns an error if no recorded run is active or native shutdown fails.
    pub async fn end_recorded(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(self.recorded.is_some(), "Recorded runner is not active");
        recorded::disable_capture();
        let result = if self.state() == NodeState::Running {
            self.stop().await
        } else {
            Ok(())
        };
        self.recorded = None;
        result
    }
}
