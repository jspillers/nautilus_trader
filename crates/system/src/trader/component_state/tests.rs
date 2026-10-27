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

use std::cell::Cell;

use nautilus_common::{
    actor::{
        DataActor, DataActorConfig, DataActorCore, DataActorNative,
        registry::try_get_actor_unchecked,
    },
    clock::VirtualClock,
    component::{register_component_actor, reset_component, start_component, stop_component},
    enums::Environment,
    nautilus_actor,
};
use nautilus_model::{
    enums::OrderType,
    identifiers::{ActorId, StrategyId},
    orders::OrderTestBuilder,
    types::Quantity,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;

use super::*;
use crate::{NautilusKernel, NautilusKernelBuilder};

const ACTOR: &str = "STATE-ACTOR";
const STRATEGY: &str = "State-001";

#[derive(Clone, Copy, Debug, Default)]
enum HookBehavior {
    #[default]
    Normal,
    SaveError,
    FailLoad,
    AdvanceClock,
    ChangeTraderIdentity,
}

#[derive(Debug)]
struct StateActor {
    core: DataActorCore,
    identity: UUID4,
    loads: Rc<Cell<usize>>,
    behavior: HookBehavior,
    owner: std::rc::Weak<RefCell<Trader>>,
}

impl DataActor for StateActor {
    fn on_save(&self) -> anyhow::Result<PersistedComponentState> {
        anyhow::ensure!(
            !matches!(self.behavior, HookBehavior::SaveError),
            "actual native on_save failure"
        );
        anyhow::ensure!(
            self.owner.upgrade().unwrap().try_borrow_mut().is_ok(),
            "Trader borrowed during on_save"
        );
        Ok(IndexMap::from([(
            "operational-v1".into(),
            self.identity.to_string().into_bytes(),
        )]))
    }

    fn on_load(&mut self, state: PersistedComponentState) -> anyhow::Result<()> {
        self.loads.set(self.loads.get() + 1);
        anyhow::ensure!(
            !matches!(self.behavior, HookBehavior::FailLoad),
            "actual native on_load failure"
        );
        anyhow::ensure!(
            self.owner.upgrade().unwrap().try_borrow_mut().is_ok(),
            "Trader borrowed during on_load"
        );
        self.identity = String::from_utf8(state["operational-v1"].clone())?
            .parse()
            .map_err(anyhow::Error::msg)?;

        match self.behavior {
            HookBehavior::AdvanceClock => {
                let clock = self.clock_rc();
                let clock = clock.borrow();
                clock
                    .as_any()
                    .downcast_ref::<VirtualClock>()
                    .unwrap()
                    .set_time((clock.timestamp_ns().as_u64() + 1).into());
            }
            HookBehavior::ChangeTraderIdentity => {
                self.owner.upgrade().unwrap().borrow_mut().instance_id = UUID4::new();
            }
            _ => {}
        }
        Ok(())
    }
}
nautilus_actor!(StateActor);

#[derive(Debug)]
struct StateStrategy {
    core: StrategyCore,
    identity: UUID4,
}
impl DataActor for StateStrategy {
    fn on_save(&self) -> anyhow::Result<PersistedComponentState> {
        Ok(IndexMap::from([(
            "unrelated-state-v1".into(),
            self.identity.to_string().into_bytes(),
        )]))
    }

    fn on_load(&mut self, state: PersistedComponentState) -> anyhow::Result<()> {
        self.identity = String::from_utf8(state["unrelated-state-v1"].clone())?
            .parse()
            .map_err(anyhow::Error::msg)?;
        Ok(())
    }
}
nautilus_strategy!(StateStrategy);

fn kernel(instance: UUID4) -> anyhow::Result<NautilusKernel> {
    NautilusKernelBuilder::new("state".into(), "STATE-001".into(), Environment::Backtest)
        .with_instance_id(instance)
        .with_load_state(false)
        .with_save_state(false)
        .build()
}

fn install(kernel: &NautilusKernel) -> anyhow::Result<Rc<Cell<usize>>> {
    let loads = Rc::new(Cell::new(0));
    kernel.trader.borrow_mut().add_actor(StateActor {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some(ActorId::from(ACTOR)),
            ..Default::default()
        }),
        identity: UUID4::new(),
        loads: loads.clone(),
        behavior: HookBehavior::Normal,
        owner: Rc::downgrade(&kernel.trader),
    })?;
    kernel.trader.borrow_mut().add_strategy(StateStrategy {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            order_id_tag: Some("001".into()),
            ..Default::default()
        }),
        identity: UUID4::new(),
    })?;
    Ok(loads)
}

fn source(instance: UUID4) -> anyhow::Result<Vec<RegisteredComponentState>> {
    std::thread::spawn(move || {
        let kernel = kernel(instance)?;
        install(&kernel)?;
        [ACTOR, STRATEGY]
            .into_iter()
            .map(|id| {
                Trader::export_component_state(
                    &kernel.trader,
                    ComponentId::from(id),
                    ComponentStateLimits::default(),
                )
            })
            .collect()
    })
    .join()
    .expect("native source thread")
}

#[rstest]
fn registered_actor_and_unrelated_strategy_use_original_typed_state_callbacks() -> anyhow::Result<()>
{
    let instance = UUID4::new();
    let source = source(instance)?;
    let kernel = kernel(instance)?;
    let loads = install(&kernel)?;

    for snapshot in source {
        let current = actor_registration(&snapshot.component_id.inner()).unwrap();
        anyhow::ensure!(current.registration_id != snapshot.registration.registration_id);
        anyhow::ensure!(current.generation == snapshot.registration.generation);
        let before = Trader::export_component_state(
            &kernel.trader,
            snapshot.component_id,
            ComponentStateLimits::default(),
        )?;
        anyhow::ensure!(before.state != snapshot.state);
        Trader::restore_component_state(
            &kernel.trader,
            snapshot.clone(),
            ComponentStateLimits::default(),
        )?;
        let after = Trader::export_component_state(
            &kernel.trader,
            snapshot.component_id,
            ComponentStateLimits::default(),
        )?;
        anyhow::ensure!(after.state == snapshot.state);
        anyhow::ensure!(after.registration == current);
        anyhow::ensure!(
            Trader::restore_component_state(
                &kernel.trader,
                snapshot,
                ComponentStateLimits::default()
            )
            .is_err()
        );
    }
    anyhow::ensure!(loads.get() == 1);
    anyhow::ensure!(kernel.cache.borrow().accounts_all_owned().is_empty());
    anyhow::ensure!(
        kernel
            .cache
            .borrow()
            .orders(None, None, None, None, None)
            .is_empty()
    );
    Ok(())
}

#[rstest]
fn component_state_bounds_reject_before_load_and_debug_does_not_disclose_values()
-> anyhow::Result<()> {
    let instance = UUID4::new();
    let snapshot = source(instance)?.remove(0);
    let kernel = kernel(instance)?;
    let loads = install(&kernel)?;

    for limits in [
        ComponentStateLimits {
            max_entries: 0,
            ..Default::default()
        },
        ComponentStateLimits {
            max_key_bytes: 1,
            ..Default::default()
        },
        ComponentStateLimits {
            max_value_bytes: 1,
            ..Default::default()
        },
        ComponentStateLimits {
            max_total_bytes: 1,
            ..Default::default()
        },
        ComponentStateLimits {
            max_entries: 65,
            ..Default::default()
        },
    ] {
        anyhow::ensure!(
            Trader::restore_component_state(&kernel.trader, snapshot.clone(), limits).is_err()
        );
    }
    let mut oversized = snapshot.clone();
    oversized.state.insert("secret".into(), vec![42; 65_537]);
    anyhow::ensure!(
        Trader::restore_component_state(&kernel.trader, oversized, ComponentStateLimits::default())
            .is_err()
    );
    anyhow::ensure!(loads.get() == 0);
    anyhow::ensure!(!format!("{snapshot:?}").contains("operational-v1"));
    Trader::restore_component_state(&kernel.trader, snapshot, ComponentStateLimits::default())?;
    anyhow::ensure!(loads.get() == 1);
    Ok(())
}

#[rstest]
fn mismatched_source_identity_generation_kind_and_schema_do_not_call_load() -> anyhow::Result<()> {
    let instance = UUID4::new();
    let snapshot = source(instance)?.remove(0);
    let kernel = kernel(instance)?;
    let loads = install(&kernel)?;
    let mut invalid = vec![];
    let mut value = snapshot.clone();
    value.native_instance_id = UUID4::new();
    invalid.push(value);
    let mut value = snapshot.clone();
    value.trader_id = "OTHER-001".into();
    invalid.push(value);
    let mut value = snapshot.clone();
    value.kind = ComponentStateKind::Strategy;
    invalid.push(value);
    let mut value = snapshot.clone();
    value.registration.generation += 1;
    invalid.push(value);
    let mut value = snapshot.clone();
    value.component_id = ComponentId::from("MISSING");
    invalid.push(value);
    let mut value = snapshot.clone();
    value.lifecycle_revision = 2;
    invalid.push(value);
    let mut value = snapshot.clone();
    value.schema = "unsupported".into();
    invalid.push(value);

    for value in invalid {
        anyhow::ensure!(
            Trader::restore_component_state(&kernel.trader, value, ComponentStateLimits::default())
                .is_err()
        );
    }
    anyhow::ensure!(loads.get() == 0);
    Trader::restore_component_state(&kernel.trader, snapshot, ComponentStateLimits::default())?;
    Ok(())
}

#[rstest]
fn reset_ready_running_and_timer_ownership_are_not_virgin_ready() -> anyhow::Result<()> {
    let kernel = kernel(UUID4::new())?;
    install(&kernel)?;
    let id = ComponentId::from(ACTOR);
    let clock = kernel.trader.borrow().clocks[&id].clone();
    clock
        .borrow_mut()
        .set_time_alert_ns("pre-start", 100.into(), None, Some(false))?;
    anyhow::ensure!(
        Trader::export_component_state(&kernel.trader, id, ComponentStateLimits::default())
            .is_err()
    );
    clock.borrow_mut().cancel_timers();
    start_component(&id.inner())?;
    anyhow::ensure!(
        Trader::export_component_state(&kernel.trader, id, ComponentStateLimits::default())
            .is_err()
    );
    stop_component(&id.inner())?;
    reset_component(&id.inner())?;
    anyhow::ensure!(component_state(&id.inner())? == ComponentState::Ready);
    anyhow::ensure!(
        Trader::export_component_state(&kernel.trader, id, ComponentStateLimits::default())
            .is_err()
    );
    Ok(())
}

#[rstest]
fn callback_failures_are_preserved_and_failed_load_cannot_be_retried() -> anyhow::Result<()> {
    let instance = UUID4::new();
    let snapshot = source(instance)?.remove(0);
    let kernel = kernel(instance)?;
    let loads = install(&kernel)?;
    {
        let mut actor =
            try_get_actor_unchecked::<StateActor>(&ActorId::from(ACTOR).inner()).unwrap();
        actor.behavior = HookBehavior::SaveError;
    }
    let error = Trader::export_component_state(
        &kernel.trader,
        snapshot.component_id,
        ComponentStateLimits::default(),
    )
    .unwrap_err();
    anyhow::ensure!(error.to_string().contains("actual native on_save failure"));
    {
        let mut actor =
            try_get_actor_unchecked::<StateActor>(&ActorId::from(ACTOR).inner()).unwrap();
        actor.behavior = HookBehavior::FailLoad;
    }
    let error = Trader::restore_component_state(
        &kernel.trader,
        snapshot.clone(),
        ComponentStateLimits::default(),
    )
    .unwrap_err();
    anyhow::ensure!(error.to_string().contains("actual native on_load failure"));
    anyhow::ensure!(
        Trader::restore_component_state(&kernel.trader, snapshot, ComponentStateLimits::default())
            .is_err()
    );
    anyhow::ensure!(loads.get() == 1);
    Ok(())
}

#[rstest]
fn replaced_native_registration_and_missing_verified_origin_reject() -> anyhow::Result<()> {
    let instance = UUID4::new();
    let snapshot = source(instance)?.remove(0);
    let kernel = kernel(instance)?;
    install(&kernel)?;
    anyhow::ensure!(
        kernel
            .restore_recorded_component_state(snapshot.clone(), ComponentStateLimits::default())
            .is_err()
    );
    let id = snapshot.component_id;
    let actor = StateActor {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some(ACTOR.into()),
            ..Default::default()
        }),
        identity: UUID4::new(),
        loads: Rc::new(Cell::new(0)),
        behavior: HookBehavior::Normal,
        owner: Rc::downgrade(&kernel.trader),
    };
    register_component_actor(actor);
    anyhow::ensure!(
        Trader::export_component_state(&kernel.trader, id, ComponentStateLimits::default())
            .is_err()
    );
    anyhow::ensure!(
        Trader::restore_component_state(&kernel.trader, snapshot, ComponentStateLimits::default())
            .is_err()
    );
    Ok(())
}

#[rstest]
fn live_kernel_cannot_use_operational_state_restore() -> anyhow::Result<()> {
    let instance = UUID4::new();
    let snapshot = source(instance)?.remove(0);
    let kernel = NautilusKernelBuilder::new("live".into(), "STATE-001".into(), Environment::Live)
        .with_clock_factory(|| Rc::new(RefCell::new(VirtualClock::new())))
        .with_load_state(false)
        .with_save_state(false)
        .with_instance_id(instance)
        .build()?;
    let loads = install(&kernel)?;
    anyhow::ensure!(
        kernel
            .restore_recorded_component_state(snapshot, ComponentStateLimits::default())
            .is_err()
    );
    anyhow::ensure!(loads.get() == 0);
    Ok(())
}

#[rstest]
fn running_virtual_sandbox_without_verified_origin_never_calls_load() -> anyhow::Result<()> {
    let instance = UUID4::new();
    let snapshot = source(instance)?.remove(0);
    let mut kernel =
        NautilusKernelBuilder::new("sandbox".into(), "STATE-001".into(), Environment::Sandbox)
            .with_clock_factory(|| Rc::new(RefCell::new(VirtualClock::new())))
            .with_load_state(false)
            .with_save_state(false)
            .with_instance_id(instance)
            .build()?;
    kernel.start();
    kernel.start_trader()?;
    let loads = install(&kernel)?;
    let error = kernel
        .restore_recorded_component_state(snapshot, ComponentStateLimits::default())
        .unwrap_err();
    anyhow::ensure!(error.to_string().contains("verified recorded origin"));
    anyhow::ensure!(loads.get() == 0);
    kernel.stop_trader();
    Ok(())
}

#[rstest]
fn component_state_export_rejects_actual_target_owned_native_order_history() -> anyhow::Result<()> {
    let kernel = kernel(UUID4::new())?;
    install(&kernel)?;
    let id = ComponentId::from(STRATEGY);
    let mut builder = OrderTestBuilder::new(OrderType::Market);
    builder
        .strategy_id(StrategyId::from(STRATEGY))
        .instrument_id("STATE.XNAS".into())
        .quantity(Quantity::from(1));
    kernel
        .cache
        .borrow_mut()
        .add_order(builder.build(), None, None, false)?;
    let error = Trader::export_component_state(&kernel.trader, id, ComponentStateLimits::default())
        .unwrap_err();
    anyhow::ensure!(error.to_string().contains("financial history"));
    anyhow::ensure!(
        Trader::export_component_state(
            &kernel.trader,
            ComponentId::from(ACTOR),
            ComponentStateLimits::default()
        )
        .is_ok()
    );
    Ok(())
}

#[rstest]
#[case::clock_advanced(HookBehavior::AdvanceClock)]
#[case::trader_changed(HookBehavior::ChangeTraderIdentity)]
fn native_restore_rejects_callback_changes_to_clock_or_trader_identity(
    #[case] behavior: HookBehavior,
) -> anyhow::Result<()> {
    let instance = UUID4::new();
    let snapshot = source(instance)?.remove(0);
    let kernel = kernel(instance)?;
    let loads = install(&kernel)?;
    {
        let mut actor =
            try_get_actor_unchecked::<StateActor>(&ActorId::from(ACTOR).inner()).unwrap();
        actor.behavior = behavior;
    }
    let error = Trader::restore_component_state(
        &kernel.trader,
        snapshot.clone(),
        ComponentStateLimits::default(),
    )
    .unwrap_err();
    anyhow::ensure!(error.to_string().contains("during state restore"));
    anyhow::ensure!(loads.get() == 1);
    anyhow::ensure!(
        Trader::restore_component_state(&kernel.trader, snapshot, ComponentStateLimits::default())
            .is_err()
    );
    anyhow::ensure!(loads.get() == 1);
    Ok(())
}

#[cfg(feature = "live")]
#[rstest]
fn native_restore_rejects_mixed_live_target_clock_before_callback() -> anyhow::Result<()> {
    use nautilus_common::{clock::Clock, live::clock::LiveClock};

    let instance = UUID4::new();
    let snapshot = source(instance)?.remove(0);
    let calls = Rc::new(Cell::new(0));
    let kernel =
        NautilusKernelBuilder::new("mixed".into(), "STATE-001".into(), Environment::Backtest)
            .with_instance_id(instance)
            .with_load_state(false)
            .with_save_state(false)
            .with_clock_factory(move || -> Rc<RefCell<dyn Clock>> {
                let next = calls.get();
                calls.set(next + 1);

                if next == 0 {
                    Rc::new(RefCell::new(VirtualClock::new()))
                } else {
                    Rc::new(RefCell::new(LiveClock::new(None)))
                }
            })
            .build()?;
    let loads = install(&kernel)?;
    anyhow::ensure!(kernel.clock.borrow().as_any().is::<VirtualClock>());
    let error =
        Trader::restore_component_state(&kernel.trader, snapshot, ComponentStateLimits::default())
            .unwrap_err();
    anyhow::ensure!(error.to_string().contains("target VirtualClock"));
    anyhow::ensure!(loads.get() == 0);
    Ok(())
}
