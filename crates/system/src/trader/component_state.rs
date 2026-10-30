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

//! Bounded standard state callbacks for registered components before their first start.

use std::{cell::RefCell, fmt::Debug, rc::Rc};

use indexmap::IndexMap;
use nautilus_common::{
    actor::registry::{ActorRegistration, actor_registration},
    clock::VirtualClock,
    component::{component_lifecycle_revision, component_state},
    enums::ComponentState,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::identifiers::{ComponentId, TraderId};
use serde::{Deserialize, Serialize};

use super::{ComponentStateCallbacks, PersistedComponentState, Trader};

const SCHEMA: &str = "nautilus-registered-component-state-v1";

/// Hard-bounded limits for one native component's opaque standard state.
///
/// Values limit the returned payload; the registered callback remains responsible for its
/// own computation and allocation. No file, database or financial cache is read here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentStateLimits {
    /// Maximum number of user-defined state keys, at most 64.
    pub max_entries: usize,
    /// Maximum UTF-8 bytes in each key, at most 256.
    pub max_key_bytes: usize,
    /// Maximum bytes in each value, at most 65536.
    pub max_value_bytes: usize,
    /// Maximum combined key/value bytes, at most 262144.
    pub max_total_bytes: usize,
}

impl Default for ComponentStateLimits {
    fn default() -> Self {
        Self {
            max_entries: 64,
            max_key_bytes: 256,
            max_value_bytes: 32_768,
            max_total_bytes: 65_536,
        }
    }
}

impl ComponentStateLimits {
    fn validate(self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=64).contains(&self.max_entries)
                && (1..=256).contains(&self.max_key_bytes)
                && (1..=65_536).contains(&self.max_value_bytes)
                && (1..=262_144).contains(&self.max_total_bytes),
            "Component state limits exceed native bounds"
        );
        Ok(())
    }

    fn require_state(self, state: &PersistedComponentState) -> anyhow::Result<()> {
        self.validate()?;
        anyhow::ensure!(
            state.len() <= self.max_entries,
            "Component state entry bound exceeded"
        );
        let mut total = 0usize;

        for (key, value) in state {
            anyhow::ensure!(
                !key.is_empty() && key.len() <= self.max_key_bytes,
                "Component state key bound exceeded"
            );
            anyhow::ensure!(
                value.len() <= self.max_value_bytes,
                "Component state value bound exceeded"
            );
            total = total
                .checked_add(key.len())
                .and_then(|total| total.checked_add(value.len()))
                .ok_or_else(|| anyhow::anyhow!("Component state byte count overflow"))?;
            anyhow::ensure!(
                total <= self.max_total_bytes,
                "Component state total bound exceeded"
            );
        }
        Ok(())
    }
}

/// The registered native callback family; execution algorithms are unsupported.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentStateKind {
    /// A registered data actor.
    Actor,
    /// A registered trading strategy.
    Strategy,
}

/// Actual bounded `on_save` output from a registered component at its first Ready state.
///
/// The original registration UUID is source evidence, not a replacement for a fresh native
/// registration. A recorded restore matches the component kind/ID and per-ID generation.
/// Callers must bind these opaque bytes to verified source/configuration provenance and
/// review the component's `on_load` contract. This is not a financial cache snapshot.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisteredComponentState {
    /// Versioned standard callback export schema.
    pub schema: String,
    /// Native trader which owns the registered callback.
    pub trader_id: TraderId,
    /// Original source process identity, fixed by verified recorded origin during restore.
    pub native_instance_id: UUID4,
    /// Actually tracked native component ID.
    pub component_id: ComponentId,
    /// Typed callback family registered by the trader.
    pub kind: ComponentStateKind,
    /// Actual source registry insertion; never transplanted into the replay registry.
    pub registration: ActorRegistration,
    /// Native lifecycle witness, exactly one before the first start.
    pub lifecycle_revision: u64,
    /// Opaque standard callback state; values are never logged by native debug formatting.
    pub state: IndexMap<String, Vec<u8>>,
}

impl Debug for RegisteredComponentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RegisteredComponentState))
            .field("component_id", &self.component_id)
            .field("kind", &self.kind)
            .field("registration", &self.registration)
            .field("state_entries", &self.state.len())
            .finish_non_exhaustive()
    }
}

impl Trader {
    fn recorded_component_clock(&self, id: ComponentId) -> anyhow::Result<UnixNanos> {
        let clock = self
            .clocks
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("Component {id} has no trader-owned clock"))?;
        let clock = clock.try_borrow()?;
        anyhow::ensure!(
            clock.as_any().is::<VirtualClock>(),
            "Component state restore requires target VirtualClock"
        );
        Ok(clock.timestamp_ns())
    }

    fn ready_state_callback(
        &self,
        component_id: ComponentId,
    ) -> anyhow::Result<(
        ComponentStateKind,
        ComponentStateCallbacks,
        ActorRegistration,
    )> {
        let id = component_id.inner();
        let actor = self
            .actor_state_callbacks
            .iter()
            .find(|(value, _)| value.inner() == id);
        let strategy = self
            .strategy_state_callbacks
            .iter()
            .find(|(value, _)| value.inner() == id);
        let (kind, callbacks) = match (actor, strategy) {
            (Some((_, callbacks)), None) => (ComponentStateKind::Actor, *callbacks),
            (None, Some((_, callbacks))) => (ComponentStateKind::Strategy, *callbacks),
            _ => anyhow::bail!(
                "Component {component_id} has no unambiguous registered state callback"
            ),
        };
        let registration = actor_registration(&id)
            .ok_or_else(|| anyhow::anyhow!("Component {component_id} is not registered"))?;
        let clock = self
            .clocks
            .get(&component_id)
            .ok_or_else(|| anyhow::anyhow!("Component {component_id} has no trader-owned clock"))?;
        let clock = clock.try_borrow()?;
        anyhow::ensure!(
            clock.actor_registration() == Some(registration),
            "Component {component_id} native registration changed"
        );
        anyhow::ensure!(
            component_state(&id)? == ComponentState::Ready
                && component_lifecycle_revision(&id)? == Some(1)
                && clock.timer_count() == 0,
            "Component {component_id} requires its first Ready state without timers"
        );

        if let Some((strategy_id, _)) = strategy {
            let cache = self.cache.try_borrow()?;
            anyhow::ensure!(
                cache
                    .orders(None, None, Some(strategy_id), None, None)
                    .is_empty()
                    && cache
                        .positions(None, None, Some(strategy_id), None, None)
                        .is_empty(),
                "Component {component_id} already has native financial history"
            );
        }
        Ok((kind, callbacks, registration))
    }

    /// Exports bounded standard state from an actually registered component before first start.
    ///
    /// The trader borrow is released before the existing typed `on_save` callback runs.
    /// Native registration, Ready revision and timer ownership are checked again afterwards.
    /// No persistence, financial cache restoration or lifecycle transition is performed.
    ///
    /// # Errors
    ///
    /// Returns an error for an untracked/replaced component, nonvirgin lifecycle, timers or
    /// financial history, invalid/exceeded bounds, borrow conflicts or callback failure.
    pub fn export_component_state(
        trader: &Rc<RefCell<Self>>,
        component_id: ComponentId,
        limits: ComponentStateLimits,
    ) -> anyhow::Result<RegisteredComponentState> {
        limits.validate()?;
        let (trader_id, native_instance_id, kind, callbacks, registration) = {
            let trader = trader.try_borrow()?;
            let (kind, callbacks, registration) = trader.ready_state_callback(component_id)?;
            (
                trader.trader_id,
                trader.instance_id,
                kind,
                callbacks,
                registration,
            )
        };
        let state = (callbacks.save)(component_id.inner())?;
        limits.require_state(&state)?;
        {
            let trader = trader.try_borrow()?;
            let (actual_kind, _, actual_registration) =
                trader.ready_state_callback(component_id)?;
            anyhow::ensure!(
                trader.trader_id == trader_id
                    && trader.instance_id == native_instance_id
                    && actual_kind == kind
                    && actual_registration == registration,
                "Component registration changed during state export"
            );
        }
        Ok(RegisteredComponentState {
            schema: SCHEMA.to_owned(),
            trader_id,
            native_instance_id,
            component_id,
            kind,
            registration,
            lifecycle_revision: 1,
            state,
        })
    }

    pub(crate) fn restore_component_state(
        trader: &Rc<RefCell<Self>>,
        snapshot: RegisteredComponentState,
        limits: ComponentStateLimits,
    ) -> anyhow::Result<()> {
        limits.require_state(&snapshot.state)?;
        anyhow::ensure!(
            snapshot.schema == SCHEMA
                && snapshot.lifecycle_revision == 1
                && snapshot.registration.actor_id == snapshot.component_id.inner(),
            "Registered component state schema or source lifecycle differs"
        );
        let (load, registration, clock_time) = {
            let mut trader = trader.try_borrow_mut()?;
            anyhow::ensure!(
                snapshot.trader_id == trader.trader_id
                    && snapshot.native_instance_id == trader.instance_id,
                "Registered component state source identity differs"
            );
            let (kind, callbacks, registration) =
                trader.ready_state_callback(snapshot.component_id)?;
            let clock_time = trader.recorded_component_clock(snapshot.component_id)?;
            anyhow::ensure!(
                kind == snapshot.kind
                    && registration.generation == snapshot.registration.generation,
                "Registered component state kind or generation differs"
            );
            anyhow::ensure!(
                !callbacks.recorded_restore_attempted,
                "Component state restore was already attempted"
            );
            let callback = match kind {
                ComponentStateKind::Actor => {
                    trader
                        .actor_state_callbacks
                        .iter_mut()
                        .find_map(|(id, callback)| {
                            (id.inner() == snapshot.component_id.inner()).then_some(callback)
                        })
                }
                ComponentStateKind::Strategy => trader
                    .strategy_state_callbacks
                    .iter_mut()
                    .find_map(|(id, callback)| {
                        (id.inner() == snapshot.component_id.inner()).then_some(callback)
                    }),
            }
            .ok_or_else(|| anyhow::anyhow!("Registered state callback disappeared"))?;

            // A failed hook can have partial user state; never retry it as an unapplied restore
            callback.recorded_restore_attempted = true;
            (callbacks.load, registration, clock_time)
        };
        load(snapshot.component_id.inner(), snapshot.state)?;
        let trader = trader.try_borrow()?;
        let (kind, _, actual) = trader.ready_state_callback(snapshot.component_id)?;
        anyhow::ensure!(
            trader.trader_id == snapshot.trader_id
                && trader.instance_id == snapshot.native_instance_id
                && kind == snapshot.kind
                && actual == registration
                && trader.recorded_component_clock(snapshot.component_id)? == clock_time,
            "Component identity, registration or clock changed during state restore"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;
