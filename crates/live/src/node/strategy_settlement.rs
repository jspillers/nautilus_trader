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

//! Strategy registration correlation at an online execution-event boundary.

use std::{cell::UnsafeCell, fmt::Debug, rc::Rc};

use ahash::AHashMap;
use nautilus_common::{
    component::{Component, component_state, get_component},
    enums::ComponentState,
    messages::execution::{
        ExecutionClientSettlement, QueryStrategySettlement, StrategySettlementMarker,
        StrategySettlementResult,
    },
    msgbus,
};
use nautilus_core::UUID4;

use super::{LiveNode, NodeState};

struct Pending {
    query: QueryStrategySettlement,
    component: Option<Rc<UnsafeCell<dyn Component>>>,
    conflicting: bool,
}

#[derive(Default)]
pub(super) struct PendingStrategySettlements(AHashMap<UUID4, Pending>);

impl Debug for PendingStrategySettlements {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(PendingStrategySettlements))
            .field("count", &self.0.len())
            .finish()
    }
}

impl PendingStrategySettlements {
    pub(super) fn begin(&mut self, query: &QueryStrategySettlement) {
        match self.0.entry(query.command_id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().conflicting = true;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Pending {
                    query: query.clone(),
                    component: get_component(&query.strategy_id.inner()),
                    conflicting: false,
                });
            }
        }
    }

    fn complete(&mut self, query: &QueryStrategySettlement) -> bool {
        self.0.remove(&query.command_id).is_some_and(|pending| {
            !pending.conflicting
                && pending.query == *query
                && pending
                    .component
                    .as_ref()
                    .zip(get_component(&query.strategy_id.inner()).as_ref())
                    .is_some_and(|(before, current)| Rc::ptr_eq(before, current))
        })
    }
}

impl LiveNode {
    pub(super) fn finish_strategy_settlement(
        &mut self,
        marker: StrategySettlementMarker,
        eligible: bool,
    ) {
        let same_registration = self.strategy_settlements.complete(&marker.query);
        let identity_matches = marker.native_instance_id == self.instance_id()
            && marker.query.expected_instance_id == self.instance_id()
            && marker.query.trader_id == self.config.trader_id
            && self
                .kernel
                .trader
                .borrow()
                .strategy_ids()
                .contains(&marker.query.strategy_id)
            && component_state(&marker.query.strategy_id.inner())
                .is_ok_and(|state| state == ComponentState::Running);
        let mut result =
            if same_registration && identity_matches && self.state() == NodeState::Running {
                let clients = self
                    .kernel
                    .exec_engine
                    .borrow()
                    .inspect_strategy_settlement(marker.query.strategy_id);
                let state = if clients.is_empty()
                    || clients
                        .iter()
                        .any(|client| client.state == ExecutionClientSettlement::Unsupported)
                {
                    ExecutionClientSettlement::Unsupported
                } else if !eligible
                    || clients
                        .iter()
                        .any(|client| client.state == ExecutionClientSettlement::Pending)
                {
                    ExecutionClientSettlement::Pending
                } else {
                    ExecutionClientSettlement::Quiescent
                };
                StrategySettlementResult {
                    query: marker.query,
                    native_instance_id: marker.native_instance_id,
                    state,
                    clients,
                    native_boundary: eligible,
                    observed_at: self.kernel.clock.borrow().timestamp_ns(),
                    error: (!eligible).then(|| {
                        "Native execution queues, callbacks, or report collection remain pending"
                            .to_string()
                    }),
                }
            } else {
                marker.unsupported(
                    "Native instance, strategy registration, or live runner boundary changed",
                )
            };
        result.observed_at = self.kernel.clock.borrow().timestamp_ns();
        msgbus::publish_any(result.query.topic().into(), &result);
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::{
        actor::{DataActor, DataActorConfig, DataActorCore, registry::deregister_actor},
        component::{deregister_component, register_component_actor},
        nautilus_actor,
    };
    use rstest::rstest;

    use super::*;

    #[derive(Debug)]
    struct Registration {
        core: DataActorCore,
    }

    impl DataActor for Registration {}
    nautilus_actor!(Registration);

    fn register(query: &QueryStrategySettlement) {
        register_component_actor(Registration {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some(query.strategy_id.as_str().into()),
                ..Default::default()
            }),
        });
    }

    fn query() -> QueryStrategySettlement {
        QueryStrategySettlement {
            trader_id: "SETTLEMENT-001".into(),
            expected_instance_id: UUID4::new(),
            strategy_id: "SettlementRegistration-001".into(),
            command_id: UUID4::new(),
            ts_init: 1.into(),
        }
    }

    fn deregister(query: &QueryStrategySettlement) {
        deregister_component(&query.strategy_id.inner());
        deregister_actor(&query.strategy_id.inner());
    }

    #[rstest]
    fn settlement_registration_accepts_only_original_query_once() {
        let query = query();
        register(&query);
        let mut pending = PendingStrategySettlements::default();
        pending.begin(&query);

        assert!(pending.complete(&query));
        assert!(!pending.complete(&query));
        deregister(&query);
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn settlement_registration_rejects_removed_or_same_id_replaced_component(
        #[case] replace: bool,
    ) {
        let query = query();
        register(&query);
        let mut pending = PendingStrategySettlements::default();
        pending.begin(&query);
        deregister(&query);
        if replace {
            register(&query);
        }

        assert!(!pending.complete(&query));
        deregister(&query);
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn settlement_registration_rejects_duplicate_or_changed_query(#[case] changed: bool) {
        let query = query();
        register(&query);
        let mut pending = PendingStrategySettlements::default();
        pending.begin(&query);
        let mut altered = query.clone();
        if changed {
            altered.expected_instance_id = UUID4::new();
        } else {
            pending.begin(&query);
        }

        assert!(!pending.complete(&altered));
        assert!(!pending.complete(&query));
        deregister(&query);
    }

    #[rstest]
    fn settlement_registration_rejects_missing_component_at_dispatch() {
        let query = query();
        deregister(&query);
        let mut pending = PendingStrategySettlements::default();
        pending.begin(&query);
        register(&query);

        assert!(!pending.complete(&query));
        deregister(&query);
    }
}
