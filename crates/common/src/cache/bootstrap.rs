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

//! Counts actual native bootstrap reads without retaining loaded state or issuing reads.

use std::collections::BTreeMap;

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::identifiers::TraderId;
use serde::{Deserialize, Serialize};

/// One native backing read consumed during bootstrap.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapLoadRead {
    /// Native read path, including the separate position-OMS general read.
    pub kind: BootstrapLoadReadKind,
    /// Actual registered component for actor/strategy state reads.
    pub component_id: Option<String>,
    /// Whether this actual database read returned successfully.
    pub succeeded: bool,
    /// Entry counts before sanitization, filtering, or native consumption.
    pub counts: BTreeMap<String, usize>,
}

/// The supported native bootstrap read paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapLoadReadKind {
    General,
    CacheAll,
    IndexOrderPosition,
    IndexOrderClient,
    PositionOmsGeneral,
    ActorState,
    StrategyState,
}

/// Ordered observations from actual cache and component-state bootstrap calls.
///
/// These counts are evidence of consumed reads, not a state snapshot or restore input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapLoads {
    /// Actor registration inventory observed before the native bootstrap reads.
    pub actor_ids: Vec<String>,
    /// Strategy registration inventory observed before the native bootstrap reads.
    pub strategy_ids: Vec<String>,
    /// Ordered actual read observations, including empty reads and failures.
    pub reads: Vec<BootstrapLoadRead>,
    /// Native index rebuild, integrity check and position-ID count setup completed.
    pub cache_rebuilt: bool,
    /// Actual native integrity check result; absent if the pass did not finish.
    pub cache_integrity: Option<bool>,
    /// Every registered component-state load and applicable `on_load` callback succeeded.
    pub component_states_loaded: bool,
    /// The bound was exceeded; this observation cannot establish complete bootstrap.
    pub overflowed: bool,
}

impl BootstrapLoads {
    /// Returns whether every expected actual bootstrap read and native load pass succeeded.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        use BootstrapLoadReadKind as Kind;

        if !self.cache_rebuilt
            || self.cache_integrity != Some(true)
            || !self.component_states_loaded
            || self.overflowed
        {
            return false;
        }
        let expected = [
            (Kind::General, None),
            (Kind::CacheAll, None),
            (Kind::IndexOrderPosition, None),
            (Kind::IndexOrderClient, None),
            (Kind::PositionOmsGeneral, None),
        ]
        .into_iter()
        .chain(
            self.actor_ids
                .iter()
                .map(|id| (Kind::ActorState, Some(id.as_str()))),
        )
        .chain(
            self.strategy_ids
                .iter()
                .map(|id| (Kind::StrategyState, Some(id.as_str()))),
        );

        if self.reads.len() != 5 + self.actor_ids.len() + self.strategy_ids.len() {
            return false;
        }
        self.reads.iter().zip(expected).all(|(read, (kind, id))| {
            let fields = if kind == Kind::CacheAll {
                &[
                    "accounts",
                    "currencies",
                    "greeks",
                    "instrument_closes",
                    "instruments",
                    "orders",
                    "positions",
                    "synthetics",
                    "yield_curves",
                ][..]
            } else {
                &["entries"][..]
            };
            read.kind == kind
                && read.component_id.as_deref() == id
                && read.succeeded
                && read.counts.len() == fields.len()
                && fields.iter().all(|field| read.counts.contains_key(*field))
        })
    }
    /// Returns whether the complete actual read inventory was empty.
    #[must_use]
    pub fn is_complete_empty(&self) -> bool {
        self.is_complete()
            && self
                .reads
                .iter()
                .all(|read| read.counts.values().all(|count| *count == 0))
    }
}

/// Completed or incomplete native bootstrap evidence observed at the Running boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapLoadWitness {
    /// Actual native process identity.
    pub native_instance_id: UUID4,
    /// Actual native trader identity.
    pub trader_id: TraderId,
    /// Native owner-thread Running observation.
    pub observed_at: UnixNanos,
    /// Successful complete read/rebuild/component pass, never inferred from empty missing reads.
    pub completed: bool,
    /// Actual installed native backing at Running.
    pub cache_backing: bool,
    /// Actual native execution engine cache-load setting.
    pub load_cache: bool,
    /// Actual native component-state load setting.
    pub load_state: bool,
    /// Actual native component-state save setting.
    pub save_state: bool,
    /// Actual native cache-flush setting.
    pub flush_on_start: bool,
    /// Actual kernel event-store parent, including crash recovery or configured replay.
    pub parent_run_id: Option<String>,
    /// Actual Running registered Actor inventory.
    pub actor_ids: Vec<String>,
    /// Actual Running registered Strategy inventory.
    pub strategy_ids: Vec<String>,
    /// Actual Running registered execution client inventory.
    pub execution_client_ids: Vec<String>,
    /// Ordered buffered observations from consumed reads.
    pub loads: BootstrapLoads,
}

impl BootstrapLoadWitness {
    /// Returns whether this witness establishes the first supported empty cold bootstrap.
    #[must_use]
    pub fn is_complete_empty(&self) -> bool {
        self.completed
            && self.cache_backing
            && self.load_cache
            && self.load_state
            && self.save_state
            && !self.flush_on_start
            && self.parent_run_id.is_none()
            && self.actor_ids == self.loads.actor_ids
            && self.strategy_ids == self.loads.strategy_ids
            && self.loads.is_complete_empty()
    }
}

pub(super) fn record(
    report: &std::cell::RefCell<Option<BootstrapLoads>>,
    kind: BootstrapLoadReadKind,
    component_id: Option<String>,
    succeeded: bool,
    counts: BTreeMap<String, usize>,
) {
    let mut report = report.borrow_mut();
    let Some(report) = report.as_mut() else {
        return;
    };
    let limit = 5usize
        .saturating_add(report.actor_ids.len())
        .saturating_add(report.strategy_ids.len());
    if report.reads.len() >= limit {
        report.overflowed = true;
        return;
    }
    report.reads.push(BootstrapLoadRead {
        kind,
        component_id,
        succeeded,
        counts,
    });
}
