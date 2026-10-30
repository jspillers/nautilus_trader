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

//! Correlated online execution settlement at a native runner boundary.

use std::fmt::Display;

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::identifiers::{ClientId, StrategyId, TraderId};
use serde::{Deserialize, Serialize};

/// Queries existing execution work after earlier native trading commands.
///
/// The caller holds entry admission paused throughout the query and checks its
/// native order/position ownership separately. Quiescence closes no order or
/// position, stops no component, and authorizes no future submission.
/// This Rust command adds no dedicated external codec or Python facade.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub struct QueryStrategySettlement {
    pub trader_id: TraderId,
    pub expected_instance_id: UUID4,
    pub strategy_id: StrategyId,
    pub command_id: UUID4,
    pub ts_init: UnixNanos,
}

impl QueryStrategySettlement {
    /// Returns the native topic for this strategy's correlated result.
    #[must_use]
    pub fn topic(&self) -> String {
        format!("events.execution.strategy_settlement.{}", self.strategy_id)
    }
}

impl Display for QueryStrategySettlement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryStrategySettlement(strategy_id={}, command_id={})",
            self.strategy_id, self.command_id
        )
    }
}

/// Capability of one execution client for its existing strategy-owned work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionClientSettlement {
    Unsupported,
    Pending,
    Quiescent,
}

/// Snapshot of one actual registered client, without a runtime ordering claim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategySettlementClient {
    pub client_id: ClientId,
    pub state: ExecutionClientSettlement,
    pub error: Option<String>,
}

/// Internal FIFO marker, never an external message or settlement certificate.
#[derive(Debug)]
pub struct StrategySettlementMarker {
    pub query: QueryStrategySettlement,
    pub native_instance_id: UUID4,
    pub observed_at: UnixNanos,
}

impl StrategySettlementMarker {
    /// Returns an unsupported result when no matching live boundary is available.
    #[must_use]
    pub fn unsupported(self, error: &str) -> StrategySettlementResult {
        StrategySettlementResult {
            query: self.query,
            native_instance_id: self.native_instance_id,
            state: ExecutionClientSettlement::Unsupported,
            clients: Vec::new(),
            native_boundary: false,
            observed_at: self.observed_at,
            error: Some(error.to_string()),
        }
    }
}

impl Display for StrategySettlementMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StrategySettlementMarker({})", self.query)
    }
}

/// Native client state plus conservative live-runner execution eligibility.
///
/// `Quiescent` requires the same strategy registration and native instance,
/// supported clients with no scoped work, and no queued execution commands or
/// events, pending callbacks, or report collections at the marker. Unrelated
/// execution traffic can conservatively produce `Pending`. No queue is drained
/// or paused and no quiet-time interval is inferred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategySettlementResult {
    pub query: QueryStrategySettlement,
    pub native_instance_id: UUID4,
    pub state: ExecutionClientSettlement,
    pub clients: Vec<StrategySettlementClient>,
    pub native_boundary: bool,
    pub observed_at: UnixNanos,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::messages::execution::TradingCommand;

    #[rstest]
    fn native_settlement_query_roundtrips_bare_and_wrapped_json_and_messagepack() {
        let query = QueryStrategySettlement {
            trader_id: "SETTLEMENT-001".into(),
            expected_instance_id: UUID4::new(),
            strategy_id: "SettlementTarget-001".into(),
            command_id: UUID4::new(),
            ts_init: 100.into(),
        };
        let json = serde_json::to_vec(&query).unwrap();
        let messagepack = rmp_serde::to_vec_named(&query).unwrap();
        assert_eq!(
            serde_json::from_slice::<QueryStrategySettlement>(&json).unwrap(),
            query
        );
        assert_eq!(
            rmp_serde::from_slice::<QueryStrategySettlement>(&messagepack).unwrap(),
            query
        );
        let command = TradingCommand::QueryStrategySettlement(query.clone());
        assert_eq!(
            serde_json::from_slice::<TradingCommand>(&json).unwrap(),
            command
        );
        assert_eq!(
            rmp_serde::from_slice::<TradingCommand>(&messagepack).unwrap(),
            command
        );
        assert_eq!(command.strategy_id(), Some(query.strategy_id));
        assert_eq!(command.client_id(), None);
        assert_eq!(command.params(), None);
        assert_eq!(command.ts_init(), query.ts_init);
    }
}
