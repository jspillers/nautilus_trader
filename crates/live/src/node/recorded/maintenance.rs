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

//! Native due maintenance and recomputed report futures for recorded execution.

use std::fmt::Debug;

use futures_util::FutureExt;
use nautilus_common::runner::recorded::{
    RecordedRunnerDispatch, RuntimeDispatchScope, describe_reconciliation,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    super::reconciliation::{
        OpenOrderReportResult, OpenOrderReportTask, PositionReportTask, PositionReportTaskResult,
        ReconciliationDue, ReportTaskOutcome, TargetedOrderReportTask,
    },
    LiveNode,
};
use crate::execution::manager::TargetedOrderReportResult;

/// Decisions made by the existing native maintenance timer before its work executes.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::node) struct MaintenanceActions {
    pub(in crate::node) reconciliation: Option<ReconciliationDue>,
    pub(in crate::node) purge: PurgeActions,
    pub(in crate::node) audit_books: bool,
    pub(in crate::node) prune_fills: bool,
    pub(in crate::node) queue_monitor: bool,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::node) struct PurgeActions {
    pub(in crate::node) orders: bool,
    pub(in crate::node) positions: bool,
    pub(in crate::node) accounts: bool,
}
impl MaintenanceActions {
    pub(in crate::node) fn describe(self) -> RecordedRunnerDispatch {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        let mut witness = describe_reconciliation("timer", &value, !self.queue_monitor);
        witness.semantic = json!({"operation":"timer","actions":value});
        witness
    }
}

#[derive(Default)]
pub(super) struct ReportTasks {
    pub(super) open: Option<OpenOrderReportTask>,
    pub(super) targeted: Option<TargetedOrderReportTask>,
    pub(super) position: Option<PositionReportTask>,
}
impl Debug for ReportTasks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ReportTasks))
            .field("open", &self.open.is_some())
            .field("targeted", &self.targeted.is_some())
            .field("position", &self.position.is_some())
            .finish()
    }
}
impl ReportTasks {
    pub(super) fn is_empty(&self) -> bool {
        self.open.is_none() && self.targeted.is_none() && self.position.is_none()
    }
}

pub(in crate::node) fn describe_open_outcome(
    result: &ReportTaskOutcome<OpenOrderReportResult>,
) -> RecordedRunnerDispatch {
    match result {
        ReportTaskOutcome::Completed(result) => describe_reconciliation(
            "open_report",
            &json!({"reports":result.reports,"queried_clients":result.queried_clients,"failed_clients":result.failed_clients}),
            result.failed_clients.is_empty(),
        ),
        ReportTaskOutcome::TimedOut => {
            describe_reconciliation("open_report", &json!({"timeout":true}), false)
        }
    }
}
pub(in crate::node) fn describe_targeted_outcome(
    result: &ReportTaskOutcome<Vec<TargetedOrderReportResult>>,
) -> RecordedRunnerDispatch {
    match result {
        ReportTaskOutcome::Completed(result) => describe_reconciliation(
            "targeted_report",
            &serde_json::to_value(result).unwrap_or(Value::Null),
            true,
        ),
        ReportTaskOutcome::TimedOut => {
            describe_reconciliation("targeted_report", &json!({"timeout":true}), false)
        }
    }
}
pub(in crate::node) fn describe_position_outcome(
    result: &ReportTaskOutcome<PositionReportTaskResult>,
) -> RecordedRunnerDispatch {
    match result {
        ReportTaskOutcome::Completed(PositionReportTaskResult::Positions(result)) => {
            describe_reconciliation(
                "position_report",
                &json!({"reports":result.reports,"queried_clients":result.queried_clients,"failed_clients":result.failed_clients}),
                result.failed_clients.is_empty(),
            )
        }
        ReportTaskOutcome::Completed(PositionReportTaskResult::Fills(result)) => {
            let reports = result
                .reports
                .iter()
                .map(|(key, reports)| json!({"instrument":key.0,"account":key.1,"reports":reports}))
                .collect::<Vec<_>>();
            describe_reconciliation(
                "position_report",
                &json!({"fill_reports":reports,"successful_keys":result.successful_keys}),
                true,
            )
        }
        ReportTaskOutcome::TimedOut => {
            describe_reconciliation("position_report", &json!({"timeout":true}), false)
        }
    }
}

impl LiveNode {
    pub(super) fn process_recorded_maintenance(
        &mut self,
        witness: &RecordedRunnerDispatch,
    ) -> anyhow::Result<()> {
        let mut tasks =
            std::mem::take(&mut self.recorded.as_mut().expect("recorded state").reports);
        let result = self.apply_recorded_maintenance(witness, &mut tasks);
        self.recorded.as_mut().expect("recorded state").reports = tasks;
        result
    }

    fn apply_recorded_maintenance(
        &mut self,
        witness: &RecordedRunnerDispatch,
        tasks: &mut ReportTasks,
    ) -> anyhow::Result<()> {
        let operation = witness.semantic.get("operation").and_then(Value::as_str);
        match operation {
            Some("timer") => {
                let actions: MaintenanceActions =
                    serde_json::from_value(
                        witness.semantic.get("actions").cloned().ok_or_else(|| {
                            anyhow::anyhow!("Native maintenance decisions absent")
                        })?,
                    )?;
                Self::require_recorded_front(witness, actions.describe())?;
                if let Some(due) = actions.reconciliation {
                    let config = &self.config.exec_engine;
                    anyhow::ensure!(
                        (!due.inflight || config.inflight_check_interval_ms > 0)
                            && (!due.open
                                || config
                                    .open_check_interval_secs
                                    .is_some_and(|value| value > 0.0))
                            && (!due.position
                                || config
                                    .position_check_interval_secs
                                    .is_some_and(|value| value > 0.0)),
                        "Recorded maintenance requests a disabled native check"
                    );
                }
                let _scope = RuntimeDispatchScope::enter(|| actions.describe());
                if let Some(due) = actions.reconciliation {
                    self.apply_reconciliation_due(
                        due,
                        &mut tasks.open,
                        tasks.targeted.as_ref(),
                        &mut tasks.position,
                    );
                }

                if actions.purge.orders {
                    self.exec_manager.purge_closed_orders();
                }

                if actions.purge.positions {
                    self.exec_manager.purge_closed_positions();
                }

                if actions.purge.accounts {
                    self.exec_manager.purge_account_events();
                }

                if actions.audit_books {
                    self.kernel.cache.borrow_mut().audit_own_order_books();
                }

                if actions.prune_fills {
                    self.exec_manager.prune_recent_fills_cache(60.0);
                    self.exec_manager.prune_processed_fills();
                    self.exec_manager.prune_order_local_activity();
                }
            }
            Some("open_report") => {
                let task = tasks
                    .open
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("Native open-order report task absent"))?;
                let result = task.future.as_mut().now_or_never().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Native open-order report remains pending at recorded completion"
                    )
                })?;
                let actual = describe_open_outcome(&result);
                Self::require_recorded_front(witness, actual.clone())?;
                let _scope = RuntimeDispatchScope::enter(|| actual);
                drop(tasks.open.take());
                tasks.targeted = self.apply_open_order_report_outcome(result, false);
            }
            Some("targeted_report") => {
                let task = tasks
                    .targeted
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("Native targeted report task absent"))?;
                let planned = task.planned_client_order_ids.clone();
                let result = task.future.as_mut().now_or_never().ok_or_else(|| {
                    anyhow::anyhow!("Native targeted report remains pending at recorded completion")
                })?;
                let actual = describe_targeted_outcome(&result);
                Self::require_recorded_front(witness, actual.clone())?;
                let _scope = RuntimeDispatchScope::enter(|| actual);
                drop(tasks.targeted.take());
                self.apply_targeted_order_report_outcome(result, &planned);
            }
            Some("position_report") => {
                let task = tasks
                    .position
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("Native position report task absent"))?;
                let result = task.future.as_mut().now_or_never().ok_or_else(|| {
                    anyhow::anyhow!("Native position report remains pending at recorded completion")
                })?;
                let actual = describe_position_outcome(&result);
                Self::require_recorded_front(witness, actual.clone())?;
                let _scope = RuntimeDispatchScope::enter(|| actual);
                drop(tasks.position.take());
                tasks.position = self.apply_position_report_outcome(result, false);
            }
            _ => anyhow::bail!("Unsupported native maintenance completion"),
        }
        Ok(())
    }
}
