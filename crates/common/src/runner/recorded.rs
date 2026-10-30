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

//! Witnesses for native runner queue consumption during recorded execution.

use std::cell::RefCell;

use ahash::AHashMap;
use nautilus_model::data::DataRef;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{TimeEventMessage, TradingCommandMessage};
use crate::{
    messages::{DataEvent, ExecutionEvent, data::DataCommand},
    signal::Signal,
};

/// An existing native owner-thread runner channel or callback boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordedChannel {
    TimeEvents,
    ExecutionEvents,
    ExecutionCommands,
    DataEvents,
    DataCommands,
    ExternalMessages,
    SystemEvents,
    SystemCommands,
    Callbacks,
    Maintenance,
    Lifecycle,
}

/// A queue-front witness. It contains no executable financial event.
///
/// Semantic matching preserves economic and routing fields while excluding generated
/// timestamps and opaque execution IDs. Original payloads and generated outputs are never
/// rewritten; their complete identities remain in the ordinary native journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedRunnerDispatch {
    /// Existing native channel which consumed this work.
    pub channel: RecordedChannel,
    /// Monotonic consumption ordinal within this channel and node run.
    pub ordinal: u64,
    /// Routing and economic content used to correlate recomputed work.
    pub semantic: Value,
    /// Whether this boundary is supported for recorded execution.
    pub supported: bool,
}

#[derive(Default)]
struct Capture {
    enabled: bool,
    depth: usize,
    ordinals: AHashMap<RecordedChannel, u64>,
    startup: Option<(Vec<RecordedRunnerDispatch>, usize)>,
    failure: Option<String>,
}
thread_local! {static CAPTURE:RefCell<Capture> = RefCell::new(Capture::default());}

/// Enables capture for a fresh native runner, resetting only diagnostic ordinals.
pub fn enable_capture() {
    CAPTURE.with(|state| {
        *state.borrow_mut() = Capture {
            enabled: true,
            ..Default::default()
        }
    });
}
/// Disables native runner dispatch capture on this thread.
pub fn disable_capture() {
    CAPTURE.with(|state| state.borrow_mut().enabled = false);
}
/// Returns whether native dispatch capture is enabled.
#[must_use]
pub fn capture_enabled() -> bool {
    CAPTURE.with(|state| state.borrow().enabled)
}
/// Returns the next native dispatch ordinal for this channel.
///
/// # Panics
///
/// Panics if the checked native dispatch ordinal is exhausted.
#[must_use]
pub fn next_ordinal(channel: RecordedChannel) -> u64 {
    CAPTURE.with(|state| {
        state
            .borrow()
            .ordinals
            .get(&channel)
            .copied()
            .unwrap_or_default()
            .checked_add(1)
            .expect("Native dispatch ordinal exhausted")
    })
}

/// Completion of one captured native dispatch. An absent completion leaves an open boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedRunnerCompletion {
    /// Channel of the completed native dispatch.
    pub channel: RecordedChannel,
    /// Ordinal of the completed native dispatch.
    pub ordinal: u64,
}

/// Prevents nested helper calls from recording one consumed queue item twice.
#[derive(Debug)]
pub struct RuntimeDispatchScope {
    active: bool,
    outer: Option<RecordedRunnerCompletion>,
}
impl RuntimeDispatchScope {
    /// Captures one actual native queue dispatch when enabled.
    ///
    /// # Panics
    ///
    /// Panics if the checked native dispatch ordinal is exhausted.
    #[must_use]
    pub fn enter(describe: impl FnOnce() -> RecordedRunnerDispatch) -> Self {
        let capture = CAPTURE.with(|state| {
            let mut state = state.borrow_mut();
            if !state.enabled {
                return None;
            }
            let outer = state.depth == 0;
            state.depth += 1;
            Some(outer)
        });
        let mut scope = Self {
            active: capture.is_some(),
            outer: None,
        };

        if capture == Some(true) {
            let mut witness = describe();
            witness.ordinal = CAPTURE.with(|state| {
                let mut state = state.borrow_mut();
                let ordinal = state.ordinals.entry(witness.channel).or_default();
                *ordinal = ordinal
                    .checked_add(1)
                    .expect("Native dispatch ordinal exhausted");
                *ordinal
            });
            CAPTURE.with(|state| {
                let mut state = state.borrow_mut();
                if let Some((expected, index)) = &mut state.startup {
                    let matches = expected.get(*index) == Some(&witness);
                    *index += 1;

                    if !matches && state.failure.is_none() {
                        state.failure = Some(
                            "Native startup dispatch differs from the recorded source".to_string(),
                        );
                    }
                }
            });
            scope.outer = Some(RecordedRunnerCompletion {
                channel: witness.channel,
                ordinal: witness.ordinal,
            });
            crate::msgbus::dispatch_tap_publish("events.runtime.dispatch".into(), &witness);
        }
        scope
    }
}
impl Drop for RuntimeDispatchScope {
    fn drop(&mut self) {
        if self.active {
            if !std::thread::panicking()
                && let Some(completion) = &self.outer
            {
                crate::msgbus::dispatch_tap_publish(
                    "events.runtime.dispatch_complete".into(),
                    completion,
                );
            }
            CAPTURE.with(|state| state.borrow_mut().depth -= 1);
        }
    }
}

/// Records an explicitly excluded source-only callback without executing it.
///
/// The native recorded owner validates the declared actor role and registration first.
///
/// # Errors
///
/// Returns an error for a nonmonotonic or unsupported witness.
pub fn observe_excluded(witness: &RecordedRunnerDispatch) -> anyhow::Result<()> {
    anyhow::ensure!(
        witness.supported && witness.ordinal == next_ordinal(witness.channel),
        "Excluded dispatch does not match native source ordering"
    );
    CAPTURE.with(|state| {
        state
            .borrow_mut()
            .ordinals
            .insert(witness.channel, witness.ordinal)
    });
    Ok(())
}

fn normalize(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            fields.retain(|key, _| {
                !key.starts_with("ts_")
                    && !matches!(
                        key.as_str(),
                        "event_id"
                            | "report_id"
                            | "command_id"
                            | "request_id"
                            | "correlation_id"
                            | "causation_id"
                            | "client_order_id"
                            | "venue_order_id"
                            | "position_id"
                            | "trade_id"
                            | "order_list_id"
                            | "parent_order_id"
                            | "linked_order_ids"
                            | "exec_spawn_id"
                    )
            });

            for value in fields.values_mut() {
                normalize(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                normalize(value);
            }
        }
        _ => {}
    }
}
fn frame(channel: RecordedChannel, semantic: Value, supported: bool) -> RecordedRunnerDispatch {
    RecordedRunnerDispatch {
        channel,
        ordinal: 0,
        semantic,
        supported,
    }
}
fn serialized<T: Serialize>(channel: RecordedChannel, value: &T) -> RecordedRunnerDispatch {
    match serde_json::to_value(value) {
        Ok(mut value) => {
            normalize(&mut value);
            frame(channel, value, true)
        }
        Err(e) => frame(channel, json!({"unsupported":e.to_string()}), false),
    }
}

fn original<T: Serialize>(channel: RecordedChannel, value: &T) -> RecordedRunnerDispatch {
    match serde_json::to_value(value) {
        Ok(value) => frame(channel, value, true),
        Err(e) => frame(channel, json!({"unsupported":e.to_string()}), false),
    }
}

/// Describes a recomputed command at its native direct endpoint.
#[must_use]
pub fn describe_trading_command(message: &TradingCommandMessage) -> RecordedRunnerDispatch {
    let mut witness = serialized(RecordedChannel::ExecutionCommands, message.command());
    witness.semantic =
        json!({"endpoint":message.endpoint().to_string(),"command":witness.semantic});
    witness
}
/// Describes a native data-engine command.
#[must_use]
pub fn describe_data_command(command: &DataCommand) -> RecordedRunnerDispatch {
    match command {
        DataCommand::Subscribe(value) => serialized(RecordedChannel::DataCommands, value),
        DataCommand::Unsubscribe(value) => serialized(RecordedChannel::DataCommands, value),
        DataCommand::Request(value) => serialized(RecordedChannel::DataCommands, value),
        #[cfg(feature = "defi")]
        _ => frame(
            RecordedChannel::DataCommands,
            json!({"unsupported":"data command"}),
            false,
        ),
    }
}
/// Describes a recomputed execution event at its native FIFO front.
#[must_use]
pub fn describe_execution_event(event: &ExecutionEvent) -> RecordedRunnerDispatch {
    match event {
        ExecutionEvent::Order(value) => serialized(RecordedChannel::ExecutionEvents, value),
        ExecutionEvent::Account(value) => serialized(RecordedChannel::ExecutionEvents, value),
        ExecutionEvent::StrategySettlement(value) => describe_strategy_settlement(value),
        _ => frame(
            RecordedChannel::ExecutionEvents,
            json!({"unsupported":"execution batch/report"}),
            false,
        ),
    }
}
/// Describes a native execution FIFO settlement marker.
#[must_use]
pub fn describe_strategy_settlement(
    marker: &crate::messages::execution::StrategySettlementMarker,
) -> RecordedRunnerDispatch {
    serialized(
        RecordedChannel::ExecutionEvents,
        &json!({"strategy_settlement":marker.query}),
    )
}
/// Describes a channel form outside the supported recorded scope.
#[must_use]
pub fn describe_unsupported(channel: RecordedChannel, reason: &str) -> RecordedRunnerDispatch {
    frame(channel, json!({"unsupported":reason}), false)
}

/// Describes a native timer message without using an event UUID as a callback identity.
#[must_use]
pub fn describe_time_event(message: &TimeEventMessage) -> RecordedRunnerDispatch {
    frame(
        RecordedChannel::TimeEvents,
        json!({"name":message.event().name}),
        true,
    )
}

fn describe_data(data: DataRef<'_>) -> RecordedRunnerDispatch {
    let channel = RecordedChannel::DataEvents;
    let mut witness = match data {
        DataRef::Instrument(value) => original(channel, value),
        DataRef::BookDelta(value) => original(channel, value),
        DataRef::BookDeltas(value) => original(channel, value),
        DataRef::BookDepth(value) => original(channel, value),
        DataRef::Quote(value) => original(channel, value),
        DataRef::Trade(value) => original(channel, value),
        DataRef::Bar(value) => original(channel, value),
        DataRef::MarkPrice(value) => original(channel, value),
        DataRef::IndexPrice(value) => original(channel, value),
        DataRef::FundingRate(value) => original(channel, value),
        DataRef::InstrumentStatus(value) => original(channel, value),
        DataRef::InstrumentClose(value) => original(channel, value),
        DataRef::Custom(value) => value.data.as_any().downcast_ref::<Signal>().map_or_else(
            || frame(channel, json!({"unsupported":"custom data"}), false),
            |signal| original(channel, signal),
        ),
        DataRef::OptionGreeks(_) => frame(channel, json!({"unsupported":"data kind"}), false),
        #[cfg(feature = "defi")]
        DataRef::Defi(_) => frame(channel, json!({"unsupported":"defi data"}), false),
    };
    let kind = match data {
        DataRef::Custom(value) => value.data_type.type_name().to_string(),
        _ => format!("{:?}", std::mem::discriminant(&data)),
    };
    witness.semantic = json!({"kind":kind,"data":witness.semantic});
    witness
}
/// Describes original normalized data at the native `DataEngine` owner boundary.
#[must_use]
pub fn describe_data_event(event: &DataEvent) -> RecordedRunnerDispatch {
    match event {
        DataEvent::Data(value) => describe_data(DataRef::from(value)),
        DataEvent::Instrument(value) => describe_data(DataRef::Instrument(value)),
        DataEvent::FundingRate(value) => describe_data(DataRef::FundingRate(value)),
        DataEvent::InstrumentStatus(value) => describe_data(DataRef::InstrumentStatus(value)),
        _ => frame(
            RecordedChannel::DataEvents,
            json!({"unsupported":"data response or event"}),
            false,
        ),
    }
}
/// Describes one standard native callback-drain boundary.
#[must_use]
pub fn describe_callbacks(budget: usize) -> RecordedRunnerDispatch {
    frame(RecordedChannel::Callbacks, json!({"budget":budget}), true)
}

/// Describes maintenance with no due financial/reconciliation operation.
#[must_use]
pub fn describe_maintenance(no_actions_due: bool) -> RecordedRunnerDispatch {
    frame(
        RecordedChannel::Maintenance,
        json!({"no_actions_due":no_actions_due}),
        no_actions_due,
    )
}

/// Installs immutable startup witnesses for validation of standard native startup.
pub fn begin_startup_validation(expected: Vec<RecordedRunnerDispatch>) {
    enable_capture();
    CAPTURE.with(|state| state.borrow_mut().startup = Some((expected, 0)));
}
/// Completes startup validation without replaying already consumed native work.
///
/// # Errors
///
/// Returns an error if native startup differs or leaves an original witness unconsumed.
pub fn finish_startup_validation() -> anyhow::Result<()> {
    CAPTURE.with(|state| {
        let mut state = state.borrow_mut();
        if let Some(e) = state.failure.take() {
            anyhow::bail!(e);
        }
        let (expected, index) = state
            .startup
            .take()
            .ok_or_else(|| anyhow::anyhow!("Startup validation was not installed"))?;
        anyhow::ensure!(
            index == expected.len(),
            "Native startup did not consume every original witness"
        );
        Ok(())
    })
}
/// Describes the actual transition into the native Running phase.
#[must_use]
pub fn describe_started(value: Value) -> RecordedRunnerDispatch {
    frame(RecordedChannel::Lifecycle, value, true)
}

/// Describes an original external bus message without changing its native encoding.
///
/// Only market data and native Signals are original inputs in this recorded scope.
/// Financial events, commands and arbitrary custom decoder forms remain unsupported.
#[must_use]
pub fn describe_external(message: &crate::msgbus::BusMessage) -> RecordedRunnerDispatch {
    use crate::msgbus::BusPayloadType;
    let supported = match message.payload_type {
        BusPayloadType::Instrument
        | BusPayloadType::OrderBookDeltas
        | BusPayloadType::OrderBookDepth
        | BusPayloadType::QuoteTick
        | BusPayloadType::TradeTick
        | BusPayloadType::Bar
        | BusPayloadType::MarkPriceUpdate
        | BusPayloadType::IndexPriceUpdate
        | BusPayloadType::FundingRateUpdate => true,
        BusPayloadType::Custom(name) => name.as_str() == "Signal",
        _ => false,
    };
    RecordedRunnerDispatch {
        channel: RecordedChannel::ExternalMessages,
        ordinal: 0,
        semantic: serde_json::to_value(message).unwrap_or(Value::Null),
        supported,
    }
}

/// Describes a recomputed native reconciliation completion, excluding generated identities.
#[must_use]
pub fn describe_reconciliation(
    operation: &str,
    value: &Value,
    supported: bool,
) -> RecordedRunnerDispatch {
    let mut witness = serialized(RecordedChannel::Maintenance, value);
    witness.supported &= supported;

    // Bulk client reports describe an unordered collection; native application keeps its order
    if matches!(operation, "open_report" | "position_report")
        && let Some(Value::Array(reports)) = witness.semantic.get_mut("reports")
    {
        reports.sort_by_key(Value::to_string);
    }
    witness.semantic = json!({"operation":operation,"result":witness.semantic});
    witness
}

/// Returns whether native recorded startup is currently validating its actual inputs.
#[must_use]
pub fn startup_validation_active() -> bool {
    CAPTURE.with(|state| {
        let state = state.borrow();
        state.enabled && state.startup.is_some()
    })
}
