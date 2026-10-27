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

use nautilus_common::{actor::registry::ActorRegistration, timer::TimerBinding};
use rstest::rstest;

use super::*;

#[rstest]
fn native_timer_dispatch_round_trips_without_deduplicating_the_original_time_event() {
    let registry = default_registry();
    let event = TimeEvent::new("owned".into(), UUID4::new(), 10.into(), 11.into());
    let owner = ActorRegistration {
        actor_id: "OWNER-001".into(),
        registration_id: UUID4::new(),
        generation: 2,
    };
    let dispatch = RecordedTimeEventDispatch {
        event: event.clone(),
        binding: TimerBinding {
            target: Some(owner.actor_id),
            owner: Some(owner),
            execution_owner: None,
            callback_generation: 3,
            timer_generation: Some(5),
        },
        actual_owner: Some(owner),
        actual_execution_owner: None,
    };
    let (tag, encoded) = registry.encode(&dispatch).unwrap().unwrap();
    let decoded: RecordedTimeEventDispatch = rmp_serde::from_slice(&encoded.payload).unwrap();
    assert_eq!(tag.as_str(), PAYLOAD_TYPE_TIME_EVENT_DISPATCH);
    assert_eq!(decoded, dispatch);
    assert!(encoded.index_keys.is_empty());
    assert_eq!(registry.identity_for_any(&dispatch), None);
    assert_eq!(
        registry.encode(&event).unwrap().unwrap().0.as_str(),
        PAYLOAD_TYPE_TIME_EVENT
    );
    assert!(decoded.has_current_native_owner());
}

#[rstest]
fn native_runner_dispatch_and_completion_round_trip_as_distinct_forensic_facts() {
    use nautilus_common::runner::recorded::RecordedChannel;

    let registry = default_registry();
    let dispatch = RecordedRunnerDispatch {
        channel: RecordedChannel::ExecutionEvents,
        ordinal: 7,
        semantic: serde_json::json!({"type":"OrderFilled","last_qty":"1"}),
        supported: true,
    };
    let completion = RecordedRunnerCompletion {
        channel: dispatch.channel,
        ordinal: dispatch.ordinal,
    };
    let (tag, encoded) = registry.encode(&dispatch).unwrap().unwrap();
    assert_eq!(tag.as_str(), PAYLOAD_TYPE_RUNTIME_DISPATCH);
    let decoded: RecordedRunnerDispatch = rmp_serde::from_slice(&encoded.payload).unwrap();
    assert_eq!(decoded, dispatch);
    assert!(encoded.index_keys.is_empty());
    assert_eq!(registry.identity_for_any(&dispatch), None);
    let (tag, encoded) = registry.encode(&completion).unwrap().unwrap();
    assert_eq!(tag.as_str(), PAYLOAD_TYPE_RUNTIME_DISPATCH_COMPLETE);
    let decoded: RecordedRunnerCompletion = rmp_serde::from_slice(&encoded.payload).unwrap();
    assert_eq!(decoded, completion);
    assert!(encoded.index_keys.is_empty());
    assert_eq!(registry.identity_for_any(&completion), None);
}

#[rstest]
fn bootstrap_load_witness_round_trips_as_source_evidence_without_identity_or_order_indexes() {
    use nautilus_common::cache::bootstrap::BootstrapLoads;
    let witness = BootstrapLoadWitness {
        native_instance_id: UUID4::new(),
        trader_id: "BOOTSTRAP-001".into(),
        observed_at: 42.into(),
        completed: false,
        cache_backing: true,
        load_cache: true,
        load_state: true,
        save_state: true,
        flush_on_start: false,
        parent_run_id: None,
        actor_ids: vec![],
        strategy_ids: vec![],
        execution_client_ids: vec!["XNAS".to_string()],
        loads: BootstrapLoads {
            actor_ids: vec![],
            strategy_ids: vec![],
            reads: vec![],
            cache_rebuilt: false,
            cache_integrity: None,
            component_states_loaded: false,
            overflowed: false,
        },
    };
    let registry = default_registry();
    let (tag, encoded) = registry.encode(&witness).unwrap().unwrap();
    assert_eq!(tag.as_str(), PAYLOAD_TYPE_BOOTSTRAP_LOAD);
    let decoded: BootstrapLoadWitness = rmp_serde::from_slice(&encoded.payload).unwrap();
    assert_eq!(decoded, witness);
    assert!(!decoded.is_complete_empty());
    assert!(encoded.index_keys.is_empty());
    assert_eq!(registry.identity_for_any(&witness), None);
}
