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

use nautilus_common::runner::recorded::{self, RecordedRunnerDispatch};
use nautilus_model::data::{Data, QuoteTick};
use rstest::rstest;

use super::*;

fn original() -> DataEvent {
    DataEvent::Data(Data::Quote(QuoteTick::new(
        "AAPL.XNAS".into(),
        "9.00".into(),
        "10.00".into(),
        "1".into(),
        "1".into(),
        1.into(),
        2.into(),
    )))
}
fn expected(event: &DataEvent) -> RecordedRunnerDispatch {
    let mut expected = recorded::describe_data_event(event);
    expected.ordinal = recorded::next_ordinal(expected.channel);
    expected
}

#[rstest]
fn wrong_recorded_front_remains_owned_without_dispatch_or_consumption() {
    recorded::enable_capture();
    let mut runner = AsyncRunner::new();
    let event = original();
    let expected = expected(&event);
    runner.data_evt_tx.send(event).unwrap();
    let mut wrong = expected.clone();
    wrong.semantic = serde_json::json!({"wrong":"front"});
    assert!(runner.take_recorded(&wrong).is_err());
    assert!(matches!(
        runner.recorded_front,
        Some(PendingRunnerEvent::DataEvent(_))
    ));
    assert!(!runner.recorded_external_empty());
    assert_eq!(recorded::next_ordinal(expected.channel), expected.ordinal);
    let front = runner.take_recorded(&expected).unwrap();
    let PendingRunnerEvent::DataEvent(front) = front else {
        panic!("original native front missing")
    };
    assert_eq!(
        recorded::describe_data_event(front.message()).semantic,
        expected.semantic
    );
    assert!(runner.recorded_front.is_none());
    assert!(runner.recorded_external_empty());
    assert!(runner.take_recorded(&expected).is_err());
    recorded::disable_capture();
}

#[rstest]
fn missing_native_dispatch_cannot_be_replaced_by_a_recorded_output() {
    recorded::enable_capture();
    let mut runner = AsyncRunner::new();
    let expected = expected(&original());
    assert!(
        runner
            .take_recorded(&expected)
            .err()
            .unwrap()
            .to_string()
            .contains("front is absent")
    );
    assert!(runner.recorded_front.is_none());
    assert!(runner.recorded_external_empty());
    assert_eq!(recorded::next_ordinal(expected.channel), expected.ordinal);
    recorded::disable_capture();
}

#[rstest]
fn recorded_time_admission_preserves_pending_data_and_its_exact_front() {
    recorded::enable_capture();
    let mut runner = AsyncRunner::new();
    let first = original();
    let expected = expected(&first);
    runner.data_evt_tx.send(first).unwrap();
    assert!(!runner.recorded_external_empty());
    assert!(runner.recorded_time_empty());
    let mut wrong = expected.clone();
    wrong.semantic = serde_json::json!({"wrong":"front"});
    assert!(runner.take_recorded(&wrong).is_err());
    assert!(!runner.recorded_external_empty());
    assert!(runner.recorded_time_empty());
    assert_eq!(recorded::next_ordinal(expected.channel), expected.ordinal);
    let actual = runner.take_recorded(&expected).unwrap();
    let PendingRunnerEvent::DataEvent(actual) = actual else {
        panic!("original native front missing")
    };
    assert_eq!(
        recorded::describe_data_event(actual.message()).semantic,
        expected.semantic
    );
    assert!(runner.recorded_external_empty());
    recorded::disable_capture();
}
