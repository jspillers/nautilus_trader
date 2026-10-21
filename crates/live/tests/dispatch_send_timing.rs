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

//! Diagnostic state follows the native runner through builder/start/run rebinding.
use nautilus_common::{
    live::{dispatch::current_dispatch_send_timing, runner::get_data_event_sender},
    messages::DataEvent,
};
use nautilus_live::runner::AsyncRunner;
use nautilus_model::data::{Data, QuoteTick};
use rstest::rstest;

fn input() -> DataEvent {
    DataEvent::Data(Data::Quote(QuoteTick::new(
        "TIMING.SIM".into(),
        "0.40".into(),
        "0.41".into(),
        "1".into(),
        "1".into(),
        0.into(),
        0.into(),
    )))
}

#[rstest]
fn timing_survives_all_native_bindings_and_preexisting_client_handles() {
    let runner = AsyncRunner::new();
    runner.bind_senders();
    let adapter = get_data_event_sender();
    runner.bind_senders();
    let observer = get_data_event_sender();
    assert!(observer.enable_send_timing());
    runner.bind_senders();
    let actor = get_data_event_sender();
    assert!(adapter.same_channel(&actor));
    adapter.send(input()).unwrap();
    actor.send(input()).unwrap();
    let mut channels = runner.take_channels();
    for expected in 1..=2 {
        channels.data_evt_rx.try_recv().unwrap().dispatch(|event| {
            assert!(matches!(event, DataEvent::Data(Data::Quote(_))));
            let timing = current_dispatch_send_timing().unwrap();
            assert_eq!(timing.ordinal, expected);
            assert!(timing.send_called_ns > 0);
            assert!(timing.dispatch_started_ns >= timing.send_called_ns);
        });
        assert!(current_dispatch_send_timing().is_none());
    }
    assert_eq!(observer.send_timing_counts(), (2, 0));
}

#[rstest]
fn another_runner_cannot_reset_or_inherit_the_original_diagnostic_state() {
    let original = AsyncRunner::new();
    original.bind_senders();
    let observer = get_data_event_sender();
    observer.enable_send_timing();
    let other = AsyncRunner::new();
    other.bind_senders();
    let other_sender = get_data_event_sender();
    other_sender.send(input()).unwrap();
    original.bind_senders();
    get_data_event_sender().send(input()).unwrap();
    assert_eq!(observer.send_timing_counts(), (1, 0));
    assert_eq!(other_sender.send_timing_counts(), (0, 0));
    other
        .take_channels()
        .data_evt_rx
        .try_recv()
        .unwrap()
        .dispatch(|_| {
            assert!(current_dispatch_send_timing().is_none());
        });
    original
        .take_channels()
        .data_evt_rx
        .try_recv()
        .unwrap()
        .dispatch(|_| {
            assert_eq!(current_dispatch_send_timing().unwrap().ordinal, 1);
        });
}

#[rstest]
fn repeated_binding_keeps_unrequested_observations_disabled() {
    let runner = AsyncRunner::new();
    runner.bind_senders();
    let original = get_data_event_sender();
    runner.bind_senders();
    let rebound = get_data_event_sender();
    assert!(original.same_channel(&rebound));
    rebound.send(input()).unwrap();
    runner
        .take_channels()
        .data_evt_rx
        .try_recv()
        .unwrap()
        .dispatch(|_| {
            assert!(current_dispatch_send_timing().is_none());
        });
    assert_eq!(original.send_timing_counts(), (0, 0));
}
