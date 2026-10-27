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

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_core::{DurationNanos, UUID4};
use rstest::rstest;

use super::*;
use crate::{
    actor::{
        Actor,
        registry::{
            ActorRegistration, actor_registration, deregister_actor, register_actor,
            try_get_actor_unchecked,
        },
    },
    clock::{Clock, VirtualClock},
    msgbus::{self, BusTap, Endpoint, MStr, Topic},
    runner::TimeEventMessage,
};

#[derive(Debug)]
struct Owner {
    id: Ustr,
    calls: Rc<RefCell<Vec<TimeEvent>>>,
}
impl Actor for Owner {
    fn id(&self) -> Ustr {
        self.id
    }
    fn handle(&mut self, _: &dyn Any) {}
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
struct Tap(RefCell<Vec<RecordedTimeEventDispatch>>);
impl BusTap for Tap {
    fn on_publish(&self, _: MStr<Topic>, value: &dyn Any) {
        if let Some(dispatch) = value.downcast_ref::<RecordedTimeEventDispatch>() {
            self.0.borrow_mut().push(dispatch.clone());
        }
    }
    fn on_send(&self, _: MStr<Endpoint>, _: &dyn Any) {}
}

fn owner(id: &str) -> (ActorRegistration, Rc<RefCell<Vec<TimeEvent>>>) {
    let id = Ustr::from(id);
    let calls = Rc::new(RefCell::new(Vec::new()));
    register_actor(Owner {
        id,
        calls: calls.clone(),
    });
    (actor_registration(&id).unwrap(), calls)
}

fn clock(owner: ActorRegistration) -> VirtualClock {
    let mut clock = VirtualClock::new();
    assert!(clock.bind_actor_registration(owner));
    let id = owner.actor_id;
    let callback = TimeEventCallback::RustLocal(Rc::new(move |event| {
        if let Some(actor) = try_get_actor_unchecked::<Owner>(&id) {
            actor.calls.borrow_mut().push(event);
        }
    }))
    .with_native_target(id);
    clock.register_default_handler(callback);
    clock
        .set_timer_ns(
            "recorded",
            DurationNanos::new(10),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    clock
}

fn dispatch(clock: &VirtualClock, owner: ActorRegistration) -> RecordedTimeEventDispatch {
    let event = TimeEvent::new("recorded".into(), UUID4::new(), 10.into(), 11.into());
    let handler = clock.match_handlers(vec![event.clone()]).pop().unwrap();
    RecordedTimeEventDispatch {
        event,
        binding: handler.callback.binding().unwrap().clone(),
        actual_owner: Some(owner),
        actual_execution_owner: None,
    }
}

#[rstest]
fn recorded_timer_resolves_original_payload_and_fresh_registration_uuid() {
    let (registered, calls) = owner("recorded-positive");
    let mut clock = clock(registered);
    let mut dispatch = dispatch(&clock, registered);
    let mut source = registered;
    source.registration_id = UUID4::new();
    dispatch.binding.owner = Some(source);
    dispatch.actual_owner = Some(source);
    let original = dispatch.event.clone();
    clock.set_time(20.into());
    clock.recorded_time_event(&dispatch).unwrap().run();
    assert_eq!(*calls.borrow(), vec![original]);
    assert_eq!(clock.next_time_ns("recorded"), Some(20.into()));
    assert_eq!(clock.timestamp_ns(), 20);
    deregister_actor(&registered.actor_id);
}

#[rstest]
#[case("callback")]
#[case("timer")]
#[case("owner")]
#[case("actual")]
#[case("unbound")]
fn recorded_timer_rejects_binding_mismatch_without_consuming(#[case] mismatch: &str) {
    let (registered, calls) = owner("recorded-mismatch");
    let mut clock = clock(registered);
    let mut dispatch = dispatch(&clock, registered);
    match mismatch {
        "callback" => dispatch.binding.callback_generation += 1,
        "timer" => dispatch.binding.timer_generation = Some(999),
        "owner" => dispatch.binding.owner.as_mut().unwrap().generation += 1,
        "actual" => dispatch.actual_owner.as_mut().unwrap().registration_id = UUID4::new(),
        "unbound" => dispatch.binding.target = None,
        _ => unreachable!(),
    }
    assert!(clock.recorded_time_event(&dispatch).is_err());
    assert!(calls.borrow().is_empty());
    assert_eq!(clock.next_time_ns("recorded"), Some(10.into()));
    deregister_actor(&registered.actor_id);
}

#[rstest]
fn queued_native_callback_witness_preserves_lease_and_actual_replacement() {
    let (first, old_calls) = owner("recorded-replaced");
    let mut clock = clock(first);
    let events = clock.advance_time(10.into(), true);
    let handler = clock.match_handlers(events).pop().unwrap();
    let queued = TimeEventMessage::new(handler.event, handler.callback);
    clock.cancel_timers();
    let (replacement, new_calls) = owner("recorded-replaced");
    assert_eq!(replacement.generation, first.generation + 1);
    let tap = Rc::new(Tap::default());
    msgbus::set_bus_tap(tap.clone());
    assert!(queued.dispatch());
    msgbus::clear_bus_tap();
    assert!(old_calls.borrow().is_empty());
    assert_eq!(new_calls.borrow().len(), 1);
    let witnesses = tap.0.borrow();
    assert_eq!(witnesses.len(), 1);
    assert_eq!(witnesses[0].binding.owner, Some(first));
    assert_eq!(witnesses[0].actual_owner, Some(replacement));
    assert!(!witnesses[0].has_current_native_owner());
    drop(witnesses);
    deregister_actor(&replacement.actor_id);
}

#[rstest]
fn recorded_timer_rejects_cancelled_and_replaced_native_timer() {
    let (registered, calls) = owner("recorded-timer-replaced");
    let mut clock = clock(registered);
    let dispatch = dispatch(&clock, registered);
    clock.cancel_timer("recorded");
    assert!(clock.recorded_time_event(&dispatch).is_err());
    clock
        .set_timer_ns(
            "recorded",
            DurationNanos::new(10),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    assert!(clock.recorded_time_event(&dispatch).is_err());
    assert!(calls.borrow().is_empty());
    deregister_actor(&registered.actor_id);
}

#[rstest]
fn recorded_timer_rejects_unbound_runtime_without_consuming_or_panicking() {
    let (registered, calls) = owner("recorded-runtime-unbound");
    let source_clock = clock(registered);
    let dispatch = dispatch(&source_clock, registered);
    let mut runtime_clock = VirtualClock::new();
    runtime_clock.register_default_handler(TimeEventCallback::from(|_| {}));
    runtime_clock
        .set_timer_ns(
            "recorded",
            DurationNanos::new(10),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    let result = runtime_clock.recorded_time_event(&dispatch);
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("no native routing metadata")
    );
    assert_eq!(runtime_clock.next_time_ns("recorded"), Some(10.into()));
    assert!(calls.borrow().is_empty());
    deregister_actor(&registered.actor_id);
}

#[rstest]
fn client_clock_dispatch_retains_native_registration_and_rejects_retirement() {
    use crate::clock::{ExecutionClockLease, ExecutionClockRegistration};
    let lease = ExecutionClockLease::new(ExecutionClockRegistration {
        client_id: "RECORDED-CLIENT".into(),
        registration_id: UUID4::new(),
        generation: 1,
    });
    let mut clock = VirtualClock::new();
    assert!(clock.bind_execution_registration(lease.clone()));
    let calls = Rc::new(RefCell::new(Vec::new()));
    let capture = calls.clone();
    clock.register_default_handler(TimeEventCallback::RustLocal(Rc::new(move |event| {
        capture.borrow_mut().push(event);
    })));
    clock
        .set_timer_ns(
            "client-arrival",
            DurationNanos::new(10),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    let event = TimeEvent::new("client-arrival".into(), UUID4::new(), 10.into(), 11.into());
    let handler = clock.match_handlers(vec![event.clone()]).pop().unwrap();
    let dispatch = RecordedTimeEventDispatch {
        event,
        binding: handler.callback.binding().unwrap().clone(),
        actual_owner: None,
        actual_execution_owner: Some(lease.registration()),
    };
    assert!(dispatch.has_current_native_owner());
    clock.recorded_time_event(&dispatch).unwrap().run();
    assert_eq!(calls.borrow().len(), 1);
    let next = clock.next_time_ns("client-arrival");
    lease.retire();
    assert!(clock.recorded_time_event(&dispatch).is_err());
    assert_eq!(clock.next_time_ns("client-arrival"), next);
    let tap = Rc::new(Tap::default());
    msgbus::set_bus_tap(tap.clone());
    handler.run();
    msgbus::clear_bus_tap();
    assert_eq!(tap.0.borrow()[0].actual_execution_owner, None);
    assert!(!tap.0.borrow()[0].has_current_native_owner());
}

#[cfg(feature = "live")]
#[rstest]
#[case(false)]
#[case(true)]
fn explicit_live_callbacks_keep_the_execution_clock_lease(#[case] repeating: bool) {
    use std::sync::{Arc, mpsc};

    use crate::{
        clock::{ExecutionClockLease, ExecutionClockRegistration},
        live::clock::LiveClock,
        runner::TimeEventSender,
    };

    #[derive(Debug)]
    struct Sender(mpsc::Sender<TimeEventMessage>);
    impl TimeEventSender for Sender {
        fn send(&self, message: TimeEventMessage) {
            let _ = self.0.send(message);
        }
    }

    let (sender, receiver) = mpsc::channel();
    let mut clock = LiveClock::new(Some(Arc::new(Sender(sender))));
    let lease = ExecutionClockLease::new(ExecutionClockRegistration {
        client_id: "EXPLICIT-CLIENT".into(),
        registration_id: UUID4::new(),
        generation: 1,
    });
    assert!(clock.bind_execution_registration(lease.clone()));
    let tap = Rc::new(Tap::default());
    msgbus::set_bus_tap(tap.clone());
    let callback = TimeEventCallback::RustLocal(Rc::new(|_| {}));

    if repeating {
        clock
            .set_timer_ns(
                "explicit",
                DurationNanos::from_millis(10),
                None,
                None,
                Some(callback),
                None,
                None,
            )
            .unwrap();
    } else {
        let alert = clock.timestamp_ns() + DurationNanos::from_millis(10);
        clock
            .set_time_alert_ns("explicit", alert, Some(callback), None)
            .unwrap();
    }
    assert!(
        receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap()
            .dispatch()
    );
    clock.cancel_timers();
    let dispatches = tap.0.borrow();
    let dispatch = dispatches.last().expect("actual explicit callback witness");
    assert_eq!(dispatch.binding.execution_owner, Some(lease.registration()));
    assert_eq!(dispatch.actual_execution_owner, Some(lease.registration()));
    assert!(dispatch.has_current_native_owner());
    msgbus::clear_bus_tap();
}
