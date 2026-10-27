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

//! Real-time and virtual `Clock` implementations.
//!
//! Defines the [`Clock`] contract, the user-facing [`ClockApi`] facade, and the deterministic
//! [`VirtualClock`] used for controlled time advancement. Shared validation and callback registration
//! support virtual and live clock implementations.

mod api;
mod registration;
pub use registration::{ExecutionClockLease, ExecutionClockRegistration};
#[path = "virtual.rs"]
mod virtual_clock;

#[cfg(test)]
mod tests;

use std::{any::Any, collections::BTreeMap, fmt::Debug, time::Duration};

use ahash::AHashMap;
pub use api::ClockApi; // Re-export
use jiff::Timestamp;
use nautilus_core::{
    DurationNanos, UnixNanos,
    correctness::{check_positive_u64, check_valid_string_utf8},
    datetime::try_datetime_to_unix_nanos,
};
use ustr::Ustr;
pub use virtual_clock::VirtualClock; // Re-export

use crate::{
    actor::registry::ActorRegistration,
    timer::{
        RecordedTimeEventDispatch, TimeEvent, TimeEventCallback, TimeEventHandler, Timer,
        TimerBinding,
    },
};

/// Provides time access, timer scheduling, and callback registration.
///
/// An active timer is one that has not expired.
pub trait Clock: Debug + Any {
    /// Returns the current UTC timestamp.
    fn utc_now(&self) -> Timestamp {
        self.timestamp_ns().to_datetime_utc()
    }

    /// Returns the current UNIX timestamp in nanoseconds (ns).
    fn timestamp_ns(&self) -> UnixNanos;

    /// Returns the current UNIX timestamp in microseconds (μs).
    fn timestamp_us(&self) -> u64;

    /// Returns the current UNIX timestamp in milliseconds (ms).
    fn timestamp_ms(&self) -> u64;

    /// Returns the current UNIX timestamp in seconds.
    fn timestamp(&self) -> f64;

    /// Returns the names of active timers in the clock.
    fn timer_names(&self) -> Vec<&str>;

    /// Returns the count of active timers in the clock.
    fn timer_count(&self) -> usize;

    /// Returns whether an active timer named `name` exists.
    fn timer_exists(&self, name: &Ustr) -> bool;

    /// Associates this component clock with its existing native actor registration.
    /// Custom clocks remain unsupported for recorded native callback routing.
    fn bind_actor_registration(&mut self, _owner: ActorRegistration) -> bool {
        false
    }

    /// Returns the native component registration which owns this clock, when supported.
    fn actor_registration(&self) -> Option<ActorRegistration> {
        None
    }

    /// Associates an exclusively owned client clock with its native registration lease.
    fn bind_execution_registration(&mut self, _owner: ExecutionClockLease) -> bool {
        false
    }

    /// Returns the current execution-client clock lease, when supported.
    fn execution_registration(&self) -> Option<ExecutionClockLease> {
        None
    }

    /// Resolves one recorded native timer firing without advancing other timers.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported clocks or an unbound, stale or mismatched registration.
    fn recorded_time_event(
        &mut self,
        _dispatch: &RecordedTimeEventDispatch,
    ) -> anyhow::Result<TimeEventHandler> {
        anyhow::bail!("Recorded timer dispatch is unsupported by this Clock")
    }

    /// Registers the callback used when a timer has no named callback.
    fn register_default_handler(&mut self, callback: TimeEventCallback);

    /// Cancels the registered default event handler, if any.
    ///
    /// Releases the held callback so any Python object owned by it can be dropped.
    /// `Trader::release_component` calls this at component retirement to break the cycle
    /// between a Python component and its clock: the clock holds the callback as a
    /// `Py<PyAny>` that Python's cycle collector cannot reach through.
    fn cancel_default_handler(&mut self);

    /// Cancels all registered named event callbacks, preserving the default handler.
    ///
    /// Releases callbacks registered via [`Clock::set_time_alert_ns`] or
    /// [`Clock::set_timer_ns`] with an explicit `callback` argument.
    /// `Trader::release_component` calls this at component retirement, breaking the same
    /// cycle as [`Clock::cancel_default_handler`].
    fn cancel_callbacks(&mut self);

    /// Sets a timer to alert at the specified time.
    ///
    /// See [`Clock::set_time_alert_ns`] for flag semantics.
    ///
    /// # Callback
    ///
    /// - `Some(callback)` registers and uses `callback` for the named alert.
    /// - `None` uses a callback registered under `name`, falling back to the default callback.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `name` is invalid.
    /// - `alert_time` is before the UNIX epoch or outside the [`UnixNanos`] range.
    /// - The alert is in the past and `allow_past` is `Some(false)`.
    /// - No explicit, named, or default callback is available.
    fn set_time_alert(
        &mut self,
        name: &str,
        alert_time: Timestamp,
        callback: Option<TimeEventCallback>,
        allow_past: Option<bool>,
    ) -> anyhow::Result<()> {
        self.set_time_alert_ns(
            name,
            try_datetime_to_unix_nanos(alert_time)?,
            callback,
            allow_past,
        )
    }

    /// Sets a timer to alert at the specified time.
    ///
    /// Any active timer registered under the same `name` is canceled with a warning before the
    /// new alert is scheduled. `allow_past` defaults to `true`.
    ///
    /// # Flags
    ///
    /// | `allow_past` | Behavior                                                               |
    /// | ------------ | ---------------------------------------------------------------------- |
    /// | `true`       | A past alert is moved to the current time and fires immediately.       |
    /// | `false`      | An alert earlier than the current time returns an error.               |
    ///
    /// # Callback
    ///
    /// - `Some(callback)` registers and uses `callback` for the named alert.
    /// - `None` uses a callback registered under `name`, falling back to the default callback.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `name` is invalid.
    /// - `alert_time_ns` is earlier than now and `allow_past` is `Some(false)`.
    /// - No explicit, named, or default callback is available.
    fn set_time_alert_ns(
        &mut self,
        name: &str,
        alert_time_ns: UnixNanos,
        callback: Option<TimeEventCallback>,
        allow_past: Option<bool>,
    ) -> anyhow::Result<()>;

    /// Sets a timer to fire time events at every interval between the start and stop times.
    ///
    /// Any active timer registered under the same `name` is canceled with a warning before the
    /// new timer is scheduled.
    ///
    /// See [`Clock::set_timer_ns`] for flag semantics.
    ///
    /// # Callback
    ///
    /// - `Some(callback)` registers and uses `callback` for the named timer.
    /// - `None` uses a callback registered under `name`, falling back to the default callback.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `name` is invalid.
    /// - `interval` is zero or exceeds `u64::MAX` nanoseconds.
    /// - `start_time` or `stop_time` is before the UNIX epoch or out of range for `UnixNanos`.
    /// - The first event timestamp is out of range for `UnixNanos`.
    /// - The first event is in the past when past times are disallowed.
    /// - The stop time is not after the start time.
    /// - The stop time is not after the current time when past times are disallowed.
    /// - No explicit, named, or default callback is available.
    #[expect(clippy::too_many_arguments)]
    fn set_timer(
        &mut self,
        name: &str,
        interval: Duration,
        start_time: Option<Timestamp>,
        stop_time: Option<Timestamp>,
        callback: Option<TimeEventCallback>,
        allow_past: Option<bool>,
        fire_immediately: Option<bool>,
    ) -> anyhow::Result<()> {
        self.set_timer_ns(
            name,
            duration_to_nanos(interval)?,
            start_time.map(try_datetime_to_unix_nanos).transpose()?,
            stop_time.map(try_datetime_to_unix_nanos).transpose()?,
            callback,
            allow_past,
            fire_immediately,
        )
    }

    /// Sets a timer to fire time events at every interval between the start and stop times.
    ///
    /// Any active timer registered under the same `name` is canceled with a warning before the
    /// new timer is scheduled. `allow_past` defaults to `true`, and `fire_immediately` defaults to
    /// `false`.
    ///
    /// # Start Time
    ///
    /// - `None` or `Some(0)`: Uses the current time as start time.
    /// - `Some(non_zero)`: Uses the specified timestamp as start time.
    ///
    /// # Flags
    ///
    /// | `allow_past` | `fire_immediately` | First event behavior                                |
    /// | ------------ | ------------------ | --------------------------------------------------- |
    /// | `true`       | `true`             | Fires at the start time, including a past start.    |
    /// | `true`       | `false`            | Fires one interval after the start, including past. |
    /// | `false`      | `true`             | A past start time returns an error.                 |
    /// | `false`      | `false`            | A past first event returns an error.                |
    ///
    /// # Callback
    ///
    /// - `Some(callback)` registers and uses `callback` for the named timer.
    /// - `None` uses a callback registered under `name`, falling back to the default callback.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `name` is invalid.
    /// - `interval_ns` is zero.
    /// - `start_time_ns + interval_ns` is out of range for `UnixNanos` when not firing immediately.
    /// - The first event is in the past when past times are disallowed.
    /// - The stop time is not after the start time.
    /// - The stop time is not after the current time when past times are disallowed.
    /// - No explicit, named, or default callback is available.
    #[expect(clippy::too_many_arguments)]
    fn set_timer_ns(
        &mut self,
        name: &str,
        interval_ns: DurationNanos,
        start_time_ns: Option<UnixNanos>,
        stop_time_ns: Option<UnixNanos>,
        callback: Option<TimeEventCallback>,
        allow_past: Option<bool>,
        fire_immediately: Option<bool>,
    ) -> anyhow::Result<()>;

    /// Returns the next trigger timestamp for the active timer named `name`.
    ///
    /// Returns `None` if no active timer with that name exists.
    fn next_time_ns(&self, name: &str) -> Option<UnixNanos>;

    /// Cancels the timer named `name`, if it exists.
    fn cancel_timer(&mut self, name: &str);

    /// Cancels all timers.
    fn cancel_timers(&mut self);

    /// Resets scheduling state while preserving the default callback.
    ///
    /// The reset clears all timers and named callbacks. Static clocks also reset their stored time.
    fn reset(&mut self);
}

impl dyn Clock {
    /// Returns a reference to this clock as `Any` for downcasting.
    pub fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Returns a mutable reference to this clock as `Any` for downcasting.
    pub fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn duration_to_nanos(duration: Duration) -> anyhow::Result<DurationNanos> {
    DurationNanos::try_from(duration)
        .map_err(|_| anyhow::anyhow!("Interval exceeds u64 nanoseconds"))
}

/// Registry for timer event callbacks.
///
/// Provides shared callback registration and retrieval logic used by both
/// `VirtualClock` and `LiveClock`.
#[derive(Debug, Default)]
pub struct CallbackRegistry {
    default_callback: Option<RegisteredCallback>,
    callbacks: AHashMap<Ustr, RegisteredCallback>,
    owner: Option<ActorRegistration>,
    execution_owner: Option<ExecutionClockLease>,
    callback_generation: u64,
    timer_generation: u64,
    timer_bindings: AHashMap<Ustr, u64>,
}

#[derive(Clone, Debug)]
struct RegisteredCallback {
    callback: TimeEventCallback,
    generation: u64,
}

impl CallbackRegistry {
    /// Creates an empty callback registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the callback used when no callback exists for a timer name.
    ///
    /// # Panics
    ///
    /// Panics if the checked callback generation is exhausted.
    pub fn register_default_handler(&mut self, callback: TimeEventCallback) {
        self.callback_generation = self
            .callback_generation
            .checked_add(1)
            .expect("Clock callback generation exhausted");
        self.default_callback = Some(RegisteredCallback {
            callback,
            generation: self.callback_generation,
        });
    }

    /// Removes the default callback, preserving all named callbacks.
    pub fn cancel_default_handler(&mut self) {
        self.default_callback = None;
    }

    /// Registers a callback for `name`, replacing any existing callback for that name.
    ///
    /// # Panics
    ///
    /// Panics if the checked callback generation is exhausted.
    pub fn register_callback(&mut self, name: Ustr, callback: TimeEventCallback) {
        self.callback_generation = self
            .callback_generation
            .checked_add(1)
            .expect("Clock callback generation exhausted");
        self.callbacks.insert(
            name,
            RegisteredCallback {
                callback,
                generation: self.callback_generation,
            },
        );
    }

    /// Binds routing metadata to the native registry incarnation which owns this clock.
    pub fn bind_actor_registration(&mut self, owner: ActorRegistration) {
        self.owner = Some(owner);
    }

    pub(crate) fn bind_execution_registration(&mut self, owner: ExecutionClockLease) -> bool {
        if self.owner.is_some() {
            return false;
        }
        self.execution_owner = Some(owner);
        true
    }

    pub(crate) fn execution_registration(&self) -> Option<ExecutionClockLease> {
        self.execution_owner.clone()
    }

    pub(crate) const fn actor_registration(&self) -> Option<ActorRegistration> {
        self.owner
    }

    pub(crate) fn next_timer_binding(&mut self, name: Ustr) -> anyhow::Result<()> {
        self.timer_generation = self
            .timer_generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Clock timer generation exhausted"))?;
        self.timer_bindings.insert(name, self.timer_generation);
        Ok(())
    }

    pub(crate) fn recorded_handler(
        &self,
        dispatch: &RecordedTimeEventDispatch,
    ) -> anyhow::Result<TimeEventHandler> {
        anyhow::ensure!(
            dispatch.has_current_native_owner(),
            "Recorded callback has no supported current native owner"
        );
        let callback = self
            .get_callback(&dispatch.event.name)
            .ok_or_else(|| anyhow::anyhow!("Recorded callback registration is absent"))?;
        let actual = callback
            .binding()
            .ok_or_else(|| anyhow::anyhow!("Runtime callback has no native routing metadata"))?;
        let owner_matches = match (dispatch.binding.owner, actual.owner) {
            (Some(source), Some(runtime)) => {
                source.actor_id == runtime.actor_id
                    && source.generation == runtime.generation
                    && crate::actor::registry::actor_registration(&runtime.actor_id)
                        == Some(runtime)
            }
            (None, None) => dispatch
                .binding
                .execution_owner
                .zip(actual.execution_owner)
                .is_some_and(|(source, runtime)| {
                    source.client_id == runtime.client_id
                        && source.generation == runtime.generation
                        && self.execution_owner.as_ref().is_some_and(|lease| {
                            lease.is_active() && lease.registration() == runtime
                        })
                }),
            _ => false,
        };
        anyhow::ensure!(
            owner_matches
                && actual.target == dispatch.binding.target
                && actual.callback_generation == dispatch.binding.callback_generation
                && actual.timer_generation == dispatch.binding.timer_generation,
            "Recorded callback or timer registration changed"
        );
        Ok(TimeEventHandler::new(dispatch.event.clone(), callback))
    }

    /// Returns whether a named or default callback is available for `name`.
    #[must_use]
    pub fn has_any_callback(&self, name: &Ustr) -> bool {
        self.callbacks.contains_key(name) || self.default_callback.is_some()
    }

    /// Returns the callback for `name`, falling back to the default callback.
    #[must_use]
    pub fn get_callback(&self, name: &Ustr) -> Option<TimeEventCallback> {
        let entry = self
            .callbacks
            .get(name)
            .or(self.default_callback.as_ref())?;
        let target = entry.callback.binding().and_then(|binding| binding.target);
        if self.owner.is_none() && target.is_none() && self.execution_owner.is_none() {
            return Some(entry.callback.clone());
        }
        let binding = TimerBinding {
            target,
            owner: self.owner,
            execution_owner: self
                .execution_owner
                .as_ref()
                .map(ExecutionClockLease::registration),
            callback_generation: entry.generation,
            timer_generation: self.timer_bindings.get(name).copied(),
        };
        Some(
            entry
                .callback
                .clone()
                .with_runtime_binding(binding, self.execution_owner.clone()),
        )
    }

    /// Creates a handler for `event` using its named callback or the default callback.
    ///
    /// # Panics
    ///
    /// Panics if neither a named nor default callback exists for the event.
    #[must_use]
    pub fn get_handler(&self, event: TimeEvent) -> TimeEventHandler {
        let callback = self
            .get_callback(&event.name)
            .unwrap_or_else(|| panic!("Event '{}' should have associated handler", event.name));

        TimeEventHandler::new(event, callback)
    }

    /// Clears all named callbacks, preserving the default callback.
    pub fn clear(&mut self) {
        self.callbacks.clear();
    }
}

/// Validates and normalizes parameters for a time alert.
///
/// `allow_past` defaults to `true`. When enabled, a past alert timestamp is replaced with
/// `ts_now`. Returns the interned name and normalized alert timestamp.
///
/// # Errors
///
/// Returns an error if `name` is invalid or the alert is in the past when past alerts are
/// disallowed.
pub fn validate_and_prepare_time_alert(
    name: &str,
    mut alert_time_ns: UnixNanos,
    allow_past: Option<bool>,
    ts_now: UnixNanos,
) -> anyhow::Result<(Ustr, UnixNanos)> {
    check_valid_string_utf8(name, stringify!(name))?;

    let name = Ustr::from(name);
    let allow_past = allow_past.unwrap_or(true);

    if alert_time_ns < ts_now {
        if allow_past {
            log::warn!(
                "Timer '{name}' alert time {} was in the past, adjusted to current time for immediate firing",
                alert_time_ns.to_rfc3339(),
            );
            alert_time_ns = ts_now;
        } else {
            anyhow::bail!(
                "Timer '{name}' alert time {} was in the past (current time is {ts_now})",
                alert_time_ns.to_rfc3339(),
            );
        }
    }

    Ok((name, alert_time_ns))
}

/// Validates and normalizes parameters for an interval timer.
///
/// A missing or zero `start_time_ns` resolves to `ts_now`. `allow_past` defaults to `true`, and
/// `fire_immediately` defaults to `false`. Returns the interned name, normalized start and stop
/// times, and resolved flag values.
///
/// # Errors
///
/// Returns an error if:
/// - `name` is invalid.
/// - `interval_ns` is zero.
/// - `start_time_ns + interval_ns` is out of range for `UnixNanos` when not firing immediately.
/// - The first event is in the past when past times are disallowed.
/// - The stop time is not after the normalized start time.
/// - The stop time is not after `ts_now` when past times are disallowed.
pub fn validate_and_prepare_timer(
    name: &str,
    interval_ns: DurationNanos,
    start_time_ns: Option<UnixNanos>,
    stop_time_ns: Option<UnixNanos>,
    allow_past: Option<bool>,
    fire_immediately: Option<bool>,
    ts_now: UnixNanos,
) -> anyhow::Result<(Ustr, UnixNanos, Option<UnixNanos>, bool, bool)> {
    check_valid_string_utf8(name, stringify!(name))?;
    check_positive_u64(interval_ns.as_u64(), stringify!(interval_ns))?;

    let name = Ustr::from(name);
    let allow_past = allow_past.unwrap_or(true);
    let fire_immediately = fire_immediately.unwrap_or(false);

    let start_time_ns = start_time_ns
        .filter(|start_time_ns| *start_time_ns != 0)
        .unwrap_or(ts_now);

    let next_event_time = if fire_immediately {
        start_time_ns
    } else {
        start_time_ns.checked_add(interval_ns).ok_or_else(|| {
            anyhow::anyhow!("Timer '{name}' first event time exceeds UnixNanos range")
        })?
    };

    if !allow_past && next_event_time < ts_now {
        anyhow::bail!(
            "Timer '{name}' next event time {} would be in the past (current time is {ts_now})",
            next_event_time.to_rfc3339(),
        );
    }

    if let Some(stop_time) = stop_time_ns {
        if stop_time <= start_time_ns {
            anyhow::bail!(
                "Timer '{name}' stop time {} must be after start time {}",
                stop_time.to_rfc3339(),
                start_time_ns.to_rfc3339(),
            );
        }

        if !allow_past && stop_time <= ts_now {
            anyhow::bail!(
                "Timer '{name}' stop time {} is in the past (current time is {ts_now})",
                stop_time.to_rfc3339(),
            );
        }
    }

    Ok((
        name,
        start_time_ns,
        stop_time_ns,
        allow_past,
        fire_immediately,
    ))
}

// Cancels and removes the active timer registered under `name`, if any.
//
// Shared by `VirtualClock` and `LiveClock` to enforce one active timer per name.
pub(crate) fn replace_existing_timer<T: Timer>(timers: &mut BTreeMap<Ustr, T>, name: &Ustr) {
    let Some(mut timer) = timers.remove(name) else {
        return;
    };

    if timer.is_expired() {
        return;
    }

    timer.cancel();
    log::warn!("Timer '{name}' replaced");
}
