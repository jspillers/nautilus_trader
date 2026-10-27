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

//! Native callback binding facts for recorded-input execution.

use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::TimeEvent;
use crate::{actor::registry::ActorRegistration, clock::ExecutionClockRegistration};

/// Routing metadata captured from the actual callback carried to dispatch.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerBinding {
    /// The component ID selected by the framework-installed callback.
    pub target: Option<Ustr>,
    /// The native registration which owned the clock when the timer was armed.
    pub owner: Option<ActorRegistration>,
    /// The exclusive native execution-client clock owner, when this is a client callback.
    #[serde(default)]
    pub execution_owner: Option<ExecutionClockRegistration>,
    /// The callback registration generation on that component clock.
    pub callback_generation: u64,
    /// The timer registration generation retained by the dispatched lease.
    pub timer_generation: Option<u64>,
}

/// A dispatch witness, not an executable callback or an external input decoder.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedTimeEventDispatch {
    /// Original scheduled event identity and payload timestamps.
    pub event: TimeEvent,
    /// Routing metadata retained by the actual callback lease.
    pub binding: TimerBinding,
    /// Native registration present when the framework callback selected its target.
    pub actual_owner: Option<ActorRegistration>,
    /// The execution registration still admitted at dispatch, when applicable.
    #[serde(default)]
    pub actual_execution_owner: Option<ExecutionClockRegistration>,
}

impl RecordedTimeEventDispatch {
    /// Whether native routing selected the original supported registration.
    #[must_use]
    pub fn has_current_native_owner(&self) -> bool {
        let actor = self.binding.target.is_some()
            && self.binding.owner.is_some()
            && self.binding.owner == self.actual_owner
            && self
                .binding
                .owner
                .is_some_and(|owner| Some(owner.actor_id) == self.binding.target)
            && self.binding.execution_owner.is_none();
        let client = self.binding.owner.is_none()
            && self.binding.target.is_none()
            && self.binding.execution_owner.is_some()
            && self.binding.execution_owner == self.actual_execution_owner;
        self.binding.callback_generation > 0
            && self.binding.timer_generation.is_some()
            && (actor || client)
    }
}
