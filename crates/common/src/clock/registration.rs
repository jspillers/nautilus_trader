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

//! Native execution-client clock ownership leases.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use nautilus_core::UUID4;
use nautilus_model::identifiers::ClientId;
use serde::{Deserialize, Serialize};

/// Identity assigned by the existing native execution-client registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionClockRegistration {
    /// The registered execution client ID.
    pub client_id: ClientId,
    /// This concrete registration incarnation.
    pub registration_id: UUID4,
    /// The checked insertion generation for this client ID.
    pub generation: u64,
}

/// A retained clock lease whose admission ends with its owning client registration.
#[derive(Clone, Debug)]
pub struct ExecutionClockLease {
    registration: ExecutionClockRegistration,
    active: Arc<AtomicBool>,
}

impl ExecutionClockLease {
    /// Creates an active lease for one native registry insertion.
    #[must_use]
    pub fn new(registration: ExecutionClockRegistration) -> Self {
        Self {
            registration,
            active: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Returns the immutable registration identity.
    #[must_use]
    pub const fn registration(&self) -> ExecutionClockRegistration {
        self.registration
    }

    /// Returns whether the owning native registration still admits callbacks.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Retires this registration without invalidating already retained callback memory.
    pub fn retire(&self) {
        self.active.store(false, Ordering::Release);
    }
}
