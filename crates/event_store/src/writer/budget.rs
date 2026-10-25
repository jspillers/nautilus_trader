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

//! Byte reservations retained from enqueue through durable acknowledgement.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::WriterBufferStats;

#[derive(Debug)]
pub(super) struct PendingBudget {
    limit: Option<usize>,
    pending: AtomicUsize,
    peak: AtomicUsize,
}

impl PendingBudget {
    pub(super) const fn new(limit: Option<usize>) -> Self {
        Self {
            limit,
            pending: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    pub(super) fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Option<Reservation>, ()> {
        let Some(limit) = self.limit else {
            return Ok(None);
        };
        let previous = self
            .pending
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= limit)
            })
            .map_err(|_| ())?;
        self.peak.fetch_max(previous + bytes, Ordering::Relaxed);
        Ok(Some(Reservation {
            budget: Arc::clone(self),
            bytes,
        }))
    }

    pub(super) fn stats(&self) -> WriterBufferStats {
        WriterBufferStats {
            pending_bytes: self.pending.load(Ordering::Acquire),
            peak_pending_bytes: self.peak.load(Ordering::Relaxed),
            max_pending_bytes: self.limit,
        }
    }
}

#[derive(Debug)]
pub(super) struct Reservation {
    budget: Arc<PendingBudget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let previous = self.budget.pending.fetch_sub(self.bytes, Ordering::Release);
        debug_assert!(previous >= self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn concurrent_reservations_never_exceed_the_limit_and_release_on_drop() {
        let budget = Arc::new(PendingBudget::new(Some(4096)));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let budget = Arc::clone(&budget);

                scope.spawn(move || {
                    for _ in 0..1000 {
                        if let Ok(reservation) = budget.reserve(1024) {
                            assert!(budget.stats().pending_bytes <= 4096);
                            std::thread::yield_now();
                            drop(reservation);
                        }
                    }
                });
            }
        });
        assert_eq!(budget.stats().pending_bytes, 0);
        assert!(budget.stats().peak_pending_bytes <= 4096);
        assert!(budget.reserve(4097).is_err());
        let reservation = budget.reserve(1).unwrap();
        assert!(budget.reserve(usize::MAX).is_err());
        drop(reservation);
        assert_eq!(budget.stats().pending_bytes, 0);
    }
}
