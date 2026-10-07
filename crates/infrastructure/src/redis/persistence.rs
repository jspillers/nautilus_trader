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

//! Local reservations around the existing native writer. No new queue, writer or retry policy.
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use nautilus_common::cache::database::{CachePersistenceHealth, CachePersistenceLimits};

#[derive(Debug, Default)]
struct Pending {
    next: u64,
    bytes: usize,
    commands: BTreeMap<u64, (Instant, usize)>,
}
#[derive(Debug)]
pub(super) struct WriteHealth {
    limits: CachePersistenceLimits,
    pending: Mutex<Pending>,
    failed: AtomicBool,
    alive: AtomicBool,
}
#[derive(Debug)]
pub(super) struct Reservation {
    health: Arc<WriteHealth>,
    id: u64,
}
impl WriteHealth {
    pub(super) fn new(limits: CachePersistenceLimits) -> anyhow::Result<Arc<Self>> {
        limits.validate()?;
        Ok(Arc::new(Self {
            limits,
            pending: Mutex::default(),
            failed: AtomicBool::new(false),
            alive: AtomicBool::new(true),
        }))
    }
    pub(super) fn fail(&self) {
        self.failed.store(true, Ordering::Release);
    }
    pub(super) fn stop(&self) {
        self.alive.store(false, Ordering::Release);
    }
    pub(super) fn reserve(self: &Arc<Self>, bytes: usize) -> anyhow::Result<Arc<Reservation>> {
        let mut p = self.pending.lock().map_err(|_| {
            self.fail();
            anyhow::anyhow!("native persistence health unavailable")
        })?;
        let retained = p.bytes.checked_add(bytes);
        if p.commands.len() >= self.limits.max_commands
            || retained.is_none_or(|n| n > self.limits.max_bytes)
        {
            self.fail();
            anyhow::bail!("native asynchronous persistence hard backlog limit exceeded");
        }
        let id = p.next;
        p.next = p.next.checked_add(1).ok_or_else(|| {
            self.fail();
            anyhow::anyhow!("native persistence reservation identity exhausted")
        })?;
        p.bytes = retained.unwrap();
        p.commands.insert(id, (Instant::now(), bytes));
        Ok(Arc::new(Reservation {
            health: self.clone(),
            id,
        }))
    }
    pub(super) fn snapshot(&self) -> CachePersistenceHealth {
        match self.pending.lock() {
            Ok(p) => CachePersistenceHealth {
                limits: self.limits,
                pending_commands: p.commands.len(),
                pending_bytes: p.bytes,
                oldest_pending_age_ms: p.commands.first_key_value().map_or(0, |(_, (at, _))| {
                    u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX)
                }),
                failed: self.failed.load(Ordering::Acquire),
                writer_alive: self.alive.load(Ordering::Acquire),
            },
            Err(_) => CachePersistenceHealth {
                limits: self.limits,
                pending_commands: self.limits.max_commands,
                pending_bytes: self.limits.max_bytes,
                oldest_pending_age_ms: u64::MAX,
                failed: true,
                writer_alive: false,
            },
        }
    }
}
#[derive(Debug)]
pub(super) struct WriterGuard(pub(super) Option<Arc<WriteHealth>>);
impl Drop for WriterGuard {
    fn drop(&mut self) {
        if let Some(health) = &self.0 {
            health.stop();
        }
    }
}
impl Reservation {
    pub(super) fn fail(&self) {
        self.health.fail();
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Ok(mut p) = self.health.pending.lock() {
            if let Some((_, bytes)) = p.commands.remove(&self.id) {
                p.bytes -= bytes;
            }
        } else {
            self.health.fail();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn health() -> Arc<WriteHealth> {
        WriteHealth::new(CachePersistenceLimits {
            entry_commands: 2,
            entry_bytes: 16,
            max_commands: 4,
            max_bytes: 32,
            max_age_ms: 10,
        })
        .unwrap()
    }
    #[rstest::rstest]
    fn soft_limit_keeps_reserved_recovery_capacity() {
        let h = health();
        let a = h.reserve(8).unwrap();
        assert!(h.snapshot().permits_entry());
        let b = h.reserve(8).unwrap();
        assert!(!h.snapshot().permits_entry());
        let c = h.reserve(8).unwrap();
        assert_eq!(h.snapshot().pending_commands, 3);
        drop((a, b, c));
        assert!(h.snapshot().permits_entry());
    }
    #[rstest::rstest]
    fn hard_limit_is_nonblocking_and_latched_even_after_drain() {
        let h = health();
        let a = h.reserve(32).unwrap();
        assert!(h.reserve(1).is_err());
        drop(a);
        assert_eq!(h.snapshot().pending_commands, 0);
        assert!(h.snapshot().failed);
        assert!(!h.snapshot().permits_entry());
        assert!(h.reserve(1).is_ok());
    }
    #[rstest::rstest]
    fn reservations_include_inflight_work_until_last_owner_drops() {
        let h = health();
        let a = h.reserve(8).unwrap();
        let in_flight = a.clone();
        drop(a);
        assert_eq!(h.snapshot().pending_bytes, 8);
        drop(in_flight);
        assert_eq!(h.snapshot().pending_bytes, 0);
    }
    #[rstest::rstest]
    fn oldest_age_expires_without_network_or_new_enqueue() {
        let h = health();
        let _a = h.reserve(1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(12));
        assert!(!h.snapshot().permits_entry());
    }
    #[rstest::rstest]
    fn unavailable_writer_and_failed_write_keep_entry_closed() {
        let h = health();
        let a = h.reserve(1).unwrap();
        a.fail();
        drop(a);
        h.stop();
        assert!(!h.snapshot().writer_alive);
        assert!(h.snapshot().failed);
        assert!(!h.snapshot().permits_entry());
    }
    #[rstest::rstest]
    fn aborted_writer_guard_closes_entry_even_without_pending_commands() {
        let h = health();
        let guard = WriterGuard(Some(h.clone()));
        assert!(h.snapshot().permits_entry());
        drop(guard);
        assert_eq!(h.snapshot().pending_commands, 0);
        assert!(!h.snapshot().permits_entry());
    }
    #[rstest::rstest]
    fn invalid_limits_are_rejected() {
        let mut l = health().limits;
        l.entry_commands = l.max_commands;
        assert!(WriteHealth::new(l).is_err());
        l = health().limits;
        l.entry_bytes = 0;
        assert!(WriteHealth::new(l).is_err());
        l = health().limits;
        l.max_age_ms = 0;
        assert!(WriteHealth::new(l).is_err());
    }
}
