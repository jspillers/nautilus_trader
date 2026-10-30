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

//! The dedicated writer-thread loop that drains the bounded channel into atomic
//! `append_batch` commits.
//!
//! The loop owns the backend instance for the lifetime of the run: it stamps `seq`,
//! computes `entry_hash`, and groups incoming entries into bounded-size or bounded-latency
//! batches. The high-watermark mirror only advances after the backend acknowledges the
//! commit. Disk-pressure or corruption errors fire the halt callback and end the loop;
//! a graceful Close drains the channel, writes the supplied `RunEnded` entry as the final
//! row, and seals the manifest.

#[cfg(not(madsim))]
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{Receiver, RecvTimeoutError, SyncSender},
    },
    time::Instant,
};

use nautilus_core::UnixNanos;
#[cfg(not(madsim))]
use nautilus_core::time::AtomicTime;

use crate::{
    backend::AppendEntry, entry::EventStoreEntry, hash::compute_entry_hash, writer::EntryDraft,
};
#[cfg(not(madsim))]
use crate::{
    backend::EventStore,
    error::EventStoreError,
    snapshot::SnapshotAnchor,
    writer::{
        WriterConfig,
        budget::Reservation,
        halt::{self, HaltCallback, HaltReason},
    },
};

/// Internal message exchanged between submit and the writer thread.
#[cfg(not(madsim))]
pub(super) enum WriterMessage {
    /// A single captured entry plus its writer-receive timestamp.
    Entry {
        /// The unsealed entry handed in by the bus capture adapter.
        draft: EntryDraft,
        /// The writer-receive timestamp stamped at submit time.
        ts_publish: UnixNanos,
    },
    /// Graceful shutdown signal. The writer thread drains pending entries, appends the
    /// `RunEnded` entry, seals the manifest, and reports the final high-watermark via
    /// `ack`.
    Close {
        /// The terminal `RunEnded` entry committed before seal.
        run_ended: EntryDraft,
        /// One-shot reply channel for the close result.
        ack: SyncSender<Result<u64, EventStoreError>>,
    },
    /// Freezes a durable prefix without exporting on the writer thread or sealing the run.
    FreezePrefix {
        /// One-shot handoff to the owned export worker.
        ack: SyncSender<Result<crate::checkpoint::FrozenPrefix, EventStoreError>>,
        /// A dropped ticket or expired export request can avoid pinning a read view.
        cancelled: Arc<AtomicBool>,
    },
    /// Records a cache snapshot anchor after all pending entries have been flushed.
    RecordSnapshotAnchor {
        /// Cache-owned reference to the snapshot blob.
        blob_ref: String,
        /// Cache-owned content hash for the snapshot blob.
        content_hash: String,
        /// One-shot reply channel for the recorded anchor.
        ack: SyncSender<Result<SnapshotAnchor, EventStoreError>>,
    },
}

#[cfg(not(madsim))]
impl WriterMessage {
    pub(super) fn accounted_bytes(&self) -> usize {
        let base = std::mem::size_of::<BudgetedMessage>()
            .max(std::mem::size_of::<AppendEntry>() + std::mem::size_of::<Option<Reservation>>());
        let aligned = |n: usize| n.checked_next_multiple_of(64).unwrap_or(usize::MAX);
        let draft_bytes = |draft: &EntryDraft| {
            draft.index_keys.iter().fold(
                aligned(draft.payload.len())
                    .saturating_add(64)
                    .saturating_add(
                        draft
                            .index_keys
                            .capacity()
                            .saturating_mul(std::mem::size_of::<crate::backend::IndexKey>()),
                    ),
                |sum, key| sum.saturating_add(aligned(key.key.capacity())),
            )
        };
        base.saturating_add(match self {
            Self::Entry { draft, .. } => draft_bytes(draft),
            Self::Close { run_ended, .. } => draft_bytes(run_ended),
            Self::FreezePrefix { .. } => 0,
            Self::RecordSnapshotAnchor {
                blob_ref,
                content_hash,
                ..
            } => aligned(blob_ref.capacity()).saturating_add(aligned(content_hash.capacity())),
        })
    }

    pub(super) fn compact_payload(&mut self) {
        let draft = match self {
            Self::Entry { draft, .. } => draft,
            Self::Close { run_ended, .. } => run_ended,
            Self::RecordSnapshotAnchor { .. } | Self::FreezePrefix { .. } => return,
        };
        // A Bytes slice can retain an arbitrarily large caller-owned backing allocation,
        // an independent exact-length copy makes the queue's payload accounting finite.
        draft.payload = bytes::Bytes::copy_from_slice(&draft.payload);
    }
}

/// Reservation follows its message into the durable batch instead of ending at dequeue.
#[cfg(not(madsim))]
pub(super) struct BudgetedMessage {
    pub(super) message: WriterMessage,
    pub(super) reservation: Option<Reservation>,
    pub(super) bytes: usize,
}

#[cfg(not(madsim))]
struct Batch {
    entries: Vec<AppendEntry>,
    reservations: Vec<Option<Reservation>>,
    bytes: usize,
}

#[cfg(not(madsim))]
impl Batch {
    fn new(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            reservations: Vec::with_capacity(capacity),
            bytes: 0,
        }
    }
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    fn would_exceed(&self, bytes: usize, config: &WriterConfig) -> bool {
        !self.is_empty()
            && config
                .max_batch_bytes
                .is_some_and(|limit| self.bytes.saturating_add(bytes) > limit)
    }
    fn full(&self, config: &WriterConfig) -> bool {
        self.entries.len() >= config.max_batch_entries
            || config
                .max_batch_bytes
                .is_some_and(|limit| self.bytes >= limit)
    }
    fn push(&mut self, entry: AppendEntry, reservation: Option<Reservation>, bytes: usize) {
        self.entries.push(entry);
        self.reservations.push(reservation);
        self.bytes = self.bytes.saturating_add(bytes);
    }
    fn clear(&mut self) {
        self.entries.clear();
        self.reservations.clear();
        self.bytes = 0;
    }
}

/// The writer thread's halt fire path: the shared callback plus the once-only latch
/// that keeps the fire exactly once across every failure origin.
#[cfg(not(madsim))]
pub(super) struct HaltSink {
    callback: HaltCallback,
    halted: Arc<AtomicBool>,
}

#[cfg(not(madsim))]
impl HaltSink {
    pub(super) fn new(callback: HaltCallback, halted: Arc<AtomicBool>) -> Self {
        Self { callback, halted }
    }

    fn fire(&self, reason: HaltReason) {
        halt::fire_once(&self.callback, &self.halted, reason);
    }
}

/// Runs the writer loop until the channel disconnects, a halt-worthy backend error fires,
/// or a Close message is received.
#[cfg(not(madsim))]
#[expect(
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
    reason = "writer thread owns the ordered entry, snapshot, flush and close lifecycle"
)]
pub(super) fn run(
    mut backend: Box<dyn EventStore + Send>,
    rx: Receiver<BudgetedMessage>,
    config: WriterConfig,
    halt: HaltSink,
    high_watermark: Arc<AtomicU64>,
    clock: &'static AtomicTime,
) {
    // Recover the next seq from the backend so a writer attached to an existing run
    // continues from the durable high-watermark rather than colliding with already-
    // committed entries.
    let mut next_seq = match backend.high_watermark() {
        Ok(hwm) => hwm + 1,
        Err(e) => {
            halt.fire(HaltReason::from_backend_error(&e));
            return;
        }
    };

    let mut batch = Batch::new(config.max_batch_entries);
    let mut batch_deadline: Option<Instant> = None;

    loop {
        let recv_result = match batch_deadline {
            None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
            Some(deadline) => {
                let now = Instant::now();
                let remaining = deadline.saturating_duration_since(now);
                rx.recv_timeout(remaining)
            }
        };

        match recv_result {
            Ok(BudgetedMessage {
                message: WriterMessage::Entry { draft, ts_publish },
                reservation,
                bytes,
            }) => {
                if batch.would_exceed(bytes, &config) {
                    if !flush(backend.as_mut(), &mut batch, &halt, high_watermark.as_ref()) {
                        return;
                    }
                    batch_deadline = None;
                }
                let append = build_append_entry(draft, ts_publish, next_seq);
                next_seq += 1;
                batch.push(append, reservation, bytes);

                if batch_deadline.is_none() {
                    batch_deadline = Some(Instant::now() + config.max_batch_latency);
                }

                // A busy receiver must not keep the elapsed batch deadline alive forever
                let deadline_reached =
                    batch_deadline.is_some_and(|deadline| Instant::now() >= deadline);
                if (batch.full(&config) || deadline_reached)
                    && !flush(backend.as_mut(), &mut batch, &halt, high_watermark.as_ref())
                {
                    return;
                }

                if batch.is_empty() {
                    batch_deadline = None;
                }
            }
            Ok(BudgetedMessage {
                message: WriterMessage::Close { run_ended, ack },
                reservation,
                bytes,
            }) => {
                if !drain_pending(
                    &rx,
                    &mut batch,
                    &mut next_seq,
                    backend.as_mut(),
                    &config,
                    &halt,
                    high_watermark.as_ref(),
                ) {
                    let _ = ack.send(Err(EventStoreError::Backend(
                        "writer fail-stopped while draining before seal".to_string(),
                    )));
                    return;
                }

                if batch.would_exceed(bytes, &config)
                    && !flush(backend.as_mut(), &mut batch, &halt, high_watermark.as_ref())
                {
                    let _ = ack.send(Err(EventStoreError::Backend(
                        "writer fail-stopped before terminal entry".to_string(),
                    )));
                    return;
                }

                let now = clock.get_time_ns();
                let run_ended_append = build_append_entry(run_ended, now, next_seq);
                batch.push(run_ended_append, reservation, bytes);

                let flush_ok = flush(backend.as_mut(), &mut batch, &halt, high_watermark.as_ref());
                if !flush_ok {
                    let _ = ack.send(Err(EventStoreError::Backend(
                        "writer fail-stopped before seal".to_string(),
                    )));
                    return;
                }

                let seal_result = backend.seal(crate::manifest::RunStatus::Ended);
                let final_result = match seal_result {
                    Ok(()) => Ok(high_watermark.load(Ordering::Acquire)),
                    Err(e) => {
                        halt.fire(HaltReason::from_backend_error(&e));
                        Err(e)
                    }
                };
                let _ = ack.send(final_result);
                return;
            }
            Ok(BudgetedMessage {
                message: WriterMessage::FreezePrefix { ack, cancelled },
                reservation,
                ..
            }) => {
                if cancelled.load(Ordering::Acquire) {
                    let _ = ack.send(Err(EventStoreError::Closed));
                    continue;
                }

                if !batch.is_empty()
                    && !flush(backend.as_mut(), &mut batch, &halt, high_watermark.as_ref())
                {
                    let _ = ack.send(Err(EventStoreError::Backend(
                        "writer fail-stopped before prefix freeze".to_string(),
                    )));
                    return;
                }
                let frozen = backend.freeze_prefix(clock.get_time_ns());
                let _ = ack.send(frozen);
                drop(reservation);
                batch_deadline = None;
            }
            Ok(BudgetedMessage {
                message:
                    WriterMessage::RecordSnapshotAnchor {
                        blob_ref,
                        content_hash,
                        ack,
                    },
                reservation,
                ..
            }) => {
                if !record_snapshot_anchor(
                    backend.as_mut(),
                    &mut batch,
                    &halt,
                    high_watermark.as_ref(),
                    blob_ref,
                    content_hash,
                    &ack,
                ) {
                    return;
                }

                drop(reservation);
                batch_deadline = None;
            }
            Err(RecvTimeoutError::Timeout) => {
                if !batch.is_empty()
                    && !flush(backend.as_mut(), &mut batch, &halt, high_watermark.as_ref())
                {
                    return;
                }
                batch_deadline = None;
            }
            Err(RecvTimeoutError::Disconnected) => {
                if !batch.is_empty() {
                    let _ = flush(backend.as_mut(), &mut batch, &halt, high_watermark.as_ref());
                }
                return;
            }
        }
    }
}

#[cfg(not(madsim))]
fn record_snapshot_anchor(
    backend: &mut dyn EventStore,
    batch: &mut Batch,
    halt: &HaltSink,
    high_watermark: &AtomicU64,
    blob_ref: String,
    content_hash: String,
    ack: &SyncSender<Result<SnapshotAnchor, EventStoreError>>,
) -> bool {
    if !batch.is_empty() && !flush(backend, batch, halt, high_watermark) {
        let _ = ack.send(Err(EventStoreError::Backend(
            "writer fail-stopped before snapshot anchor".to_string(),
        )));

        return false;
    }

    let hwm = high_watermark.load(Ordering::Acquire);
    let anchor = SnapshotAnchor::new(hwm, blob_ref, content_hash);
    let result = match backend.record_snapshot_anchor(anchor.clone()) {
        Ok(()) => Ok(anchor),
        Err(e) => {
            halt.fire(HaltReason::from_backend_error(&e));
            Err(e)
        }
    };

    let keep_running = result.is_ok();
    let _ = ack.send(result);

    keep_running
}

#[cfg(not(madsim))]
fn drain_pending(
    rx: &Receiver<BudgetedMessage>,
    batch: &mut Batch,
    next_seq: &mut u64,
    backend: &mut dyn EventStore,
    config: &WriterConfig,
    halt: &HaltSink,
    high_watermark: &AtomicU64,
) -> bool {
    while let Ok(pending) = rx.try_recv() {
        let BudgetedMessage {
            message,
            reservation,
            bytes,
        } = pending;

        match message {
            WriterMessage::Entry { draft, ts_publish } => {
                if batch.would_exceed(bytes, config) && !flush(backend, batch, halt, high_watermark)
                {
                    return false;
                }
                let append = build_append_entry(draft, ts_publish, *next_seq);
                *next_seq += 1;
                batch.push(append, reservation, bytes);
                if batch.full(config) && !flush(backend, batch, halt, high_watermark) {
                    return false;
                }
            }
            WriterMessage::Close { ack, .. } => {
                let _ = ack.send(Err(EventStoreError::Backend(
                    "writer is already closing".to_string(),
                )));
            }
            WriterMessage::FreezePrefix { ack, .. } => {
                let _ = ack.send(Err(EventStoreError::Closed));
            }
            WriterMessage::RecordSnapshotAnchor { ack, .. } => {
                let _ = ack.send(Err(EventStoreError::Backend(
                    "writer is closing before snapshot anchor".to_string(),
                )));
            }
        }
    }
    true
}

/// Commits the current batch, advances the high-watermark mirror on success, or fires the
/// halt callback on backend failure. Returns `true` if the writer loop should continue.
#[cfg(not(madsim))]
fn flush(
    backend: &mut dyn EventStore,
    batch: &mut Batch,
    halt: &HaltSink,
    high_watermark: &AtomicU64,
) -> bool {
    if batch.is_empty() {
        return true;
    }

    match backend.append_batch(&batch.entries) {
        Ok(new_hwm) => {
            high_watermark.store(new_hwm, Ordering::Release);
            batch.clear();
            true
        }
        Err(e) => {
            halt.fire(HaltReason::from_backend_error(&e));
            batch.clear();
            false
        }
    }
}

/// Builds a fully-formed [`AppendEntry`] from a draft, the writer-receive timestamp, and
/// the assigned sequence.
pub(super) fn build_append_entry(
    draft: EntryDraft,
    ts_publish: UnixNanos,
    seq: u64,
) -> AppendEntry {
    let EntryDraft {
        headers,
        topic,
        payload_type,
        payload,
        ts_init,
        index_keys,
    } = draft;

    let entry_hash = compute_entry_hash(
        seq,
        ts_init,
        ts_publish,
        topic.as_ref(),
        payload_type.as_str(),
        &payload,
        &headers,
    );

    let entry = EventStoreEntry::new(
        entry_hash,
        seq,
        headers,
        topic,
        payload_type,
        payload,
        ts_init,
        ts_publish,
    );

    AppendEntry::new(entry, index_keys)
}
