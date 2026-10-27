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

//! Immutable prefixes of a running native event store.
//!
//! A prefix proves the durable bytes through one writer-ordered high-watermark. It does not
//! seal its source, establish capture completeness or supply a full initial kernel state.
//! Snapshot anchors remain opaque cache-owned references; their blobs are not exported.

use std::{
    fmt::Debug,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, TryRecvError},
    },
    time::{Duration, Instant},
};

use nautilus_core::UnixNanos;
use serde::{Deserialize, Serialize};

use crate::{
    EventStoreError, EventStoreReader, RedbBackend, RunManifest, SnapshotAnchor,
    backend::redb::checkpoint::RedbFrozenPrefix,
    verifier::{Verifier, VerifyReport},
};

/// Explicit resource limits for one export, not a trading capacity claim.
#[derive(Clone, Debug)]
pub struct CheckpointLimits {
    /// Maximum rows per native destination transaction.
    pub batch_entries: usize,
    /// Maximum encoded entry or index bytes per destination transaction.
    pub batch_bytes: usize,
    /// Maximum physical destination file size, checked between native commits.
    pub max_output_bytes: u64,
    /// Maximum source-file growth while the MVCC view is pinned, checked between batches.
    pub max_source_growth_bytes: u64,
    /// Cache budget for the destination database, independent of the active writer's cache.
    pub cache_size_bytes: usize,
    /// Cooperative deadline, including time waiting for the writer barrier.
    pub timeout: Duration,
}

impl CheckpointLimits {
    pub(crate) fn validate(&self) -> Result<(), CheckpointError> {
        if self.batch_entries == 0
            || self.batch_bytes == 0
            || self.max_output_bytes == 0
            || self.cache_size_bytes == 0
            || self.timeout.is_zero()
        {
            return Err(CheckpointError::InvalidLimits);
        }
        Ok(())
    }

    pub(crate) fn check(
        &self,
        started: Instant,
        cancelled: &AtomicBool,
    ) -> Result<(), CheckpointError> {
        if cancelled.load(Ordering::Acquire) {
            return Err(CheckpointError::Cancelled);
        }

        if started.elapsed() >= self.timeout {
            return Err(CheckpointError::Expired);
        }
        Ok(())
    }
}

/// Metadata frozen at the writer barrier without changing the source run lifecycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixMetadata {
    /// Native manifest at the barrier; its status remains `Running`.
    pub source_manifest: RunManifest,
    /// Last durable source sequence included in the prefix.
    pub high_watermark: u64,
    /// Native writer timestamp at the barrier, not the replay ordering authority.
    #[serde(with = "crate::wire::nanos_as_u64")]
    pub frozen_at_ns: UnixNanos,
    /// Opaque cache reference only, without a claim about full kernel state.
    pub snapshot_anchor: Option<SnapshotAnchor>,
}

/// Artifact identity separate from a source run's lifecycle status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointKind {
    /// An immutable sequence prefix, not a completed run or an exact replay qualification.
    PrefixCheckpoint,
}

/// Publication state of the checkpoint artifact, independent of `RunStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointStatus {
    /// Private staging output which must never be opened as a completed checkpoint.
    Writing,
    /// All frozen rows were exported and the completion metadata committed durably.
    Complete,
}

/// Native content identity of a checkpoint artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixCheckpointDescriptor {
    /// Explicit native artifact kind.
    pub kind: CheckpointKind,
    /// Checkpoint descriptor schema, independent of the source payload schema.
    pub schema_version: u32,
    /// Artifact completion, without synthesizing a `RunEnded` event.
    pub status: CheckpointStatus,
    /// Original source identity and frozen high-watermark.
    pub prefix: PrefixMetadata,
    /// Count and digest of original encoded entry rows in ascending sequence order.
    pub entries: TableDigest,
    /// Count and digest of original client-order index rows in key order.
    pub client_order_index: TableDigest,
    /// Count and digest of original venue-order index rows in key order.
    pub venue_order_index: TableDigest,
}

/// Hash of native table keys and original values, with explicit lengths.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDigest {
    /// Number of rows included in the digest.
    pub rows: u64,
    /// BLAKE3 over length-delimited native keys and values.
    pub blake3: [u8; 32],
}

/// A frozen native read handle. It is consumed by the owned export worker.
///
/// The read transaction pins redb pages until dropped. Applications use
/// `EventStoreWriter::start_prefix_checkpoint` rather than retaining this handle.
pub struct FrozenPrefix {
    pub(crate) inner: RedbFrozenPrefix,
}

impl Debug for FrozenPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(FrozenPrefix))
            .field("metadata", &self.inner.metadata)
            .finish_non_exhaustive()
    }
}

/// One owned export operation. Dropping the ticket requests cancellation without blocking a node.
///
/// Cancellation and deadlines are checked between bounded reads and commits. Filesystem calls
/// are not forcibly interrupted. Actual MVCC page growth and shared-device latency require
/// workload qualification; byte checks are not filesystem quotas.
#[derive(Debug)]
pub struct CheckpointJob {
    result: Receiver<Result<PrefixCheckpointDescriptor, CheckpointError>>,
    cancelled: Arc<AtomicBool>,
}

impl CheckpointJob {
    /// Requests cooperative cancellation and release of the frozen native view.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns an available native export result without waiting.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker disconnected without a result.
    pub fn try_result(
        &self,
    ) -> Result<Option<Result<PrefixCheckpointDescriptor, CheckpointError>>, CheckpointError> {
        match self.result.try_recv() {
            Ok(result) => Ok(Some(result)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(CheckpointError::WorkerClosed),
        }
    }

    /// Waits up to `timeout` for the native export result, outside the node owner thread.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker disconnected without a result.
    pub fn wait_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<Result<PrefixCheckpointDescriptor, CheckpointError>>, CheckpointError> {
        match self.result.recv_timeout(timeout) {
            Ok(result) => Ok(Some(result)),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => Err(CheckpointError::WorkerClosed),
        }
    }
}

impl Drop for CheckpointJob {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[cfg(not(madsim))]
pub(crate) fn spawn_export(
    frozen: Receiver<Result<FrozenPrefix, EventStoreError>>,
    destination: PathBuf,
    limits: CheckpointLimits,
    cancelled: Arc<AtomicBool>,
    active: Arc<AtomicBool>,
    started: Instant,
) -> Result<CheckpointJob, CheckpointError> {
    let (tx, result) = mpsc::sync_channel(1);
    let cancelled_for_worker = cancelled.clone();
    let active_guard = ActiveJob(active);
    std::thread::Builder::new()
        .name("event-store-prefix-export".to_string())
        .spawn(move || {
            let active_guard = active_guard;
            let export = (|| {
                limits.check(started, &cancelled_for_worker)?;
                let prefix = loop {
                    limits.check(started, &cancelled_for_worker)?;
                    let remaining = limits
                        .timeout
                        .saturating_sub(started.elapsed())
                        .min(Duration::from_millis(10));

                    match frozen.recv_timeout(remaining) {
                        Ok(result) => break result.map_err(CheckpointError::Source)?,
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            return Err(CheckpointError::WorkerClosed);
                        }
                    }
                };
                limits.check(started, &cancelled_for_worker)?;
                prefix
                    .inner
                    .export(&destination, &limits, &cancelled_for_worker, started)
            })();
            cancelled_for_worker.store(true, Ordering::Release);
            drop(active_guard);
            let _ = tx.send(export);
        })
        .map_err(|e| CheckpointError::Destination(format!("spawn prefix export: {e}")))?;
    Ok(CheckpointJob { result, cancelled })
}

#[cfg(not(madsim))]
struct ActiveJob(Arc<AtomicBool>);

#[cfg(not(madsim))]
impl Drop for ActiveJob {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Read-only access to an explicitly completed prefix artifact.
///
/// This library reader runs in-process. The native `verify --prefix-checkpoint` binary
/// supplies the existing verifier's subprocess boundary for potentially damaged files.
#[derive(Debug)]
pub struct PrefixCheckpointReader {
    descriptor: PrefixCheckpointDescriptor,
    reader: EventStoreReader<RedbBackend>,
}

impl PrefixCheckpointReader {
    /// Opens a completed prefix artifact, refusing ordinary runs and partial output.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid kind, descriptor, source identity or native storage.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CheckpointError> {
        let (backend, descriptor) = RedbBackend::open_prefix_checkpoint(path.as_ref())?;
        Ok(Self {
            descriptor,
            reader: EventStoreReader::new(backend),
        })
    }

    /// Returns the explicit prefix identity and original content digests.
    #[must_use]
    pub const fn descriptor(&self) -> &PrefixCheckpointDescriptor {
        &self.descriptor
    }

    /// Returns the existing native reader; its manifest still describes a running source.
    #[must_use]
    pub const fn reader(&self) -> &EventStoreReader<RedbBackend> {
        &self.reader
    }

    /// Verifies native entry integrity and exact exported table identities.
    ///
    /// # Errors
    ///
    /// Returns an error when a native read or verification operation cannot complete.
    pub fn verify(self) -> Result<PrefixCheckpointVerifyReport, CheckpointError> {
        let backend = self.reader.into_inner();
        let actual = backend.prefix_table_digests()?;
        let matches = actual
            == (
                self.descriptor.entries.clone(),
                self.descriptor.client_order_index.clone(),
                self.descriptor.venue_order_index.clone(),
            );
        let run_integrity = Verifier::new(Box::new(backend))
            .verify()
            .map_err(|e| CheckpointError::Verification(e.to_string()))?;
        Ok(PrefixCheckpointVerifyReport {
            descriptor: self.descriptor,
            table_digests_match: matches,
            run_integrity,
        })
    }
}

/// Structural prefix verification, without a full-state or exact-replay assertion.
#[derive(Debug)]
pub struct PrefixCheckpointVerifyReport {
    /// Explicit artifact/source identity.
    pub descriptor: PrefixCheckpointDescriptor,
    /// Whether every original encoded entry/index table matches its captured digest.
    pub table_digests_match: bool,
    /// Existing native entry, index and manifest checks. Source status remains `Running`.
    pub run_integrity: VerifyReport,
}

impl PrefixCheckpointVerifyReport {
    /// Returns whether structural checks and native table identities passed.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.table_digests_match && self.run_integrity.is_clean()
    }
}

/// Failures of optional prefix export, distinct from required capture backpressure.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// A per-writer job is already active, or optional admission could not enter the queue.
    #[error("prefix checkpoint is busy")]
    Busy,
    /// Per-job limits must be explicit and nonzero where progress requires them.
    #[error("invalid prefix checkpoint limits")]
    InvalidLimits,
    /// Cancellation requested by the owner or a dropped ticket.
    #[error("prefix checkpoint cancelled")]
    Cancelled,
    /// The cooperative deadline elapsed.
    #[error("prefix checkpoint deadline elapsed")]
    Expired,
    /// A configured byte bound was exceeded.
    #[error("prefix checkpoint limit exceeded: {0}")]
    Limit(&'static str),
    /// A required native source read or writer flush failed.
    #[error("prefix checkpoint source: {0}")]
    Source(#[from] EventStoreError),
    /// Destination creation, publication or write failed.
    #[error("prefix checkpoint destination: {0}")]
    Destination(String),
    /// The complete destination exists, but its directory durability is uncertain.
    #[error(
        "complete prefix exists at {destination:?}; publication directory sync failed: {reason}"
    )]
    PublicationUncertain {
        /// Completed destination which must be inspected before retrying.
        destination: PathBuf,
        /// Native filesystem failure after atomic publication.
        reason: String,
    },
    /// Artifact metadata or verification failed.
    #[error("prefix checkpoint verification: {0}")]
    Verification(String),
    /// Worker closed without handing back its terminal result.
    #[error("prefix checkpoint worker closed")]
    WorkerClosed,
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::sync::mpsc::{Receiver, SyncSender};

    use bytes::Bytes;
    use indexmap::IndexMap;
    use nautilus_core::time::get_atomic_clock_static;
    use redb::{ReadableDatabase, TableDefinition};
    use rstest::rstest;
    use tempfile::TempDir;
    use ustr::Ustr;

    use super::*;
    use crate::{
        AppendEntry, EventStore, EventStoreEntry, EventStoreWriter, Headers, IndexKey, IndexKind,
        RegisteredComponents, RunStatus, ScanDirection, WriterConfig, codec, compute_entry_hash,
        writer::EntryDraft,
    };

    fn manifest() -> RunManifest {
        RunManifest {
            run_id: "native-prefix-source".to_string(),
            parent_run_id: None,
            instance_id: "prefix-instance".to_string(),
            binary_hash: "binary".to_string(),
            schema_version: 1,
            crate_versions: "lock".to_string(),
            feature_flags: vec![],
            adapter_versions: IndexMap::new(),
            config_hash: "config".to_string(),
            registered_components: RegisteredComponents::default(),
            seed: None,
            start_ts_init: 0.into(),
            end_ts_init: None,
            high_watermark: 0,
            status: RunStatus::Running,
        }
    }

    fn draft(seq: u64) -> EntryDraft {
        EntryDraft {
            headers: Headers::empty(),
            topic: "native.prefix.event".into(),
            payload_type: Ustr::from("PrefixTest"),
            payload: Bytes::from(vec![u8::try_from(seq).unwrap(); 64]),
            ts_init: seq.into(),
            index_keys: vec![
                IndexKey::new(IndexKind::ClientOrderId, format!("client-{}", seq / 2)),
                IndexKey::new(IndexKind::VenueOrderId, format!("venue-{seq}")),
            ],
        }
    }

    fn append(seq: u64) -> AppendEntry {
        let value = draft(seq);
        let hash = compute_entry_hash(
            seq,
            value.ts_init,
            value.ts_init,
            value.topic.as_ref(),
            value.payload_type.as_str(),
            &value.payload,
            &value.headers,
        );
        AppendEntry::new(
            EventStoreEntry::new(
                hash,
                seq,
                value.headers,
                value.topic,
                value.payload_type,
                value.payload,
                value.ts_init,
                value.ts_init,
            ),
            value.index_keys,
        )
    }

    fn limits() -> CheckpointLimits {
        CheckpointLimits {
            batch_entries: 2,
            batch_bytes: 1024,
            max_output_bytes: 32 * 1024 * 1024,
            max_source_growth_bytes: 32 * 1024 * 1024,
            cache_size_bytes: 1024 * 1024,
            timeout: Duration::from_secs(5),
        }
    }

    fn backend(directory: &TempDir) -> RedbBackend {
        let mut backend = RedbBackend::new(directory.path()).with_cache_size(1024 * 1024);
        backend.open_run(manifest()).unwrap();
        backend
    }

    fn finished(job: &CheckpointJob) -> Result<PrefixCheckpointDescriptor, CheckpointError> {
        job.wait_timeout(Duration::from_secs(10))
            .unwrap()
            .expect("checkpoint job must finish")
    }

    #[rstest]
    fn prefix_keeps_original_rows_indices_and_anchor_while_source_appends() {
        let directory = TempDir::new().unwrap();
        let mut source = backend(&directory);
        let original = (1..=8).map(append).collect::<Vec<_>>();
        source.append_batch(&original).unwrap();
        let anchor = SnapshotAnchor::new(5, "opaque-cache-reference", "cache-owned-hash");
        source.record_snapshot_anchor(anchor.clone()).unwrap();
        let frozen = source.freeze_prefix(123.into()).unwrap();
        let output = directory.path().join("prefix.checkpoint");
        let target = output.clone();

        let worker = std::thread::spawn(move || {
            frozen
                .inner
                .export(&target, &limits(), &AtomicBool::new(false), Instant::now())
        });

        for seq in 9..=128 {
            source.append_batch(&[append(seq)]).unwrap();
        }
        let descriptor = worker.join().unwrap().unwrap();
        assert_eq!(descriptor.prefix.high_watermark, 8);
        assert_eq!(descriptor.prefix.snapshot_anchor, Some(anchor));
        assert_eq!(descriptor.prefix.source_manifest.status, RunStatus::Running);
        assert_eq!(source.manifest().unwrap().status, RunStatus::Running);
        assert_eq!(source.high_watermark().unwrap(), 128);
        let reader = PrefixCheckpointReader::open(&output).unwrap();
        let rows = reader
            .reader()
            .scan_range(1, 128, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows,
            original
                .iter()
                .map(|value| value.entry.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            reader
                .reader()
                .lookup(IndexKind::ClientOrderId, "client-1")
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            reader
                .reader()
                .lookup(IndexKind::VenueOrderId, "venue-9")
                .unwrap(),
            None
        );
        assert!(reader.reader().scan_seq(9).unwrap().is_none());
        assert!(reader.verify().unwrap().is_clean());
        assert!(RedbBackend::open_sealed_file(&output).is_err());
        let source_path = source.current_path().unwrap().to_path_buf();
        drop(source);
        // Compare original native encoded row bytes, not only deserialized payloads
        let source_db = redb::Database::builder()
            .open_read_only(source_path)
            .unwrap();
        let output_db = redb::Database::builder().open_read_only(output).unwrap();
        let source_tx = source_db.begin_read().unwrap();
        let output_tx = output_db.begin_read().unwrap();
        let entries = TableDefinition::<u64, &[u8]>::new("entries");
        let source_table = source_tx.open_table(entries).unwrap();
        let output_table = output_tx.open_table(entries).unwrap();

        for seq in 1..=8 {
            assert_eq!(
                source_table.get(seq).unwrap().unwrap().value(),
                output_table.get(seq).unwrap().unwrap().value()
            );
        }
    }

    #[rstest]
    fn writer_freeze_flushes_pending_entries_and_export_failure_does_not_halt_capture() {
        let directory = TempDir::new().unwrap();
        let backend = backend(&directory);
        let halted = Arc::new(AtomicBool::new(false));
        let halted_clone = halted.clone();

        let writer = EventStoreWriter::spawn(
            Box::new(backend),
            get_atomic_clock_static(),
            Arc::new(move |_| {
                halted_clone.store(true, Ordering::Release);
            }),
            WriterConfig {
                max_batch_entries: 100,
                max_batch_latency: Duration::from_secs(60),
                ..WriterConfig::default()
            },
        )
        .unwrap();
        for seq in 1..=4 {
            writer.submit(draft(seq)).unwrap();
        }
        let failed = writer
            .start_prefix_checkpoint(
                directory.path().join("missing").join("bad.checkpoint"),
                limits(),
            )
            .unwrap();
        assert!(matches!(
            finished(&failed),
            Err(CheckpointError::Destination(_))
        ));
        assert_eq!(writer.high_watermark(), 4);
        writer.submit(draft(5)).unwrap();
        let output = directory.path().join("good.checkpoint");
        let succeeded = writer.start_prefix_checkpoint(&output, limits()).unwrap();
        assert_eq!(finished(&succeeded).unwrap().prefix.high_watermark, 5);
        assert!(!halted.load(Ordering::Acquire));
        let prefix = PrefixCheckpointReader::open(output).unwrap();
        assert!(prefix.verify().unwrap().is_clean());
        writer
            .close(EntryDraft {
                payload_type: "RunEnded".into(),
                ..draft(6)
            })
            .unwrap();
    }

    #[rstest]
    fn cancelled_and_bounded_exports_release_the_native_view_without_publication() {
        for mode in ["cancelled", "deadline", "bytes"] {
            let directory = TempDir::new().unwrap();
            let mut backend = backend(&directory);
            backend.append_batch(&[append(1)]).unwrap();
            let source_path = backend.current_path().unwrap().to_path_buf();
            let frozen = backend.freeze_prefix(5.into()).unwrap();
            drop(backend);
            let mut bound = limits();
            let cancelled = AtomicBool::new(mode == "cancelled");
            if mode == "deadline" {
                bound.timeout = Duration::ZERO;
            }

            if mode == "bytes" {
                bound.max_output_bytes = 1;
            }
            let output = directory.path().join("cancelled.checkpoint");
            let result = frozen
                .inner
                .export(&output, &bound, &cancelled, Instant::now());
            assert!(result.is_err());
            assert!(!output.exists());
            assert!(directory.path().read_dir().unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains("partial")
            }));
            // The original database owner and its read transaction have both been released
            assert!(
                redb::Database::builder()
                    .open_read_only(source_path)
                    .is_ok()
            );
        }
    }

    #[rstest]
    fn existing_artifacts_are_never_overwritten_and_partial_metadata_is_rejected() {
        let directory = TempDir::new().unwrap();
        let mut source = backend(&directory);
        source.append_batch(&[append(1)]).unwrap();
        let output = directory.path().join("existing.checkpoint");
        std::fs::write(&output, b"original evidence").unwrap();
        let frozen = source.freeze_prefix(5.into()).unwrap();
        assert!(
            frozen
                .inner
                .export(&output, &limits(), &AtomicBool::new(false), Instant::now())
                .is_err()
        );
        assert_eq!(std::fs::read(&output).unwrap(), b"original evidence");
        std::fs::remove_file(&output).unwrap();
        let mut descriptor = source
            .freeze_prefix(6.into())
            .unwrap()
            .inner
            .export(&output, &limits(), &AtomicBool::new(false), Instant::now())
            .unwrap();
        descriptor.status = CheckpointStatus::Writing;
        let db = redb::Database::open(&output).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write
                .open_table(TableDefinition::<&str, &[u8]>::new("prefix_checkpoint"))
                .unwrap();
            table
                .insert(
                    "descriptor",
                    codec::encode_to_vec(&descriptor).unwrap().as_slice(),
                )
                .unwrap();
        }
        write.commit().unwrap();
        drop(db);
        assert!(PrefixCheckpointReader::open(&output).is_err());
        assert_eq!(source.manifest().unwrap().status, RunStatus::Running);
        source.append_batch(&[append(2)]).unwrap();
        assert_eq!(source.high_watermark().unwrap(), 2);
    }

    #[rstest]
    fn prefix_verifier_detects_missing_original_index_rows() {
        let directory = TempDir::new().unwrap();
        let mut source = backend(&directory);
        source.append_batch(&[append(1)]).unwrap();
        let output = directory.path().join("index.checkpoint");
        source
            .freeze_prefix(1.into())
            .unwrap()
            .inner
            .export(&output, &limits(), &AtomicBool::new(false), Instant::now())
            .unwrap();
        let db = redb::Database::open(&output).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write
                .open_table(TableDefinition::<&str, u64>::new("venue_order_id_idx"))
                .unwrap();
            table.remove("venue-1").unwrap();
        }
        write.commit().unwrap();
        drop(db);
        let report = PrefixCheckpointReader::open(output)
            .unwrap()
            .verify()
            .unwrap();
        assert!(!report.is_clean());
        assert!(!report.table_digests_match);
    }

    struct GateBackend {
        inner: RedbBackend,
        reached: SyncSender<()>,
        release: Receiver<()>,
        first: AtomicBool,
    }
    impl EventStore for GateBackend {
        fn open_run(&mut self, manifest: RunManifest) -> Result<(), EventStoreError> {
            self.inner.open_run(manifest)
        }
        fn append_batch(&mut self, entries: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.inner.append_batch(entries)
        }
        fn scan_range(
            &self,
            from: u64,
            to: u64,
            direction: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            self.inner.scan_range(from, to, direction)
        }
        fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            self.inner.scan_seq(seq)
        }
        fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
            self.inner.lookup(kind, key)
        }
        fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            self.inner.iter_index_keys(kind)
        }
        fn latest_snapshot_anchor(&self) -> Result<Option<SnapshotAnchor>, EventStoreError> {
            self.inner.latest_snapshot_anchor()
        }
        fn freeze_prefix(&self, now: UnixNanos) -> Result<FrozenPrefix, EventStoreError> {
            if self.first.swap(false, Ordering::AcqRel) {
                self.reached.send(()).unwrap();
                self.release.recv().unwrap();
            }
            self.inner.freeze_prefix(now)
        }
        fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
            self.inner.seal(status)
        }
        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            self.inner.manifest()
        }
        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            self.inner.high_watermark()
        }
    }
    struct ReleaseOnDrop(SyncSender<()>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            let _ = self.0.try_send(());
        }
    }

    #[rstest]
    fn dropped_ticket_and_busy_admission_do_not_poison_the_writer() {
        let directory = TempDir::new().unwrap();
        let (reached, reached_rx) = mpsc::sync_channel(1);
        let (release, release_rx) = mpsc::sync_channel(1);
        let backend = GateBackend {
            inner: backend(&directory),
            reached,
            release: release_rx,
            first: AtomicBool::new(true),
        };
        let halted = Arc::new(AtomicBool::new(false));
        let halted_clone = halted.clone();

        let writer = EventStoreWriter::spawn(
            Box::new(backend),
            get_atomic_clock_static(),
            Arc::new(move |_| {
                halted_clone.store(true, Ordering::Release);
            }),
            WriterConfig {
                channel_capacity: 1,
                ..WriterConfig::default()
            },
        )
        .unwrap();
        let release_on_drop = ReleaseOnDrop(release);
        let cancelled_path = directory.path().join("dropped.checkpoint");
        let job = writer
            .start_prefix_checkpoint(&cancelled_path, limits())
            .unwrap();
        reached_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(matches!(
            writer.start_prefix_checkpoint(directory.path().join("busy.checkpoint"), limits()),
            Err(CheckpointError::Busy)
        ));
        writer.submit(draft(1)).unwrap();
        drop(job);
        release_on_drop.0.send(()).unwrap();
        let until = Instant::now() + Duration::from_secs(3);
        let next = loop {
            match writer.start_prefix_checkpoint(directory.path().join("next.checkpoint"), limits())
            {
                Ok(job) => break job,
                Err(CheckpointError::Busy) if Instant::now() < until => std::thread::yield_now(),
                other => panic!("checkpoint admission did not recover: {other:?}"),
            }
        };
        assert_eq!(finished(&next).unwrap().prefix.high_watermark, 1);
        assert!(!cancelled_path.exists());
        assert!(!halted.load(Ordering::Acquire));
        drop(writer);
    }
}
