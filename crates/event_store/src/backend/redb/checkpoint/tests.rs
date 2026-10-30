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

use std::{
    io,
    sync::{
        Barrier,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use indexmap::IndexMap;
use redb::{StorageBackend, backends::FileBackend};
use rstest::rstest;
use tempfile::TempDir;

use super::*;
use crate::{
    AppendEntry, EventStore, Headers, IndexKey, IndexKind, PrefixCheckpointReader,
    RegisteredComponents, RunManifest, compute_entry_hash,
};

fn source(directory: &TempDir) -> RedbBackend {
    let mut source = RedbBackend::new(directory.path()).with_cache_size(1024 * 1024);
    source
        .open_run(RunManifest {
            run_id: "checkpoint-faults".into(),
            parent_run_id: None,
            instance_id: "checkpoint-test".into(),
            binary_hash: "binary".into(),
            schema_version: 1,
            crate_versions: "lock".into(),
            feature_flags: vec![],
            adapter_versions: IndexMap::new(),
            config_hash: "config".into(),
            registered_components: RegisteredComponents::default(),
            seed: None,
            start_ts_init: 0.into(),
            end_ts_init: None,
            high_watermark: 0,
            status: RunStatus::Running,
        })
        .unwrap();
    source
        .append_batch(&(1..=3).map(|seq| append(seq, 128)).collect::<Vec<_>>())
        .unwrap();
    source
}

fn append(seq: u64, bytes: usize) -> AppendEntry {
    let headers = Headers::empty();
    let payload = Bytes::from(vec![7; bytes]);
    let hash = compute_entry_hash(
        seq,
        seq.into(),
        seq.into(),
        "native.checkpoint.fault",
        "CheckpointFault",
        &payload,
        &headers,
    );
    AppendEntry::new(
        EventStoreEntry::new(
            hash,
            seq,
            headers,
            "native.checkpoint.fault".into(),
            "CheckpointFault".into(),
            payload,
            seq.into(),
            seq.into(),
        ),
        vec![IndexKey::new(
            IndexKind::VenueOrderId,
            format!("venue-{seq}"),
        )],
    )
}

fn limits() -> CheckpointLimits {
    CheckpointLimits {
        batch_entries: 1,
        batch_bytes: 64 * 1024,
        max_output_bytes: 64 * 1024 * 1024,
        max_source_growth_bytes: 64 * 1024 * 1024,
        cache_size_bytes: 1024 * 1024,
        timeout: Duration::from_secs(30),
    }
}

fn descriptor(view: &RedbFrozenPrefix) -> PrefixCheckpointDescriptor {
    PrefixCheckpointDescriptor {
        kind: CheckpointKind::PrefixCheckpoint,
        schema_version: 1,
        status: CheckpointStatus::Writing,
        prefix: view.metadata.clone(),
        entries: TableDigest::default(),
        client_order_index: TableDigest::default(),
        venue_order_index: TableDigest::default(),
    }
}

#[derive(Debug)]
struct FaultBackend {
    inner: FileBackend,
    mode: Arc<AtomicU8>,
    cancelled: Arc<AtomicBool>,
}

struct StopSampling(Arc<AtomicBool>);

impl Drop for StopSampling {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl StorageBackend for FaultBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }
    fn set_len(&self, length: u64) -> io::Result<()> {
        self.inner.set_len(length)
    }
    fn sync_data(&self) -> io::Result<()> {
        match self.mode.load(Ordering::Acquire) {
            2 => return Err(io::Error::other("destination sync failed")),
            3 => self.cancelled.store(true, Ordering::Release),
            4 => std::thread::sleep(Duration::from_millis(20)),
            _ => {}
        }
        self.inner.sync_data()
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        if self.mode.load(Ordering::Acquire) == 1 {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "destination is full",
            ));
        }
        self.inner.write(offset, data)
    }
    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
fn native_destination_fault_and_mid_copy_cancel_release_source(#[case] mode: u8) {
    let directory = TempDir::new().unwrap();
    let mut source = source(&directory);
    let frozen = source.freeze_prefix(10.into()).unwrap().inner;
    let partial = directory.path().join("native.partial");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&partial)
        .unwrap();
    let action = Arc::new(AtomicU8::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let db = Database::builder()
        .set_cache_size(1024 * 1024)
        .create_with_backend(FaultBackend {
            inner: FileBackend::new(file).unwrap(),
            mode: action.clone(),
            cancelled: cancelled.clone(),
        })
        .unwrap();
    initialize(&db, &descriptor(&frozen)).unwrap();
    let mut bounds = limits();
    if mode == 4 {
        bounds.timeout = Duration::from_millis(5);
    }
    action.store(mode, Ordering::Release);
    let result = frozen.copy_entries(&db, &partial, &bounds, &cancelled, Instant::now());

    match mode {
        1 | 2 => assert!(
            matches!(result, Err(CheckpointError::Destination(_))),
            "{result:?}"
        ),
        3 => assert!(
            matches!(result, Err(CheckpointError::Cancelled)),
            "{result:?}"
        ),
        4 => assert!(
            matches!(result, Err(CheckpointError::Expired)),
            "{result:?}"
        ),
        _ => unreachable!(),
    }
    action.store(0, Ordering::Release);
    drop(db);
    drop(frozen);
    source.append_batch(&[append(4, 128)]).unwrap();
    assert_eq!(source.high_watermark().unwrap(), 4);
    assert_eq!(source.manifest().unwrap().status, RunStatus::Running);
    assert!(!directory.path().join("published.checkpoint").exists());
}

#[rstest]
fn source_growth_limit_releases_the_read_view_and_keeps_capture_running() {
    let directory = TempDir::new().unwrap();
    let mut source = source(&directory);
    let frozen = source.freeze_prefix(10.into()).unwrap().inner;
    let initial_bytes = frozen.source_bytes;
    source
        .append_batch(
            &(4..=260)
                .map(|seq| append(seq, 32 * 1024))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    assert!(fs::metadata(source.current_path().unwrap()).unwrap().len() > initial_bytes);
    let mut bounds = limits();
    bounds.max_source_growth_bytes = 0;
    let output = directory.path().join("growth.checkpoint");
    let result = frozen.export(&output, &bounds, &AtomicBool::new(false), Instant::now());
    assert!(matches!(
        result,
        Err(CheckpointError::Limit(
            "source growth while MVCC view is pinned"
        ))
    ));
    assert!(!output.exists());
    assert!(directory.path().read_dir().unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("partial")
    }));
    source.append_batch(&[append(261, 128)]).unwrap();
    let next = directory.path().join("next.checkpoint");
    let mut next_bounds = limits();
    next_bounds.batch_entries = 64;
    next_bounds.batch_bytes = 1024 * 1024;
    source
        .freeze_prefix(11.into())
        .unwrap()
        .inner
        .export(&next, &next_bounds, &AtomicBool::new(false), Instant::now())
        .unwrap();
    assert!(
        PrefixCheckpointReader::open(next)
            .unwrap()
            .verify()
            .unwrap()
            .is_clean()
    );
    assert_eq!(source.high_watermark().unwrap(), 261);
}

#[rstest]
fn concurrent_publication_never_overwrites_the_winner() {
    let directory = TempDir::new().unwrap();
    let mut source = source(&directory);
    let first = source.freeze_prefix(10.into()).unwrap().inner;
    let second = source.freeze_prefix(11.into()).unwrap().inner;
    let output = directory.path().join("winner.checkpoint");
    let barrier = Arc::new(Barrier::new(2));
    let workers = [first, second].map(|view| {
        let target = output.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            view.export(&target, &limits(), &AtomicBool::new(false), Instant::now())
        })
    });
    let results = workers.map(|worker| worker.join().unwrap());
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(
        PrefixCheckpointReader::open(&output)
            .unwrap()
            .verify()
            .unwrap()
            .is_clean()
    );
    let original = fs::read(&output).unwrap();
    let e = publication_error(&output, &io::Error::other("directory sync failed"));
    assert!(
        matches!(e, CheckpointError::PublicationUncertain { destination, .. } if destination == output)
    );
    assert_eq!(fs::read(output).unwrap(), original);
    source.append_batch(&[append(4, 128)]).unwrap();
    assert_eq!(source.manifest().unwrap().status, RunStatus::Running);
}

#[rstest]
#[ignore = "explicit release workload diagnostic; emits measurements, not a capacity guarantee"]
fn checkpoint_workload_qualification() {
    use nautilus_core::time::get_atomic_clock_static;

    use crate::{
        EventStoreWriter,
        writer::{EntryDraft, WriterConfig},
    };

    const SEED: u64 = 4096;
    const APPENDS: u64 = 4096;
    const PAYLOAD_BYTES: usize = 4096;
    // The seed allocates ~64.25 MiB and redb grows in allocation extents. Allow
    // two doublings while the MVCC view is pinned in this diagnostic only.
    const EXPORT_SOURCE_GROWTH_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

    for export in [false, true] {
        let directory = TempDir::new().unwrap();
        let backend = source(&directory);
        let source_path = backend.current_path().unwrap().to_path_buf();
        let RunDatabase::ReadWrite(database) = &backend.state().unwrap().db else {
            unreachable!()
        };
        let database = database.clone();
        let halted = Arc::new(AtomicBool::new(false));
        let halt = halted.clone();

        let writer = EventStoreWriter::spawn(
            Box::new(backend),
            get_atomic_clock_static(),
            Arc::new(move |_| {
                halt.store(true, Ordering::Release);
            }),
            WriterConfig {
                channel_capacity: 8192,
                max_pending_bytes: Some(64 * 1024 * 1024),
                max_batch_entries: 128,
                max_batch_bytes: Some(1024 * 1024),
                ..WriterConfig::default()
            },
        )
        .unwrap();
        let draft = |seq: u64| {
            let value = append(seq, PAYLOAD_BYTES);
            EntryDraft {
                headers: value.entry.headers,
                topic: value.entry.topic,
                payload_type: value.entry.payload_type,
                payload: value.entry.payload,
                ts_init: value.entry.ts_init,
                index_keys: value.index_keys,
            }
        };

        for seq in 4..=SEED {
            writer.submit(draft(seq)).unwrap();
        }
        let until = Instant::now() + Duration::from_secs(60);
        while writer.high_watermark() < SEED {
            assert!(Instant::now() < until, "seed capture stalled");
            std::thread::yield_now();
        }
        let before_bytes = fs::metadata(&source_path).unwrap().len();
        let before_pages = database
            .begin_write()
            .unwrap()
            .stats()
            .unwrap()
            .allocated_pages();
        let observe = Arc::new(AtomicBool::new(true));
        let _stop_sampling = StopSampling(observe.clone());
        let sampling = observe.clone();
        let sampled_database = database.clone();

        let observer = std::thread::spawn(move || {
            let mut peak_pages = before_pages;

            while sampling.load(Ordering::Acquire) {
                let write = sampled_database.begin_write().unwrap();
                peak_pages = peak_pages.max(write.stats().unwrap().allocated_pages());
                drop(write);
                std::thread::sleep(Duration::from_millis(10));
            }
            peak_pages
        });
        let output = directory.path().join("workload.checkpoint");
        let started = Instant::now();
        let job = if export {
            let mut bounds = limits();
            bounds.batch_entries = 128;
            bounds.batch_bytes = 1024 * 1024;
            bounds.max_output_bytes = 128 * 1024 * 1024;
            bounds.max_source_growth_bytes = EXPORT_SOURCE_GROWTH_BUDGET_BYTES;
            bounds.timeout = Duration::from_secs(60);
            Some(writer.start_prefix_checkpoint(&output, bounds).unwrap())
        } else {
            None
        };
        let completion = job.map(|job| {
            std::thread::spawn(move || {
                let descriptor = job
                    .wait_timeout(Duration::from_secs(60))
                    .unwrap()
                    .unwrap()
                    .unwrap();
                (descriptor, started.elapsed().as_nanos())
            })
        });
        let mut submit_ns = Vec::new();
        let mut backlog_peak = 0;

        for seq in SEED + 1..=SEED + APPENDS {
            let value = draft(seq);
            let before = Instant::now();
            writer.submit(value).unwrap();
            submit_ns.push(before.elapsed().as_nanos());
            backlog_peak = backlog_peak.max(seq - writer.high_watermark());
        }

        while writer.high_watermark() < SEED + APPENDS {
            assert!(Instant::now() < until, "concurrent capture stalled");
            std::thread::yield_now();
        }
        let capture_ns = started.elapsed().as_nanos();
        let export_ns = completion.map(|worker| {
            let (descriptor, elapsed) = worker.join().unwrap();
            assert_eq!(descriptor.prefix.high_watermark, SEED);
            elapsed
        });
        writer.submit(draft(SEED + APPENDS + 1)).unwrap();

        while writer.high_watermark() < SEED + APPENDS + 1 {
            assert!(Instant::now() < until, "post-release capture stalled");
            std::thread::yield_now();
        }
        observe.store(false, Ordering::Release);
        let peak_pages = observer.join().unwrap();
        let after_pages = database
            .begin_write()
            .unwrap()
            .stats()
            .unwrap()
            .allocated_pages();
        let page_size = database.begin_write().unwrap().stats().unwrap().page_size();
        let after_bytes = fs::metadata(&source_path).unwrap().len();
        let pending_peak = writer.buffer_stats().peak_pending_bytes;
        submit_ns.sort_unstable();

        if export {
            assert!(
                PrefixCheckpointReader::open(&output)
                    .unwrap()
                    .verify()
                    .unwrap()
                    .is_clean()
            );
        }
        assert!(!halted.load(Ordering::Acquire));
        assert_eq!(writer.high_watermark(), SEED + APPENDS + 1);
        println!(
            "{{\"schema\":\"native-prefix-workload-v1\",\"scenario\":\"{}\",\"seed_entries\":{SEED},\"initial_small_entries\":3,\"initial_small_payload_bytes\":128,\"appended_entries\":{APPENDS},\"reclamation_probe_entries\":1,\"subsequent_payload_bytes\":{PAYLOAD_BYTES},\"export_source_growth_budget_bytes\":{EXPORT_SOURCE_GROWTH_BUDGET_BYTES},\"capture_elapsed_ns\":{capture_ns},\"export_elapsed_ns\":{},\"submit_p50_ns\":{},\"submit_p99_ns\":{},\"submit_max_ns\":{},\"backlog_entries_peak\":{backlog_peak},\"pending_bytes_peak\":{pending_peak},\"source_file_before_bytes\":{before_bytes},\"source_file_after_bytes\":{after_bytes},\"allocated_pages_before\":{before_pages},\"allocated_pages_sampled_peak\":{peak_pages},\"allocated_pages_after_release_and_commit\":{after_pages},\"page_size_bytes\":{page_size},\"observer\":\"native_write_transaction_stats_10ms_both_cases\",\"capacity_qualified\":false,\"exact_replay_qualified\":false}}",
            if export {
                "concurrent_export"
            } else {
                "writer_only"
            },
            export_ns.map_or_else(|| "null".to_string(), |value| value.to_string()),
            submit_ns[submit_ns.len() / 2],
            submit_ns[submit_ns.len() * 99 / 100],
            submit_ns.last().unwrap(),
        );
        drop(writer);
        drop(database);
    }
}
