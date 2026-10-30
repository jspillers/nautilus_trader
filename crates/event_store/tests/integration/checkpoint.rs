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

//! Explicit prefix verification uses the existing native subprocess boundary.

use std::{
    cell::{Cell, RefCell},
    fs,
    io::{Seek, SeekFrom, Write},
    process::Command,
    rc::Rc,
    time::Duration,
};

use bytes::Bytes;
use indexmap::IndexMap;
use nautilus_common::{
    clock::{Clock, VirtualClock},
    enums::Environment,
    timer::{TimeEvent, TimeEventCallback, TimeEventHandler},
};
use nautilus_core::{UUID4, time::get_atomic_clock_static};
use nautilus_event_store::{
    CheckpointError, CheckpointLimits, CheckpointStatus, EventStore, EventStoreConfig,
    EventStoreError, EventStoreLifecycle, EventStoreWriter, HaltSignal, Headers, IndexKey,
    IndexKind, PrefixCheckpointDescriptor, PrefixCheckpointReader, RedbBackend,
    RegisteredComponents, RunManifest, RunStatus, build_run_id, codec, open_run,
    writer::{EntryDraft, WriterConfig, noop_halt},
};
use redb::{ReadableTable, TableDefinition};
use rstest::rstest;
use tempfile::TempDir;

fn checkpoint(directory: &TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
    let mut backend = RedbBackend::new(directory.path()).with_cache_size(1024 * 1024);
    backend
        .open_run(RunManifest {
            run_id: "prefix-verifier".into(),
            parent_run_id: None,
            instance_id: "prefix-test".into(),
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
    let source = backend.current_path().unwrap().to_path_buf();
    let writer = EventStoreWriter::spawn(
        Box::new(backend),
        get_atomic_clock_static(),
        noop_halt(),
        WriterConfig::default(),
    )
    .unwrap();

    for seq in 1..=16 {
        writer
            .submit(EntryDraft {
                headers: Headers::empty(),
                topic: "native.checkpoint.test".into(),
                payload_type: "CheckpointTest".into(),
                payload: Bytes::from(vec![1; 4096]),
                ts_init: seq.into(),
                index_keys: vec![IndexKey::new(
                    IndexKind::VenueOrderId,
                    format!("venue-{seq}"),
                )],
            })
            .unwrap();
    }
    let output = directory.path().join("prefix.checkpoint");
    let job = writer
        .start_prefix_checkpoint(
            &output,
            CheckpointLimits {
                batch_entries: 2,
                batch_bytes: 16 * 1024,
                max_output_bytes: 32 * 1024 * 1024,
                max_source_growth_bytes: 32 * 1024 * 1024,
                cache_size_bytes: 1024 * 1024,
                timeout: Duration::from_secs(10),
            },
        )
        .unwrap();
    job.wait_timeout(Duration::from_secs(15))
        .unwrap()
        .unwrap()
        .unwrap();
    drop(writer);
    (output, source)
}

fn verify(path: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_verify"))
        .arg("--prefix-checkpoint")
        .arg(path)
        .output()
        .unwrap()
}

fn checkpoint_limits() -> CheckpointLimits {
    CheckpointLimits {
        batch_entries: 2,
        batch_bytes: 16 * 1024,
        max_output_bytes: 32 * 1024 * 1024,
        max_source_growth_bytes: 32 * 1024 * 1024,
        cache_size_bytes: 1024 * 1024,
        timeout: Duration::from_secs(10),
    }
}

fn fire_native_timer(delivered: &Rc<Cell<u64>>, at: u64) {
    let delivered = delivered.clone();
    let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_| delivered.set(delivered.get() + 1));
    TimeEventHandler::new(
        TimeEvent::new(
            "checkpoint-lifecycle".into(),
            UUID4::new(),
            at.into(),
            at.into(),
        ),
        TimeEventCallback::from(callback),
    )
    .run();
}

#[rstest]
fn checkpoint_lifecycle_public_handle_keeps_capture_running() {
    let directory = TempDir::new().unwrap();
    let config = EventStoreConfig {
        base_dir: directory.path().to_path_buf(),
        redb_cache_size_bytes: 1024 * 1024,
        nonblocking_submit: true,
        ..Default::default()
    };
    let instance_id = UUID4::new();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let mut store = EventStoreLifecycle::boot(Some(config), instance_id, clock).unwrap();
    store
        .open(
            instance_id,
            &RegisteredComponents::default(),
            Environment::Backtest,
        )
        .unwrap();
    let run_id = store.run_id().unwrap().to_string();
    let delivered = Rc::new(Cell::new(0));
    fire_native_timer(&delivered, 1);
    let first_path = directory.path().join("first.checkpoint");
    let first = store
        .start_prefix_checkpoint(&first_path, checkpoint_limits())
        .unwrap();
    fire_native_timer(&delivered, 2);
    let first = first
        .wait_timeout(Duration::from_secs(15))
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.prefix.high_watermark, 2);
    assert_eq!(first.prefix.source_manifest.status, RunStatus::Running);
    assert_eq!(first.prefix.source_manifest.end_ts_init, None);
    assert_eq!(store.run_id(), Some(run_id.as_str()));
    assert!(!store.is_halted());
    assert_eq!(delivered.get(), 2);
    assert!(
        PrefixCheckpointReader::open(&first_path)
            .unwrap()
            .verify()
            .unwrap()
            .is_clean()
    );

    let second_path = directory.path().join("second.checkpoint");
    let second = store
        .start_prefix_checkpoint(&second_path, checkpoint_limits())
        .unwrap()
        .wait_timeout(Duration::from_secs(15))
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.prefix.high_watermark, 3);
    assert_eq!(second.prefix.source_manifest.status, RunStatus::Running);
    store.seal(10.into());
    let source =
        RedbBackend::open_sealed(directory.path(), &instance_id.to_string(), &run_id).unwrap();
    assert_eq!(source.manifest().unwrap().status, RunStatus::Ended);
    assert_eq!(source.high_watermark().unwrap(), 4);
    assert!(matches!(
        store.start_prefix_checkpoint(
            directory.path().join("sealed.checkpoint"),
            checkpoint_limits()
        ),
        Err(CheckpointError::Source(EventStoreError::Closed))
    ));
}

#[rstest]
fn checkpoint_inactive_lifecycle_and_closed_session_reject_export() {
    let directory = TempDir::new().unwrap();
    let instance_id = UUID4::new();
    let config = EventStoreConfig {
        base_dir: directory.path().to_path_buf(),
        redb_cache_size_bytes: 1024 * 1024,
        ..Default::default()
    };

    for config in [None, Some(config.clone())] {
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
        let store = EventStoreLifecycle::boot(config, instance_id, clock).unwrap();
        assert!(matches!(
            store.start_prefix_checkpoint(
                directory.path().join("inactive.checkpoint"),
                checkpoint_limits()
            ),
            Err(CheckpointError::Source(EventStoreError::Closed))
        ));
    }
    let mut session = open_run(
        &config,
        &instance_id.to_string(),
        build_run_id(1.into()),
        None,
        1.into(),
        &RegisteredComponents::default(),
        HaltSignal::new(),
        get_atomic_clock_static(),
    )
    .unwrap();
    session.close(2.into()).unwrap();
    assert!(matches!(
        session.start_prefix_checkpoint(
            directory.path().join("closed.checkpoint"),
            checkpoint_limits()
        ),
        Err(CheckpointError::Source(EventStoreError::Closed))
    ));
    assert!(!directory.path().join("inactive.checkpoint").exists());
    assert!(!directory.path().join("closed.checkpoint").exists());
}

#[rstest]
fn checkpoint_binary_preserves_source_running_and_requires_explicit_kind() {
    let directory = TempDir::new().unwrap();
    let (path, source) = checkpoint(&directory);
    let original = fs::read(&path).unwrap();
    let source_original = fs::read(&source).unwrap();
    let valid = verify(&path);
    let text = String::from_utf8(valid.stdout).unwrap();
    assert_eq!(valid.status.code(), Some(0), "{text}");
    assert!(text.contains("artifact_kind=prefix_checkpoint source_run_id=prefix-verifier source_status=Running high_watermark=16"));
    assert!(
        text.contains("table_digests_match=true markers=not-exported exact_replay=unqualified")
    );
    let wrong = Command::new(env!("CARGO_BIN_EXE_verify"))
        .arg(&path)
        .output()
        .unwrap();
    assert_ne!(wrong.status.code(), Some(0));
    assert_ne!(verify(&source).status.code(), Some(0));
    assert_eq!(fs::read(path).unwrap(), original);
    assert_eq!(fs::read(source).unwrap(), source_original);
}

#[rstest]
#[case("index")]
#[case("hash")]
#[case("partial")]
fn checkpoint_binary_rejects_mutated_native_content(#[case] mode: &str) {
    let directory = TempDir::new().unwrap();
    let (path, source) = checkpoint(&directory);
    let source_original = fs::read(&source).unwrap();
    let db = redb::Database::open(&path).unwrap();
    let write = db.begin_write().unwrap();

    match mode {
        "index" => {
            write
                .open_table(TableDefinition::<&str, u64>::new("venue_order_id_idx"))
                .unwrap()
                .remove("venue-1")
                .unwrap();
        }
        "hash" => {
            let mut table = write
                .open_table(TableDefinition::<u64, &[u8]>::new("entries"))
                .unwrap();
            let raw = table.get(1).unwrap().unwrap().value().to_vec();
            let mut entry: nautilus_event_store::EventStoreEntry =
                codec::decode_from_slice(&raw).unwrap();
            entry.payload = Bytes::from_static(b"changed");
            table
                .insert(1, codec::encode_to_vec(&entry).unwrap().as_slice())
                .unwrap();
        }
        "partial" => {
            let mut table = write
                .open_table(TableDefinition::<&str, &[u8]>::new("prefix_checkpoint"))
                .unwrap();
            let raw = table.get("descriptor").unwrap().unwrap().value().to_vec();
            let mut descriptor: PrefixCheckpointDescriptor =
                codec::decode_from_slice(&raw).unwrap();
            descriptor.status = CheckpointStatus::Writing;
            table
                .insert(
                    "descriptor",
                    codec::encode_to_vec(&descriptor).unwrap().as_slice(),
                )
                .unwrap();
        }
        _ => unreachable!(),
    }
    write.commit().unwrap();
    drop(db);
    let changed = fs::read(&path).unwrap();
    let output = verify(&path);
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(fs::read(path).unwrap(), changed);
    assert_eq!(fs::read(source).unwrap(), source_original);
}

#[rstest]
#[case(false)]
#[case(true)]
fn checkpoint_binary_contains_truncation_and_zero_tail_damage(#[case] zero_tail: bool) {
    let directory = TempDir::new().unwrap();
    let (path, source) = checkpoint(&directory);
    let source_original = fs::read(&source).unwrap();
    let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    let length = file.metadata().unwrap().len();
    file.set_len(8192).unwrap();
    if zero_tail {
        file.seek(SeekFrom::Start(8192)).unwrap();
        file.write_all(&vec![0; usize::try_from(length - 8192).unwrap()])
            .unwrap();
    }
    file.sync_all().unwrap();
    drop(file);
    let changed = fs::read(&path).unwrap();
    let output = verify(&path);
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("quarantine=not-performed"));
    assert_eq!(fs::read(path).unwrap(), changed);
    assert_eq!(fs::read(source).unwrap(), source_original);
}

#[cfg(debug_assertions)]
#[rstest]
#[case(false)]
#[case(true)]
fn checkpoint_binary_contains_worker_abort_and_timeout(#[case] timeout: bool) {
    let directory = TempDir::new().unwrap();
    let (path, _) = checkpoint(&directory);
    let original = fs::read(&path).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_verify"));
    if timeout {
        command
            .env("NAUTILUS_EVENT_STORE_VERIFY_SLEEP_WORKER_MS", "250")
            .env("NAUTILUS_EVENT_STORE_VERIFY_TIMEOUT_SECS", "0");
    } else {
        command.env("NAUTILUS_EVENT_STORE_VERIFY_ABORT_WORKER", "1");
    }
    let output = command
        .arg("--prefix-checkpoint")
        .arg(&path)
        .output()
        .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(1), "{text}");
    assert!(text.contains("worker_status=") && text.contains("quarantine=not-performed"));
    assert_eq!(fs::read(path).unwrap(), original);
}
