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

//! Native redb snapshot export. Original encoded rows are copied without payload translation.

use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

use nautilus_core::{UUID4, UnixNanos};
use redb::{
    Database, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition, TableError,
};

use super::{
    CLIENT_ORDER_INDEX, ENTRIES_TABLE, RedbBackend, RunDatabase, RunState, SNAPSHOT_ANCHOR_KEY,
    SNAPSHOT_ANCHOR_TABLE, VENUE_ORDER_INDEX, begin_immediate_write, check_embedded_seq,
    map_commit_err, map_database_err, map_storage_err, map_table_err, map_transaction_err,
};
use crate::{
    EventStoreError, RunStatus, SnapshotAnchor,
    checkpoint::{
        CheckpointError, CheckpointKind, CheckpointLimits, CheckpointStatus,
        PrefixCheckpointDescriptor, PrefixMetadata, TableDigest,
    },
    codec,
    entry::EventStoreEntry,
    format,
};

const CHECKPOINT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("prefix_checkpoint");
const CHECKPOINT_KEY: &str = "descriptor";

/// Native MVCC view plus database ownership, consumed by one export operation.
#[derive(Debug)]
pub(crate) struct RedbFrozenPrefix {
    pub(crate) metadata: PrefixMetadata,
    source_path: PathBuf,
    source_bytes: u64,
    transaction: ReadTransaction,
    // A read transaction owns memory, but Database::drop closes the file; keep the owner alive
    _database: Arc<Database>,
}

impl RedbFrozenPrefix {
    pub(crate) fn new(
        metadata: PrefixMetadata,
        transaction: ReadTransaction,
        database: Arc<Database>,
        source_path: PathBuf,
        source_bytes: u64,
    ) -> Self {
        Self {
            metadata,
            source_path,
            source_bytes,
            transaction,
            _database: database,
        }
    }

    pub(crate) fn export(
        self,
        destination: &Path,
        limits: &CheckpointLimits,
        cancelled: &AtomicBool,
        started: Instant,
    ) -> Result<PrefixCheckpointDescriptor, CheckpointError> {
        limits.check(started, cancelled)?;

        if destination
            .extension()
            .is_none_or(|value| value != "checkpoint")
        {
            return Err(CheckpointError::Destination(
                "prefix artifacts require the .checkpoint extension".to_string(),
            ));
        }

        if destination.exists() {
            return Err(CheckpointError::Destination(
                "destination already exists".to_string(),
            ));
        }
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let staging = parent.join(format!(".prefix-{}.partial", UUID4::new()));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&staging)
            .map_err(|e| destination_error(&e))?;
        let cleanup = StagingFile(staging.clone());
        let db = Database::builder()
            .set_cache_size(limits.cache_size_bytes)
            .create_file(file)
            .map_err(|e| CheckpointError::Destination(e.to_string()))?;
        let mut descriptor = PrefixCheckpointDescriptor {
            kind: CheckpointKind::PrefixCheckpoint,
            schema_version: 1,
            status: CheckpointStatus::Writing,
            prefix: self.metadata.clone(),
            entries: TableDigest::default(),
            client_order_index: TableDigest::default(),
            venue_order_index: TableDigest::default(),
        };
        initialize(&db, &descriptor)?;
        self.check_bounds(&staging, limits, cancelled, started)?;
        descriptor.entries = self.copy_entries(&db, &staging, limits, cancelled, started)?;
        descriptor.client_order_index = self.copy_index(
            CLIENT_ORDER_INDEX,
            &db,
            &staging,
            limits,
            cancelled,
            started,
        )?;
        descriptor.venue_order_index =
            self.copy_index(VENUE_ORDER_INDEX, &db, &staging, limits, cancelled, started)?;
        descriptor.status = CheckpointStatus::Complete;
        write_descriptor(&db, &descriptor)?;
        drop(db);
        self.check_bounds(&staging, limits, cancelled, started)?;
        // Hard-link publication is atomic and refuses any existing file, unlike rename
        fs::hard_link(&staging, destination).map_err(|e| destination_error(&e))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|e| publication_error(destination, &e))?;
        drop(cleanup);
        Ok(descriptor)
    }

    fn check_bounds(
        &self,
        destination: &Path,
        limits: &CheckpointLimits,
        cancelled: &AtomicBool,
        started: Instant,
    ) -> Result<(), CheckpointError> {
        limits.check(started, cancelled)?;

        if fs::metadata(destination)
            .map_err(|e| destination_error(&e))?
            .len()
            > limits.max_output_bytes
        {
            return Err(CheckpointError::Limit("physical output bytes"));
        }
        let source_bytes = fs::metadata(&self.source_path)
            .map_err(|e| {
                CheckpointError::Source(EventStoreError::Backend(format!("source metadata: {e}")))
            })?
            .len();

        if source_bytes.saturating_sub(self.source_bytes) > limits.max_source_growth_bytes {
            return Err(CheckpointError::Limit(
                "source growth while MVCC view is pinned",
            ));
        }
        Ok(())
    }

    fn copy_entries(
        &self,
        db: &Database,
        output: &Path,
        limits: &CheckpointLimits,
        cancelled: &AtomicBool,
        started: Instant,
    ) -> Result<TableDigest, CheckpointError> {
        if self.metadata.high_watermark == 0 {
            return Ok(TableDigest {
                rows: 0,
                blake3: *blake3::hash(&[]).as_bytes(),
            });
        }
        let source = self
            .transaction
            .open_table(ENTRIES_TABLE)
            .map_err(map_table_err)?;
        let mut rows = source
            .range(1..=self.metadata.high_watermark)
            .map_err(map_storage_err)?;
        let mut pending = rows.next().transpose().map_err(map_storage_err)?;
        let mut hash = blake3::Hasher::new();
        let mut copied = 0_u64;

        while pending.is_some() {
            self.check_bounds(output, limits, cancelled, started)?;
            let write = destination_store(begin_immediate_write(db))?;
            {
                let mut target =
                    destination_store(write.open_table(ENTRIES_TABLE).map_err(map_table_err))?;
                let mut count = 0;
                let mut bytes = 0_usize;

                while let Some((key, value)) = pending.take() {
                    let size = value.value().len().saturating_add(8);
                    if size > limits.batch_bytes {
                        return Err(CheckpointError::Limit(
                            "one encoded entry exceeds batch bytes",
                        ));
                    }

                    if count > 0
                        && (count == limits.batch_entries
                            || bytes.saturating_add(size) > limits.batch_bytes)
                    {
                        pending = Some((key, value));
                        break;
                    }
                    let seq = key.value();
                    if seq != copied + 1 {
                        return Err(CheckpointError::Source(EventStoreError::Gap {
                            prev: copied,
                            next: seq,
                            missing: copied + 1,
                        }));
                    }
                    let raw = value.value();
                    let entry = codec::decode_from_slice::<EventStoreEntry>(raw).map_err(|e| {
                        EventStoreError::Corrupted(format!("decode prefix entry {seq}: {e}"))
                    })?;
                    check_embedded_seq(seq, &entry)?;
                    if entry.recompute_hash() != entry.entry_hash {
                        return Err(EventStoreError::HashMismatch { seq }.into());
                    }
                    destination_store(target.insert(seq, raw).map_err(map_storage_err))?;
                    digest_pair(&mut hash, &seq.to_le_bytes(), raw);
                    copied += 1;
                    count += 1;
                    bytes += size;
                    pending = rows.next().transpose().map_err(map_storage_err)?;
                }
            }
            destination_store(write.commit().map_err(map_commit_err))?;
        }

        if copied != self.metadata.high_watermark {
            return Err(EventStoreError::Gap {
                prev: copied,
                next: self.metadata.high_watermark.saturating_add(1),
                missing: copied.saturating_add(1),
            }
            .into());
        }
        self.check_bounds(output, limits, cancelled, started)?;
        Ok(TableDigest {
            rows: copied,
            blake3: *hash.finalize().as_bytes(),
        })
    }

    fn copy_index(
        &self,
        definition: TableDefinition<&str, u64>,
        db: &Database,
        output: &Path,
        limits: &CheckpointLimits,
        cancelled: &AtomicBool,
        started: Instant,
    ) -> Result<TableDigest, CheckpointError> {
        let source = self
            .transaction
            .open_table(definition)
            .map_err(map_table_err)?;
        let mut rows = source.iter().map_err(map_storage_err)?;
        let mut pending = rows.next().transpose().map_err(map_storage_err)?;
        let mut hash = blake3::Hasher::new();
        let mut copied = 0;

        while pending.is_some() {
            self.check_bounds(output, limits, cancelled, started)?;
            let write = destination_store(begin_immediate_write(db))?;
            {
                let mut target =
                    destination_store(write.open_table(definition).map_err(map_table_err))?;
                let mut count = 0;
                let mut bytes = 0_usize;

                while let Some((key, value)) = pending.take() {
                    let size = key.value().len().saturating_add(8);
                    if size > limits.batch_bytes {
                        return Err(CheckpointError::Limit("one index row exceeds batch bytes"));
                    }

                    if count > 0
                        && (count == limits.batch_entries
                            || bytes.saturating_add(size) > limits.batch_bytes)
                    {
                        pending = Some((key, value));
                        break;
                    }
                    let seq = value.value();
                    if seq == 0 || seq > self.metadata.high_watermark {
                        return Err(CheckpointError::Verification(
                            "frozen index points outside prefix".to_string(),
                        ));
                    }
                    destination_store(target.insert(key.value(), seq).map_err(map_storage_err))?;
                    digest_pair(&mut hash, key.value().as_bytes(), &seq.to_le_bytes());
                    copied += 1;
                    count += 1;
                    bytes += size;
                    pending = rows.next().transpose().map_err(map_storage_err)?;
                }
            }
            destination_store(write.commit().map_err(map_commit_err))?;
        }
        self.check_bounds(output, limits, cancelled, started)?;
        Ok(TableDigest {
            rows: copied,
            blake3: *hash.finalize().as_bytes(),
        })
    }
}

impl RedbBackend {
    pub(crate) fn open_prefix_checkpoint(
        path: &Path,
    ) -> Result<(Self, PrefixCheckpointDescriptor), CheckpointError> {
        let db = Database::builder()
            .set_cache_size(super::DEFAULT_CACHE_SIZE_BYTES)
            .open_read_only(path)
            .map_err(map_database_err)?;
        format::verify_store_format(&db)?;
        let transaction = db.begin_read().map_err(map_transaction_err)?;
        let table = transaction
            .open_table(CHECKPOINT_TABLE)
            .map_err(map_table_err)?;
        let value = table
            .get(CHECKPOINT_KEY)
            .map_err(map_storage_err)?
            .ok_or_else(|| {
                CheckpointError::Verification("checkpoint descriptor is missing".to_string())
            })?;
        let descriptor: PrefixCheckpointDescriptor = codec::decode_from_slice(value.value())
            .map_err(|e| CheckpointError::Verification(format!("checkpoint descriptor: {e}")))?;
        if descriptor.kind != CheckpointKind::PrefixCheckpoint
            || descriptor.schema_version != 1
            || descriptor.status != CheckpointStatus::Complete
        {
            return Err(CheckpointError::Verification(
                "checkpoint kind/schema/completion is invalid".to_string(),
            ));
        }
        let manifest = Self::read_manifest(&db)?.ok_or_else(|| {
            CheckpointError::Verification("checkpoint manifest is missing".to_string())
        })?;

        if manifest != descriptor.prefix.source_manifest
            || manifest.high_watermark != descriptor.prefix.high_watermark
            || manifest.status != RunStatus::Running
            || manifest.end_ts_init.is_some()
            || descriptor.entries.rows != descriptor.prefix.high_watermark
        {
            return Err(CheckpointError::Verification(
                "checkpoint source identity or boundary differs".to_string(),
            ));
        }
        let anchor = read_anchor(&transaction)?;
        if anchor != descriptor.prefix.snapshot_anchor
            || anchor
                .as_ref()
                .is_some_and(|value| value.high_watermark > descriptor.prefix.high_watermark)
        {
            return Err(CheckpointError::Verification(
                "checkpoint snapshot anchor differs".to_string(),
            ));
        }
        drop(value);
        drop(table);
        drop(transaction);
        let high_watermark = descriptor.prefix.high_watermark;
        let backend = Self {
            base_dir: PathBuf::new(),
            cache_size_bytes: super::DEFAULT_CACHE_SIZE_BYTES,
            state: Some(RunState {
                db: RunDatabase::ReadOnly(db),
                manifest,
                high_watermark,
                max_ts_init: UnixNanos::default(),
                file_path: path.to_path_buf(),
            }),
        };
        Ok((backend, descriptor))
    }

    pub(crate) fn prefix_table_digests(
        &self,
    ) -> Result<(TableDigest, TableDigest, TableDigest), EventStoreError> {
        let transaction = self.state()?.db.begin_read()?;
        let entries = transaction
            .open_table(ENTRIES_TABLE)
            .map_err(map_table_err)?;
        let mut hash = blake3::Hasher::new();
        let mut count = 0;

        for row in entries.iter().map_err(map_storage_err)? {
            let (key, value) = row.map_err(map_storage_err)?;
            digest_pair(&mut hash, &key.value().to_le_bytes(), value.value());
            count += 1;
        }
        let entries = TableDigest {
            rows: count,
            blake3: *hash.finalize().as_bytes(),
        };
        let index_digest =
            |definition: TableDefinition<&str, u64>| -> Result<TableDigest, EventStoreError> {
                let table = transaction.open_table(definition).map_err(map_table_err)?;
                let mut hash = blake3::Hasher::new();
                let mut count = 0;

                for row in table.iter().map_err(map_storage_err)? {
                    let (key, value) = row.map_err(map_storage_err)?;
                    digest_pair(
                        &mut hash,
                        key.value().as_bytes(),
                        &value.value().to_le_bytes(),
                    );
                    count += 1;
                }
                Ok(TableDigest {
                    rows: count,
                    blake3: *hash.finalize().as_bytes(),
                })
            };
        Ok((
            entries,
            index_digest(CLIENT_ORDER_INDEX)?,
            index_digest(VENUE_ORDER_INDEX)?,
        ))
    }
}

pub(super) fn require_run_artifact<D: ReadableDatabase + ?Sized>(
    db: &D,
) -> Result<(), EventStoreError> {
    let transaction = db.begin_read().map_err(map_transaction_err)?;
    match transaction.open_table(CHECKPOINT_TABLE) {
        Err(TableError::TableDoesNotExist(_)) => Ok(()),
        Ok(_) => Err(EventStoreError::Backend(
            "prefix checkpoint is not a source run".to_string(),
        )),
        Err(e) => Err(map_table_err(e)),
    }
}

pub(super) fn read_anchor(
    transaction: &ReadTransaction,
) -> Result<Option<SnapshotAnchor>, EventStoreError> {
    let table = transaction
        .open_table(SNAPSHOT_ANCHOR_TABLE)
        .map_err(map_table_err)?;
    table
        .get(SNAPSHOT_ANCHOR_KEY)
        .map_err(map_storage_err)?
        .map(|value| {
            codec::decode_from_slice(value.value())
                .map_err(|e| EventStoreError::Corrupted(format!("snapshot anchor: {e}")))
        })
        .transpose()
}

fn initialize(
    db: &Database,
    descriptor: &PrefixCheckpointDescriptor,
) -> Result<(), CheckpointError> {
    destination_store(RedbBackend::initialize_fresh(
        db,
        &descriptor.prefix.source_manifest,
    ))?;

    if let Some(anchor) = &descriptor.prefix.snapshot_anchor {
        let write = destination_store(begin_immediate_write(db))?;
        {
            let mut table = destination_store(
                write
                    .open_table(SNAPSHOT_ANCHOR_TABLE)
                    .map_err(map_table_err),
            )?;
            let bytes = codec::encode_to_vec(anchor)
                .map_err(|e| CheckpointError::Destination(e.to_string()))?;
            destination_store(
                table
                    .insert(SNAPSHOT_ANCHOR_KEY, bytes.as_slice())
                    .map_err(map_storage_err),
            )?;
        }
        destination_store(write.commit().map_err(map_commit_err))?;
    }
    write_descriptor(db, descriptor)
}

fn write_descriptor(
    db: &Database,
    descriptor: &PrefixCheckpointDescriptor,
) -> Result<(), CheckpointError> {
    let write = destination_store(begin_immediate_write(db))?;
    {
        let mut table =
            destination_store(write.open_table(CHECKPOINT_TABLE).map_err(map_table_err))?;
        let bytes = codec::encode_to_vec(descriptor)
            .map_err(|e| CheckpointError::Destination(e.to_string()))?;
        destination_store(
            table
                .insert(CHECKPOINT_KEY, bytes.as_slice())
                .map_err(map_storage_err),
        )?;
    }
    destination_store(write.commit().map_err(map_commit_err))?;
    Ok(())
}

fn digest_pair(hash: &mut blake3::Hasher, key: &[u8], value: &[u8]) {
    hash.update(&(key.len() as u64).to_le_bytes());
    hash.update(key);
    hash.update(&(value.len() as u64).to_le_bytes());
    hash.update(value);
}

fn destination_store<T>(result: Result<T, EventStoreError>) -> Result<T, CheckpointError> {
    result.map_err(|e| CheckpointError::Destination(e.to_string()))
}

fn destination_error(e: &std::io::Error) -> CheckpointError {
    CheckpointError::Destination(e.to_string())
}

fn publication_error(destination: &Path, e: &std::io::Error) -> CheckpointError {
    CheckpointError::PublicationUncertain {
        destination: destination.to_path_buf(),
        reason: e.to_string(),
    }
}

struct StagingFile(PathBuf);
impl Drop for StagingFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[cfg(all(test, not(madsim)))]
mod tests;
