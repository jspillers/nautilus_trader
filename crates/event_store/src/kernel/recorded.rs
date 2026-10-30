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

//! Verified source identity for an offline native recorded run.

use std::{
    fmt::{Debug, Write as _},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use aws_lc_rs::digest::{Context, SHA256};
use nautilus_common::{clock::VirtualClock, runner::recorded};
use nautilus_system::event_store::RunIdentity;
use serde::{Deserialize, Serialize};

use super::{
    Environment, EventStoreConfig, EventStoreLifecycle, EventStoreLifecycleOptions, KernelError,
    RunId, RunManifest, UUID4, UnixNanos,
};
use crate::PrefixCheckpointReader;

/// Original source identity derived from a verified immutable native prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedOriginSource {
    /// Original logical run ID; unchanged signed session inputs retain this value.
    pub run_id: RunId,
    /// Original native process identity from the source manifest.
    pub instance_id: String,
    /// Canonical path of the original immutable source prefix.
    pub journal_path: PathBuf,
    /// SHA-256 of the complete original prefix file.
    pub journal_sha256: String,
    /// Actual original native high-watermark.
    pub high_watermark: u64,
    /// Actual original source manifest build/configuration identity.
    pub identity: RunIdentity,
}

/// Verified source admission. Construction always opens and verifies the actual native prefix.
#[derive(Clone, Debug)]
pub struct RecordedRunOrigin {
    source: RecordedOriginSource,
}
impl RecordedRunOrigin {
    /// Verifies an immutable native prefix and its expected whole-file SHA-256.
    ///
    /// No state is restored and the original file is opened read-only. This establishes
    /// source identity, not economic replay qualification or complete input closure.
    ///
    /// # Errors
    ///
    /// Returns an error for a digest mismatch, non-prefix/invalid native storage, parent run,
    /// unsafe run ID, or source verification failure.
    pub fn from_prefix(path: impl AsRef<Path>, expected_sha256: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            expected_sha256.len() == 64
                && expected_sha256
                    .bytes()
                    .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value)),
            "Recorded source requires a lowercase SHA-256"
        );
        let path = path.as_ref().canonicalize()?;
        anyhow::ensure!(
            file_sha256(&path)? == expected_sha256,
            "Recorded source file digest differs"
        );
        let reader = PrefixCheckpointReader::open(&path)?;
        let manifest = reader.reader().manifest()?;
        let high_watermark = reader.reader().high_watermark()?;
        anyhow::ensure!(
            reader.verify()?.is_clean(),
            "Recorded source native prefix verification failed"
        );
        anyhow::ensure!(
            manifest.parent_run_id.is_none(),
            "Recorded source requires cold genesis without a parent"
        );
        validate_run_id(&manifest.run_id, manifest.start_ts_init)?;
        anyhow::ensure!(
            file_sha256(&path)? == expected_sha256,
            "Recorded source changed during verification"
        );
        Ok(Self {
            source: RecordedOriginSource {
                run_id: manifest.run_id.clone(),
                instance_id: manifest.instance_id.clone(),
                journal_path: path,
                journal_sha256: expected_sha256.to_owned(),
                high_watermark,
                identity: identity_from(&manifest),
            },
        })
    }
    /// Returns the actual verified original source identity.
    #[must_use]
    pub const fn source(&self) -> &RecordedOriginSource {
        &self.source
    }
}

/// Original source relation recorded separately from actual constructed journal identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedRunOriginWitness {
    /// Verified original source, retained without rewriting its identity.
    pub source: RecordedOriginSource,
    /// Actual constructed native journal identity, also present in its manifest.
    pub constructed_identity: RunIdentity,
    /// Actual native destination path; original source file is never replaced.
    pub artifact_path: PathBuf,
}

impl EventStoreLifecycleOptions {
    /// Selects a verified origin for exclusively created offline native recorded journals.
    ///
    /// This mode always uses the native redb backend with exclusive file creation. It is
    /// accepted only during recorded Sandbox startup under a native `VirtualClock`.
    #[must_use]
    pub fn with_recorded_origin(mut self, origin: RecordedRunOrigin) -> Self {
        self.recorded_origin = Some(origin);
        self
    }
}
impl EventStoreLifecycle {
    /// Returns the actual frozen recorded origin after the derived journal opened.
    #[must_use]
    pub const fn recorded_origin(&self) -> Option<&RecordedRunOriginWitness> {
        self.recorded_origin.as_ref()
    }

    pub(super) fn validate_recorded_origin(
        &self,
        instance_id: UUID4,
        environment: Environment,
        config: &EventStoreConfig,
    ) -> Result<Option<RecordedRunOriginWitness>, KernelError> {
        let Some(origin) = self.options.recorded_origin.as_ref() else {
            return Ok(None);
        };
        let validate = || -> anyhow::Result<_> {
            anyhow::ensure!(
                recorded::startup_validation_active(),
                "Original run identity requires native recorded startup"
            );
            anyhow::ensure!(
                environment == Environment::Sandbox
                    && self.clock.borrow().as_any().is::<VirtualClock>(),
                "Original run identity requires offline Sandbox and VirtualClock"
            );
            anyhow::ensure!(
                config.replay_from_run_id.is_none() && self.parent_run_id.is_none(),
                "Recorded origin cannot restore a parent run"
            );
            anyhow::ensure!(
                self.session.is_none(),
                "Recorded origin cannot reopen a used lifecycle"
            );
            anyhow::ensure!(
                origin.source.instance_id == instance_id.to_string(),
                "Recorded source native instance differs"
            );
            anyhow::ensure!(
                file_sha256(&origin.source.journal_path)? == origin.source.journal_sha256,
                "Recorded source changed before startup"
            );
            let path = config
                .base_dir
                .join(instance_id.to_string())
                .join(format!("{}.redb", origin.source.run_id));
            anyhow::ensure!(
                path != origin.source.journal_path && !path.exists(),
                "Recorded artifact destination already exists or equals its source"
            );
            Ok(RecordedRunOriginWitness {
                source: origin.source.clone(),
                constructed_identity: config.identity.clone(),
                artifact_path: path,
            })
        };
        validate()
            .map(Some)
            .map_err(|e| KernelError::RecordedOrigin(format!("{e:#}")))
    }
}

pub(super) fn identity_from(manifest: &RunManifest) -> RunIdentity {
    RunIdentity {
        binary_hash: manifest.binary_hash.clone(),
        schema_version: manifest.schema_version,
        crate_versions: manifest.crate_versions.clone(),
        feature_flags: manifest.feature_flags.clone(),
        adapter_versions: manifest.adapter_versions.clone(),
        config_hash: manifest.config_hash.clone(),
        seed: manifest.seed,
    }
}
fn validate_run_id(run_id: &str, start: UnixNanos) -> anyhow::Result<()> {
    let (timestamp, suffix) = run_id
        .split_once('-')
        .ok_or_else(|| anyhow::anyhow!("Invalid native run ID"))?;
    anyhow::ensure!(
        timestamp.parse::<u64>()? == start.as_u64()
            && suffix.len() == 8
            && suffix
                .bytes()
                .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value)),
        "Invalid native run ID grammar"
    );
    Ok(())
}
fn file_sha256(path: &Path) -> anyhow::Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Context::new(&SHA256);
    let mut buffer = vec![0u8; 32 * 1024].into_boxed_slice();
    loop {
        let size = file.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        digest.update(&buffer[..size]);
    }
    let mut encoded = String::with_capacity(64);

    for value in digest.finish().as_ref() {
        write!(encoded, "{value:02x}")?;
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests;
