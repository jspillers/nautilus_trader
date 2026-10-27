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

use std::{cell::RefCell, io::Write, rc::Rc, time::Duration};

use nautilus_common::clock::VirtualClock;
use nautilus_core::time::get_atomic_clock_static;
use nautilus_system::RegisteredComponents;
use rstest::rstest;

use super::*;
use crate::{CheckpointLimits, EventStore, HaltSignal, RedbBackend, open_run_with_options};

fn source_origin() -> anyhow::Result<(tempfile::TempDir, UUID4, RecordedRunOrigin)> {
    let temp = tempfile::tempdir()?;
    let instance = UUID4::new();
    let config = EventStoreConfig {
        base_dir: temp.path().join("source"),
        identity: RunIdentity {
            binary_hash: "a".repeat(64),
            config_hash: "b".repeat(64),
            schema_version: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    clock.borrow().set_time(10.into());
    let mut source = EventStoreLifecycle::boot(Some(config), instance, clock)?;
    source.open(
        instance,
        &RegisteredComponents::default(),
        Environment::Sandbox,
    )?;
    let prefix = temp.path().join("original-prefix.checkpoint");
    let ticket = source.session.as_ref().unwrap().start_prefix_checkpoint(
        &prefix,
        CheckpointLimits {
            batch_entries: 10,
            batch_bytes: 64 * 1024,
            max_output_bytes: 32 * 1024 * 1024,
            max_source_growth_bytes: 32 * 1024 * 1024,
            cache_size_bytes: 1024 * 1024,
            timeout: Duration::from_secs(5),
        },
    )?;
    ticket
        .wait_timeout(Duration::from_secs(5))?
        .ok_or_else(|| anyhow::anyhow!("native prefix timeout"))??;
    let sha = file_sha256(&prefix)?;
    let origin = RecordedRunOrigin::from_prefix(&prefix, &sha)?;
    source.seal(20.into());
    drop(source);
    Ok((temp, instance, origin))
}

#[rstest]
fn verified_origin_keeps_original_id_and_actual_constructed_identity() -> anyhow::Result<()> {
    let (temp, instance, origin) = source_origin()?;
    let original_bytes = std::fs::read(&origin.source.journal_path)?;
    let config = EventStoreConfig {
        base_dir: temp.path().join("derived"),
        identity: RunIdentity {
            binary_hash: "c".repeat(64),
            config_hash: "d".repeat(64),
            schema_version: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut lifecycle = EventStoreLifecycle::boot_with_options(
        Some(config.clone()),
        instance,
        Rc::new(RefCell::new(VirtualClock::new())),
        EventStoreLifecycleOptions::new().with_recorded_origin(origin.clone()),
    )?;
    recorded::begin_startup_validation(vec![]);
    let result = lifecycle.open(
        instance,
        &RegisteredComponents::default(),
        Environment::Sandbox,
    );
    recorded::disable_capture();
    result?;
    let witness = lifecycle.recorded_origin().unwrap();
    anyhow::ensure!(witness.source == origin.source);
    anyhow::ensure!(
        witness.constructed_identity
            == identity_from(lifecycle.session.as_ref().unwrap().manifest())
    );
    anyhow::ensure!(witness.constructed_identity.config_hash == config.identity.config_hash);
    anyhow::ensure!(lifecycle.run_id() == Some(origin.source.run_id.as_str()));
    anyhow::ensure!(
        lifecycle.session.as_ref().unwrap().manifest().config_hash == config.identity.config_hash
    );
    anyhow::ensure!(std::fs::read(&origin.source.journal_path)? == original_bytes);
    anyhow::ensure!(witness.artifact_path != origin.source.journal_path);
    lifecycle.seal(30.into());
    Ok(())
}

#[rstest]
fn original_run_identity_requires_actual_recorded_startup() -> anyhow::Result<()> {
    let (temp, instance, origin) = source_origin()?;
    recorded::disable_capture();
    let mut lifecycle = EventStoreLifecycle::boot_with_options(
        Some(EventStoreConfig {
            base_dir: temp.path().join("offline"),
            ..Default::default()
        }),
        instance,
        Rc::new(RefCell::new(VirtualClock::new())),
        EventStoreLifecycleOptions::new().with_recorded_origin(origin),
    )?;
    anyhow::ensure!(
        lifecycle
            .open(
                instance,
                &RegisteredComponents::default(),
                Environment::Sandbox
            )
            .is_err()
    );
    anyhow::ensure!(lifecycle.run_id().is_none());
    Ok(())
}

#[rstest]
fn original_source_change_is_rejected_before_derived_open() -> anyhow::Result<()> {
    let (temp, instance, origin) = source_origin()?;
    let source_path = origin.source.journal_path.clone();
    let mut lifecycle = EventStoreLifecycle::boot_with_options(
        Some(EventStoreConfig {
            base_dir: temp.path().join("offline"),
            ..Default::default()
        }),
        instance,
        Rc::new(RefCell::new(VirtualClock::new())),
        EventStoreLifecycleOptions::new().with_recorded_origin(origin),
    )?;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&source_path)?
        .write_all(&[0])?;
    recorded::begin_startup_validation(vec![]);
    let result = lifecycle.open(
        instance,
        &RegisteredComponents::default(),
        Environment::Sandbox,
    );
    recorded::disable_capture();
    anyhow::ensure!(result.is_err());
    anyhow::ensure!(lifecycle.run_id().is_none());
    Ok(())
}

#[rstest]
fn recorded_destination_collision_is_exclusive_and_does_not_mutate_existing_bytes()
-> anyhow::Result<()> {
    let (temp, instance, origin) = source_origin()?;
    let destination = temp.path().join("collision");
    let mut backend = RedbBackend::new(&destination).with_exclusive_create();
    let manifest = PrefixCheckpointReader::open(&origin.source.journal_path)?
        .reader()
        .manifest()?;
    backend.open_run(manifest.clone())?;
    let path = backend.run_path(&instance.to_string(), &origin.source.run_id);
    drop(backend);
    let before = std::fs::read(&path)?;
    let mut collision = RedbBackend::new(&destination).with_exclusive_create();
    anyhow::ensure!(collision.open_run(manifest).is_err());
    anyhow::ensure!(std::fs::read(&path)? == before);
    Ok(())
}

#[rstest]
fn low_level_open_cannot_bypass_recorded_lifecycle_admission() -> anyhow::Result<()> {
    let (temp, instance, origin) = source_origin()?;
    let destination = temp.path().join("low-level");
    let config = EventStoreConfig {
        base_dir: destination.clone(),
        ..Default::default()
    };
    let run_id = origin.source.run_id.clone();
    let options = EventStoreLifecycleOptions::new().with_recorded_origin(origin);
    let result = open_run_with_options(
        &config,
        &instance.to_string(),
        run_id,
        None,
        10.into(),
        &RegisteredComponents::default(),
        HaltSignal::new(),
        get_atomic_clock_static(),
        &options,
    );
    anyhow::ensure!(result.is_err());
    anyhow::ensure!(!destination.exists());
    Ok(())
}
