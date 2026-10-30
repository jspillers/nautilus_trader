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

use indexmap::IndexMap;
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    cache::Cache,
    component::{component_lifecycle_revision, component_state},
    enums::ComponentState,
    nautilus_actor,
};
use nautilus_execution::engine::SnapshotAnchorer;
use nautilus_model::identifiers::ComponentId;
use nautilus_system::{
    NautilusKernel, NautilusKernelBuilder,
    trader::{ComponentStateLimits, RegisteredComponentState, Trader},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

use super::*;
use crate::{
    PrefixCheckpointReader, ScanDirection,
    capture::{EncodeError, EncodedPayload, default_registry},
};

#[derive(Debug)]
struct SourceStore {
    lifecycle: Rc<RefCell<EventStoreLifecycle>>,
    run_id: Option<String>,
    parent: Option<String>,
}

impl KernelEventStoreTrait for SourceStore {
    fn restore_parent_cache(&mut self, instance: UUID4, cache: &mut Cache) -> anyhow::Result<()> {
        self.lifecycle
            .borrow()
            .restore_parent_cache(instance, cache)
            .map(|_| ())
            .map_err(Into::into)
    }
    fn open(
        &mut self,
        instance: UUID4,
        components: &RegisteredComponents,
        environment: Environment,
    ) -> anyhow::Result<()> {
        let mut lifecycle = self.lifecycle.borrow_mut();
        lifecycle.open(instance, components, environment)?;
        self.run_id = lifecycle.run_id().map(str::to_owned);
        self.parent = lifecycle.parent_run_id().map(str::to_owned);
        Ok(())
    }
    fn snapshot_anchorer(&self) -> Option<SnapshotAnchorer> {
        self.lifecycle.borrow().snapshot_anchorer()
    }
    fn seal(&mut self, now: UnixNanos) {
        self.lifecycle.borrow_mut().seal(now);
        self.run_id = None;
    }
    fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }
    fn parent_run_id(&self) -> Option<&str> {
        self.parent.as_deref()
    }
    fn is_halted(&self) -> bool {
        self.lifecycle.borrow().is_halted()
    }
}

#[derive(Debug)]
struct OpaqueActor {
    core: DataActorCore,
    identity: String,
}
impl DataActor for OpaqueActor {
    fn on_save(&self) -> anyhow::Result<IndexMap<String, Vec<u8>>> {
        Ok(IndexMap::from([(
            "identity-v1".into(),
            self.identity.clone().into_bytes(),
        )]))
    }
    fn on_load(&mut self, state: IndexMap<String, Vec<u8>>) -> anyhow::Result<()> {
        self.identity = String::from_utf8(state["identity-v1"].clone())?;
        Ok(())
    }
}
nautilus_actor!(OpaqueActor);

#[derive(Debug)]
struct OpaqueStrategy {
    core: StrategyCore,
    identity: String,
}
impl DataActor for OpaqueStrategy {
    fn on_save(&self) -> anyhow::Result<IndexMap<String, Vec<u8>>> {
        Ok(IndexMap::from([(
            "strategy-v1".into(),
            self.identity.clone().into_bytes(),
        )]))
    }
    fn on_load(&mut self, state: IndexMap<String, Vec<u8>>) -> anyhow::Result<()> {
        self.identity = String::from_utf8(state["strategy-v1"].clone())?;
        Ok(())
    }
}
nautilus_strategy!(OpaqueStrategy);

fn install(kernel: &NautilusKernel) -> anyhow::Result<()> {
    kernel.trader.borrow_mut().add_actor(OpaqueActor {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some("OPAQUE-ACTOR".into()),
            ..Default::default()
        }),
        identity: UUID4::new().to_string(),
    })?;
    kernel.trader.borrow_mut().add_strategy(OpaqueStrategy {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some("Opaque-001".into()),
            order_id_tag: Some("001".into()),
            ..Default::default()
        }),
        identity: UUID4::new().to_string(),
    })?;
    Ok(())
}

fn captured_source() -> anyhow::Result<(
    tempfile::TempDir,
    UUID4,
    RecordedRunOrigin,
    Vec<RegisteredComponentState>,
)> {
    std::thread::spawn(move || {
        let temp = tempfile::tempdir()?;
        let instance = UUID4::new();
        let config = EventStoreConfig {
            base_dir: temp.path().join("actual-source"),
            ..Default::default()
        };
        let mut registry = default_registry();
        registry.register::<RegisteredComponentState, _>(
            "Test.ComponentState.v1".into(),
            |state| {
                let bytes = rmp_serde::to_vec_named(state)
                    .map_err(|e| EncodeError::Serialize(e.to_string()))?;
                Ok(EncodedPayload::new(bytes.into(), vec![]))
            },
        );
        let lifecycle = Rc::new(RefCell::new(None));
        let shared = lifecycle.clone();
        let mut kernel =
            NautilusKernelBuilder::new("source".into(), "OPAQUE-001".into(), Environment::Sandbox)
                .with_instance_id(instance)
                .with_load_state(false)
                .with_save_state(false)
                .with_clock_factory(|| Rc::new(RefCell::new(VirtualClock::new())))
                .with_event_store(move |instance, clock| {
                    let store = Rc::new(RefCell::new(EventStoreLifecycle::boot_with_options(
                        Some(config),
                        instance,
                        clock,
                        EventStoreLifecycleOptions::new().with_encoder_registry(registry),
                    )?));
                    *shared.borrow_mut() = Some(store.clone());
                    Ok(Box::new(SourceStore {
                        lifecycle: store,
                        run_id: None,
                        parent: None,
                    }))
                })
                .build()?;
        let lifecycle = lifecycle.borrow().as_ref().unwrap().clone();
        kernel.start();
        kernel.start_trader()?;
        install(&kernel)?;
        let snapshots = ["OPAQUE-ACTOR", "Opaque-001"]
            .into_iter()
            .map(|id| {
                Trader::export_component_state(
                    &kernel.trader,
                    ComponentId::from(id),
                    ComponentStateLimits::default(),
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        for snapshot in &snapshots {
            nautilus_common::msgbus::publish_any(
                "test.registered.component.state".into(),
                snapshot,
            );
        }
        let prefix = temp.path().join("actual-source.checkpoint");
        let job = lifecycle.borrow().start_prefix_checkpoint(
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
        job.wait_timeout(Duration::from_secs(5))?
            .ok_or_else(|| anyhow::anyhow!("source prefix timeout"))??;
        let reader = PrefixCheckpointReader::open(&prefix)?;
        let captured = reader
            .reader()
            .scan_range(1, reader.reader().high_watermark()?, ScanDirection::Forward)
            .filter_map(|entry| match entry {
                Ok(entry) if entry.payload_type.as_str() == "Test.ComponentState.v1" => Some(
                    rmp_serde::from_slice::<RegisteredComponentState>(&entry.payload)
                        .map_err(Into::into),
                ),
                Ok(_) => None,
                Err(e) => Some(Err(e.into())),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(captured == snapshots);
        anyhow::ensure!(reader.reader().manifest()?.instance_id == instance.to_string());
        let verified = reader.verify()?;
        anyhow::ensure!(
            verified.is_clean(),
            "actual source prefix verification failed: {verified:?}"
        );
        let origin = RecordedRunOrigin::from_prefix(&prefix, &file_sha256(&prefix)?)?;
        kernel.stop_trader();
        lifecycle.borrow_mut().seal(20.into());
        Ok((temp, instance, origin, captured))
    })
    .join()
    .expect("source native callback thread")
}

#[rstest]
fn verified_recorded_origin_restores_actual_registered_actor_and_strategy_callbacks()
-> anyhow::Result<()> {
    let (temp, instance, origin, snapshots) = captured_source()?;
    let config = EventStoreConfig {
        base_dir: temp.path().join("state-recorded"),
        ..Default::default()
    };
    let mut kernel =
        NautilusKernelBuilder::new("recorded".into(), "OPAQUE-001".into(), Environment::Sandbox)
            .with_instance_id(instance)
            .with_load_state(false)
            .with_save_state(false)
            .with_clock_factory(|| Rc::new(RefCell::new(VirtualClock::new())))
            .with_event_store(move |instance, clock| {
                Ok(Box::new(EventStoreLifecycle::boot_with_options(
                    Some(config),
                    instance,
                    clock,
                    EventStoreLifecycleOptions::new().with_recorded_origin(origin),
                )?))
            })
            .build()?;
    recorded::begin_startup_validation(vec![]);
    kernel.start();
    recorded::finish_startup_validation()?;
    recorded::disable_capture();
    kernel.start_trader()?;
    install(&kernel)?;
    anyhow::ensure!(kernel.event_store().unwrap().recorded_origin_instance_id() == Some(instance));

    for snapshot in snapshots {
        let id = snapshot.component_id;
        let before =
            Trader::export_component_state(&kernel.trader, id, ComponentStateLimits::default())?;
        anyhow::ensure!(
            before.registration.registration_id != snapshot.registration.registration_id
        );
        anyhow::ensure!(before.state != snapshot.state);
        kernel
            .restore_recorded_component_state(snapshot.clone(), ComponentStateLimits::default())?;
        let after =
            Trader::export_component_state(&kernel.trader, id, ComponentStateLimits::default())?;
        anyhow::ensure!(after.state == snapshot.state);
        anyhow::ensure!(after.registration == before.registration);
        anyhow::ensure!(component_state(&id.inner())? == ComponentState::Ready);
        anyhow::ensure!(component_lifecycle_revision(&id.inner())? == Some(1));
        anyhow::ensure!(
            kernel
                .restore_recorded_component_state(snapshot, ComponentStateLimits::default())
                .is_err()
        );
    }
    anyhow::ensure!(kernel.cache.borrow().accounts_all_owned().is_empty());
    anyhow::ensure!(
        kernel
            .cache
            .borrow()
            .orders(None, None, None, None, None)
            .is_empty()
    );
    anyhow::ensure!(
        kernel
            .cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    kernel.stop_trader();
    Ok(())
}
