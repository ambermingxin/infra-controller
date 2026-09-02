/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{StreamExt, stream};
use nv_redfish::core::Bmc;
use nv_redfish::{Resource, ServiceRoot};

use crate::HealthError;
use crate::collectors::inventory::{
    DiscoveredEntity, EntityInventory, GpuIdentity, SharedInventory,
};
use crate::collectors::runtime::{IterationResult, PeriodicCollector};
use crate::endpoint::BmcEndpoint;

/// Configuration for the entity discovery collector
pub struct EntityDiscoveryCollectorConfig<B: Bmc> {
    pub(crate) shared: SharedInventory<B>,

    /// Bounds local fan-out to the endpoint Redfish operation limit.
    pub request_concurrency: NonZeroUsize,

    /// Resolve GPU identity from the PCIe device beneath each chassis.
    ///
    /// Costs one extra Redfish collection fetch per chassis, so it is opt-in.
    pub gpu_identity: bool,
}

pub struct EntityDiscoveryCollector<B: Bmc> {
    endpoint: Arc<BmcEndpoint>,
    bmc: Arc<B>,
    shared: SharedInventory<B>,
    request_concurrency: usize,
    gpu_identity: bool,
    generation: u64,
}

impl<B: Bmc + 'static> PeriodicCollector<B> for EntityDiscoveryCollector<B> {
    type Config = EntityDiscoveryCollectorConfig<B>;

    fn new_runner(
        bmc: Arc<B>,
        endpoint: Arc<BmcEndpoint>,
        config: Self::Config,
    ) -> Result<Self, HealthError> {
        Ok(Self {
            endpoint,
            bmc,
            shared: config.shared,
            request_concurrency: config.request_concurrency.get(),
            gpu_identity: config.gpu_identity,
            generation: 0,
        })
    }

    async fn run_iteration(&mut self) -> Result<IterationResult, HealthError> {
        let fetch_failures = AtomicUsize::new(0);
        let entities = self.discover_entities(&fetch_failures).await?;
        let entity_count = entities.len();

        self.generation = self.generation.wrapping_add(1);
        self.shared.store(Some(Arc::new(EntityInventory {
            entities,
            discovered_at: std::time::Instant::now(),
            generation: self.generation,
        })));

        tracing::info!(
            bmc = %self.endpoint.addr.mac,
            rack_id = self.endpoint.rack_id.as_ref().map(tracing::field::display),
            entity_count,
            generation = self.generation,
            "Published entity inventory snapshot"
        );

        Ok(IterationResult {
            refresh_triggered: true,
            entity_count: Some(entity_count),
            fetch_failures: fetch_failures.load(Ordering::Relaxed),
        })
    }

    fn collector_type(&self) -> &'static str {
        "entity_discovery_collector"
    }

    async fn stop(&mut self) {
        // Clear the snapshot so readers stop emitting for a removed endpoint.
        self.shared.store(None);
    }
}

impl<B: Bmc + 'static> EntityDiscoveryCollector<B> {
    fn record_failure<T, E: std::fmt::Debug>(
        &self,
        result: Result<T, E>,
        context: &str,
        fetch_failures: &AtomicUsize,
    ) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(error) => {
                fetch_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    ?error,
                    context,
                    bmc_address = ?self.endpoint.addr,
                    rack_id = self.endpoint.rack_id.as_ref().map(tracing::field::display),
                    "Discovery fetch failed"
                );
                None
            }
        }
    }

    async fn discover_entities(
        &self,
        fetch_failures: &AtomicUsize,
    ) -> Result<Vec<DiscoveredEntity<B>>, HealthError> {
        let service_root = ServiceRoot::new(self.bmc.clone()).await?;

        let mut entities = Vec::new();
        let mut sensor_ids = HashSet::new();

        if let Some(systems) = service_root.systems().await? {
            for system in systems.members().await? {
                let system = Arc::new(system);

                self.discover_processors(&system, fetch_failures, &mut entities, &mut sensor_ids)
                    .await;
                self.discover_memory(&system, fetch_failures, &mut entities, &mut sensor_ids)
                    .await;
                self.discover_drives(&system, fetch_failures, &mut entities, &mut sensor_ids)
                    .await;
            }
        }

        if let Some(chassis_list) = service_root.chassis().await? {
            for chassis in chassis_list.members().await? {
                let chassis = Arc::new(chassis);

                self.discover_power_supplies(
                    &chassis,
                    fetch_failures,
                    &mut entities,
                    &mut sensor_ids,
                )
                .await;
                self.discover_chassis(&chassis, fetch_failures, &mut entities, &mut sensor_ids)
                    .await;
            }
        }

        Ok(entities)
    }

    async fn discover_processors(
        &self,
        system: &Arc<nv_redfish::computer_system::ComputerSystem<B>>,
        fetch_failures: &AtomicUsize,
        entities: &mut Vec<DiscoveredEntity<B>>,
        sensor_ids: &mut HashSet<String>,
    ) {
        let processors = self
            .record_failure(system.processors().await, "get processors", fetch_failures)
            .flatten()
            .unwrap_or_default();

        let discovered: Vec<_> = stream::iter(processors)
            .map(|processor| async move {
                let processor = Arc::new(processor);
                let env = processor.environment_sensor_links().await;
                let metric = processor.metrics_sensor_links().await;
                (processor, env, metric)
            })
            .buffer_unordered(self.request_concurrency)
            .collect()
            .await;

        for (entity, env, metric) in discovered {
            let env = self
                .record_failure(env, "get processor environment sensors", fetch_failures)
                .unwrap_or_default();
            let metric = self
                .record_failure(metric, "get processor metric sensors", fetch_failures)
                .unwrap_or_default();
            let sensors: Vec<_> = env.into_iter().chain(metric).collect();
            for sensor in &sensors {
                sensor_ids.insert(sensor.odata_id().to_string());
            }
            entities.push(DiscoveredEntity::Processor {
                entity,
                system: system.clone(),
                sensors,
            });
        }
    }

    async fn discover_memory(
        &self,
        system: &Arc<nv_redfish::computer_system::ComputerSystem<B>>,
        fetch_failures: &AtomicUsize,
        entities: &mut Vec<DiscoveredEntity<B>>,
        sensor_ids: &mut HashSet<String>,
    ) {
        let memory_modules = self
            .record_failure(
                system.memory_modules().await,
                "get memory modules",
                fetch_failures,
            )
            .flatten()
            .unwrap_or_default();

        let discovered: Vec<_> = stream::iter(memory_modules)
            .map(|memory| async move {
                let memory = Arc::new(memory);
                let sensors = memory.environment_sensor_links().await;
                (memory, sensors)
            })
            .buffer_unordered(self.request_concurrency)
            .collect()
            .await;

        for (entity, sensors) in discovered {
            let sensors = self
                .record_failure(sensors, "get memory environment sensors", fetch_failures)
                .unwrap_or_default();
            for sensor in &sensors {
                sensor_ids.insert(sensor.odata_id().to_string());
            }
            entities.push(DiscoveredEntity::Memory {
                entity,
                system: system.clone(),
                sensors,
            });
        }
    }

    async fn discover_drives(
        &self,
        system: &Arc<nv_redfish::computer_system::ComputerSystem<B>>,
        fetch_failures: &AtomicUsize,
        entities: &mut Vec<DiscoveredEntity<B>>,
        sensor_ids: &mut HashSet<String>,
    ) {
        let storage_list = self
            .record_failure(
                system.storage_controllers().await,
                "get storage",
                fetch_failures,
            )
            .flatten()
            .unwrap_or_default();

        for storage in storage_list {
            let storage = Arc::new(storage);
            let drives = self
                .record_failure(storage.drives().await, "get drives", fetch_failures)
                .flatten()
                .unwrap_or_default();

            let discovered: Vec<_> = stream::iter(drives)
                .map(|drive| async move {
                    let drive = Arc::new(drive);
                    let sensors = drive.environment_sensor_links().await;
                    (drive, sensors)
                })
                .buffer_unordered(self.request_concurrency)
                .collect()
                .await;

            for (entity, sensors) in discovered {
                let sensors = self
                    .record_failure(sensors, "get drive environment sensors", fetch_failures)
                    .unwrap_or_default();
                for sensor in &sensors {
                    sensor_ids.insert(sensor.odata_id().to_string());
                }
                entities.push(DiscoveredEntity::Drive {
                    entity,
                    storage: storage.clone(),
                    system: system.clone(),
                    sensors,
                });
            }
        }
    }

    async fn discover_power_supplies(
        &self,
        chassis: &Arc<nv_redfish::chassis::Chassis<B>>,
        fetch_failures: &AtomicUsize,
        entities: &mut Vec<DiscoveredEntity<B>>,
        sensor_ids: &mut HashSet<String>,
    ) {
        let power_supplies = self
            .record_failure(
                chassis.power_supplies().await,
                "get power supplies",
                fetch_failures,
            )
            .unwrap_or_default();

        let discovered: Vec<_> = stream::iter(power_supplies)
            .map(|ps| async move {
                let ps = Arc::new(ps);
                let sensors = ps.metrics_sensor_links().await;
                (ps, sensors)
            })
            .buffer_unordered(self.request_concurrency)
            .collect()
            .await;

        for (entity, sensors) in discovered {
            let sensors = self
                .record_failure(sensors, "get power supply metric sensors", fetch_failures)
                .unwrap_or_default();
            for sensor in &sensors {
                sensor_ids.insert(sensor.odata_id().to_string());
            }
            entities.push(DiscoveredEntity::PowerSupply {
                entity,
                chassis: chassis.clone(),
                sensors,
            });
        }
    }

    async fn discover_chassis(
        &self,
        chassis: &Arc<nv_redfish::chassis::Chassis<B>>,
        fetch_failures: &AtomicUsize,
        entities: &mut Vec<DiscoveredEntity<B>>,
        sensor_ids: &mut HashSet<String>,
    ) {
        let sensors = match chassis.sensor_links().await {
            Ok(Some(sensors)) => sensors,
            Ok(None) => Vec::new(),
            Err(error) => {
                fetch_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    ?error,
                    bmc_address = ?self.endpoint.addr,
                    rack_id = self.endpoint.rack_id.as_ref().map(tracing::field::display),
                    "Failed to get chassis sensors"
                );
                Vec::new()
            }
        };

        let sensors: Vec<_> = sensors
            .into_iter()
            .filter(|sensor| sensor_ids.insert(sensor.odata_id().to_string()))
            .collect();

        let gpu = if self.gpu_identity {
            self.resolve_gpu_identity(chassis, fetch_failures).await
        } else {
            None
        };

        // A sensorless chassis is normally not worth tracking, but one with a
        // resolved GPU identity is still needed to attribute SSE log records.
        if sensors.is_empty() && gpu.is_none() {
            return;
        }

        entities.push(DiscoveredEntity::Chassis {
            entity: chassis.clone(),
            sensors,
            gpu,
        });
    }

    /// Resolve the identity of the device currently occupying a chassis slot.
    ///
    /// Returns `None` for chassis without PCIe devices, which is the common
    /// case: only GPU and NVSwitch chassis carry them.
    async fn resolve_gpu_identity(
        &self,
        chassis: &Arc<nv_redfish::chassis::Chassis<B>>,
        fetch_failures: &AtomicUsize,
    ) -> Option<GpuIdentity> {
        let collection = self
            .record_failure(
                chassis.pcie_devices().await,
                "get chassis pcie devices",
                fetch_failures,
            )
            .flatten()?;

        let devices = self.record_failure(
            collection.members().await,
            "get chassis pcie device members",
            fetch_failures,
        )?;

        gpu_identity_from_chassis(chassis, &devices)
    }
}

/// Read the identity of the GPU a chassis holds, from the chassis and its PCIe
/// devices.
pub(in crate::collectors) fn gpu_identity_from_chassis<B: Bmc>(
    chassis: &nv_redfish::chassis::Chassis<B>,
    devices: &[nv_redfish::pcie_device::PcieDevice<B>],
) -> Option<GpuIdentity> {
    gpu_identity_from_devices(devices).map(|mut identity| {
        identity.chassis_serial = chassis.raw().serial_number.clone().flatten();
        identity
    })
}

/// Pick the GPU among a chassis' PCIe devices and read its identity.
///
/// A GPU chassis holds a single GPU device, but other chassis can hold several
/// unrelated devices. An identity is only claimed when the device is
/// unambiguous, otherwise the label would name an arbitrary device.
pub(in crate::collectors) fn gpu_identity_from_devices<B: Bmc>(
    devices: &[nv_redfish::pcie_device::PcieDevice<B>],
) -> Option<GpuIdentity> {
    let device = devices
        .iter()
        .find(|device| device.id().to_string().to_ascii_uppercase().contains("GPU"))
        .or_else(|| devices.first().filter(|_| devices.len() == 1))?;

    let raw = device.raw();
    let identity = GpuIdentity {
        uuid: raw.uuid.flatten().map(|uuid| uuid.to_string()),
        serial: raw.serial_number.clone().flatten(),
        model: raw.model.clone().flatten(),
        // Filled in by the caller, which has the enclosing chassis.
        chassis_serial: None,
    };

    (!identity.is_empty()).then_some(identity)
}

#[cfg(test)]
mod bmc_mock_integration_tests {
    use std::collections::BTreeMap;

    use bmc_mock::test_support::{TestBmc, TestBmcHandle, nvidia_dgx_h100_bmc, wiwynn_gb200_bmc};
    use nv_redfish::Resource as _;

    use super::gpu_identity_from_chassis;
    use crate::collectors::inventory::GpuIdentity;

    /// Resolve a GPU identity for every chassis the mock BMC exposes, keyed by
    /// chassis id. This is the same traversal the discovery collector performs,
    /// minus the failure accounting.
    async fn identities_by_chassis(h: &TestBmcHandle) -> BTreeMap<String, Option<GpuIdentity>> {
        let chassis_list = h
            .service_root
            .chassis()
            .await
            .expect("chassis collection")
            .expect("chassis collection is present");

        let mut identities = BTreeMap::new();
        for chassis in chassis_list.members().await.expect("chassis members") {
            let identity = match chassis.pcie_devices().await.expect("pcie devices") {
                Some(collection) => {
                    let devices = collection.members().await.expect("pcie device members");
                    gpu_identity_from_chassis::<TestBmc>(&chassis, &devices)
                }
                None => None,
            };
            identities.insert(chassis.id().to_string(), identity);
        }
        identities
    }

    #[tokio::test]
    async fn dgx_h100_resolves_identity_for_every_gpu_chassis() {
        let identities = identities_by_chassis(&nvidia_dgx_h100_bmc().await).await;

        let gpu_chassis: Vec<_> = identities
            .iter()
            .filter(|(id, _)| id.starts_with("HGX_GPU_SXM_"))
            .collect();
        assert_eq!(gpu_chassis.len(), 8, "DGX H100 exposes 8 GPU chassis");

        for (chassis_id, identity) in &gpu_chassis {
            let identity = identity
                .as_ref()
                .unwrap_or_else(|| panic!("{chassis_id} must resolve a GPU identity"));
            assert!(
                identity.uuid.is_some(),
                "{chassis_id} must carry a PCIe device UUID"
            );
            assert!(
                identity.serial.is_some(),
                "{chassis_id} must carry a GPU serial"
            );
            assert_eq!(
                identity.model.as_deref(),
                Some("H100 80GB HBM3"),
                "{chassis_id} model"
            );
            assert!(
                identity.chassis_serial.is_some(),
                "{chassis_id} must carry the GPU module chassis serial"
            );
        }

        // The UUID is what distinguishes one physical GPU from another, so a
        // shared UUID across slots would make the label useless.
        let uuids: std::collections::BTreeSet<_> = gpu_chassis
            .iter()
            .filter_map(|(_, identity)| identity.as_ref()?.uuid.clone())
            .collect();
        assert_eq!(uuids.len(), 8, "each GPU chassis reports a distinct UUID");
    }

    #[tokio::test]
    async fn non_gpu_chassis_resolve_no_identity() {
        let identities = identities_by_chassis(&nvidia_dgx_h100_bmc().await).await;

        for (chassis_id, identity) in &identities {
            if chassis_id.starts_with("HGX_GPU_SXM_") {
                continue;
            }
            assert!(
                identity.is_none(),
                "{chassis_id} is not a GPU chassis but resolved {identity:?}"
            );
        }
    }

    #[tokio::test]
    async fn gb200_resolves_identity_for_every_gpu_chassis() {
        let identities = identities_by_chassis(&wiwynn_gb200_bmc().await).await;

        let resolved: Vec<_> = identities
            .iter()
            .filter_map(|(id, identity)| identity.as_ref().map(|i| (id, i)))
            .collect();
        assert_eq!(resolved.len(), 4, "Wiwynn GB200 exposes 4 GPU chassis");

        for (chassis_id, identity) in resolved {
            assert!(
                identity.uuid.is_some(),
                "{chassis_id} must carry a PCIe device UUID"
            );
        }
    }
}
