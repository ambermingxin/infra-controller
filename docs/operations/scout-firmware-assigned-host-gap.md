# Scout Firmware Assigned-Host State Gap

## Summary

Scout-based host firmware upgrades can enter a waiting state that the Scout API
does not currently appear to serve when the host is assigned to an instance.

This is a suspected state-shape coverage gap. The managed-host controller can
create a `WaitingForScoutUpgrade` host firmware state in two places:

- top-level host reprovisioning for unassigned or ready hosts
- nested instance host reprovisioning for assigned hosts

The Scout action and status-report handlers currently match only the top-level
form. If an assigned host enters the nested form, Scout may receive no firmware
upgrade action and the controller may wait until the Scout firmware deadline
expires.

## Relevant State Shapes

Top-level host reprovisioning:

```rust
ManagedHostState::HostReprovision {
    reprovision_state: HostReprovisionState::WaitingForScoutUpgrade { .. },
    ..
}
```

Assigned-host reprovisioning:

```rust
ManagedHostState::Assigned {
    instance_state: InstanceState::HostReprovision {
        reprovision_state: HostReprovisionState::WaitingForScoutUpgrade { .. },
    },
}
```

The state machine intentionally supports both shapes through
`HostFirmwareScenario::actual_new_state`:

- [`crates/machine-controller/src/handler.rs`](../../crates/machine-controller/src/handler.rs)

`HostFirmwareScenario::Ready` stores host firmware progress as top-level
`ManagedHostState::HostReprovision`.

`HostFirmwareScenario::Instance` stores host firmware progress under
`ManagedHostState::Assigned { instance_state: InstanceState::HostReprovision }`.

## Current Scout API Behavior

Scout receives firmware work through `forge_agent_control`:

- [`crates/api-core/src/handlers/machine_scout.rs`](../../crates/api-core/src/handlers/machine_scout.rs)

That handler currently emits `Action::FirmwareUpgrade` only when the machine
state is top-level `ManagedHostState::HostReprovision` with
`HostReprovisionState::WaitingForScoutUpgrade`.

Scout reports completion through `report_scout_firmware_upgrade_status`:

- [`crates/api-core/src/handlers/host_reprovisioning.rs`](../../crates/api-core/src/handlers/host_reprovisioning.rs)

That handler also destructures only the top-level host reprovisioning form and
rewrites the result back into the top-level form.

Neither path appears to match or preserve the nested assigned-host form.

## Expected Failure Mode

The likely failure mode is:

1. An assigned host is approved for host firmware upgrade.
2. The controller enters:

   ```rust
   ManagedHostState::Assigned {
       instance_state: InstanceState::HostReprovision {
           reprovision_state: HostReprovisionState::WaitingForScoutUpgrade { .. },
       },
   }
   ```

3. Scout polls `forge_agent_control`.
4. The API does not match the nested state and returns no firmware upgrade
   action.
5. The controller continues waiting for a Scout result.
6. The `WaitingForScoutUpgrade` deadline eventually expires and the firmware
   upgrade fails or retries.

## Why This Matters

This can affect Scout-script firmware upgrades for machines that are still
assigned to an instance and enter the assigned-host firmware reprovisioning
path. The top-level unassigned-host path is not affected by this specific state
shape mismatch.

PXE handling already recognizes assigned host reprovisioning as a discovery
image boot case, which is another signal that assigned-host host reprovisioning
is expected to be valid:

- [`crates/api-core/src/ipxe.rs`](../../crates/api-core/src/ipxe.rs)

## Suggested Fix

Teach both Scout handlers to handle both state shapes:

- `ManagedHostState::HostReprovision { reprovision_state: WaitingForScoutUpgrade, .. }`
- `ManagedHostState::Assigned { instance_state: InstanceState::HostReprovision { reprovision_state: WaitingForScoutUpgrade } }`

The status-report handler should preserve the original outer state shape when
writing the result:

- top-level input should remain top-level `HostReprovision`
- assigned-host input should remain `Assigned { instance_state:
  HostReprovision }`

Avoid converting assigned-host firmware state into top-level host
reprovisioning state, because that would lose the instance lifecycle context.

## Suggested Tests

Add API-core tests covering:

- `forge_agent_control` returns `Action::FirmwareUpgrade` for top-level
  `WaitingForScoutUpgrade`
- `forge_agent_control` returns `Action::FirmwareUpgrade` for nested assigned
  `WaitingForScoutUpgrade`
- `report_scout_firmware_upgrade_status` accepts and updates top-level
  `WaitingForScoutUpgrade`
- `report_scout_firmware_upgrade_status` accepts and updates nested assigned
  `WaitingForScoutUpgrade`
- stale `upgrade_task_id` is rejected in both shapes

Existing related test targets from the Scout firmware work are useful starting
points:

```bash
RUSTC_WRAPPER= cargo test -p carbide-api-core --test integration rejects_stale_task_id
RUSTC_WRAPPER= cargo test -p carbide-scout firmware_upgrade_status_request --bin forge-scout
```
