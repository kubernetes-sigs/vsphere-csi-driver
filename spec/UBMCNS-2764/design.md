<!-- markdownlint-disable -->
# CnsNodeVMBatchAttachment: stop the detach retry loop for snapshotted VMs

Tracks [UBMCNS-2764](https://vmw-jira.broadcom.net/browse/UBMCNS-2764).

## Problem

On one Supervisor, the syncer's `CnsNodeVMBatchAttachment` controller reconciled the same 51 VM Service
VMs in a loop for more than seven days. Each reconcile:

1. called CNS `DetachVolume` for every data volume (59 on the traced VM). Every call failed with
   `InvalidDeviceSpec` / `com.vmware.vim.vm.error.removeDiskAfterVMSnapshot`, because the disks belong to a
   VM snapshot;
2. batch-attached the same volumes, which failed with `InvalidPowerState`;
3. tried to write status. The API server rejected the write with `Duplicate value` for the
   `<volume>:detaching` entries in `status.volumes`.

Because the status write never succeeded, no progress was ever saved, so each reconcile repeated the same
work 2–5 minutes later. The result was about 1.15 million VC tasks per day, about 9.5 million task IDs and
10 million event IDs consumed in a week, and test automation that ran out of memory reading VC task history.

## Expected vs. buggy behavior

VC refusing to remove a disk that is part of a VM snapshot is **expected**. The bug is that CSI:

- retries a fault that cannot succeed while the snapshot exists;
- issues an attach that cannot succeed while the old attachment is still in place;
- corrupts its own status so it can never record what it learned;
- does not tell anyone why the detach is stuck.

## Root cause

All code paths below are in `pkg/syncer/cnsoperator/controller/cnsnodevmbatchattachment/`.

### When a PVC is detached, attached, or both

| PVC in spec | Attached on VC | VC backing = spec | Detach list | Attach list |
|---|---|---|---|---|
| no | yes | n/a | yes | no |
| yes | no | n/a | no | yes |
| yes | yes | yes | no | only if the status entry is missing or failed |
| yes | yes | **no** | **yes** | **yes** |

The last row is the "controller key, unit number, disk mode or sharing changed" path: detach, then
re-attach with the new backing. In the ticket, all 59 PVCs were in both lists.

### Cause 1: the duplicate status key is created deterministically

For a PVC in the last row whose detach fails:

| Step | `status.volumes` names for the claim |
|---|---|
| start | `vol-a` |
| reconcile N, `updatePvcStatusEntryName` renames it for detach | `vol-a:detaching` |
| reconcile N, the failed attach writes by volume name, finds no `vol-a`, and appends one | `vol-a:detaching`, `vol-a` |
| reconcile N+1, the rename loop skips the first entry and renames the second | `vol-a:detaching`, `vol-a:detaching` |

`status.volumes` is `+listType=map +listMapKey=name`, so every later `Status().Update` is rejected.

### Cause 2: snapshot faults are not classified

`detachVolumes` treats every fault other than `ManagedObjectNotFound` as retryable on every reconcile. The
fault reaches CSI as `CnsFault` with reason `Task failed with fault vim.fault.InvalidDeviceSpec`.

### Cause 3: attach is issued for volumes whose detach just failed

A disk that is still attached with its old backing cannot be re-attached with a new one. The attempt costs
one failed VC call per volume and produces the twin `vol-a` entry from cause 1.

### Cause 4 (latent): non-FlatVer2 backings are misread

`listAttachedFcdsForVM` read `DiskMode` and `Sharing` only from `VirtualDiskFlatVer2BackingInfo`. For every
other backing it recorded empty strings. CnsRegisterVolume and the `cns.vmware.com.protected/disk-backing`
annotation do support other backings (SeSparse, SparseVer2, RDM, and more). Those empty strings never equal
the spec, so such a volume was always planned for detach and re-attach. On a VM without a snapshot, that
is a live detach of a running VM's disk.

Which of these explains the ticket (spec changed by VM Operator, or cause 4) is visible in the bundle's
`List of attached FCDs map[...]` log line. The fix below covers both.

### Cause 5: the real error is hidden

When the status write failed, the reconcile event carried only the write error, not the detach fault.
Backoff caps at 5 minutes (`MaxBackOffDurationForReconciler`), which is why the flood rate stayed flat.

## Design

### Status writes are structurally safe

- `dedupeVolumeStatus` collapses `status.volumes` entries that share a name, keeping the entry with the
  newest condition transition time. It runs when the instance is read and again right before every
  status write. A CR that is already wedged therefore heals on its first reconcile after upgrade.
- `updatePvcStatusEntryName` removes the un-suffixed entry when a `<name>:detaching` entry already exists
  for the same claim, instead of renaming it into a duplicate.
- A failed attach does not append a new `<name>` entry while a `<name>:detaching` entry exists for the same
  claim. It records the attach condition on the existing entry instead.

### Snapshot-blocked detaches are classified and gated

- `volumes.IsDiskInVMSnapshotFault` recognizes `InvalidDeviceSpec`, and `CnsFault` whose reason or message
  names `InvalidDeviceSpec` or `removeDiskAfterVMSnapshot`. When it matches, `DetachVolume` wraps the
  sentinel `volumes.ErrDiskInVMSnapshot` into its error. The returned fault type string is unchanged, so
  Prometheus fault labels and the vanilla and WCP `ControllerUnpublishVolume` callers are not affected.
- On that fault type, the volume gets `VolumeDetached=False` with reason `DetachBlockedBySnapshot` and an
  actionable message.
- Before calling CNS for a volume that is already `DetachBlockedBySnapshot`, the controller reads the VM's
  `snapshot` property once per reconcile. This is a property read, not a VC task. If the VM still has
  snapshots, the detach is skipped. Once the snapshots are gone, the detach is attempted again.
- The gate is per volume and opens only after VC has actually returned the snapshot fault for that volume.
  Independent disks, which are not part of a snapshot, keep detaching normally.

State machine for one volume:

```
Attached ──detach──▶ DetachFailed (generic fault, retried with backoff)
    │
    └─detach──▶ DetachBlockedBySnapshot ──(VM still has snapshot)──▶ skipped, no VC call
                        │
                        └──(snapshot deleted)──▶ detach attempted ──▶ detached, entry removed
```

### Attach waits for a pending detach

`processDetach` returns the PVCs whose detach did not succeed. They are removed from the attach batch for
this reconcile and get `VolumeAttached=False` with reason `AttachPendingDetach`. If the detach error was
spurious and the disk is actually gone, the next reconcile sees it as not attached and attaches it
normally. Other volumes in the same CR are not affected.

### Backing modes are read from every disk backing type

`diskBackingModes` reads `DiskMode` from FlatVer1, FlatVer2, SparseVer1, SparseVer2, SeSparse,
RawDiskMappingVer1 and LocalPMem backings, and `Sharing` where the type has it. When the type has no
sharing field, it reports `sharingNone`. Output for FlatVer2 is unchanged.

### Events and backoff

- When the status write fails, the warning event carries both the reconcile error and the write error.
- Each newly blocked volume emits a `DetachBlockedBySnapshot` warning event on the CR.
- When every remaining failure is snapshot-blocked, the reconcile is requeued at
  `MaxBackOffDurationForReconciler` directly. Such requeues cost one property read.

## API impact

New condition reason constants only: `DetachBlockedBySnapshot` and `AttachPendingDetach`. There is no CRD
schema change and no new field, and existing reasons keep their meaning.

## Upgrade and recovery

No manual action is needed. With the new syncer, a wedged CR is deduplicated on its first reconcile, its
status write succeeds, and snapshot-blocked volumes stop generating VC tasks. Deleting the VM snapshots
lets the pending detaches complete.

For clusters still on an older build, the manual recovery is:

1. Delete the `VirtualMachineSnapshot` objects for the affected VMs.
2. Remove the duplicate `<volume>:detaching` entries:
   `kubectl -n <ns> edit cnsnodevmbatchattachment <vm> --subresource=status`.

## Alternatives rejected

- **Skip every detach while the VM has any snapshot.** This would also block independent disks, which VC
  allows to be detached.
- **Admission check in VM Operator only.** That is a useful follow-up, but it does not heal CRs that are
  already stuck, and it does not cover snapshots created after the PVC was removed.
- **A longer backoff cap.** This only lowers the flood rate. It does not stop it, and it does not fix the
  status corruption.

## Test plan

The unit tests live in `cnsnodevmbatchattachment_test.go` and `pkg/common/cns-lib/volume/manager_test.go`.

New behavior:

- Two reconciles of a backing mismatch with a failing detach keep status names unique.
- Dedup of the exact ticket shape, and dedup as a no-op on a valid status.
- The rename drops the twin when a suffixed entry exists, and a failed attach does not append a twin.
- The snapshot fault sets `DetachBlockedBySnapshot`.
- A blocked volume on a VM that still has snapshots issues no CNS call. Once the snapshot is gone, the
  detach is retried and succeeds.
- An independent disk without the blocked reason is still detached.
- A failed detach is removed from the attach batch, and other volumes still attach.
- `diskBackingModes` handles every backing type, and a SeSparse disk that matches the spec is not detached.
- `IsDiskInVMSnapshotFault` is covered by a table test.
- The event on a failed status write contains the root cause.
- When all failures are snapshot-blocked, the requeue is at the maximum backoff.

Regression guards:

- A normal backing change still detaches and re-attaches.
- `ManagedObjectNotFound` is still treated as detached.
- Generic faults are not gated.
- A CR being deleted keeps its finalizer while blocked.
- A disk reassigned to a different PVC still yields two distinct status entries.
- Stale-entry cleanup still works.
- The existing backing-change and attach-list tests still pass.

Manual validation on a Supervisor:

1. Create a VM with data PVCs and take a `VirtualMachineSnapshot`.
2. Remove the PVCs from the spec, re-add them with a different unit number, and remove them again.
3. Expect `DetachBlockedBySnapshot`, successful status writes, and at most one failed `detachDisk` per
   volume.
4. Delete the snapshot and expect the CR to converge to `Ready=True`.

## Follow-ups (other owners)

- **VM Operator:** warn or reject at admission when a PVC is removed from `spec.volumes` while a
  `VirtualMachineSnapshot` includes that disk.
- **CNS (vsan-health):** mark the `detachvolume` VC task as failed when its inner `DetachDisk_Task` fails.
