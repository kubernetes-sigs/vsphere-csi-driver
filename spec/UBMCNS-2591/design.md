<!-- markdownlint-disable -->
# Deleted-volume snapshot quota accounting

Extends [UBMCNS-2572: snapshot Used reconciliation](../UBMCNS-2572/design-no-reserve-fix.md)
with DeletedVolumeInfo (DVI) as an additional inventory source. SnapService can retain
snapshot bytes after CnsVolumeInfo (CVI) disappears; those bytes must remain charged
to the tenant's snapshot StoragePolicyUsage (SPU).

## Scope and ownership

Enable DVI observation only when `supports_snap_service_local_protection` is enabled,
within the existing Supervisor and `StorageQuotaM2` boundaries. Reuse the existing
snapshot SPU worker and its field-only `Used` patches. Reservation accounting and
PVC SPUs retain the behavior specified by UBMCNS-2572.

DPO/Metadata Manager owns DVI creation, sizing, and deletion. CSI reads DVI and owns
its contribution to the existing CSI snapshot SPU. This differs from the separate
MM-owned `DeletedPVC` SPU proposed in the
[deleted-volume design](https://vmw-confluence.broadcom.net/spaces/SABU/pages/2880115822/Deleted+Volume+Quota+Mgmt+Design):
that writer must not also charge these bytes.

## Provisional API contract

Consume `protectionandrecovery.vmware.com/v1alpha1`, kind `DeletedVolumeInfo`, resource
`deletedvolumeinfos`, through a dynamic informer and a private Go read model. Keep
the GVR and decoding in one adapter so the external API can evolve without generated
client dependencies. The protection service installs the CRD.

DVI lives in a tenant system namespace. Its `spec` supplies:

| Field | Accounting use |
|---|---|
| `volumeID` | Handoff identity; one DVI per deleted volume. |
| `namespace` | Tenant user namespace charged by the SPU. |
| `storageClassName` | Required storage association. |
| `volumeAttributeClassName` | Optional; when set, selects `vac-<name>-snapshot-usage` instead of `<storageClassName>-snapshot-usage`. |
| `storagePolicyID` | Must match the target SPU's policy. |
| `sizeBytes` | Required nonnegative integer snapshot bytes. |
| `validSize` | Required boolean; false means the size is last-known. |

## Accounting and handoff

For each SPU, with DVI enabled:

```text
Used = sum(matching CVI snapshot capacity)
     + sum(matching DVI.sizeBytes where no CVI exists for volumeID)
```

CVI takes precedence even while terminating, without snapshot information, or
associated with another SPU. The producer must persist DVI before releasing the CVI
finalizer and remove DVI after restore. CVI events also enqueue the matching DVI's
SPU, including when its namespace or policy association differs.

As in UBMCNS-2572, independent watches and SPU patches provide eventual convergence;
the handoff and policy moves are not atomic.

## Reconciliation and failures

Index DVI by SPU association and volume ID; add a volume-ID index to the shared CVI
cache. DVI add/update/delete events enqueue affected keys, including both keys when
an association changes. Workers recompute absolute totals. Startup enumeration and
periodic full sync use the same queue, including SPUs with no remaining contributors.

| Condition | Behavior |
|---|---|
| Capability disabled | No DVI client/informer or contribution. |
| Initial CVI or DVI cache incomplete | Wait before publishing snapshot `Used`. Missing CRD/list failures retry; existing values remain. |
| `validSize=false` | Count last-known `sizeBytes`; set the existing missing-snapshot-capacity annotation. |
| Malformed required data | Retain the affected SPU's `Used`, mark incomplete inventory, and retry. |
| DVI cannot be associated with an SPU | Enqueue all snapshot SPUs; block their combined totals until the record is corrected or removed. |
| Contributor exists before its SPU | Retry until the SPU is created. |

## Delivery and validation

Grant CSI `get/list/watch` on DVI across tenant system namespaces in Supervisor
manifests **1.33–1.35**. Runtime accounting requires the external CRD and producer
lifecycle above.

[Unit tests](../../pkg/syncer/spu_deleted_volume_info_test.go) cover gating, decoding,
namespace/policy/VAC attribution, stale and malformed records, event routing,
missing API recovery, startup repair, and CVI/DVI handoffs. The full syncer suite,
focused race tests, and repository lint script pass.

Implementation: [DVI adapter](../../pkg/syncer/spu_deleted_volume_info.go) and
[snapshot SPU reconciler](../../pkg/syncer/spu_snapshot_reconciler.go).
