# Host-local storage with PodVM 2.0: summary of new changes

## The core reason

With PodVM 2.0, one Supervisor Node (a "VirtualNode") stands for a **whole vSphere cluster**,
not one ESX host. Today's design assumes a Supervisor Node *is* an ESX host, so
`kubernetes.io/hostname` is used to mean "ESX host" everywhere. That assumption no longer
holds, so every place that uses the hostname to identify the ESX host has to change.

---

## 1. Supervisor PV node affinity: zone only, plus a host-pin annotation

| Today | New |
|---|---|
| Supervisor PV affinity = `zone` + `kubernetes.io/hostname=<ESX node name>` | Supervisor PV affinity = **zone only**. The real host goes in a new PV annotation, **`cns.vmware.com/podvm-host-pinned: host-45`** (an ESX host MoID). |

**Why (the upgrade gap, Appendix G):**
- vCenter and the Supervisor are upgraded to 9.2 before the ESX hosts. During that window,
  host-local PVs get created with `hostname=<esx node>` in their affinity.
- When an ESX host is upgraded, its own Node is removed and the host becomes part of a
  VirtualNode. The VirtualNode doesn't carry that hostname, and may have no hostname label
  at all.
- PV affinity can't be changed after creation. So the PV's affinity matches no Node any more,
  and pods using it can never be scheduled again, even though the disk is healthy.
- With zone-only affinity, the PV stays valid whichever way the host is represented.
- Kubernetes affinity no longer stops a pod from landing on the wrong host. The scheduler,
  through the vSphere scheduler plugins, must read the pin and keep the pod on that host.
  CSI writes the pin; the scheduler only reads it.

This applies **whenever host-local is enabled, even if PodVM v2 is off**, so that PVs created
before PodVM v2 arrives don't break once it does.

## 2. Host identity is the ESX host MoID, not a Node hostname

| Today | New |
|---|---|
| The host is identified by its Supervisor Node name (`kubernetes.io/hostname`) | The host is identified by its **ESX host MoID** (e.g. `host-21`) |
| The driver gets the host from CNS's placement result and turns it into a hostname for the PV | The driver returns the MoID in a new volume attribute **`cns.vmware.com/esxi-host-moid`**. The provisioner copies it onto the PV (attribute + pin annotation) and onto the **Supervisor PVC** (annotation). |
| PVC `csi.vsphere.volume-accessible-topology` = zone + hostname | Zone only. The host is in the separate `esxi-host-moid` annotation, and both are written in the same update. |

**Why:** a VirtualNode name refers to a cluster, not a host, so a Node name can't say which
host holds the disk. The MoID is also what CNS uses for host references.

## 3. Guest (VKS): no more renaming of the host key

| Today | New |
|---|---|
| Guest `topology.csi.vmware.com/host` holds the ESX **Supervisor Node name** (e.g. `esx-1`) | It holds the **ESX host MoID** (`host-21`) |
| pvCSI renames `topology.csi.vmware.com/host` ↔ `kubernetes.io/hostname` | **No rename.** The key and the MoID value go through to the Supervisor PVC annotation unchanged. |
| The guest node gets its host from the `kubernetes.io/hostname` in its boot-disk PVC's accessible-topology annotation | It gets the host from the boot-disk PVC's **`cns.vmware.com/esxi-host-moid`** annotation |
| pvCSI turns `kubernetes.io/hostname` back into the VKS key on the response | The guest PV still gets zone + `topology.csi.vmware.com/host=<MoID>`, but with no reverse rename |

**Why:** the rename only worked because, in the Supervisor, "ESX host" and "Node hostname"
were the same thing. With VirtualNodes they aren't, so `esx-1` can't be turned into a
Supervisor Node. The doc says the rename must be replaced because "these keys no longer
represent the same identity". A MoID means the same thing in both clusters, so guest labels
stay stable whether the Supervisor uses dedicated Nodes or VirtualNodes.

## 4. How host requests travel from provisioner to driver

| Today | New |
|---|---|
| The provisioner puts zone + hostname into the normal CSI `AccessibilityRequirements` | The provisioner reads the guest's host + zone segments from the PVC annotation and sends them in a new CreateVolume parameter, **`cns.vmware.com/host-local-topology`**. The normal CSI topology then contains only keys the Supervisor nodes actually report (zone). |
| The driver gets candidate hosts from the hostnames in the topology | The driver gets candidate hosts from the MoIDs in that parameter, combined with the allowed zones, namespace, policy and snapshot-source constraints |

**Why:** Supervisor Nodes don't advertise `topology.csi.vmware.com/host`. If that key went
into the normal topology, it would end up in the Supervisor PV's affinity and match nothing.
The private parameter keeps it out of the affinity.

Example Supervisor PVC annotation written by pvCSI:

```
csi.vsphere.volume-requested-topology: >-
  [{"topology.kubernetes.io/zone":"zone-1","topology.csi.vmware.com/host":"host-21"},
   {"topology.kubernetes.io/zone":"zone-1","topology.csi.vmware.com/host":"host-24"}]
```

Each object is one alternative, and zone/host pairs must stay together.

## 5. Supervisor host inventory: one Node, many hosts

| Today | New |
|---|---|
| Node-name → host-MoID map, one to one, built by the node informer | **One Node → many hosts.** A dedicated ESX Node still maps to its own host (from the `vmware-system-esxi-node-moid` annotation). A VirtualNode's hosts come from the new **`NodeMapping` CR** (`nodemappings.virtualnode.vmware.com`, `status.hosts[]`, `Ready` condition). |
| — | VirtualNodes are recognised by the Node annotation `pod.vmware.com/node-type: virtual-node` (proposed, not final) |
| CSINode advertises hostname (+ zone) | A VirtualNode's CSINode advertises **zone only**. A dedicated ESX Node keeps hostname + zone. |
| ListVolumes maps host → Node one to one | ListVolumes maps every ESX host to the Node that owns it, including VirtualNodes |

**Why:** the driver still has to translate between Nodes and physical hosts, for placement,
for registration, and for reporting which Node a volume is published to in ListVolumes.
During a rolling upgrade, some hosts have their own Node while others belong to a
VirtualNode. NodeMapping lists only the hosts without their own Node, so ownership is never
ambiguous.

## 6. Choosing the host for WaitForFirstConsumer (direct PodVMs)

| Today | New |
|---|---|
| The scheduler picks a Node, and that Node *is* the host | **Dedicated Node:** same as today, the Node gives its one host. **VirtualNode:** the Node is a whole cluster, so the driver reads a new **Pod placement CR** (proposed, still being defined) to find the host the scheduler/DRS chose for the pod, after checking the Pod, PVC, Node, Ready condition and generation. |

**Why:** knowing "Node = cluster X" doesn't tell CSI which host to place the disk on. The
scheduler has to record its host choice before volume binding starts provisioning.

Immediate binding and shared volumes don't need the placement CR.

## 7. Other changes

- **Feature gates:**
  - PodVM v2 is a separate gate: Supervisor capability `supports_podvm_v2`, plus a pvCSI FSS
    `podvm-v2-support`.
  - Host-local (`supports_host_local_storage` / `host-local-storage-support`) and PodVM v2
    are independent of each other.
  - The provisioner reads `supports_podvm_v2` once at startup.
- **When PodVM v2 is on, zonal StorageClasses drop the hostname key.** This keeps topology
  consistent when dedicated Nodes (which report hostname) and VirtualNodes (which report only
  zone) exist side by side. Already in the provisioner as commit f545baf03.
- **CnsRegisterVolume:** for host-local volumes, writes zone-only affinity, the host pin on
  the PV, and the MoID mirror annotation on the PVC. It no longer depends on a Node name.
- **Snapshots and clones:** keep today's source-host and linked-clone logic, combined with
  the new scheduling and guest host constraints.
- **Mostly unchanged:**
  - Shared zonal storage (zone only, never pinned).
  - Attach, which uses the Pod VM UUID.
  - Detach, which uses the saved VM UUID and needs `CSI_Detach_Supported`.
  - The `hostLocalPolicy` StorageClass annotation and parameter.
  - CNS `Hosts` / placement host.

### Feature combinations (Supervisor PV contract)

| HostLocalStorage | PodVM v2 | New Supervisor volume contract |
|---|---|---|
| Off | Off | Existing shared behaviour; no host-local |
| On | Off/absent | Host-local PV: zone only + mandatory host pin |
| Off | On | Shared PV: zone only, no pin |
| On | On | Host-local PV: zone only + mandatory host pin (on dedicated Nodes and VirtualNodes) |

---

## How CSI knows which host was picked

CSI doesn't decide the host. **CNS tells it.** This is the same mechanism the current code
uses; only what CSI does with the answer changes.

```
WCP CSI controller                       CNS (vCenter)
──────────────────                       ─────────────
CnsVolumeCreateSpec
  Hosts = [host-21, host-24]   ───────►  picks a host + its local datastore,
  StoragePolicyID = <host-local>         creates the disk
                               ◄───────  task result: CnsPlacementResult
                                           .Host = host-21
volumeInfo.Host = host-21
```

1. CSI sends the allowed hosts in `CnsVolumeCreateSpec.Hosts`:
   - **WFFC on a dedicated Node:** that Node's one host.
   - **WFFC on a VirtualNode:** the host from the Pod placement CR.
   - **Immediate:** every eligible host.
2. CNS chooses one and reports it in `CnsPlacementResult.Host`.
3. CSI reads it (`volumeInfo.Host`).
   - Today: CSI looks up that host's zone and hostname and puts them in the PV topology.
   - New: CSI puts the host MoID in the VolumeContext as `cns.vmware.com/esxi-host-moid` and
     returns zone-only topology.
4. Before returning, CSI checks that the host is not empty and is one of the hosts it
   offered. If not, it fails rather than create a PV it can't change later.

| Case | How CSI knows the host |
|---|---|
| Linked clone | The clone is created by datastore (`Datastores`, not `Hosts`), and CNS may not return a placement host. The clone sits on the source volume's datastore, which only one host mounts, so that host is the answer. |
| Restore from a host-local snapshot | CSI has already narrowed `Hosts` to the single host that owns the source disk, so CNS can only choose that one, and it reports it as usual. |
| CnsRegisterVolume (existing disk) | No create happens. CSI reads the disk datastore's mounting host (`Datastore.host[0]`). |

**Retries (doc §9.1, §15):**
- **With transactions on:** a retry calls CNS CreateVolume again (`manager.go:831/1051`). CNS
  returns the existing volume along with its placement host, so the host is still known.
- **Without transactions:** a retry can return a cached success result with no Host
  (`manager.go:625`). For host-local that is incomplete; the design says to fail and retry
  rather than invent a host.
- **If the provisioner restarts mid-way:** its in-memory cache of the response is lost, so
  recovery depends on CNS create being idempotent.

---

## What the doc says is still open

- The final Node-type label, and in what order the zone and the CSINode trigger get published.
- The schema of the Pod placement CR, and how stale placement attempts for the same pod are
  fenced off.
- Scheduler enforcement of `podvm-host-pinned`. This is a **prerequisite before turning
  host-local on**, even without PodVM v2.
- Existing PVs that already have hostname affinity. The new design stops new ones from being
  created but does **not** repair existing ones.
- Only one vCenter is supported. MoIDs are only unique within a vCenter.

## In one line

Hosts are identified by ESX host MoID instead of Node hostname; Supervisor PVs pin by zone
plus a host annotation the scheduler enforces; the guest uses MoIDs end to end with no
renaming; and the driver learns that one Node can stand for many hosts.
