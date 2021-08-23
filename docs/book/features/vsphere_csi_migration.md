<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
# vSphere CSI Driver - vSphere CSI Migration

- [Introduction](#introduction)
- [Things to consider before turning on Migration](#consider-followings-before-turning-on-migration)
- [How to enable vSphere CSI Migration](#how-to-enable-vsphere-csi-migration)

## Introduction <a id="introduction"></a>

**Note:** Feature to migrate in-tree vSphere volumes to CSI is released as **beta** with [v2.1.0](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v2.1.0).

vSphere CSI driver and CNS bring in a lot of features that are not available in the in-tree vSphere volume plugin.

Refer to the following feature comparisons table to know what is added in the vSphere CSI driver.

| Feature | In-tree vSphere Volume Plugin | vSphere CSI Driver |
|---------|-------------------------------|--------------------|
| Block volume (`ReadWriteOnce` access mode) | Supported. Block volumes are backed by vmdks. | Supported. Block volumes are backed by vSphere Improved Virtual Disk(management layer on top of vmdk). |
| File volume (`ReadWriteMany`/`ReadOnlyMany` access modes) | Not Supported | Supported. File volumes are backed by vSAN file shares. |
| Dynamic provisioning(with and without SPBM) | Supported for block volumes only | Supported for block and file volumes |
| Static Provisioning | Supported for block volumes only | Supported for block and file volumes |
| Expand volume (Offline) | Not supported | Supported for block volumes |
| Storage vMotion of block volumes | Not supported | Not supported |
| vSphere UI integration(CNS dashboard, vSAN virtual objects, vSphere space) | Not supported | Supported |
| [Tanzu™ Kubernetes Grid™ Service](https://docs.vmware.com/en/VMware-vSphere/7.0/vmware-vsphere-with-kubernetes/GUID-152BE7D2-E227-4DAA-B527-557B564D9718.html) | Not supported | Supported |
| Tolerate Datacenter name, Datastore name and Storage policy name changes | Not Supported | supported |
| Volume Encryption | Not supported | Supported |
| Kubernetes Cluster spread across multiple vCenter Servers | Supported | Not supported |
| Kubernetes Cluster spread across multiple datacenters within a vCenter Server | Supported | Supported |
| Volume Topology(with `waitForFirstConsumer`) | Supported | Supported |
| Thick disk provisioning | Supported on all datastore types (vSAN, VVOL, VMFS and NFS) | Supported only on vSAN Datastore using Storage Policy Capability - `Object space reservation` |
| Raw Block Volume | Supported | Not supported |
| Inline volumes in Pod spec | Supported | Not supported |

In addition to the above feature comparison, one of the most important things customers need to consider is that Kubernetes will deprecate In-tree vSphere volume plugin and it will be removed in the future Kubernetes releases.
Volumes provisioned using vSphere in-tree plugin do not get the additional new features supported by the vSphere CSI driver.

Kubernetes has provided a seamless procedure to help migrate in-tree vSphere volumes to a vSphere CSI driver. After in-tree vSphere volumes migrated to vSphere CSI driver, all subsequent operations on migrated volumes are performed by the vSphere CSI driver.
Migrated vSphere volume will not get additional capabilities vSphere CSI driver supports.

## Things to consider before turning on Migration <a id="consider-followings-before-turning-on-migration"></a>

- vSphere CSI Migration is released with `beta` feature-gate in Kubernetes 1.19. Refer [release note announcement](https://github.com/kubernetes/kubernetes/blob/master/CHANGELOG/CHANGELOG-1.19.md#csi-migration---azuredisk-and-vsphere-beta).
- Kubernetes 1.19 release has deprecated vSAN raw policy parameters for the in-tree vSphere Volume plugin and these parameters will be removed in a future release. Refer [deprecation announcement](https://github.com/kubernetes/kubernetes/blob/master/CHANGELOG/CHANGELOG-1.19.md#deprecation)
- Following vSphere in-tree StorageClass parameters will not be supported after enabling migration.
  - `hostfailurestotolerate`
  - `forceprovisioning`
  - `cachereservation`
  - `diskstripes`
  - `objectspacereservation`
  - `iopslimit`
  - `diskformat`
- Storage Policy consumed by in-tree vSphere volume should not be renamed or deleted. Volume migration requires original storage policy used for provisioning the volume to be present on vCenter for registration of volume as Container Volume in vSphere.
- Datastore consumed by in-tree vSphere volume should not be renamed. Volume migration relies on the original datastore name present on the volume source for registration of volume as Container Volume in vSphere.
- For statically created vSphere in-tree Persistent Volume Claims and Persistent Volumes, make sure to add the following annotations before enabling migration. Statically provisioned in-tree vSphere volumes can't be migrated to CSI without adding these annotations. This also applies to new static in-tree PVs and PVCs created after the migration is enabled.

    Annotation on PV

      annotations:
        pv.kubernetes.io/provisioned-by: kubernetes.io/vsphere-volume

    Annotation on PVC

      annotations:
        volume.beta.kubernetes.io/storage-provisioner: kubernetes.io/vsphere-volume

- vSphere CSI Driver does not support Provisioning `eagerzeroedthick` and `zeroedthick` volume. After the migration is enabled, when a new volume is requested using the in-tree provisioner and `diskformat` parameter set to `eagerzeroedthick` or `zeroedthick`, volume creation will be failed by vSphere CSI Driver. Post migration only supported value for `diskformat` parameter will be `thin`. Existing volumes created before the migration using disk format `eagerzeroedthick` or `zeroedthick` will be migrated to CSI.
- vSphere CSI Driver does not support raw vSAN policy parameters. After the migration is enabled, when a new volume is requested using in-tree provisioner and vSAN raw policy parameters, Volume Creation will be failed by vSphere CSI Driver.
- vSphere CSI Migration requires vSphere 7.0u1. Customers who have in-tree vSphere volumes must upgrade vSphere to 7.0u1. Customers who do not need to migrate in-tree vSphere volumes can use vSphere 67u3 and above.
- vSphere CSI driver does not support volumes formatted with the Windows file system. Migrated in-tree vSphere volumes using the windows file system can't be used with vSphere CSI driver.
- In-tree vSphere volume plugin is heavily relying on the name of the datastore set on the PV’s Source. After migration is enabled, Storage DRS or vmotion should not be enabled. If storage DRS moves disk from one datastore to another further volume operations may break.

## How to enable vSphere CSI Migration <a id="how-to-enable-vsphere-csi-migration"></a>

In Kubernetes 1.19 release, vSphere CSI Migration is available with `beta` feature-gates.

To try out vSphere CSI migration in beta for vSphere plugin, perform the following steps.

1. Upgrade vSphere to 7.0u1.
2. Upgrade kubernetes to 1.19 release.
3. Ensure that your version of kubectl is also at 1.19 or later.
4. Install vSphere Cloud Provider Interface (CPI). Please follow guideline mentioned at https://vsphere-csi-driver.sigs.k8s.io/driver-deployment/prerequisites.html#vsphere_cpi
5. Install vSphere CSI Driver [v2.3.0](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v2.3.0)
   - Make sure to enable csi-migration feature gate in the deployment yaml file.

           apiVersion: v1
           data:
             "csi-migration": "true"
           kind: ConfigMap
           metadata:
             name: internal-feature-states.csi.vsphere.vmware.com
             namespace: vmware-system-csi

6. Install admission webhook.
   - vSphere CSI driver does not support provisioning of volume by specifying migration specific parameters in the StorageClass.
     These parameters were added by vSphere CSI translation library, and should not be used in the storage class directly.

     Validating admission controller helps prevent user from creating or updating StorageClass using `csi.vsphere.vmware.com` as provisioner with these parameters.

     - `csimigration`
     - `datastore-migrationparam`
     - `diskformat-migrationparam`
     - `hostfailurestotolerate-migrationparam`
     - `forceprovisioning-migrationparam`
     - `cachereservation-migrationparam`
     - `diskstripes-migrationparam`
     - `objectspacereservation-migrationparam`
     - `iopslimit-migrationparam`

    This Validating admission controller also helps prevent user from creating or updating StorageClass using `kubernetes.io/vsphere-volume` as provisioner with `AllowVolumeExpansion` to `true`.

   - Pre-requisite: `kubectl`, `openssl` and `base64` commands should be pre-installed on the system from where we can invoke admission webhook installation scripts.
   - Installation steps:
     - Script is available to deploy the admission webhook and it is located at https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/v2.3.0/manifests/vanilla on the repository.
        - Download the scripts

            $ curl -O https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/v2.3.0/manifests/vanilla/generate-signed-webhook-certs.sh

            $ curl -O https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/v2.3.0/manifests/vanilla/create-validation-webhook.sh

            $ curl -O https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/v2.3.0/manifests/vanilla/validatingwebhook.yaml

        - Generate the self-signed certificate

            $ bash generate-signed-webhook-certs.sh
              creating certs in tmpdir /tmp/tmp.jmZqh2bAwJ
              Generating RSA private key, 2048 bit long modulus (2 primes)
              ........................................+++++
              ............+++++
              e is 65537 (0x010001)
              certificatesigningrequest.certificates.k8s.io "vsphere-webhook-svc.vmware-system-csi" deleted
              Warning: certificates.k8s.io/v1beta1 CertificateSigningRequest is deprecated in v1.19+, unavailable in v1.22+; use certificates.k8s.io/v1 CertificateSigningRequest
              certificatesigningrequest.certificates.k8s.io/vsphere-webhook-svc.vmware-system-csi created
              NAME                                    AGE   SIGNERNAME                     REQUESTOR          CONDITION
              vsphere-webhook-svc.vmware-system-csi   0s    kubernetes.io/legacy-unknown   kubernetes-admin   Pending
              certificatesigningrequest.certificates.k8s.io/vsphere-webhook-svc.vmware-system-csi approved
              secret/vsphere-webhook-certs configured
        - Create the validation webhook

            $ bash create-validation-webhook.sh
              service/vsphere-webhook-svc created
              validatingwebhookconfiguration.admissionregistration.k8s.io/validation.csi.vsphere.vmware.com created
              serviceaccount/vsphere-csi-webhook created
              role.rbac.authorization.k8s.io/vsphere-csi-webhook-role created
              rolebinding.rbac.authorization.k8s.io/vsphere-csi-webhook-role-binding created
              deployment.apps/vsphere-csi-webhook created

7. Enable feature flags `CSIMigration` and `CSIMigrationvSphere`

   - `CSIMigrationvSphere` flag enables shims and translation logic to route volume operations from the vSphere in-tree plugin to vSphere CSI plugin. Supports falling back to in-tree vSphere plugin if a node does not have a vSphere CSI plugin installed and configured.
   - `CSIMigrationvSphere` requires `CSIMigration` feature flag to be enabled. This flag is enabling CSI migration on the Kubernetes Cluster.

    7.1 Steps for the control plane node(s)

   - Enable feature flags `CSIMigration` and `CSIMigrationvSphere` on `kube-controller` and `kubelet` on all control plane nodes.
   - update kube-controller-manager manifest file and following arguments. This file is generally available at `/etc/kubernetes/manifests/kube-controller-manager.yaml`

         `- --feature-gates=CSIMigration=true,CSIMigrationvSphere=true`

   - update kubelet configuration file and add following flags. This file is generally available at `/var/lib/kubelet/config.yaml`

            featureGates:
              CSIMigration: true
              CSIMigrationvSphere: true

   - Restart the kubelet on the control plane nodes using the command:

         systemctl restart kubelet

   - Verify that the kubelet is functioning correctly using the following command:

         systemctl status kubelet

   - If there are any issues with the kubelet, check the logs on the control plane node using the following command:

         journalctl -xe

    7.2 Steps for the worker node(s)

   - Enable feature flags `CSIMigration` and `CSIMigrationvSphere` on `kubelet` on all workload nodes. Please note that before changing the configuration on the Kubelet on each node we **must drain** the node (remove running application workloads).  
   - Node drain example.

         $ kubectl drain k8s-node1 --force --ignore-daemonsets
          node/k8s-node1 cordoned
          WARNING: deleting Pods not managed by ReplicationController, ReplicaSet, Job, DaemonSet or StatefulSet: default/vcppod; ignoring DaemonSet-managed Pods: kube-system/kube-flannel-ds-amd64-gs7fr, kube-system/kube-proxy-rbjx4, vmware-system-csi/vsphere-csi-node-fh9f6
          evicting pod default/vcppod
            pod/vcppod evicted
            node/k8s-node1 evicted

   - After migration is enabled, make sure `csinodes` instance for the node is updated with `storage.alpha.kubernetes.io/migrated-plugins` annotation.

         $ kubectl describe csinodes k8s-node1
           Name:               k8s-node1
           Labels:             <none>
           Annotations:        storage.alpha.kubernetes.io/migrated-plugins: kubernetes.io/vsphere-volume
           CreationTimestamp:  Wed, 29 Apr 2020 17:51:35 -0700
           Spec:
             Drivers:
               csi.vsphere.vmware.com:
                 Node ID:  k8s-node1
           Events:         <none>

   - Restart the kubelet on the workload nodes using the command:

         systemctl restart kubelet

   - Verify that the kubelet is functioning correctly using the following command:

         systemctl status kubelet

   - If there are any issues with the kubelet, check the logs on the workload node using the following command:

         journalctl -xe

   - Once the kubelet is restarted, `uncordon` the node so that it can be used for scheduling workloads:

         kubectl uncordon k8s-node1

   - Repeat these steps for all workload nodes in the Kubernetes Cluster.

8. There is also an optional `CSIMigrationvSphereComplete` flag that can be enabled if all the nodes have CSI migration enabled. `CSIMigrationvSphereComplete` helps stop registering the vSphere in-tree plugin in kubelet and volume controllers and enables shims and translation logic to route volume operations from the vSphere in-tree plugin to vSphere CSI plugin. `CSIMigrationvSphereComplete` flag requires `CSIMigration` and `CSIMigrationvSphere` feature flags enabled and vSphere CSI plugin installed and configured on all nodes in the cluster.

9. Verify vSphere in-tree PVCs and PVs are migrated to vSphere CSI driver, verify `pv.kubernetes.io/migrated-to: csi.vsphere.vmware.com` annotations are present on PVCs and PVs.

     Annotations on PVCs

        Annotations:   pv.kubernetes.io/bind-completed: yes
                       pv.kubernetes.io/bound-by-controller: yes
                       pv.kubernetes.io/migrated-to: csi.vsphere.vmware.com
                       volume.beta.kubernetes.io/storage-provisioner: kubernetes.io/vsphere-volume

     Annotations on PVs

        Annotations:     kubernetes.io/createdby: vsphere-volume-dynamic-provisioner
                         pv.kubernetes.io/bound-by-controller: yes
                         pv.kubernetes.io/migrated-to: csi.vsphere.vmware.com
                         pv.kubernetes.io/provisioned-by: kubernetes.io/vsphere-volume

   New in-tree vSphere volumes created by vSphere CSI driver after migration is enabled, can be identified by following annotations. PV spec will still hold vSphere Volume Path, so in case when migration needs to be disabled, provisioned volume can be used by the in-tree vSphere plugin.

   Annotations on PVCs

        Annotations:   pv.kubernetes.io/bind-completed: yes
                       pv.kubernetes.io/bound-by-controller: yes
                       volume.beta.kubernetes.io/storage-provisioner: csi.vsphere.vmware.com

   Annotations on PVs

       Annotations:     pv.kubernetes.io/provisioned-by: csi.vsphere.vmware.com
