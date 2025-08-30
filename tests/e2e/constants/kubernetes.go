/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package constants

const (
	KubeconfigEnvVar   = "KUBECONFIG"
	BusyBoxImageEnvVar = "BUSYBOX_IMAGE"
	WindowsImageEnvVar = "WINDOWS_IMAGE"
	DefaultlocalhostIP = "127.0.0.1"
)
const (
	ApiServerIPs                              = "API_SERVER_IPS"
	AttacherContainerName                     = "csi-attacher"
	NginxImage                                = "registry.k8s.io/nginx-slim:0.26"
	NginxImage4upg                            = "registry.k8s.io/nginx-slim:0.27"
	RetainClaimPolicy                         = "Retain"
	CloudInitLabel                            = "CloudInit"
	ConfigSecret                              = "vsphere-config-secret"
	ContollerClusterKubeConfig                = "CONTROLLER_CLUSTER_KUBECONFIG"
	ControlPlaneLabel                         = "node-role.kubernetes.io/control-plane"
	CrdCNSNodeVMAttachment                    = "cnsnodevmattachments"
	CrdCNSVolumeMetadatas                     = "cnsvolumemetadatas"
	CrdCNSFileAccessConfig                    = "cnsfileaccessconfigs"
	CrdtriggercsifullsyncsName                = "csifullsync"
	CrdGroup                                  = "cns.vmware.com"
	CrdVersion                                = "v1alpha1"
	CrdVirtualMachineImages                   = "virtualmachineimages"
	CrdVirtualMachines                        = "virtualmachines"
	CrdVirtualMachineService                  = "virtualmachineservice"
	CsiSystemNamespace                        = "vmware-system-csi"
	CsiFssCM                                  = "internal-feature-states.csi.vsphere.vmware.com"
	CsiVolAttrVolType                         = "vSphere CNS Block Volume"
	CsiDriverContainerName                    = "vsphere-csi-controller"
	GcNodeUser                                = "vmware-system-user"
	GcKubeConfigPath                          = "GC_KUBE_CONFIG"
	GcSshKey                                  = "TEST-CLUSTER-SSH-KEY"
	GcManifestPath                            = "testing-manifests/tkg/"
	HostdServiceName                          = "hostd"
	InvalidFSType                             = "ext10"
	KcmManifest                               = "/etc/kubernetes/manifests/kube-controller-manager.yaml"
	KubeAPIPath                               = "/etc/kubernetes/manifests/"
	KubeAPIfile                               = "kube-apiserver.yaml"
	KubeSystemNamespace                       = "kube-system"
	KubeletConfigYaml                         = "/var/lib/kubelet/config.yaml"
	PassorwdFilePath                          = "/etc/vmware/wcp/.storageUser"
	PodContainerCreatingState                 = "ContainerCreating"
	PvcHealthAnnotation                       = "volumehealth.storage.kubernetes.io/health"
	PvcHealthTimestampAnnotation              = "volumehealth.storage.kubernetes.io/health-timestamp"
	ProvisionerContainerName                  = "csi-provisioner"
	QuotaName                                 = "cns-test-quota"
	RegionKey                                 = "topology.csi.vmware.com/k8s-region"
	RestartOperation                          = "restart"
	RqLimit                                   = "200Gi"
	RqLimitScaleTest                          = "900Gi"
	DefaultrqLimit                            = "20Gi"
	RqStorageType                             = ".storageclass.storage.k8s.io/requests.storage"
	ResizerContainerName                      = "csi-resizer"
	ScParamDatastoreURL                       = "DatastoreURL"
	ScParamFsType                             = "csi.storage.k8s.io/fstype"
	ScParamStoragePolicyID                    = "storagePolicyID"
	ScParamStoragePolicyName                  = "StoragePolicyName"
	SvStorageClassName                        = "SVStorageClass"
	SyncerContainerName                       = "vsphere-syncer"
	Snapshotapigroup                          = "snapshot.storage.k8s.io"
	DefaultNginxStorageClassName              = "nginx-sc"
	MountPath                                 = "/usr/share/nginx/html"
	ServiceName                               = "nginx"
	SpsServiceName                            = "sps"
	SnapshotterContainerName                  = "csi-snapshotter"
	SshdPort                                  = "22"
	SshSecretName                             = "SSH_SECRET_NAME"
	SvcRunningMessage                         = "Running"
	StartOperation                            = "start"
	SvcStoppedMessage                         = "Stopped"
	StopOperation                             = "stop"
	StatusOperation                           = "status"
	SvClusterDistribution                     = "SupervisorCluster"
	TkgClusterDistribution                    = "TKGService"
	VanillaClusterDistribution                = "CSI-Vanilla"
	VanillaClusterDistributionWithSpecialChar = "CSI-\tVanilla-#Test"
	VcClusterAPI                              = "/api/vcenter/namespace-management/clusters"
	VcRestSessionIdHeaderName                 = "vmware-api-session-Id"
	VpxdServiceName                           = "vpxd"
	VSphereCSIControllerPodNamePrefix         = "vsphere-csi-controller"
	VmUUIDLabel                               = "vmware-system-vm-uuid"
	VsphereCloudProviderConfiguration         = "vsphere-cloud-provider.conf"
	VsphereControllerManager                  = "vmware-system-tkg-controller-manager"
	VSphereCSIConf                            = "csi-vsphere.conf"
	WcpServiceName                            = "wcp"
	VmcWcpHost                                = "10.2.224.24" //This is the LB IP of VMC WCP and its constant
	DevopsTKG                                 = "test-cluster-e2e-script"
	CloudadminTKG                             = "test-cluster-e2e-script-1"
	VmOperatorAPI                             = "/apis/vmoperator.vmware.com/v1alpha1/"
	DevopsUser                                = "testuser"
	ZoneKey                                   = "topology.csi.vmware.com/k8s-zone"
	TkgAPI                                    = "/apis/run.tanzu.vmware.com/v1alpha3/namespaces" +
		"/test-gc-e2e-demo-ns/tanzukubernetesclusters/"
	Topologykey                                = "topology.csi.vmware.com"
	TkgHATopologyKey                           = "topology.kubernetes.io"
	TkgHAccessibleAnnotationKey                = "csi.vsphere.volume-accessible-topology"
	TkgHARequestedAnnotationKey                = "csi.vsphere.volume-requested-topology"
	DatstoreSharedBetweenClusters              = "DATASTORE_SHARED_BETWEEN_TWO_CLUSTERS"
	DatastoreUrlSpecificToCluster              = "DATASTORE_URL_SPECIFIC_TO_CLUSTER"
	StoragePolicyForDatastoreSpecificToCluster = "STORAGE_POLICY_FOR_DATASTORE_SPECIFIC_TO_CLUSTER"
	TopologyCluster                            = "TOPOLOGY_CLUSTERS"
	TopologyLength                             = 5
	TkgshaTopologyLevels                       = 1
	VmClassBestEffortSmall                     = "best-effort-small"
	VmcPrdEndpoint                             = "https://vmc.vmware.com/vmc/api/orgs/"
	VsphereClusterIdConfigMapName              = "vsphere-csi-cluster-id"
	AuthAPI                                    = "https://console.cloud.vmware.com/csp/gateway/am/api/auth" +
		"/api-tokens/authorize"
	StoragePolicyQuota         = "-storagepolicyquota"
	PodVMOnStretchedSupervisor = "stretched-svc"
	StretchedSVCTopologyLevels = 1
	VolExtensionName           = "volume.cns.vsphere.vmware.com"
	SnapshotExtensionName      = "snapshot.cns.vsphere.vmware.com"
	VmServiceExtensionName     = "vmservice.cns.vsphere.vmware.com"
	PvcUsage                   = "-pvc-usage"
	SnapshotUsage              = "-snapshot-usage"
	VmUsage                    = "-vm-usage"
	DiskSize1Gi                = int64(1024)
	StorageQuotaWebhookPrefix  = "storage-quota-webhook"
	DevopsKubeConf             = "DEV_OPS_USER_KUBECONFIG"
	QuotaSupportedVCVersion    = "9.0.0"
)

// For busybox pod image
const (
	BusyBoxImageOnGcr = "busybox"
)

// For VCP to CSI migration tests.
const (
	VcpProvisionerName              = "kubernetes.io/vsphere-volume"
	VcpScParamDatastoreName         = "datastore"
	VcpScParamPolicyName            = "storagePolicyName"
	VcpScParamFstype                = "fstype"
	MigratedToAnnotation            = "pv.kubernetes.io/migrated-to"
	MigratedPluginAnnotation        = "storage.alpha.kubernetes.io/migrated-plugins"
	PvcAnnotationStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	PvAnnotationProvisionedBy       = "pv.kubernetes.io/provisioned-by"
)

// For Preferential datatsore
const (
	PreferredDSCat              = "cns.vmware.topology-preferred-datastores"
	PreferredTagDesc            = "preferred datastore tag"
	NfsStoragePolicyName        = "NFS_STORAGE_POLICY_NAME"
	NfstoragePolicyDatastoreUrl = "NFS_STORAGE_POLICY_DATASTORE_URL"
	WorkerClusterMap            = "WORKER_CLUSTER_MAP"
	DatastoreClusterMap         = "DATASTORE_CLUSTER_MAP"
)

// VolumeSnapshotClass env variables for tkg-snapshot
const (
	DeletionPolicy = "Delete"
)

// Windows env variables
const (
	InvalidNtfsFSType = "NtFs1"
	NtfsFSType        = "NTFS"
	WindowsImageOnMcr = "servercore"
)

// MultiSvc env variables
const (
	RoleCnsDatastore                 = "CNS-SUPERVISOR-DATASTORE"
	RoleCnsSearchAndSpbm             = "CNS-SUPERVISOR-SEARCH-AND-SPBM"
	RoleCnsHostConfigStorageAndCnsVm = "CNS-SUPERVISOR-HOST-CONFIG-STORAGE-AND-CNS-VM"
)

// Storage policy usages for storage quota validation
var UsageSuffixes = []string{
	"-pvc-usage",
	"-latebinding-pvc-usage",
	"-snapshot-usage",
	"-latebinding-snapshot-usage",
	"-vm-usage",
	"-latebinding-vm-usage",
}
