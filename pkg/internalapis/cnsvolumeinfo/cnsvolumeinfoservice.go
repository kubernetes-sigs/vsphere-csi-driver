package cnsvolumeinfo

import (
	"context"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsvolumeinfoconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/config"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

type volumeInfo struct {
	// volumeInfoInformer is the informer for VolumeInfo CRs
	volumeInfoInformer cache.SharedIndexInformer
	// k8sClient helps operate on CnsVolumeInfo custom resource.
	k8sClient client.Client
	// Per volume lock for concurrent access to CnsFileVolumeClient instances.
	// Keys are strings representing volume handles (or SV-PVC names).
	// Values are individual sync.Mutex locks that need to be held
	// to make updates to the CnsFileVolumeClient instance on the API server.
	volumeLock *sync.Map
}

var (
	// volumeInfoServiceInstance is instance of volumeInfo and implements
	// interface for VolumeInfoService.
	volumeInfoServiceInstance *volumeInfo

	// csiNamespace is the namespace on which vSphere CSI Driver is running
	csiNamespace = common.GetCSINamespace()
)

const (
	// CRDGroupName represent the group of cnsvolumeinfo CRD.
	CRDGroupName = "cns.vmware.com"

	// FileVolumePrefix represents the prefix for RWX volume's volumeHandle.
	FileVolumePrefix = "file:"
)

// VolumeInfoService exposes interfaces to support Operate on cnsvolumeinfo CR
// for multi vCenter CSI topology feature.
// It will maintain internal state to map volume id to vCenter

type VolumeInfoService interface {
	// GetvCenterForVolumeID return vCenter for the given VolumeID
	GetvCenterForVolumeID(ctx context.Context, volumeID string) (string, error)

	// CreateVolumeInfo creates VolumeInfo CR to persist VolumeID to vCenter mapping
	CreateVolumeInfo(ctx context.Context, volumeID string, vCenter string) error

	// CreateVolumeInfoWithPolicyInfo creates VolumeInfo CR to persist VolumeID,
	// pvcnamespace, storage policy info and  vCenter details
	CreateVolumeInfoWithPolicyInfo(ctx context.Context, volumeID, pvcnamespace, storagePolicyId,
		storageClassName, vCenter string, capacity *resource.Quantity) error

	// DeleteVolumeInfo deletes VolumeInfo CR for the given VolumeID
	DeleteVolumeInfo(ctx context.Context, volumeID string) error

	// ListAllVolumeInfos lists all the VolumeInfo CRs present in the cluster
	ListAllVolumeInfos() []interface{}

	// VolumeInfoCrExistsForVolume returns true if VolumeInfo CR for
	// a given volume exists
	VolumeInfoCrExistsForVolume(ctx context.Context, volumeID string) (bool, error)

	// GetVolumeInfoForVolumeID fetches VolumeInfo CR for the given VolumeID and returns cnsvolumeinfo object
	GetVolumeInfoForVolumeID(ctx context.Context, volumeID string) (*cnsvolumeinfov1alpha1.CNSVolumeInfo, error)

	// PatchVolumeInfo patches the CNSVolumeInfo instance associated with volumeID in given parameters.
	PatchVolumeInfo(ctx context.Context, volumeID string, patchBytes []byte, retries int) error

	// AddVmUUIDToAttachedVmList adds the given VM UUID to the list of attached VMs list.
	AddVmUUIDToAttachedVmList(ctx context.Context, volumeID string, vmUUID string) error

	// RemoeVmUUIDFromAttachedVmList remvoes the given VM UUID from the list of attached VMs list.
	RemoeVmUUIDFromAttachedVmList(ctx context.Context, volumeID string, vmUUID string) error
}

// InitVolumeInfoService returns the singleton VolumeInfoService.
func InitVolumeInfoService(ctx context.Context) (VolumeInfoService, error) {
	log := logger.GetLogger(ctx)
	if volumeInfoServiceInstance == nil {
		log.Info("Initializing volumeInfo service...")
		// This is idempotent if CRD is pre-created then we continue with
		// initialization of volumeInfoServiceInstance.
		volumeInfoServiceInitErr := k8s.CreateCustomResourceDefinitionFromManifest(ctx,
			cnsvolumeinfoconfig.EmbedCnsVolumeInfoFile, cnsvolumeinfoconfig.EmbedCnsVolumeInfoFileName)

		if volumeInfoServiceInitErr != nil {
			return nil, logger.LogNewErrorf(log, "failed to create volume info CRD. Error: %v",
				volumeInfoServiceInitErr)
		}
		config, volumeInfoServiceInitErr := k8s.GetKubeConfig(ctx)
		if volumeInfoServiceInitErr != nil {
			return nil, logger.LogNewErrorf(log, "failed to get kubeconfig. err: %v", volumeInfoServiceInitErr)
		}
		volumeInfoServiceInstance = &volumeInfo{}
		volumeInfoServiceInstance.k8sClient, volumeInfoServiceInitErr =
			k8s.NewClientForGroup(ctx, config, CRDGroupName)
		if volumeInfoServiceInitErr != nil {
			volumeInfoServiceInstance = nil
			return nil, logger.LogNewErrorf(log, "failed to create k8sClient for volumeinfo service. "+
				"Err: %v", volumeInfoServiceInitErr)
		}
		log.Infof("Starting Informer for cnsvolumeinfo")
		informer, err := k8s.GetDynamicInformer(ctx, cnsvolumeinfov1alpha1.SchemeGroupVersion.Group,
			cnsvolumeinfov1alpha1.SchemeGroupVersion.Version, "cnsvolumeinfoes",
			csiNamespace, config, true)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to create dynamic informer for cnsvolumeinfoes "+
				"CRD. Err: %v", err)
		}
		volumeInfoServiceInstance.volumeInfoInformer = informer.Informer()
		go func() {
			stopCh := make(chan struct{})
			informer.Informer().Run(stopCh)
		}()
		log.Info("volumeInfo service initialized")
	}
	return volumeInfoServiceInstance, nil
}

// ListAllVolumeInfos lists all the VolumeInfo CRs present in the cluster
func (volumeInfo *volumeInfo) ListAllVolumeInfos() []interface{} {
	volumeInfoCrs := volumeInfo.volumeInfoInformer.GetStore().List()
	return volumeInfoCrs
}

// VolumeInfoCrExistsForVolume returns true if VolumeInfo CR for
// a given volume exists
func (volumeInfo *volumeInfo) VolumeInfoCrExistsForVolume(ctx context.Context, volumeID string) (bool, error) {
	log := logger.GetLogger(ctx)

	actual, _ := volumeInfo.volumeLock.LoadOrStore(volumeID, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return false, logger.LogNewErrorf(log, "failed to cast lock for cnsfilevolumeclient instance: %s", volumeID)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	volumeInfoCrName := getCnsVolumeInfoCrName(ctx, volumeID)
	key := csiNamespace + "/" + volumeInfoCrName
	_, found, err := volumeInfo.volumeInfoInformer.GetStore().GetByKey(key)
	if err != nil {
		return false, logger.LogNewErrorf(log, "failed to find vCenter for VolumeID: %q", volumeID)
	}
	if !found {
		log.Debugf("VolumeInfo CR for volume %s not found", volumeID)
		return false, nil
	}
	return true, nil
}

// GetvCenterForVolumeID return vCenter for the given VolumeID
func (volumeInfo *volumeInfo) GetvCenterForVolumeID(ctx context.Context, volumeID string) (string, error) {
	log := logger.GetLogger(ctx)

	actual, _ := volumeInfo.volumeLock.LoadOrStore(volumeID, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return "", logger.LogNewErrorf(log, "failed to cast lock for cnsfilevolumeclient instance: %s", volumeID)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	// Since CNSVolumeInfo is namespaced CR, we need to prefix "namespace-name/" to obtain value from the store
	volumeInfoCrName := getCnsVolumeInfoCrName(ctx, volumeID)
	key := csiNamespace + "/" + volumeInfoCrName
	info, found, err := volumeInfo.volumeInfoInformer.GetStore().GetByKey(key)
	if err != nil || !found {
		return "", logger.LogNewErrorf(log, "Could not find vCenter for VolumeID: %q", volumeID)
	}
	cnsvolumeinfo := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(info.(*unstructured.Unstructured).Object,
		&cnsvolumeinfo)
	if err != nil {
		return "", logger.LogNewErrorf(log, "failed to parse cnsvolumeinfo object: %v, err: %v", info, err)
	}
	log.Infof("Volume ID %q is associated with VC %q", volumeID, cnsvolumeinfo.Spec.VCenterServer)
	return cnsvolumeinfo.Spec.VCenterServer, nil
}

// CreateVolumeInfo creates VolumeInfo CR to persist VolumeID to vCenter mapping
func (volumeInfo *volumeInfo) CreateVolumeInfo(ctx context.Context, volumeID string, vCenter string) error {
	log := logger.GetLogger(ctx)

	actual, _ := volumeInfo.volumeLock.LoadOrStore(volumeID, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return logger.LogNewErrorf(log, "failed to cast lock for cnsfilevolumeclient instance: %s", volumeID)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	log.Infof("creating cnsvolumeinfo for volumeID: %q and vCenter: %q mapping in the namespace: %q",
		volumeID, vCenter, csiNamespace)

	volumeInfoCrName := getCnsVolumeInfoCrName(ctx, volumeID)

	cnsvolumeinfo := cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volumeInfoCrName,
			Namespace: csiNamespace,
		},
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			VolumeID:      volumeID,
			VCenterServer: vCenter,
		},
	}
	err := volumeInfo.k8sClient.Create(ctx, &cnsvolumeinfo)
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return logger.LogNewErrorf(log, "failed to create CR for cnsvolumeInfo %v in the namespace: %q. "+
				"Error: %v", cnsvolumeinfo, csiNamespace, err)
		}
		log.Infof("cnsvolumeInfo CR already exists for VolumeID: %q", volumeID)
		return nil
	}
	log.Infof("Successfully created CNSVolumeInfo CR for volumeID: %q and "+
		"vCenter: %q mapping in the namespace: %q", volumeID, vCenter, csiNamespace)
	return nil
}

// CreateVolumeInfoWithPolicyInfo creates VolumeInfo CR to persist VolumeID to Storage policy mapping
func (volumeInfo *volumeInfo) CreateVolumeInfoWithPolicyInfo(ctx context.Context, volumeID string,
	namespace, storagePolicyId, storageClassName, vCenter string, capacity *resource.Quantity) error {
	log := logger.GetLogger(ctx)

	actual, _ := volumeInfo.volumeLock.LoadOrStore(volumeID, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return logger.LogNewErrorf(log, "failed to cast lock for cnsfilevolumeclient instance: %s", volumeID)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	log.Infof("creating cnsvolumeinfo for volumeID: %q, StoragePolicyID: %q, "+
		"StorageClassName: %q, vCenter: %q, Capacity: %+v in the namespace: %q",
		volumeID, storagePolicyId, storageClassName, vCenter, *capacity, csiNamespace)

	volumeInfoCrName := getCnsVolumeInfoCrName(ctx, volumeID)

	cnsvolumeinfo := cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volumeInfoCrName,
			Namespace: csiNamespace,
		},
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			VolumeID:         volumeID,
			Namespace:        namespace,
			VCenterServer:    vCenter,
			StoragePolicyID:  storagePolicyId,
			StorageClassName: storageClassName,
			Capacity:         capacity,
		},
	}
	err := volumeInfo.k8sClient.Create(ctx, &cnsvolumeinfo)
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return logger.LogNewErrorf(log, "failed to create CR for CnsVolumeInfo %v in the namespace: %q. "+
				"Error: %v", cnsvolumeinfo, csiNamespace, err)
		}
		log.Infof("cnsvolumeInfo CR already exists for VolumeID: %q", volumeID)
		return nil
	}
	log.Infof("Successfully created CNSVolumeInfo CR for volumeID: %q, StoragePolicyID: %q, "+
		"StorageClassName: %q, vCenter: %q, Capacity: %+v mapping in the namespace: %q",
		volumeID, storagePolicyId, storageClassName, vCenter, *capacity, csiNamespace)
	return nil
}

// DeleteVolumeInfo deletes VolumeInfo CR for the given VolumeID
func (volumeInfo *volumeInfo) DeleteVolumeInfo(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)

	actual, _ := volumeInfo.volumeLock.LoadOrStore(volumeID, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return logger.LogNewErrorf(log, "failed to cast lock for cnsfilevolumeclient instance: %s", volumeID)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	volumeInfoCrName := getCnsVolumeInfoCrName(ctx, volumeID)

	object := cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volumeInfoCrName,
			Namespace: csiNamespace,
		},
	}
	err := volumeInfo.k8sClient.Delete(ctx, &object)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("volumeInfoCR is already deleted for volumeID: %q", volumeID)
			return nil
		}
		return logger.LogNewErrorf(log, "failed to delete volumeInfo CR for volumeID: %q "+
			"from namespace: %q", volumeID, csiNamespace)
	}
	log.Infof("Successfully deleted CNSVolumeInfo CR for volumeID: %q from namespace: %q",
		volumeID, csiNamespace)
	return nil
}

// GetVolumeInfoForVolumeID return cnsVolumeInfo for the given VolumeID
func (volumeInfo *volumeInfo) GetVolumeInfoForVolumeID(ctx context.Context, volumeID string) (
	*cnsvolumeinfov1alpha1.CNSVolumeInfo, error) {
	log := logger.GetLogger(ctx)

	actual, _ := volumeInfo.volumeLock.LoadOrStore(volumeID, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return nil, logger.LogNewErrorf(log, "failed to cast lock for cnsfilevolumeclient instance: %s", volumeID)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	// Since CNSVolumeInfo is namespaced CR, we need to prefix "namespace-name/" to obtain value from the store
	volumeInfoCrName := getCnsVolumeInfoCrName(ctx, volumeID)
	key := csiNamespace + "/" + volumeInfoCrName
	info, found, err := volumeInfo.volumeInfoInformer.GetStore().GetByKey(key)
	if err != nil || !found {
		return nil, logger.LogNewErrorf(log, "Could not find CnsVolumeInfo instance for volumeID: %q", volumeID)
	}
	cnsVolumeInfo := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(info.(*unstructured.Unstructured).Object,
		&cnsVolumeInfo)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to parse cnsVolumeInfo object: %v, err: %v", info, err)
	}
	return cnsVolumeInfo, nil
}

// PatchVolumeInfo patches the CNSVolumeInfo instance associated with volumeID in given parameters.
func (volumeInfo *volumeInfo) PatchVolumeInfo(ctx context.Context, volumeID string, patchBytes []byte,
	allowedRetries int) error {
	log := logger.GetLogger(ctx)

	actual, _ := volumeInfo.volumeLock.LoadOrStore(volumeID, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return logger.LogNewErrorf(log, "failed to cast lock for cnsfilevolumeclient instance: %s", volumeID)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	volumeInfoInstance, err := volumeInfo.GetVolumeInfoForVolumeID(ctx, volumeID)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to fetch CnsVolumeInfo instance for volumeID: %q", volumeID)
	}
	attempt := 0
	for {
		attempt++
		err := volumeInfo.k8sClient.Patch(ctx, volumeInfoInstance, client.RawPatch(types.MergePatchType, patchBytes))
		if err != nil && attempt >= allowedRetries {
			log.Errorf("attempt: %d, failed to patch the cnsvolumeinfo %q namespace %q",
				attempt, volumeInfoInstance.Name, volumeInfoInstance.Namespace)
			return err
		} else if err == nil {
			log.Infof("attempt: %d, Successfully patched the cnsvolumeinfo %q namespace %q",
				attempt, volumeInfoInstance.Name, volumeInfoInstance.Namespace)
			return nil
		}
		log.Warnf("attempt %d, failed to patch cnsvolumeinfo %q namespace %q, will retry",
			attempt, volumeInfoInstance.Name, volumeInfoInstance.Namespace)
		time.Sleep(100 * time.Millisecond)
	}

}

// AddVmUUIDToAttachedVmList adds the given VM UUID to the list of attached VMs list.
func (volumeInfo *volumeInfo) AddVmUUIDToAttachedVmList(ctx context.Context, volumeID string, vmUUID string) error {
	log := logger.GetLogger(ctx)

	actual, _ := volumeInfo.volumeLock.LoadOrStore(volumeID, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return logger.LogNewErrorf(log, "failed to cast lock for cnsfilevolumeclient instance: %s", volumeID)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	volumeInfoInstance, err := volumeInfo.GetVolumeInfoForVolumeID(ctx, volumeID)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to fetch CnsVolumeInfo instance for volumeID: %q", volumeID)
	}

	if volumeInfoInstance.Spec.AttachedVms == nil {
		volumeInfoInstance.Spec.AttachedVms = make([]string, 0)
	}

	for _, attachedVm := range volumeInfoInstance.Spec.AttachedVms {
		volumeInfoInstance.Spec.AttachedVms = append(volumeInfoInstance.Spec.AttachedVms, attachedVm)
	}

	err = volumeInfo.k8sClient.Update(ctx, volumeInfoInstance)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to add VM UUID %s to cnsvolumeinfo instance %s", vmUUID, volumeInfoInstance.Name)
	}
	return nil
}

// RemoeVmUUIDFromAttachedVmList remvoes the given VM UUID from the list of attached VMs list.
func (volumeInfo *volumeInfo) RemoeVmUUIDFromAttachedVmList(ctx context.Context, volumeID string, vmUUID string) error {
	log := logger.GetLogger(ctx)

	actual, _ := volumeInfo.volumeLock.LoadOrStore(volumeID, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return logger.LogNewErrorf(log, "failed to cast lock for cnsfilevolumeclient instance: %s", volumeID)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	volumeInfoInstance, err := volumeInfo.GetVolumeInfoForVolumeID(ctx, volumeID)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to fetch CnsVolumeInfo instance for volumeID: %q", volumeID)
	}

	if volumeInfoInstance.Spec.AttachedVms == nil {
		volumeInfoInstance.Spec.AttachedVms = make([]string, 0)
	}

	for index, attachedVm := range volumeInfoInstance.Spec.AttachedVms {
		if attachedVm == vmUUID {
			volumeInfoInstance.Spec.AttachedVms = append(
				volumeInfoInstance.Spec.AttachedVms[:index],
				volumeInfoInstance.Spec.AttachedVms[index+1:]...)
			err = volumeInfo.k8sClient.Update(ctx, volumeInfoInstance)
			if err != nil {
				return logger.LogNewErrorf(log, "failed to add VM UUID %s to cnsvolumeinfo instance %s", vmUUID, volumeInfoInstance.Name)
			}
			return nil
		}
	}

	log.Debugf("Could not find VM %s in list. Returning.", vmUUID)
	return nil
}

// getCnsVolumeInfoCrName replaces "file:" with "file-" as K8s only allows alphanumeric and "-" in object name."
func getCnsVolumeInfoCrName(ctx context.Context, volumeID string) string {
	log := logger.GetLogger(ctx)

	if strings.HasPrefix(volumeID, FileVolumePrefix) {
		log.Debugf("File volume observed %s", volumeID)

		volumeInfoCrName := strings.Replace(volumeID, ":", "-", 1)
		return volumeInfoCrName
	}
	return volumeID
}
