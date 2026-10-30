package cnsvolumeinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
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

	// maxSnapshotDetailsWriteAttempts bounds the conditional writes of UpdateVolumeInfoSnapshotDetails.
	maxSnapshotDetailsWriteAttempts = 5
)

// snapshotDetailsRetryInterval is the wait between attempts of UpdateVolumeInfoSnapshotDetails.
// It is a variable so that tests can shorten it.
var snapshotDetailsRetryInterval = 100 * time.Millisecond

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
	CreateVolumeInfoWithPolicyInfo(ctx context.Context, volumeID, pvcnamespace, storagePolicyId, storageClassName,
		vCenter string, capacity *resource.Quantity, isLinkedClone bool) error

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

	// PatchVolumeInfoStatus patches the status of CNSVolumeInfo instance associated with volumeID.
	PatchVolumeInfoStatus(ctx context.Context, volumeID string, patchBytes []byte, retries int) error

	// UpdateVolumeInfoSnapshotDetails writes the aggregated snapshot capacity of volumeID to its
	// CNSVolumeInfo, conditioned on preReadResourceVersion: the resourceVersion of the CNSVolumeInfo
	// read before measured was taken. If the CNSVolumeInfo changed since then, the write is rejected,
	// the CNSVolumeInfo is read again from the API server, the capacity is re-measured from CNS
	// through volumeManager, and the write is retried. An empty preReadResourceVersion skips straight
	// to re-measuring. An error means nothing was written.
	UpdateVolumeInfoSnapshotDetails(ctx context.Context, volumeID, preReadResourceVersion string,
		measured *cnsvolume.CnsSnapshotInfo, volumeManager cnsvolume.Manager) error

	// Informer returns the shared informer of the CnsVolumeInfo CRs in the CSI namespace.
	// It has the SPUSnapshotIndex index registered.
	Informer() cache.SharedIndexInformer

	// PrecheckRestore checks, before a SnapService restore creates anything, that volumeID was deleted
	// and has a CnsDeletedVolumeInfo charged to namespace. restoredBy is the UID of the restoring
	// CnsRegisterVolume. The error message is meant for the CnsRegisterVolume status.
	PrecheckRestore(ctx context.Context, volumeID, namespace, restoredBy string) error

	// GetRestoredBy returns the UID of the CnsRegisterVolume that restored volumeID, as stamped on its
	// live CnsVolumeInfo, or "" if there is no live CnsVolumeInfo or it has no stamp.
	GetRestoredBy(ctx context.Context, volumeID string) (string, error)

	// CreateOrUpdateRestoredVolumeInfo creates or updates the CnsVolumeInfo of a volume restored by
	// SnapService, seeding its snapshot size from the CnsDeletedVolumeInfo of the volume, and records
	// the outcome in the SnapshotSizeRestored condition. It is safe to call again after an error.
	CreateOrUpdateRestoredVolumeInfo(ctx context.Context, info RestoredVolumeInfo) (RestoreResult, error)

	// MarkRestoredSnapshotSizeMeasured sets a False SnapshotSizeRestored condition of the CnsVolumeInfo
	// of volumeID to True once the CnsVolumeInfo holds a valid snapshot size.
	MarkRestoredSnapshotSizeMeasured(ctx context.Context, volumeID string) error
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
		// Indexers can only be added before the informer starts.
		if err = volumeInfoServiceInstance.volumeInfoInformer.AddIndexers(SPUSnapshotIndexers()); err != nil {
			volumeInfoServiceInstance = nil
			return nil, logger.LogNewErrorf(log, "failed to add indexers on cnsvolumeinfoes informer. Err: %v", err)
		}
		go func() {
			stopCh := make(chan struct{})
			informer.Informer().Run(stopCh)
		}()
		log.Info("volumeInfo service initialized")
	}
	return volumeInfoServiceInstance, nil
}

// Informer returns the shared informer of the CnsVolumeInfo CRs.
func (volumeInfo *volumeInfo) Informer() cache.SharedIndexInformer {
	return volumeInfo.volumeInfoInformer
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
	// Normalize: this value is used as an exact-match key against FQDN-keyed
	// maps (e.g. VolumeManagers). CRs persisted before host normalization was
	// introduced may still carry their original casing.
	return commontypes.NewFQDN(cnsvolumeinfo.Spec.VCenterServer).String(), nil
}

// CreateVolumeInfo creates VolumeInfo CR to persist VolumeID to vCenter mapping
func (volumeInfo *volumeInfo) CreateVolumeInfo(ctx context.Context, volumeID string, vCenter string) error {
	log := logger.GetLogger(ctx)
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
func (volumeInfo *volumeInfo) CreateVolumeInfoWithPolicyInfo(ctx context.Context, volumeID, pvcnamespace,
	storagePolicyId, storageClassName, vCenter string, capacity *resource.Quantity, isLinkedClone bool) error {
	log := logger.GetLogger(ctx)
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
			Namespace:        pvcnamespace,
			VCenterServer:    vCenter,
			StoragePolicyID:  storagePolicyId,
			StorageClassName: storageClassName,
			Capacity:         capacity,
			IsLinkedClone:    isLinkedClone,
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
	log.Infof("Successfully created CNSVolumeInfo CR for volumeID: %q ,StoragePolicyID: %q,"+
		"StorageClassName: %q, vCenter: %q, Capacity: %+v mapping in the namespace: %q. IsLinkedClone: %+v",
		volumeID, storagePolicyId, storageClassName, vCenter, *capacity, csiNamespace, isLinkedClone)
	return nil
}

// DeleteVolumeInfo deletes VolumeInfo CR for the given VolumeID
func (volumeInfo *volumeInfo) DeleteVolumeInfo(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)

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

// PatchVolumeInfoStatus patches the status of CNSVolumeInfo instance associated with volumeID.
func (volumeInfo *volumeInfo) PatchVolumeInfoStatus(ctx context.Context, volumeID string, patchBytes []byte,
	allowedRetries int) error {
	log := logger.GetLogger(ctx)

	volumeInfoInstance, err := volumeInfo.GetVolumeInfoForVolumeID(ctx, volumeID)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to fetch CnsVolumeInfo instance for volumeID: %q", volumeID)
	}
	attempt := 0
	for {
		attempt++
		err := volumeInfo.k8sClient.Status().Patch(ctx, volumeInfoInstance, client.RawPatch(types.MergePatchType, patchBytes))
		if err != nil && attempt >= allowedRetries {
			log.Errorf("attempt: %d, failed to patch the status of cnsvolumeinfo %q namespace %q",
				attempt, volumeInfoInstance.Name, volumeInfoInstance.Namespace)
			return err
		} else if err == nil {
			log.Infof("attempt: %d, Successfully patched the status of cnsvolumeinfo %q namespace %q",
				attempt, volumeInfoInstance.Name, volumeInfoInstance.Namespace)
			return nil
		}
		log.Warnf("attempt %d, failed to patch status of cnsvolumeinfo %q namespace %q, will retry",
			attempt, volumeInfoInstance.Name, volumeInfoInstance.Namespace)
		time.Sleep(100 * time.Millisecond)
	}
}

// UpdateVolumeInfoSnapshotDetails writes the aggregated snapshot capacity of volumeID to its
// CNSVolumeInfo only if the CNSVolumeInfo has not changed since preReadResourceVersion was read.
//
// preReadResourceVersion must be read before measured was taken. A successful write then proves
// that whatever it replaces was measured earlier than measured. A conflict means another writer
// wrote during the measurement, so measured is discarded: the CNSVolumeInfo is read again from the
// API server, the capacity is re-measured and the write is retried. Any other failure, including a
// failed re-measure, is retried as well. Every failure uses up one of maxSnapshotDetailsWriteAttempts,
// with a short wait between attempts. No write is ever made without a resourceVersion precondition.
func (volumeInfo *volumeInfo) UpdateVolumeInfoSnapshotDetails(ctx context.Context, volumeID,
	preReadResourceVersion string, measured *cnsvolume.CnsSnapshotInfo, volumeManager cnsvolume.Manager) error {
	log := logger.GetLogger(ctx)
	key := types.NamespacedName{Namespace: csiNamespace, Name: getCnsVolumeInfoCrName(ctx, volumeID)}
	resourceVersion := preReadResourceVersion
	needsRemeasure := measured == nil || resourceVersion == ""
	var capacityInMb int64
	var measuredAt time.Time
	if measured != nil {
		capacityInMb = measured.AggregatedSnapshotCapacityInMb
		measuredAt = measured.SnapshotLatestOperationCompleteTime
	}
	if measuredAt.IsZero() {
		// A replayed result may carry no completion time. A zero time would be patched as null and
		// clear the field, so use the current time; it is informational only.
		measuredAt = time.Now()
	}
	var lastErr error
	for attempt := 1; attempt <= maxSnapshotDetailsWriteAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(snapshotDetailsRetryInterval)
		}
		if needsRemeasure {
			remeasuredVersion, remeasuredMb, err := volumeInfo.remeasureSnapshotCapacity(ctx, key, volumeID,
				volumeManager)
			if apierrors.IsNotFound(err) {
				return logger.LogNewErrorf(log, "CnsVolumeInfo %q not found for volume %q", key.Name, volumeID)
			}
			if err != nil {
				log.Warnf("attempt %d: failed to re-measure the aggregated snapshot capacity of volume %q, "+
					"will retry. Error: %v", attempt, volumeID, err)
				lastErr = err
				continue
			}
			resourceVersion, capacityInMb = remeasuredVersion, remeasuredMb
			// The time is informational only; it never decides whether a write happens.
			measuredAt = time.Now()
			needsRemeasure = false
		}
		patchBytes, err := snapshotDetailsPatch(ctx, volumeID, resourceVersion, capacityInMb, measuredAt)
		if err != nil {
			return err
		}
		target := &cnsvolumeinfov1alpha1.CNSVolumeInfo{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		}
		err = volumeInfo.k8sClient.Patch(ctx, target, client.RawPatch(types.MergePatchType, patchBytes))
		if err == nil {
			log.Infof("attempt %d: updated CnsVolumeInfo %q with aggregated snapshot capacity %d MB "+
				"(precondition resourceVersion %q)", attempt, key.Name, capacityInMb, resourceVersion)
			return nil
		}
		lastErr = err
		switch {
		case apierrors.IsConflict(err):
			log.Infof("attempt %d: CnsVolumeInfo %q changed after resourceVersion %q was read; "+
				"discarding the measurement and re-measuring", attempt, key.Name, resourceVersion)
			needsRemeasure = true
		case apierrors.IsNotFound(err):
			return logger.LogNewErrorf(log, "CnsVolumeInfo %q not found for volume %q", key.Name, volumeID)
		default:
			log.Warnf("attempt %d: failed to update CnsVolumeInfo %q, will retry. Error: %v",
				attempt, key.Name, err)
		}
	}
	return logger.LogNewErrorf(log, "failed to update aggregated snapshot capacity of CnsVolumeInfo %q "+
		"after %d attempts. Error: %v", key.Name, maxSnapshotDetailsWriteAttempts, lastErr)
}

// remeasureSnapshotCapacity reads the CnsVolumeInfo live from the API server and then queries CNS for
// the aggregated snapshot capacity of volumeID, so the returned resourceVersion was read before the
// returned capacity was measured.
func (volumeInfo *volumeInfo) remeasureSnapshotCapacity(ctx context.Context, key types.NamespacedName,
	volumeID string, volumeManager cnsvolume.Manager) (string, int64, error) {
	log := logger.GetLogger(ctx)
	current := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
	if err := volumeInfo.k8sClient.Get(ctx, key, current); err != nil {
		return "", 0, fmt.Errorf("failed to read CnsVolumeInfo %q: %w", key.Name, err)
	}
	capacityInMb, err := cnsvolume.QueryAggregatedSnapshotCapacityInMb(ctx, volumeManager, volumeID)
	if err != nil {
		return "", 0, err
	}
	log.Infof("re-measured aggregated snapshot capacity %d MB for volume %q against CnsVolumeInfo "+
		"resourceVersion %q", capacityInMb, volumeID, current.ResourceVersion)
	return current.ResourceVersion, capacityInMb, nil
}

// snapshotDetailsPatch returns a merge patch that sets the snapshot details and carries
// resourceVersion, so that the API server rejects it if the CnsVolumeInfo has changed.
func snapshotDetailsPatch(ctx context.Context, volumeID, resourceVersion string, capacityInMb int64,
	measuredAt time.Time) ([]byte, error) {
	patch, err := common.GetValidatedCNSVolumeInfoPatch(ctx, &cnsvolume.CnsSnapshotInfo{
		SourceVolumeID:                      volumeID,
		AggregatedSnapshotCapacityInMb:      capacityInMb,
		SnapshotLatestOperationCompleteTime: measuredAt,
	})
	if err != nil {
		return nil, err
	}
	patch["metadata"] = map[string]interface{}{"resourceVersion": resourceVersion}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CnsVolumeInfo patch for volume %q: %w", volumeID, err)
	}
	return patchBytes, nil
}

// GetCnsVolumeInfoCrName returns the Kubernetes-safe CR name for a volumeID.
// It replaces the "file:" prefix with "file-" because Kubernetes object names
// only allow alphanumeric characters and "-".
func GetCnsVolumeInfoCrName(volumeID string) string {
	if strings.HasPrefix(volumeID, FileVolumePrefix) {
		return strings.Replace(volumeID, ":", "-", 1)
	}
	return volumeID
}

// getCnsVolumeInfoCrName replaces "file:" with "file-" as K8s only allows alphanumeric and "-" in object name."
func getCnsVolumeInfoCrName(ctx context.Context, volumeID string) string {
	log := logger.GetLogger(ctx)
	if strings.HasPrefix(volumeID, FileVolumePrefix) {
		log.Debugf("File volume observed %s", volumeID)
	}
	return GetCnsVolumeInfoCrName(volumeID)
}
