package datamover

import (
	"context"
	"errors"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/datamover/v1alpha1"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	datamoverconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/datamover/config"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

type DurableSnapshotService interface {
	UploadSnapshot(ctx context.Context, snapshotName string, csiSnapshotId string, snapshotLocation string) error
	DownloadSnapshot(ctx context.Context, csiSnapshotId string) error
	RegisterDurableSnapshotRequest(ctx context.Context, snapshotName string, response *csi.CreateSnapshotResponse) error
	IsExistingDurableSnapshotRequest(ctx context.Context, snapshotName string) (bool, *csi.CreateSnapshotResponse)
	CheckIfUploadComplete(ctx context.Context, snapshotName string) (bool, error)
}

type durableSnapshotter struct {
	// k8sClient helps operate on Upload/Download custom resource.
	k8sClient client.Client
	// volumeManager helps perform Volume Operations.
	volumeManager *cnsvolume.Manager
	// requestResponse holds the snapshot request name ask key and the response as value
	requestResponse sync.Map
	// requestUpload stores the mapping between request and upload name
	requestUpload sync.Map
}

const (
	// CRDGroupName represent the group of upload/download CRD.
	CRDGroupName = "datamover.cnsdp.vmware.com"

	// VSphereCSISnapshotIdDelimiter is the delimiter for concatenating CNS VolumeID and CNS SnapshotID
	VSphereCSISnapshotIdDelimiter = "+"
)

var (
	// durableSnapshotterInstance is instance of durableSnapshotter and implements
	// interface for DataMoverService.
	durableSnapshotterInstance *durableSnapshotter
	// durableSnapshotterInstanceLock is used for handling race conditions during
	// read, write on durableSnapshotterInstance.
	durableSnapshotterInstanceLock = &sync.RWMutex{}
)

func GetDurableSnapshotService(ctx context.Context, volumeManager *cnsvolume.Manager) (DurableSnapshotService, error) {
	log := logger.GetLogger(ctx)
	durableSnapshotterInstanceLock.RLock()
	if durableSnapshotterInstance == nil {
		durableSnapshotterInstanceLock.RUnlock()
		durableSnapshotterInstanceLock.Lock()
		defer durableSnapshotterInstanceLock.Unlock()
		if durableSnapshotterInstance == nil {
			log.Info("Initializing durable snapshot service...")
			durableSnapshotServiceInitErr := k8s.CreateCustomResourceDefinitionFromManifest(ctx,
				datamoverconfig.DataMoverUploadFile, datamoverconfig.DataMoverUploadFileName)

			if durableSnapshotServiceInitErr != nil {
				log.Errorf("failed to create upload CRD. Error: %v", durableSnapshotServiceInitErr)
				return nil, durableSnapshotServiceInitErr
			}
			durableSnapshotServiceInitErr = k8s.CreateCustomResourceDefinitionFromManifest(ctx,
				datamoverconfig.DataMoverDownloadFile, datamoverconfig.DataMoverDownloadFileName)

			if durableSnapshotServiceInitErr != nil {
				log.Errorf("failed to create download CRD. Error: %v", durableSnapshotServiceInitErr)
				return nil, durableSnapshotServiceInitErr
			}
			config, durableSnapshotServiceInitErr := k8s.GetKubeConfig(ctx)
			if durableSnapshotServiceInitErr != nil {
				log.Errorf("failed to get kubeconfig. err: %v", durableSnapshotServiceInitErr)
				return nil, durableSnapshotServiceInitErr
			}
			durableSnapshotterInstance = &durableSnapshotter{
				volumeManager: volumeManager,
			}
			durableSnapshotterInstance.k8sClient, durableSnapshotServiceInitErr =
				k8s.NewClientForGroup(ctx, config, CRDGroupName)
			if durableSnapshotServiceInitErr != nil {
				durableSnapshotterInstance = nil
				log.Errorf("failed to create k8sClient. Err: %v", durableSnapshotServiceInitErr)
				return nil, durableSnapshotServiceInitErr
			}
			log.Info("durable snapshot service initialized")
		}
	} else {
		durableSnapshotterInstanceLock.RUnlock()
	}
	return durableSnapshotterInstance, nil
}

func (durableSnapshotter *durableSnapshotter) UploadSnapshot(ctx context.Context, snapshotName string, csiSnapshotId string, snapshotLocation string) error {
	log := logger.GetLogger(ctx)
	veleroNs := "velero"
	volumeId, snapshotId, err := ParseCSISnapshotID(csiSnapshotId)
	if err != nil {
		return err
	}
	uploadName := GenerateUploadCRName(snapshotId)
	uploadSnapshotPEID := GenerateUploadSnapshotPeId(volumeId, snapshotId)
	log.Infof("Creating Upload CR: %s / %s", "velero", uploadName)
	uploadBuilder := ForUpload(veleroNs, uploadName).
		BackupTimestamp(time.Now()).
		NextRetryTimestamp(time.Now()).
		Phase(v1alpha1.UploadPhaseNew).
		SnapshotID(uploadSnapshotPEID).
		BackupRepositoryName(snapshotLocation)
	upload := uploadBuilder.Result()
	err = durableSnapshotter.k8sClient.Create(ctx, upload)
	if err != nil {
		log.Errorf("Failed to create Upload CR : %+v, err: %+v", upload, err)
		return err
	}
	durableSnapshotter.requestUpload.Store(snapshotName, uploadName)
	return nil
}

func (durableSnapshotter *durableSnapshotter) DownloadSnapshot(ctx context.Context, csiSnapshotId string) error {
	return nil
}

func (durableSnapshotter *durableSnapshotter) RegisterDurableSnapshotRequest(ctx context.Context, snapshotName string, response *csi.CreateSnapshotResponse) error {
	durableSnapshotter.requestResponse.Store(snapshotName, response)
	return nil
}

func (durableSnapshotter *durableSnapshotter) IsExistingDurableSnapshotRequest(ctx context.Context, snapshotName string) (bool, *csi.CreateSnapshotResponse) {
	//log := logger.GetLogger(ctx)
	val, ok := durableSnapshotter.requestResponse.Load(snapshotName)
	if ok {
		//log.Infof("Detected existing durable snapshot request: %q", snapshotName)
		response := val.(*csi.CreateSnapshotResponse)
		return ok, response
	}
	return ok, nil
}

func (durableSnapshotter *durableSnapshotter) CheckIfUploadComplete(ctx context.Context, snapshotName string) (bool, error) {
	log := logger.GetLogger(ctx)
	val, _ := durableSnapshotter.requestUpload.Load(snapshotName)
	veleroNs := "velero"
	uploadName := val.(string)
	upload := &v1alpha1.Upload{}
	err := durableSnapshotter.k8sClient.Get(ctx, client.ObjectKey{Name: uploadName, Namespace: veleroNs}, upload)
	if err != nil {
		log.Errorf("failed to retrieve upload: %q, err: %+v", uploadName, err)
		return false, err
	}
	uploadPhase := upload.Status.Phase
	if uploadPhase == v1alpha1.UploadPhaseCompleted {
		log.Infof("Snapshot %q Upload is Complete!", snapshotName)
		return true, nil
	}
	log.Infof("Snapshot %q is still uploading..", snapshotName)
	return false, nil
}

func AppendVeleroExcludeLabels(origLabels map[string]string) map[string]string {
	if origLabels == nil {
		origLabels = make(map[string]string)
	}
	origLabels["velero.io/exclude-from-backup"] = "true"
	return origLabels
}

// ParseCSISnapshotID parses the SnapshotID from CSI RPC such as DeleteSnapshot, CreateVolume from snapshot
// into a pair of CNS VolumeID and CNS SnapshotID.
func ParseCSISnapshotID(csiSnapshotID string) (string, string, error) {
	if csiSnapshotID == "" {
		return "", "", errors.New("csiSnapshotID from the input is empty")
	}

	// The expected format of the SnapshotId in the DeleteSnapshotRequest is,
	// a combination of CNS VolumeID and CNS SnapshotID concatenated by the "+" sign.
	// That is, a string of "<UUID>+<UUID>". Decompose csiSnapshotID based on the expected format.
	IDs := strings.Split(csiSnapshotID, VSphereCSISnapshotIdDelimiter)
	if len(IDs) != 2 {
		return "", "", fmt.Errorf("unexpected format in csiSnapshotID: %v", csiSnapshotID)
	}

	cnsVolumeID := IDs[0]
	cnsSnapshotID := IDs[1]

	return cnsVolumeID, cnsSnapshotID, nil
}

func GenerateUploadCRName(snapshotId string) string {
	return "upload-" + snapshotId
}

func GenerateUploadSnapshotPeId(volumeId string, snapshotId string) string {
	return "ivd:" + volumeId + ":" + snapshotId
}
