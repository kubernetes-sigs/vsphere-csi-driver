package e2e

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/gomega"
	"gitlab.eng.vmware.com/hatchway/govmomi"
	cnsmethods "gitlab.eng.vmware.com/hatchway/govmomi/cns/methods"
	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"gitlab.eng.vmware.com/hatchway/govmomi/find"
	"gitlab.eng.vmware.com/hatchway/govmomi/object"
	"gitlab.eng.vmware.com/hatchway/govmomi/pbm"
	pbmtypes "gitlab.eng.vmware.com/hatchway/govmomi/pbm/types"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25/methods"
	"gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
	vim25types "gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

type vSphere struct {
	Config    *e2eTestConfig
	Client    *govmomi.Client
	CnsClient *cnsClient
}

const (
	providerPrefix  = "vsphere://"
	virtualDiskUUID = "virtualDiskUUID"
)

// queryCNSVolumeWithResult Call CnsQueryVolume and returns CnsQueryResult to client
func (vs *vSphere) queryCNSVolumeWithResult(fcdID string) (*cnstypes.CnsQueryResult, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Connect to VC
	connect(ctx, vs)
	var volumeIds []cnstypes.CnsVolumeId
	volumeIds = append(volumeIds, cnstypes.CnsVolumeId{
		Id: fcdID,
	})
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: volumeIds,
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  100,
		},
	}
	req := cnstypes.CnsQueryVolume{
		This:   cnsVolumeManagerInstance,
		Filter: queryFilter,
	}

	err := connectCns(ctx, vs)
	if err != nil {
		return nil, err
	}
	res, err := cnsmethods.CnsQueryVolume(ctx, vs.CnsClient.Client, &req)
	if err != nil {
		return nil, err
	}
	return &res.Returnval, nil
}

// getAllDatacenters returns all the DataCenter Objects
func (vs *vSphere) getAllDatacenters(ctx context.Context) ([]*object.Datacenter, error) {
	connect(ctx, vs)
	finder := find.NewFinder(vs.Client.Client, false)
	return finder.DatacenterList(ctx, "*")
}

// getVMByUUID gets the VM object Reference from the given vmUUID
func (vs *vSphere) getVMByUUID(ctx context.Context, vmUUID string) (object.Reference, error) {
	connect(ctx, vs)
	dcList, err := vs.getAllDatacenters(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, dc := range dcList {
		datacenter := object.NewDatacenter(vs.Client.Client, dc.Reference())
		s := object.NewSearchIndex(vs.Client.Client)
		vmUUID = strings.ToLower(strings.TrimSpace(vmUUID))
		instanceUUID := !vanillaCluster
		vmMoRef, err := s.FindByUuid(ctx, datacenter, vmUUID, true, &instanceUUID)

		if err != nil || vmMoRef == nil {
			continue
		}
		return vmMoRef, nil
	}
	framework.Logf("err in getVMByUUID is %+v for vmuuid: %s", err, vmUUID)
	return nil, fmt.Errorf("Node VM with UUID:%s is not found", vmUUID)
}

// isVolumeAttachedToVM checks volume is attached to the VM by vmUUID.
// This function returns true if volume is attached to the VM, else returns false
func (vs *vSphere) isVolumeAttachedToVM(client clientset.Interface, volumeID string, vmUUID string) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vmRef, err := vs.getVMByUUID(ctx, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("vmRef: %v for the VM uuid: %s", vmRef, vmUUID)
	gomega.Expect(vmRef).NotTo(gomega.BeNil(), "vmRef should not be nil")
	vm := object.NewVirtualMachine(vs.Client.Client, vmRef.Reference())
	device, err := getVirtualDeviceByDiskID(ctx, vm, volumeID)
	if err != nil {
		framework.Logf("Failed to determine whether disk %q is still attached to the VM with UUID: %q", volumeID, vmUUID)
		return false, err
	}
	if device == nil {
		return false, nil
	}
	framework.Logf("Found the disk %q is attached to the VM with UUID: %q", volumeID, vmUUID)
	return true, nil
}

// waitForVolumeDetachedFromNode checks volume is detached from the node
// This function checks disks status every 3 seconds until detachTimeout, which is set to 360 seconds
func (vs *vSphere) waitForVolumeDetachedFromNode(client clientset.Interface, volumeID string, nodeName string) (bool, error) {
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		vmUUID := getNodeUUID(client, nodeName)
		diskAttached, err := vs.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if diskAttached == false {
			framework.Logf("Disk: %s successfully detached", volumeID)
			return true, nil
		}
		framework.Logf("Waiting for disk: %q to be detached from the node :%q", volumeID, nodeName)
		return false, nil
	})
	if err != nil {
		return false, nil
	}
	return true, nil
}

// VerifySpbmPolicyOfVolume verifies if  volume is created with specified storagePolicyName
func (vs *vSphere) VerifySpbmPolicyOfVolume(volumeID string, storagePolicyName string) (bool, error) {
	framework.Logf("Verifying volume: %s is created using storage policy: %s", volumeID, storagePolicyName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get PBM Client
	pbmClient, err := pbm.NewClient(ctx, vs.Client.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	profileID, err := pbmClient.ProfileIDByName(ctx, storagePolicyName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("storage policy id: %s for storage policy name is: %s", profileID, storagePolicyName)
	ProfileID :=
		pbmtypes.PbmProfileId{
			UniqueId: profileID,
		}
	associatedDisks, err := pbmClient.QueryAssociatedEntity(ctx, ProfileID, virtualDiskUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(associatedDisks).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to find associated disks for storage policy: %s", profileID))
	for _, ad := range associatedDisks {
		if ad.Key == volumeID {
			framework.Logf("Volume: %s is associated with storage policy: %s", volumeID, profileID)
			return true, nil
		}
	}
	framework.Logf("Volume: %s is NOT associated with storage policy: %s", volumeID, profileID)
	return false, nil
}

// GetSpbmPolicyID returns profile ID for the specified storagePolicyName
func (vs *vSphere) GetSpbmPolicyID(storagePolicyName string) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get PBM Client
	pbmClient, err := pbm.NewClient(ctx, vs.Client.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	profileID, err := pbmClient.ProfileIDByName(ctx, storagePolicyName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to get profileID from given profileName"))
	framework.Logf("storage policy id: %s for storage policy name is: %s", profileID, storagePolicyName)
	return profileID
}

// getLabelsForCNSVolume executes QueryVolume API on vCenter for requested volumeid and returns
// volume labels for requested entityType, entityName and entityNamespace
func (vs *vSphere) getLabelsForCNSVolume(volumeID string, entityType string, entityName string, entityNamespace string) (map[string]string, error) {
	queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
	if err != nil {
		return nil, err
	}
	if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
		return nil, fmt.Errorf("Failed to query cns volume %s", volumeID)
	}
	gomega.Expect(queryResult.Volumes[0].Metadata).NotTo(gomega.BeNil())
	for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
		kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
		if kubernetesMetadata.EntityType == entityType && kubernetesMetadata.EntityName == entityName && kubernetesMetadata.Namespace == entityNamespace {
			return getLabelsMapFromKeyValue(kubernetesMetadata.Labels), nil
		}
	}
	return nil, fmt.Errorf("entity %s with name %s not found in namespace %s for volume %s", entityType, entityName, entityNamespace, volumeID)
}

// waitForLabelsToBeUpdated executes QueryVolume API on vCenter and verifies
// volume labels are updated by metadata-syncer
func (vs *vSphere) waitForLabelsToBeUpdated(volumeID string, matchLabels map[string]string, entityType string, entityName string, entityNamespace string) error {
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
		framework.Logf("queryResult: %s", spew.Sdump(queryResult))
		if err != nil {
			return true, err
		}
		if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
			return true, fmt.Errorf("failed to query cns volume %s", volumeID)
		}
		gomega.Expect(queryResult.Volumes[0].Metadata).NotTo(gomega.BeNil())
		for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
			if metadata == nil {
				continue
			}
			kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
			if kubernetesMetadata.EntityType == entityType && kubernetesMetadata.EntityName == entityName && kubernetesMetadata.Namespace == entityNamespace {
				if matchLabels == nil {
					return true, nil
				}
				labelsMatch := reflect.DeepEqual(getLabelsMapFromKeyValue(kubernetesMetadata.Labels), matchLabels)
				if labelsMatch {
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		if err == wait.ErrWaitTimeout {
			return fmt.Errorf("labels are not updated to %+v for %s %q for volume %s", matchLabels, entityType, entityName, volumeID)
		}
		return err
	}

	return nil
}

// waitForMetadataToBeDeleted executes QueryVolume API on vCenter and verifies
// volume metadata for given volume has been deleted
func (vs *vSphere) waitForMetadataToBeDeleted(volumeID string, entityType string, entityName string, entityNamespace string) error {
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
		framework.Logf("queryResult: %s", spew.Sdump(queryResult))
		if err != nil {
			return true, err
		}
		if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
			return true, fmt.Errorf("failed to query cns volume %s", volumeID)
		}
		gomega.Expect(queryResult.Volumes[0].Metadata).NotTo(gomega.BeNil())
		for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
			if metadata == nil {
				continue
			}
			kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
			if kubernetesMetadata.EntityType == entityType && kubernetesMetadata.EntityName == entityName && kubernetesMetadata.Namespace == entityNamespace {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		if err == wait.ErrWaitTimeout {
			return fmt.Errorf("entityName %s of entityType %s is not deleted for volume %s", entityName, entityType, volumeID)
		}
		return err
	}

	return nil
}

// waitForCNSVolumeToBeDeleted executes QueryVolume API on vCenter and verifies
// volume entries are deleted from vCenter Database
func (vs *vSphere) waitForCNSVolumeToBeDeleted(volumeID string) error {
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
		if err != nil {
			return true, err
		}

		if len(queryResult.Volumes) == 0 {
			framework.Logf("volume %q has successfully deleted", volumeID)
			return true, nil
		}
		framework.Logf("waiting for Volume %q to be deleted.", volumeID)
		return false, nil
	})
	if err != nil {
		return err
	}
	return nil
}

// waitForCNSVolumeToBeCreate executes QueryVolume API on vCenter and verifies
// volume entries are created in vCenter Database
func (vs *vSphere) waitForCNSVolumeToBeCreated(volumeID string) error {
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
		if err != nil {
			return true, err
		}

		if len(queryResult.Volumes) == 1 && queryResult.Volumes[0].VolumeId.Id == volumeID {
			framework.Logf("volume %q has successfully created", volumeID)
			return true, nil
		}
		framework.Logf("waiting for Volume %q to be created.", volumeID)
		return false, nil
	})
	return err
}

// createFCD creates an FCD disk
func (vs *vSphere) createFCD(ctx context.Context, fcdname string, diskCapacityInMB int64, dsRef types.ManagedObjectReference) (string, error) {
	KeepAfterDeleteVM := false
	spec := types.VslmCreateSpec{
		Name:              fcdname,
		CapacityInMB:      diskCapacityInMB,
		KeepAfterDeleteVm: &KeepAfterDeleteVM,
		BackingSpec: &types.VslmCreateSpecDiskFileBackingSpec{
			VslmCreateSpecBackingSpec: types.VslmCreateSpecBackingSpec{
				Datastore: dsRef,
			},
			ProvisioningType: string(types.BaseConfigInfoDiskFileBackingInfoProvisioningTypeThin),
		},
	}
	req := types.CreateDisk_Task{
		This: *vs.Client.Client.ServiceContent.VStorageObjectManager,
		Spec: spec,
	}
	res, err := methods.CreateDisk_Task(ctx, vs.Client.Client, &req)
	if err != nil {
		return "", err
	}
	task := object.NewTask(vs.Client.Client, res.Returnval)
	taskInfo, err := task.WaitForResult(ctx, nil)
	if err != nil {
		return "", err
	}
	fcdID := taskInfo.Result.(types.VStorageObject).Config.Id.Id
	return fcdID, nil
}

// deleteFCD deletes an FCD disk
func (vs *vSphere) deleteFCD(ctx context.Context, fcdID string, dsRef types.ManagedObjectReference) error {
	req := types.DeleteVStorageObject_Task{
		This:      *vs.Client.Client.ServiceContent.VStorageObjectManager,
		Datastore: dsRef,
		Id:        types.ID{Id: fcdID},
	}
	res, err := methods.DeleteVStorageObject_Task(ctx, vs.Client.Client, &req)
	if err != nil {
		return err
	}
	task := object.NewTask(vs.Client.Client, res.Returnval)
	_, err = task.WaitForResult(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

// relocateFCD relocates an FCD disk
func (vs *vSphere) relocateFCD(ctx context.Context, fcdID string, dsRefSrc types.ManagedObjectReference, dsRefDest types.ManagedObjectReference) error {
	spec := types.VslmRelocateSpec{
		VslmMigrateSpec: types.VslmMigrateSpec{
			DynamicData: vim25types.DynamicData{},
			BackingSpec: &types.VslmCreateSpecDiskFileBackingSpec{
				VslmCreateSpecBackingSpec: types.VslmCreateSpecBackingSpec{
					Datastore: dsRefDest,
				},
			},
		},
	}
	req := types.RelocateVStorageObject_Task{
		This:      *vs.Client.Client.ServiceContent.VStorageObjectManager,
		Id:        types.ID{Id: fcdID},
		Datastore: dsRefSrc,
		Spec:      spec,
	}
	res, err := methods.RelocateVStorageObject_Task(ctx, vs.Client.Client, &req)
	if err != nil {
		return err
	}
	task := object.NewTask(vs.Client.Client, res.Returnval)
	_, err = task.WaitForResult(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}
