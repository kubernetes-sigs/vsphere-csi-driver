package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/cns"
	cnsmethods "github.com/vmware/govmomi/cns/methods"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/pbm"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/soap"
	vim25types "github.com/vmware/govmomi/vim25/types"
	vsanmethods "github.com/vmware/govmomi/vsan/methods"
	vsantypes "github.com/vmware/govmomi/vsan/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

type vSphere struct {
	Config    *e2eTestConfig
	Client    *govmomi.Client
	CnsClient *cnsClient
}

// VsanClient struct holds vim and soap client
type VsanClient struct {
	vim25Client   *vim25.Client
	serviceClient *soap.Client
}

// Creates the vsan object identities instance. This is to be queried from vsan health.
var (
	VsanQueryObjectIdentitiesInstance = vim25types.ManagedObjectReference{
		Type:  "VsanObjectSystem",
		Value: "vsan-cluster-object-system",
	}
)

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

// queryCNSVolumeSnapshotWithResult Call CnsQuerySnapshots
// and returns CnsSnapshotQueryResult to client
func (vs *vSphere) queryCNSVolumeSnapshotWithResult(fcdID string,
	snapshotId string) (*cnstypes.CnsSnapshotQueryResult, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var snapshotSpec []cnstypes.CnsSnapshotQuerySpec
	snapshotSpec = append(snapshotSpec, cnstypes.CnsSnapshotQuerySpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: fcdID,
		},
		SnapshotId: &cnstypes.CnsSnapshotId{
			Id: snapshotId,
		},
	})

	queryFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: snapshotSpec,
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  100,
		},
	}

	req := cnstypes.CnsQuerySnapshots{
		This:                cnsVolumeManagerInstance,
		SnapshotQueryFilter: queryFilter,
	}

	res, err := cnsmethods.CnsQuerySnapshots(ctx, vs.CnsClient.Client, &req)
	if err != nil {
		return nil, err
	}

	task, err := object.NewTask(e2eVSphere.Client.Client, res.Returnval), nil
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	taskInfo, err := cns.GetTaskInfo(ctx, task)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	taskResult, err := cns.GetQuerySnapshotsTaskResult(ctx, taskInfo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return taskResult, nil
}

// verifySnapshotIsDeletedInCNS verifies the snapshotId's presence on CNS
func verifySnapshotIsDeletedInCNS(volumeId string, snapshotId string) error {
	ginkgo.By(fmt.Sprintf("Invoking queryCNSVolumeSnapshotWithResult with VolumeID: %s and SnapshotID: %s",
		volumeId, snapshotId))
	querySnapshotResult, err := e2eVSphere.queryCNSVolumeSnapshotWithResult(volumeId, snapshotId)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("Task result is %+v", querySnapshotResult))
	gomega.Expect(querySnapshotResult.Entries).ShouldNot(gomega.BeEmpty())
	if querySnapshotResult.Entries[0].Snapshot.SnapshotId.Id != "" {
		return fmt.Errorf("snapshot entry is still present in CNS %s",
			querySnapshotResult.Entries[0].Snapshot.SnapshotId.Id)
	}
	return nil
}

// verifySnapshotIsCreatedInCNS verifies the snapshotId's presence on CNS
func verifySnapshotIsCreatedInCNS(volumeId string, snapshotId string) error {
	ginkgo.By(fmt.Sprintf("Invoking queryCNSVolumeSnapshotWithResult with VolumeID: %s and SnapshotID: %s",
		volumeId, snapshotId))
	querySnapshotResult, err := e2eVSphere.queryCNSVolumeSnapshotWithResult(volumeId, snapshotId)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("Task result is %+v", querySnapshotResult))
	gomega.Expect(querySnapshotResult.Entries).ShouldNot(gomega.BeEmpty())
	if querySnapshotResult.Entries[0].Snapshot.SnapshotId.Id != snapshotId {
		return fmt.Errorf("snapshot entry is not present in CNS %s", snapshotId)
	}
	return nil
}

// getAllDatacenters returns all the DataCenter Objects
func (vs *vSphere) getAllDatacenters(ctx context.Context) ([]*object.Datacenter, error) {
	connect(ctx, vs)
	finder := find.NewFinder(vs.Client.Client, false)
	return finder.DatacenterList(ctx, "*")
}

// getDatacenter returns the DataCenter Object for the given datacenterPath
func (vs *vSphere) getDatacenter(ctx context.Context, datacenterPath string) (*object.Datacenter, error) {
	connect(ctx, vs)
	finder := find.NewFinder(vs.Client.Client, false)
	return finder.Datacenter(ctx, datacenterPath)
}

// getDatastoresMountedOnHost returns the datastore references of all the datastores mounted on the specified host
func (vs *vSphere) getDatastoresMountedOnHost(ctx context.Context,
	host vim25types.ManagedObjectReference) []vim25types.ManagedObjectReference {
	connect(ctx, vs)
	var hostMo mo.HostSystem
	err := vs.Client.RetrieveOne(ctx, host, []string{"datastore"}, &hostMo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return hostMo.Datastore
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
		instanceUUID := !(vanillaCluster || guestCluster)
		vmMoRef, err := s.FindByUuid(ctx, datacenter, vmUUID, true, &instanceUUID)

		if err != nil || vmMoRef == nil {
			continue
		}
		return vmMoRef, nil
	}
	framework.Logf("err in getVMByUUID is %+v for vmuuid: %s", err, vmUUID)
	return nil, fmt.Errorf("node VM with UUID:%s is not found", vmUUID)
}

// getHostFromVMReference returns host object reference of the host on which the specified VM resides
func (vs *vSphere) getHostFromVMReference(ctx context.Context,
	vm vim25types.ManagedObjectReference) vim25types.ManagedObjectReference {
	connect(ctx, vs)
	var vmMo mo.VirtualMachine
	err := vs.Client.RetrieveOne(ctx, vm, []string{"summary.runtime.host"}, &vmMo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	host := *vmMo.Summary.Runtime.Host
	return host
}

// getVMByUUIDWithWait gets the VM object Reference from the given vmUUID with a given wait timeout
func (vs *vSphere) getVMByUUIDWithWait(ctx context.Context,
	vmUUID string, timeout time.Duration) (object.Reference, error) {
	connect(ctx, vs)
	dcList, err := vs.getAllDatacenters(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var vmMoRefForvmUUID object.Reference
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		var vmMoRefFound bool
		for _, dc := range dcList {
			datacenter := object.NewDatacenter(vs.Client.Client, dc.Reference())
			s := object.NewSearchIndex(vs.Client.Client)
			vmUUID = strings.ToLower(strings.TrimSpace(vmUUID))
			instanceUUID := !(vanillaCluster || guestCluster)
			vmMoRef, err := s.FindByUuid(ctx, datacenter, vmUUID, true, &instanceUUID)

			if err != nil || vmMoRef == nil {
				continue
			}
			if vmMoRef != nil {
				vmMoRefFound = true
				vmMoRefForvmUUID = vmMoRef
			}
		}
		if vmMoRefFound {
			framework.Logf("vmuuid: %s still exists", vmMoRefForvmUUID)
			continue
		} else {
			return nil, fmt.Errorf("node VM with UUID:%s is not found", vmUUID)
		}
	}
	return vmMoRefForvmUUID, nil
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
		framework.Logf("failed to determine whether disk %q is still attached to the VM with UUID: %q", volumeID, vmUUID)
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
func (vs *vSphere) waitForVolumeDetachedFromNode(client clientset.Interface,
	volumeID string, nodeName string) (bool, error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if supervisorCluster {
		_, err := e2eVSphere.getVMByUUIDWithWait(ctx, nodeName, supervisorClusterOperationsTimeout)
		if err == nil {
			return false, fmt.Errorf(
				"PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", nodeName, volumeID)
		} else if strings.Contains(err.Error(), "is not found") {
			return true, nil
		}
		return false, err
	}
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		var vmUUID string
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, nodeName)
		} else {
			vmUUID, _ = getVMUUIDFromNodeName(nodeName)
		}
		diskAttached, err := vs.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if !diskAttached {
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
	gomega.Expect(associatedDisks).NotTo(gomega.BeEmpty(),
		fmt.Sprintf("Unable to find associated disks for storage policy: %s", profileID))
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
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to get profileID from given profileName")
	framework.Logf("storage policy id: %s for storage policy name is: %s", profileID, storagePolicyName)
	return profileID
}

// getLabelsForCNSVolume executes QueryVolume API on vCenter for requested volumeid and returns
// volume labels for requested entityType, entityName and entityNamespace
func (vs *vSphere) getLabelsForCNSVolume(volumeID string, entityType string,
	entityName string, entityNamespace string) (map[string]string, error) {
	queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
	if err != nil {
		return nil, err
	}
	if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
		return nil, fmt.Errorf("failed to query cns volume %s", volumeID)
	}
	gomega.Expect(queryResult.Volumes[0].Metadata).NotTo(gomega.BeNil())
	for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
		kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
		if kubernetesMetadata.EntityType == entityType && kubernetesMetadata.EntityName == entityName &&
			kubernetesMetadata.Namespace == entityNamespace {
			return getLabelsMapFromKeyValue(kubernetesMetadata.Labels), nil
		}
	}
	return nil, fmt.Errorf("entity %s with name %s not found in namespace %s for volume %s",
		entityType, entityName, entityNamespace, volumeID)
}

// waitForLabelsToBeUpdated executes QueryVolume API on vCenter and verifies
// volume labels are updated by metadata-syncer
func (vs *vSphere) waitForLabelsToBeUpdated(volumeID string, matchLabels map[string]string,
	entityType string, entityName string, entityNamespace string) error {
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		err := vs.verifyLabelsAreUpdated(volumeID, matchLabels, entityType, entityName, entityNamespace)
		if err == nil {
			return true, nil
		} else {
			return false, nil
		}
	})
	if err != nil {
		if err == wait.ErrWaitTimeout {
			return fmt.Errorf("labels are not updated to %+v for %s %q for volume %s",
				matchLabels, entityType, entityName, volumeID)
		}
		return err
	}

	return nil
}

// waitForMetadataToBeDeleted executes QueryVolume API on vCenter and verifies
// volume metadata for given volume has been deleted
func (vs *vSphere) waitForMetadataToBeDeleted(volumeID string, entityType string,
	entityName string, entityNamespace string) error {
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
			if kubernetesMetadata.EntityType == entityType && kubernetesMetadata.EntityName == entityName &&
				kubernetesMetadata.Namespace == entityNamespace {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		if err == wait.ErrWaitTimeout {
			return fmt.Errorf("entityName %s of entityType %s is not deleted for volume %s",
				entityName, entityType, volumeID)
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
func (vs *vSphere) createFCD(ctx context.Context, fcdname string,
	diskCapacityInMB int64, dsRef vim25types.ManagedObjectReference) (string, error) {
	KeepAfterDeleteVM := false
	spec := vim25types.VslmCreateSpec{
		Name:              fcdname,
		CapacityInMB:      diskCapacityInMB,
		KeepAfterDeleteVm: &KeepAfterDeleteVM,
		BackingSpec: &vim25types.VslmCreateSpecDiskFileBackingSpec{
			VslmCreateSpecBackingSpec: vim25types.VslmCreateSpecBackingSpec{
				Datastore: dsRef,
			},
			ProvisioningType: string(vim25types.BaseConfigInfoDiskFileBackingInfoProvisioningTypeThin),
		},
	}
	req := vim25types.CreateDisk_Task{
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
	fcdID := taskInfo.Result.(vim25types.VStorageObject).Config.Id.Id
	return fcdID, nil
}

// createFCD with valid storage policy
func (vs *vSphere) createFCDwithValidProfileID(ctx context.Context, fcdname string,
	profileID string, diskCapacityInMB int64, dsRef vim25types.ManagedObjectReference) (string, error) {
	KeepAfterDeleteVM := false
	spec := vim25types.VslmCreateSpec{
		Name:              fcdname,
		CapacityInMB:      diskCapacityInMB,
		KeepAfterDeleteVm: &KeepAfterDeleteVM,
		BackingSpec: &vim25types.VslmCreateSpecDiskFileBackingSpec{
			VslmCreateSpecBackingSpec: vim25types.VslmCreateSpecBackingSpec{
				Datastore: dsRef,
			},
			ProvisioningType: string(vim25types.BaseConfigInfoDiskFileBackingInfoProvisioningTypeThin),
		},
		Profile: []vim25types.BaseVirtualMachineProfileSpec{
			&vim25types.VirtualMachineDefinedProfileSpec{
				ProfileId: profileID,
			},
		},
	}
	req := vim25types.CreateDisk_Task{
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
	fcdID := taskInfo.Result.(vim25types.VStorageObject).Config.Id.Id
	return fcdID, nil
}

// deleteFCD deletes an FCD disk
func (vs *vSphere) deleteFCD(ctx context.Context, fcdID string, dsRef vim25types.ManagedObjectReference) error {
	req := vim25types.DeleteVStorageObject_Task{
		This:      *vs.Client.Client.ServiceContent.VStorageObjectManager,
		Datastore: dsRef,
		Id:        vim25types.ID{Id: fcdID},
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
func (vs *vSphere) relocateFCD(ctx context.Context, fcdID string,
	dsRefSrc vim25types.ManagedObjectReference, dsRefDest vim25types.ManagedObjectReference) error {
	spec := vim25types.VslmRelocateSpec{
		VslmMigrateSpec: vim25types.VslmMigrateSpec{
			DynamicData: vim25types.DynamicData{},
			BackingSpec: &vim25types.VslmCreateSpecDiskFileBackingSpec{
				VslmCreateSpecBackingSpec: vim25types.VslmCreateSpecBackingSpec{
					Datastore: dsRefDest,
				},
			},
		},
	}
	req := vim25types.RelocateVStorageObject_Task{
		This:      *vs.Client.Client.ServiceContent.VStorageObjectManager,
		Id:        vim25types.ID{Id: fcdID},
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

// verifyVolPropertiesFromCnsQueryResults verify file volume properties like
// capacity, volume type, datastore type and datacenter.
func verifyVolPropertiesFromCnsQueryResults(e2eVSphere vSphere, volHandle string) {

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s accesspoint: %s",
		queryResult.Volumes[0].Name,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
		queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints))

	//Verifying disk size specified in PVC is honored
	ginkgo.By("Verifying disk size specified in PVC is honored")
	gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
		CapacityInMb == diskSizeInMb).To(gomega.BeTrue(), "wrong disk size provisioned")

	//Verifying volume type specified in PVC is honored
	ginkgo.By("Verifying volume type specified in PVC is honored")
	gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(), "volume type is not FILE")

	// Verify if VolumeID is created on the VSAN datastores
	ginkgo.By("Verify if VolumeID is created on the VSAN datastores")
	gomega.Expect(strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:")).
		To(gomega.BeTrue(), "Volume is not provisioned on vSan datastore")

	// Verify if VolumeID is created on the datastore from list of datacenters provided in vsphere.conf
	ginkgo.By("Verify if VolumeID is created on the datastore from list of datacenters provided in vsphere.conf")
	gomega.Expect(isDatastoreBelongsToDatacenterSpecifiedInConfig(queryResult.Volumes[0].DatastoreUrl)).
		To(gomega.BeTrue(), "Volume is not provisioned on the datastore specified on config file")

}

// getClusterName methods returns the cluster and vsan client of the testbed
func getClusterName(ctx context.Context, vs *vSphere) ([]*object.ClusterComputeResource, *VsanClient, error) {
	c := newClient(ctx, vs)
	datacenter := e2eVSphere.Config.Global.Datacenters

	vsanHealthClient, err := newVsanHealthSvcClient(ctx, c.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	finder := find.NewFinder(vsanHealthClient.vim25Client, false)
	dc, err := finder.Datacenter(ctx, datacenter)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)

	clusterComputeResource, err := finder.ClusterComputeResourceList(ctx, "*")
	framework.Logf("clusterComputeResource %v", clusterComputeResource)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return clusterComputeResource, vsanHealthClient, err
}

// getHostUUID takes input of the HostInfo which has host uuid
// with the host uuid it maps the corresponding host IP and returns it
func (vs *vSphere) getHostUUID(ctx context.Context, hostInfo string) string {
	var result map[string]interface{}
	computeCluster := os.Getenv("CLUSTER_NAME")
	if computeCluster == "" {
		if guestCluster {
			computeCluster = "compute-cluster"
		} else if supervisorCluster {
			computeCluster = "wcp-app-platform-sanity-cluster"
		}
		framework.Logf("Default cluster is chosen for test")
	}

	err := json.Unmarshal([]byte(hostInfo), &result)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	lsomObject := result["lsom_objects"]
	lsomObjectInterface, _ := lsomObject.(map[string]interface{})

	//This loop get the the esx host uuid from the queryVsanObj result
	for _, lsomValue := range lsomObjectInterface {
		key, _ := lsomValue.(map[string]interface{})
		for key1, value1 := range key {
			if key1 == "owner" {
				framework.Logf("hostUUID %v, hostIP %v", key1, value1)
				finder := find.NewFinder(vs.Client.Client, false)
				dc, err := finder.Datacenter(ctx, vs.Config.Global.Datacenters)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				finder.SetDatacenter(dc)

				clusterComputeResource, err := finder.ClusterComputeResourceList(ctx, "*")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				cluster := clusterComputeResource[0]
				//TKG setup with NSX has edge-cluster enabled, this check is to skip that cluster
				if !strings.Contains(cluster.Name(), computeCluster) {
					cluster = clusterComputeResource[1]
				}

				config, err := cluster.Configuration(ctx)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for counter := range config.VsanHostConfig {
					if config.VsanHostConfig[counter].ClusterInfo.NodeUuid == value1 {
						hosts, err := cluster.Hosts(ctx)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("host name from hostUUID %v", hosts[counter].Name())
						return hosts[counter].Name()
					}
				}
			}
		}
	}
	return ""
}

// getVsanClusterResource returns the vsan cluster's details
func (vs *vSphere) getVsanClusterResource(ctx context.Context, forceRefresh ...bool) *object.ClusterComputeResource {
	refresh := false
	var cluster *object.ClusterComputeResource
	if len(forceRefresh) > 0 {
		refresh = forceRefresh[0]
	}
	if defaultCluster == nil || refresh {
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		datacenters := []string{}
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}

		for _, dc := range datacenters {
			defaultDatacenter, err := finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)

			clusterComputeResource, err := finder.ClusterComputeResourceList(ctx, "*")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			cluster = clusterComputeResource[0]

			if strings.Contains(strings.ToLower(cluster.Name()), "edge") {
				cluster = clusterComputeResource[1]
			}

			framework.Logf("Looking for cluster with the default datastore passed into test in DC: " + dc)
			datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			if err == nil {
				framework.Logf("Cluster %s found matching the datastore URL: %s", clusterComputeResource[0].Name(),
					datastoreURL)
				defaultCluster = cluster
				break
			}
		}
	} else {
		framework.Logf("Found the default Cluster already set")
	}
	framework.Logf("Returning cluster %s ", defaultCluster.Name())
	return defaultCluster
}

// getAllHostsIP reads cluster, gets hosts in it and returns IP array
func getAllHostsIP(ctx context.Context) []string {
	var result []string
	cluster := e2eVSphere.getVsanClusterResource(ctx)
	hosts, err := cluster.Hosts(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, moHost := range hosts {
		result = append(result, moHost.Name())
	}
	return result
}

// getHostConnectionState reads cluster, gets hosts in it and returns connection state of host
func getHostConnectionState(ctx context.Context, addr string) (string, error) {
	var state string
	cluster := e2eVSphere.getVsanClusterResource(ctx)
	hosts, err := cluster.Hosts(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for counter := range hosts {
		var moHost mo.HostSystem
		if hosts[counter].Name() == addr {
			err = hosts[counter].Properties(ctx, hosts[counter].Reference(), []string{"summary"}, &moHost)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "failed to get host system properties")
			framework.Logf("Host owning IP: %v, and its connection state is %s", hosts[counter].Name(),
				moHost.Summary.Runtime.ConnectionState)
			state = string(moHost.Summary.Runtime.ConnectionState)
			return state, nil
		}
	}
	return "host not found", fmt.Errorf("host not found %s", addr)
}

// VsanQueryObjectIdentities return list of vsan uuids
// example: For a PVC, It returns the vSAN object UUIDs to their identities
// It return vsanObjuuid like [4336525f-7813-d78a-e3a4-02005456da7e]
func (c *VsanClient) VsanQueryObjectIdentities(ctx context.Context,
	cluster vim25types.ManagedObjectReference) (*vsantypes.VsanObjectIdentityAndHealth, error) {
	req := vsantypes.VsanQueryObjectIdentities{
		This:    VsanQueryObjectIdentitiesInstance,
		Cluster: &cluster,
	}

	res, err := vsanmethods.VsanQueryObjectIdentities(ctx, c.serviceClient, &req)

	if err != nil {
		return nil, err
	}
	return res.Returnval, nil
}

// QueryVsanObjects takes vsan uuid as input and returns the vSANObj related
// information like lsom_objects and disk_objects.
// Example return values:
//
//	"{"disk_objects": {"525a9aa5-1142-4004-ad6f-2389eef25f06":
//	   ....lsom_objects": {"e7945f5f-4267-3e5d-334a-020063a7a5c4":......}
func (c *VsanClient) QueryVsanObjects(ctx context.Context, uuids []string, vs *vSphere) (string, error) {
	computeCluster := os.Getenv("CLUSTER_NAME")
	if computeCluster == "" {
		if guestCluster {
			computeCluster = "compute-cluster"
		} else if supervisorCluster {
			computeCluster = "wcp-app-platform-sanity-cluster"
		}
		framework.Logf("Default cluster is chosen for test")
	}
	clusterComputeResource, _, err := getClusterName(ctx, vs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	cluster := clusterComputeResource[0]
	if !strings.Contains(cluster.Name(), computeCluster) {
		cluster = clusterComputeResource[1]
	}
	config, err := cluster.Configuration(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	hostSystem := config.VsanHostConfig[0].HostSystem.String()

	hostName := strings.Split(hostSystem, ":")
	hostValue := strings.Split(hostName[1], "-")
	value := ("ha-vsan-internal-system-" + hostValue[1])
	framework.Logf("vsan internal system value %v", value)
	var (
		QueryVsanObjectsInstance = vim25types.ManagedObjectReference{
			Type:  "HostVsanInternalSystem",
			Value: value,
		}
	)
	req := vim25types.QueryVsanObjects{
		This:  QueryVsanObjectsInstance,
		Uuids: uuids,
	}
	res, err := methods.QueryVsanObjects(ctx, c.serviceClient, &req)
	if err != nil {
		framework.Logf("QueryVsanObjects Failed with err %v", err)
		return "", err
	}
	return res.Returnval, nil
}

// queryCNSVolumeWithWait gets the cns volume health status
func queryCNSVolumeWithWait(ctx context.Context, client clientset.Interface, volHandle string) error {
	waitErr := wait.Poll(pollTimeoutShort, pollTimeout, func() (bool, error) {
		framework.Logf("wait for next poll %v", pollTimeoutShort)

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(len(queryResult.Volumes)).NotTo(gomega.BeZero())
		if err != nil {
			return false, nil
		}
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			if vol.HealthStatus == healthRed {
				framework.Logf("Volume health status: %v", vol.HealthStatus)
				return true, nil
			}
		}
		return false, nil
	})
	return waitErr
}

// waitForHostConnectionState gets the connection state of the host and waits till the desired state is obtained
func waitForHostConnectionState(ctx context.Context, addr string, state string) error {
	var output string
	waitErr := wait.Poll(poll, pollTimeout, func() (bool, error) {

		output, err := getHostConnectionState(ctx, addr)
		if err != nil {
			framework.Logf("The host's %s last seen state before returning error is : %s", addr, output)
			return false, nil
		}

		if state == output {
			framework.Logf("The host's %s current state is as expected state : %s", addr, output)
			return true, nil
		}
		return false, nil
	})
	framework.Logf("The host's %s last seen state before returning is : %s", addr, output)
	return waitErr
}

// deleteVolumeSnapshotInCNS Call deleteSnapshots API
func (vs *vSphere) deleteVolumeSnapshotInCNS(fcdID string, snapshotId string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to VC
	connect(ctx, vs)

	var cnsSnapshotDeleteSpecList []cnstypes.CnsSnapshotDeleteSpec
	cnsSnapshotDeleteSpec := cnstypes.CnsSnapshotDeleteSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: fcdID,
		},
		SnapshotId: cnstypes.CnsSnapshotId{
			Id: snapshotId,
		},
	}
	cnsSnapshotDeleteSpecList = append(cnsSnapshotDeleteSpecList, cnsSnapshotDeleteSpec)

	req := cnstypes.CnsDeleteSnapshots{
		This:                cnsVolumeManagerInstance,
		SnapshotDeleteSpecs: cnsSnapshotDeleteSpecList,
	}

	res, err := cnsmethods.CnsDeleteSnapshots(ctx, vs.CnsClient.Client, &req)

	if err != nil {
		return err
	}

	task, err := object.NewTask(e2eVSphere.Client.Client, res.Returnval), nil
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	taskInfo, err := cns.GetTaskInfo(ctx, task)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	deleteSnapshotsTaskResult, err := cns.GetTaskResult(ctx, taskInfo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	deleteSnapshotsOperationRes := deleteSnapshotsTaskResult.GetCnsVolumeOperationResult()

	if deleteSnapshotsOperationRes.Fault != nil {
		err = fmt.Errorf("failed to create snapshots: fault=%+v", deleteSnapshotsOperationRes.Fault)
	}

	snapshotDeleteResult := interface{}(deleteSnapshotsTaskResult).(*cnstypes.CnsSnapshotDeleteResult)
	framework.Logf("DeleteSnapshots: Snapshot deleted successfully. volumeId: %q, snapshot id %q",
		fcdID, snapshotDeleteResult.SnapshotId)
	return err
}

// createVolumeSnapshotInCNS Call createSnapshots API and returns snapshotId to client
func (vs *vSphere) createVolumeSnapshotInCNS(fcdID string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cnsSnapshotCreateSpecList []cnstypes.CnsSnapshotCreateSpec
	cnsSnapshotCreateSpec := cnstypes.CnsSnapshotCreateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: fcdID,
		},
		Description: "Volume Snapshot created by CSI",
	}
	cnsSnapshotCreateSpecList = append(cnsSnapshotCreateSpecList, cnsSnapshotCreateSpec)

	req := cnstypes.CnsCreateSnapshots{
		This:          cnsVolumeManagerInstance,
		SnapshotSpecs: cnsSnapshotCreateSpecList,
	}

	res, err := cnsmethods.CnsCreateSnapshots(ctx, vs.CnsClient.Client, &req)

	if err != nil {
		return "", err
	}

	task, err := object.NewTask(e2eVSphere.Client.Client, res.Returnval), nil
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	taskInfo, err := cns.GetTaskInfo(ctx, task)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	createSnapshotsOperationRes := taskResult.GetCnsVolumeOperationResult()

	if createSnapshotsOperationRes.Fault != nil {
		err = fmt.Errorf("failed to create snapshots: fault=%+v", createSnapshotsOperationRes.Fault)
	}

	snapshotCreateResult := interface{}(taskResult).(*cnstypes.CnsSnapshotCreateResult)
	snapshotId := snapshotCreateResult.Snapshot.SnapshotId.Id
	snapshotCreateTime := snapshotCreateResult.Snapshot.CreateTime
	framework.Logf("CreateSnapshot: Snapshot created successfully. volumeId: %q, snapshot id %q, time stamp %+v",
		fcdID, snapshotId, snapshotCreateTime)

	return snapshotId, err
}

// verifyVolumeCompliance verifies the volume policy compliance status
func (vs *vSphere) verifyVolumeCompliance(volumeID string, shouldBeCompliant bool) {
	queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Volume id: %v compliance status: %v", volumeID, queryResult.Volumes[0].ComplianceStatus)
	if shouldBeCompliant {
		gomega.Expect(queryResult.Volumes[0].ComplianceStatus == "compliant").To(gomega.BeTrue())
	} else {
		gomega.Expect(queryResult.Volumes[0].ComplianceStatus == "compliant").To(gomega.BeFalse())
	}
}

// verifyLabelsAreUpdated executes cns QueryVolume API on vCenter and verifies if
// volume labels are updated by metadata-syncer
func (vs *vSphere) verifyLabelsAreUpdated(volumeID string, matchLabels map[string]string,
	entityType string, entityName string, entityNamespace string) error {

	queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
	framework.Logf("queryResult: %s", spew.Sdump(queryResult))
	if err != nil {
		return err
	}
	if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
		return fmt.Errorf("failed to query cns volume %s", volumeID)
	}
	gomega.Expect(queryResult.Volumes[0].Metadata).NotTo(gomega.BeNil())
	for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
		if metadata == nil {
			continue
		}
		kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
		k8sEntityName := kubernetesMetadata.EntityName
		if guestCluster {
			k8sEntityName = kubernetesMetadata.CnsEntityMetadata.EntityName
		}
		if kubernetesMetadata.EntityType == entityType && k8sEntityName == entityName &&
			kubernetesMetadata.Namespace == entityNamespace {
			if matchLabels == nil {
				return nil
			}
			labelsMatch := reflect.DeepEqual(getLabelsMapFromKeyValue(kubernetesMetadata.Labels), matchLabels)
			if guestCluster {
				labelsMatch = reflect.DeepEqual(getLabelsMapFromKeyValue(kubernetesMetadata.CnsEntityMetadata.Labels),
					matchLabels)
			}
			if labelsMatch {
				return nil
			} else {
				return fmt.Errorf("labels are not updated to %+v for %s %q for volume %s",
					matchLabels, entityType, entityName, volumeID)
			}
		}
	}
	return nil
}

// verifyDatastoreMatch verify if any of the given dsUrl matches with the datstore url for the volumeid
func (vs *vSphere) verifyDatastoreMatch(volumeID string, dsUrls []string) {
	actualDatastoreUrl := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
	gomega.Expect(actualDatastoreUrl).Should(gomega.BeElementOf(dsUrls),
		"Volume is not provisioned on any of the given datastores: %s, but on: %s", dsUrls,
		actualDatastoreUrl)
}

// cnsRelocateVolume relocates volume from one datastore to another using CNS relocate volume API
func (vs *vSphere) cnsRelocateVolume(e2eVSphere vSphere, ctx context.Context, fcdID string,
	dsRefDest vim25types.ManagedObjectReference) error {
	var pandoraSyncWaitTime int
	var err error
	if os.Getenv(envPandoraSyncWaitTime) != "" {
		pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		pandoraSyncWaitTime = defaultPandoraSyncWaitTime
	}

	relocateSpec := cnstypes.NewCnsBlockVolumeRelocateSpec(fcdID, dsRefDest)
	var baseCnsVolumeRelocateSpecList []cnstypes.BaseCnsVolumeRelocateSpec
	baseCnsVolumeRelocateSpecList = append(baseCnsVolumeRelocateSpecList, relocateSpec)
	req := cnstypes.CnsRelocateVolume{
		This:          cnsVolumeManagerInstance,
		RelocateSpecs: baseCnsVolumeRelocateSpecList,
	}

	cnsClient, err := newCnsClient(ctx, vs.Client.Client)
	framework.Logf("error: %v", err)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	res, err := cnsmethods.CnsRelocateVolume(ctx, cnsClient, &req)
	framework.Logf("error is: %v", err)
	if err != nil {
		return err
	}

	task, err := object.NewTask(e2eVSphere.Client.Client, res.Returnval), nil
	taskInfo, err := task.WaitForResult(ctx, nil)
	framework.Logf("taskInfo: %v", taskInfo)
	framework.Logf("error: %v", err)
	if err != nil {
		return err
	}
	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	if err != nil {
		return err
	}

	framework.Logf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime)
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

	cnsRelocateVolumeRes := taskResult.GetCnsVolumeOperationResult()

	if cnsRelocateVolumeRes.Fault != nil {
		err = fmt.Errorf("failed to relocate volume=%+v", cnsRelocateVolumeRes.Fault)
		return err
	}
	return nil
}

// fetchDsUrl4CnsVol executes query CNS volume to get the datastore
// where the volume is Present
func fetchDsUrl4CnsVol(e2eVSphere vSphere, volHandle string) string {
	framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle)
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	return queryResult.Volumes[0].DatastoreUrl
}

// verifyPreferredDatastoreMatch verify if any of the given dsUrl matches with the datstore url for the volumeid
func (vs *vSphere) verifyPreferredDatastoreMatch(volumeID string, dsUrls []string) bool {
	actualDatastoreUrl := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
	flag := false
	for _, dsUrl := range dsUrls {
		if actualDatastoreUrl == dsUrl {
			flag = true
			return flag
		}
	}
	return flag
}

// Delete CNS volume
func (vs *vSphere) deleteCNSvolume(volumeID string, isDeleteDisk bool) (*cnstypes.CnsDeleteVolumeResponse, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Connect to VC
	connect(ctx, vs)

	var volumeIds []cnstypes.CnsVolumeId
	volumeIds = append(volumeIds, cnstypes.CnsVolumeId{
		Id: volumeID,
	})

	req := cnstypes.CnsDeleteVolume{
		This:       cnsVolumeManagerInstance,
		VolumeIds:  volumeIds,
		DeleteDisk: isDeleteDisk,
	}

	err := connectCns(ctx, vs)
	if err != nil {
		return nil, err
	}
	res, err := cnsmethods.CnsDeleteVolume(ctx, vs.CnsClient.Client, &req)
	if err != nil {
		return nil, err
	}
	return res, nil
}
