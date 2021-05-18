package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi"
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

//VsanClient struct holds vim and soap client
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
func (vs *vSphere) getDatastoresMountedOnHost(ctx context.Context, host vim25types.ManagedObjectReference) []vim25types.ManagedObjectReference {
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
func (vs *vSphere) getHostFromVMReference(ctx context.Context, vm vim25types.ManagedObjectReference) vim25types.ManagedObjectReference {
	connect(ctx, vs)
	var vmMo mo.VirtualMachine
	err := vs.Client.RetrieveOne(ctx, vm, []string{"summary.runtime.host"}, &vmMo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	host := *vmMo.Summary.Runtime.Host
	return host
}

// getVMByUUIDWithWait gets the VM object Reference from the given vmUUID with a given wait timeout
func (vs *vSphere) getVMByUUIDWithWait(ctx context.Context, vmUUID string, timeout time.Duration) (object.Reference, error) {
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
func (vs *vSphere) waitForVolumeDetachedFromNode(client clientset.Interface, volumeID string, nodeName string) (bool, error) {
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		var vmUUID string
		if vanillaCluster {
			vmUUID = getNodeUUID(client, nodeName)
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
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to get profileID from given profileName")
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
		return nil, fmt.Errorf("failed to query cns volume %s", volumeID)
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
			k8sEntityName := kubernetesMetadata.EntityName
			if guestCluster {
				k8sEntityName = kubernetesMetadata.CnsEntityMetadata.EntityName
			}
			if kubernetesMetadata.EntityType == entityType && k8sEntityName == entityName && kubernetesMetadata.Namespace == entityNamespace {
				if matchLabels == nil {
					return true, nil
				}
				labelsMatch := reflect.DeepEqual(getLabelsMapFromKeyValue(kubernetesMetadata.Labels), matchLabels)
				if guestCluster {
					labelsMatch = reflect.DeepEqual(getLabelsMapFromKeyValue(kubernetesMetadata.CnsEntityMetadata.Labels), matchLabels)
				}
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
func (vs *vSphere) createFCD(ctx context.Context, fcdname string, diskCapacityInMB int64, dsRef vim25types.ManagedObjectReference) (string, error) {
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
func (vs *vSphere) createFCDwithValidProfileID(ctx context.Context, fcdname string, profileID string, diskCapacityInMB int64, dsRef vim25types.ManagedObjectReference) (string, error) {
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
func (vs *vSphere) relocateFCD(ctx context.Context, fcdID string, dsRefSrc vim25types.ManagedObjectReference, dsRefDest vim25types.ManagedObjectReference) error {
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

// verifyVolPropertiesFromCnsQueryResults verify file volume properties like capacity, volume type, datastore type and datacenter
func verifyVolPropertiesFromCnsQueryResults(e2eVSphere vSphere, volHandle string) {

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s accesspoint: %s", queryResult.Volumes[0].Name,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
		queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints))

	//Verifying disk size specified in PVC is honored
	ginkgo.By("Verifying disk size specified in PVC is honored")
	gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb == diskSizeInMb).
		To(gomega.BeTrue(), "wrong disk size provisioned")

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

//getClusterName methods returns the cluster and vsan client of the testbed
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

//getHostUUID takes input of the HostInfo which has host uuid
//with the host uuid it maps the corresponding host IP and returns it
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

//VsanQueryObjectIdentities return list of vsan uuids
//example: For a PVC, It returns the vSAN object UUIDs to their identities
//It return vsanObjuuid like [4336525f-7813-d78a-e3a4-02005456da7e]
func (c *VsanClient) VsanQueryObjectIdentities(ctx context.Context, cluster vim25types.ManagedObjectReference) (*vsantypes.VsanObjectIdentityAndHealth, error) {
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

//QueryVsanObjects takes vsan uuid as input and returns the vSANObj related information like lsom_objects and disk_objects
//example return values: "{"disk_objects": {"525a9aa5-1142-4004-ad6f-2389eef25f06": ....lsom_objects": {"e7945f5f-4267-3e5d-334a-020063a7a5c4":......}
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

//queryCNSVolumeWithWait gets the cns volume health status
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
