package e2e

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/onsi/gomega"
	"github.com/vmware/govmomi"
	cnsmethods "github.com/vmware/govmomi/cns/methods"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/soap"

	vim25types "github.com/vmware/govmomi/vim25/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

type multiVCvSphere struct {
	multivcConfig    *e2eTestConfig
	multiVcClient    []*govmomi.Client
	multiVcCnsClient []*cnsClient
}

type multiVcVsanClient struct {
	multiVCvim25Client   []*vim25.Client
	multiVCserviceClient []*soap.Client
}

/*
queryCNSVolumeWithResultForMultiVC Call CnsQueryVolume and returns CnsQueryResult to client for
a multiVC setup
*/
func (vs *multiVCvSphere) queryCNSVolumeWithResultInMultiVC(fcdID string) (*cnstypes.CnsQueryResult, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var res *cnstypes.CnsQueryVolumeResponse
	// Connect to VC
	connectMultiVC(ctx, vs)
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

	err := connectMultiVcCns(ctx, vs)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(vs.multiVcCnsClient); i++ {
		res, err = cnsmethods.CnsQueryVolume(ctx, vs.multiVcCnsClient[i].Client, &req)
		if res.Returnval.Volumes == nil {
			continue
		}

		if res.Returnval.Volumes != nil && err == nil {
			return &res.Returnval, nil
		}

		if err != nil {
			return nil, err
		}
	}
	return &res.Returnval, nil
}

/* getAllDatacentersForMultiVC returns all the DataCenter Objects for a multivc environment */
func (vs *multiVCvSphere) getAllDatacentersForMultiVC(ctx context.Context) ([]*object.Datacenter, error) {
	connectMultiVC(ctx, vs)
	var finder *find.Finder
	for i := 0; i < len(vs.multiVcClient); i++ {
		soapClient := vs.multiVcClient[i].Client.Client
		if soapClient == nil {
			return nil, fmt.Errorf("soapClient is nil")
		}
		vimClient, err := convertToVimClient(ctx, soapClient)
		if err != nil {
			return nil, err
		}
		finder = find.NewFinder(vimClient, false)
	}
	return finder.DatacenterList(ctx, "*")
}

/*
convertToVimClient converts soap client to vim client
*/
func convertToVimClient(ctx context.Context, soapClient *soap.Client) (*vim25.Client, error) {
	// Create a new *vim25.Client using the *soap.Client, context, and a RoundTripper
	vimClient, err := vim25.NewClient(ctx, soapClient)
	if err != nil {
		return nil, err
	}

	return vimClient, nil
}

/*
getVMByUUIDForMultiVC gets the VM object Reference from the given vmUUID
for a multivc environment
*/
func (vs *multiVCvSphere) getVMByUUIDForMultiVC(ctx context.Context, vmUUID string) (object.Reference, error) {
	connectMultiVC(ctx, vs)
	dcList, err := vs.getAllDatacentersForMultiVC(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, dc := range dcList {
		for i := 0; i < len(vs.multiVcClient); i++ {
			soapClient := vs.multiVcClient[i].Client.Client
			vimClient, err := vim25.NewClient(ctx, soapClient)
			if err != nil {
				continue
			}
			datacenter := object.NewDatacenter(vimClient, dc.Reference())
			s := object.NewSearchIndex(vimClient)
			vmUUID = strings.ToLower(strings.TrimSpace(vmUUID))
			instanceUUID := !(vanillaCluster || guestCluster)
			vmMoRef, err := s.FindByUuid(ctx, datacenter, vmUUID, true, &instanceUUID)

			if err != nil || vmMoRef == nil {
				continue
			}
			return vmMoRef, nil
		}
	}
	framework.Logf("err in getVMByUUID is %+v for vmuuid: %s", err, vmUUID)
	return nil, fmt.Errorf("node VM with UUID:%s is not found", vmUUID)
}

/*
getVMByUUIDWithWaitForMultiVC gets the VM object Reference from the given vmUUID with a given
wait timeout on a multivc environment
*/
func (vs *multiVCvSphere) getVMByUUIDWithWaitForMultiVC(ctx context.Context,
	vmUUID string, timeout time.Duration) (object.Reference, error) {
	connectMultiVC(ctx, vs)
	dcList, err := vs.getAllDatacentersForMultiVC(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var vmMoRefForvmUUID object.Reference
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		var vmMoRefFound bool
		for _, dc := range dcList {
			for i := 0; i < len(vs.multiVcClient); i++ {
				soapClient := vs.multiVcClient[i].Client.Client
				vimClient, err := vim25.NewClient(ctx, soapClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				datacenter := object.NewDatacenter(vimClient, dc.Reference())
				s := object.NewSearchIndex(vimClient)
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

/*
verifyVolumeIsAttachedToVMInMultiVC checks volume is attached to the VM by vmUUID on a
multivc environment. This function returns true if volume is attached to the VM, else returns
false
*/
// func (vs *multiVCvSphere) verifyVolumeIsAttachedToVMInMultiVC(client clientset.Interface, volumeID string, vmUUID string) (bool, error) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
// 	vmRef, err := vs.getVMByUUIDForMultiVC(ctx, vmUUID)
// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
// 	framework.Logf("vmRef: %v for the VM uuid: %s", vmRef, vmUUID)
// 	gomega.Expect(vmRef).NotTo(gomega.BeNil(), "vmRef should not be nil")

// 	for i := 0; i < len(vs.multiVcClient); i++ {
// 		soapClient := vs.multiVcClient[i].Client.Client
// 		vimClient, err := vim25.NewClient(ctx, soapClient)
// 		if err != nil {
// 			return false, err
// 		}
// 		vm := object.NewVirtualMachine(vimClient, vmRef.Reference())
// 		device, err := getVirtualDeviceByDiskID(ctx, vm, volumeID)
// 		if err == nil && device == nil {
// 			continue
// 		}
// 		if device != nil && err != nil {
// 			framework.Logf("failed to determine whether disk %q is still attached to the VM with UUID: %q", volumeID, vmUUID)
// 			continue
// 		}
// 		if device != nil && err == nil {
// 			framework.Logf("Found the disk %q is attached to the VM with UUID: %q", volumeID, vmUUID)
// 			return true, nil
// 		}

// 		if device == nil && err != nil {
// 			continue
// 		}
// 	}

// 	return false, nil
// }

func (vs *multiVCvSphere) verifyVolumeIsAttachedToVMInMultiVC(client clientset.Interface, volumeID string, vmUUID string) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vmRef, err := vs.getVMByUUIDForMultiVC(ctx, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("vmRef: %v for the VM uuid: %s", vmRef, vmUUID)
	gomega.Expect(vmRef).NotTo(gomega.BeNil(), "vmRef should not be nil")

	for i := 0; i < len(vs.multiVcClient); i++ {
		soapClient := vs.multiVcClient[i].Client.Client
		vimClient, err := vim25.NewClient(ctx, soapClient)
		if err != nil {
			return false, err
		}

		// Retrieve all virtual machines from the current client
		vms, err := getAllVMsInClient(ctx, vimClient)
		if err != nil {
			return false, err
		}

		// Loop through all the VMs in the current client
		for _, vm := range vms {
			device, err := getVirtualDeviceByDiskID(ctx, vm, volumeID)
			if err == nil && device == nil {
				continue
			}
			if device != nil && err != nil {
				framework.Logf("failed to determine whether disk %q is still attached to the VM with UUID: %q", volumeID, vmUUID)
				continue
			}
			if device != nil && err == nil {
				framework.Logf("Found the disk %q is attached to the VM with UUID: %q", volumeID, vmUUID)
				return true, nil
			}

			if device == nil && err != nil {
				continue
			}
		}
	}

	return false, nil
}

// Helper function to retrieve all virtual machines in a client
func getAllVMsInClient(ctx context.Context, vimClient *vim25.Client) ([]*object.VirtualMachine, error) {
	// Create a new finder
	finder := find.NewFinder(vimClient)

	// Find all virtual machines
	vms, err := finder.VirtualMachineList(ctx, "*")
	if err != nil {
		return nil, err
	}

	return vms, nil
}

/*
waitForVolumeDetachedFromNodeInMultiVC checks volume is detached from the node on a multivc environment.
This function checks disks status every 3 seconds until detachTimeout, which is set to 360 seconds
*/
func (vs *multiVCvSphere) waitForVolumeDetachedFromNodeInMultiVC(client clientset.Interface,
	volumeID string, nodeName string) (bool, error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if supervisorCluster {
		_, err := multiVCe2eVSphere.getVMByUUIDWithWaitForMultiVC(ctx, nodeName, supervisorClusterOperationsTimeout)
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
		diskAttached, err := vs.verifyVolumeIsAttachedToVMInMultiVC(client, volumeID, vmUUID)
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

// waitForCNSVolumeToBeDeleted executes QueryVolume API on vCenter and verifies
// volume entries are deleted from vCenter Database
func (vs *multiVCvSphere) waitForCNSVolumeToBeDeletedInMultiVC(volumeID string) error {
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		queryResult, err := vs.queryCNSVolumeWithResultInMultiVC(volumeID)
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
func (vs *multiVCvSphere) waitForCNSVolumeToBeCreatedInMultiVC(volumeID string) error {
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		queryResult, err := vs.queryCNSVolumeWithResultInMultiVC(volumeID)
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

// createFCDForMultiVC creates an FCD disk on a multiVC setup
func (vs *multiVCvSphere) createFCDForMultiVC(ctx context.Context, fcdname string,
	diskCapacityInMB int64, dsRef vim25types.ManagedObjectReference, clientIndex int) (string, error) {
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
		This: *vs.multiVcClient[clientIndex].ServiceContent.VStorageObjectManager,
		Spec: spec,
	}
	res, err := methods.CreateDisk_Task(ctx, vs.multiVcClient[clientIndex].Client, &req)
	if err != nil {
		return "", err
	}
	task := object.NewTask(vs.multiVcClient[clientIndex].Client, res.Returnval)
	taskInfo, err := task.WaitForResult(ctx, nil)
	if err != nil {
		return "", err
	}
	fcdID := taskInfo.Result.(vim25types.VStorageObject).Config.Id.Id
	return fcdID, nil
}

// deleteFCD deletes an FCD disk
func (vs *multiVCvSphere) deleteFCDFromMultiVC(ctx context.Context, fcdID string,
	dsRef vim25types.ManagedObjectReference, clientIndex int) error {
	req := vim25types.DeleteVStorageObject_Task{
		This:      *vs.multiVcClient[clientIndex].ServiceContent.VStorageObjectManager,
		Datastore: dsRef,
		Id:        vim25types.ID{Id: fcdID},
	}
	res, err := methods.DeleteVStorageObject_Task(ctx, vs.multiVcClient[clientIndex].Client, &req)
	if err != nil {
		return err
	}
	task := object.NewTask(vs.multiVcClient[clientIndex].Client, res.Returnval)
	_, err = task.WaitForResult(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

// // getClusterName methods returns the cluster and vsan client of the testbed
// func getClusterNamesForMultiVC(ctx context.Context, vs *multiVCvSphere) ([]*object.ClusterComputeResource, *multiVcVsanClient, error) {
// 	c := newClientForMultiVC(ctx, vs)
// 	datacenter := vs.multivcConfig.Global.Datacenters

// 	vsanHealthClient, err := newVsanHealthSvcClientForMultiVC(ctx, c)
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	finder := find.NewFinder(vsanHealthClient.multiVCvim25Client[0], false)
// 	dc, err := finder.Datacenter(ctx, datacenter)
// 	if err != nil {
// 		return nil, nil, err
// 	}
// 	finder.SetDatacenter(dc)

// 	clusterComputeResource, err := finder.ClusterComputeResourceList(ctx, "*")
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	return clusterComputeResource, vsanHealthClient, nil
// }
