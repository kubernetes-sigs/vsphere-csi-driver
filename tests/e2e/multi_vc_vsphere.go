package e2e

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi"
	cnsmethods "github.com/vmware/govmomi/cns/methods"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/pbm"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"

	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

// multiVCvSphere struct accepts multiple VC and CNS client details
type multiVCvSphere struct {
	multivcConfig    *e2eTestConfig
	multiVcClient    []*govmomi.Client
	multiVcCnsClient []*cnsClient
}

/*
queryCNSVolumeWithResultInMultiVC Call CnsQueryVolume and returns CnsQueryResult to client in
a multiVC setup
*/
func (vs *multiVCvSphere) queryCNSVolumeWithResultInMultiVC(fcdID string) (*cnstypes.CnsQueryResult, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var res *cnstypes.CnsQueryVolumeResponse
	// Connects to multiple VCs
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
		Filter: &queryFilter,
	}
	// Connects to multiple CNS clients
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

/* getAllDatacentersForMultiVC returns all the DataCenter Objects in a multivc environment */
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
getVMByUUIDForMultiVC gets the VM object Reference from the given vmUUID in a multivc environment
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
verifyVolumeIsAttachedToVMInMultiVC checks volume is attached to the VM by vmUUID.
This function returns true if volume is attached to the VM, else returns false
*/
func (vs *multiVCvSphere) verifyVolumeIsAttachedToVMInMultiVC(volumeID string,
	vmUUID string) (bool, error) {
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
	err := wait.PollUntilContextTimeout(ctx, poll, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			var vmUUID string
			if vanillaCluster {
				vmUUID = getNodeUUID(ctx, client, nodeName)
			} else {
				vmUUID, _ = getVMUUIDFromNodeName(nodeName)
			}
			diskAttached, err := vs.verifyVolumeIsAttachedToVMInMultiVC(volumeID, vmUUID)
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

/*
waitForCNSVolumeToBeDeletedInMultiVC executes QueryVolume API on vCenter and verifies
volume entries are deleted from vCenter Database
*/
func (vs *multiVCvSphere) waitForCNSVolumeToBeDeletedInMultiVC(volumeID string) error {
	err := wait.PollUntilContextTimeout(context.Background(), poll, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
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

/*
waitForLabelsToBeUpdatedInMultiVC executes QueryVolume API on vCenter and verifies
volume labels are updated by metadata-syncer
*/
func (vs *multiVCvSphere) waitForLabelsToBeUpdatedInMultiVC(volumeID string, matchLabels map[string]string,
	entityType string, entityName string, entityNamespace string) error {
	err := wait.PollUntilContextTimeout(context.Background(), poll, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			err := vs.verifyLabelsAreUpdatedInMultiVC(volumeID, matchLabels, entityType, entityName, entityNamespace)
			if err == nil {
				return true, nil
			} else {
				return false, nil
			}
		})
	if err != nil {
		if wait.Interrupted(err) {
			return fmt.Errorf("labels are not updated to %+v for %s %q for volume %s",
				matchLabels, entityType, entityName, volumeID)
		}
		return err
	}

	return nil
}

/*
verifyLabelsAreUpdatedInMultiVC executes cns QueryVolume API on vCenter and verifies if
volume labels are updated by metadata-syncer
*/
func (vs *multiVCvSphere) verifyLabelsAreUpdatedInMultiVC(volumeID string, matchLabels map[string]string,
	entityType string, entityName string, entityNamespace string) error {

	queryResult, err := vs.queryCNSVolumeWithResultInMultiVC(volumeID)
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

/*
waitForCNSVolumeToBeCreatedInMultiVC executes QueryVolume API on vCenter and verifies
volume entries are created in a multi vCenter database
*/
func (vs *multiVCvSphere) waitForCNSVolumeToBeCreatedInMultiVC(volumeID string) error {
	err := wait.PollUntilContextTimeout(context.Background(), poll, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
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

// GetSpbmPolicyID returns profile ID for the specified storagePolicyName
func (vs *multiVCvSphere) GetSpbmPolicyIDInMultiVc(storagePolicyName string) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var policyIDStrings []string

	for _, govmomiClient := range vs.multiVcClient {
		vimClient := govmomiClient.Client

		pbmClient, err := pbm.NewClient(ctx, vimClient)
		if err != nil {
			framework.Logf("Error creating PBM client: %v", err)
			continue
		}

		profileID, err := pbmClient.ProfileIDByName(ctx, storagePolicyName)
		if err != nil {
			framework.Logf("Error getting profile ID: %v", err)
			continue
		}

		framework.Logf("storage policy id: %s for storage policy name is: %s", profileID, storagePolicyName)
		policyIDStrings = append(policyIDStrings, profileID)
	}

	// Join policy ID strings with a comma separator
	return strings.Join(policyIDStrings, ",")
}
