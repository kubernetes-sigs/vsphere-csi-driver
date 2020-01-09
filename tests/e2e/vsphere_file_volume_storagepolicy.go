/*
Copyright 2019 The Kubernetes Authors.

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

package e2e

import (
	"fmt"
	"strings"

	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("[csi-file-vanilla] File Volume Provision Testing With Storage Policy", func() {
	f := framework.NewDefaultFramework("file-volume-basic")
	var (
		client    clientset.Interface
		namespace string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	/*
		Verify dynamic volume provisioning works with storage policy having compliant VSAN datastores only
		1. Create a storage policy having compliant VSAN datastores with VSANFS enabled from list of datacenters where K8s nodes are deployed on
		2. Create StorageClass with fsType as "nfs4" and storagePolicy created in step1
		3. Create a PVC with "ReadWriteMany" using the SC from above
		4. Wait for PVC to be Bound
		5. Get the VolumeID from PV
		6. Verify using CNS Query API if VolumeID retrieved from PV is present.
		   Also verify
		   Name, Capacity, VolumeType, Health, Policy matches
		   Verify if VolumeID is created on one of the VSAN datastores (compliant with storage policy) from list of datacenters provided in vsphere.conf
		   Also verify if VolumeID is created with expected storage policy
		7. Delete PVC
		8. Delete storage policy
	*/
	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode, when storage policy is offered", func() {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		testHelperForCreateFileVolumeWithNoDatastoreURLInSCWithStoragePolicy(f, client, namespace, v1.ReadWriteMany, storagePolicyNameForSharedDatastores, true)
	})

	/*
		Verify dynamic volume provisioning works with storage policy having compliant VSAN datastores only
		1. Create a storage policy having compliant VSAN datastores with VSANFS enabled from list of datacenters where K8s nodes are deployed on
		2. Create StorageClass with fsType as "nfs4" and storagePolicy created in step1
		3. Create a PVC with "ReadWriteMany" using the SC from above
		4. Wait for PVC to be Bound
		5. Get the VolumeID from PV
		6. Verify using CNS Query API if VolumeID retrieved from PV is present.
		   Also verify
		   Name, Capacity, VolumeType, Health, Policy matches
		   Also verify if VolumeID is created with expected storage policy
		7. Delete PVC
		8. Delete storage policy
	*/

	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode, when storage policy is offered and datacenters is not specified in conf file", func() {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		testHelperForCreateFileVolumeWithNoDatastoreURLInSCWithStoragePolicy(f, client, namespace, v1.ReadWriteMany, storagePolicyNameForSharedDatastores, false)
	})

	/*
		Verify dynamic volume provisioning works with storage policy having compliant VSAN datastores only
		1. Create a storage policy having compliant VSAN datastores with VSANFS enabled from list of datacenters where K8s nodes are deployed on
		2. Create StorageClass with fsType as "nfs4" and storagePolicy created in step1
		3. Create a PVC with "ReadOnlyMany" using the SC from above
		4. Wait for PVC to be Bound
		5. Get the VolumeID from PV
		6. Verify using CNS Query API if VolumeID retrieved from PV is present.
		   Also verify
		   Name, Capacity, VolumeType, Health, Policy matches
		   Verify if VolumeID is created on one of the VSAN datastores (compliant with storage policy) from list of datacenters provided in vsphere.conf
		   Also verify if VolumeID is created with expected storage policy
		7. Delete PVC
		8. Delete storage policy
	*/

	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadOnlyMany access mode, when storage policy is offered", func() {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		testHelperForCreateFileVolumeWithNoDatastoreURLInSCWithStoragePolicy(f, client, namespace, v1.ReadOnlyMany, storagePolicyNameForSharedDatastores, true)
	})
})

func testHelperForCreateFileVolumeWithNoDatastoreURLInSCWithStoragePolicy(f *framework.Framework, client clientset.Interface, namespace string, accessMode v1.PersistentVolumeAccessMode, storagePolicyName string, checkDatastoreBelongToDatacenter bool) {
	ginkgo.By(fmt.Sprintf("Invoking Test for accessMode: %s storagePolicy %s", accessMode, storagePolicyName))
	scParameters := make(map[string]string)
	scParameters[scParamFsType] = nfs4FSType
	if storagePolicyName != "" {
		scParameters[scParamStoragePolicyName] = storagePolicyName
	}
	// Create Storage class and PVC
	ginkgo.By(fmt.Sprintf("Creating Storage Class with %q", nfs4FSType))
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		err := client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	defer func() {
		err := framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := framework.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
	defer func() {
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

	ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s storagePolicy:%s", queryResult.Volumes[0].Name, queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb, queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus, queryResult.Volumes[0].StoragePolicyId))

	ginkgo.By("Verifying disk size specified in PVC is honored")
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb != diskSizeInMb {
		err = fmt.Errorf("wrong disk size provisioned")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Verifying volume type specified in PVC is honored")
	if queryResult.Volumes[0].VolumeType != testVolumeType {
		err = fmt.Errorf("volume type is not %q", testVolumeType)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// TODO: Verify HealthStatus is shown "Healthy", currently, only after 1 hour, the health status of newly created
	// volume will change from "Unknown" to "Healthy"

	// Verify if VolumeID is created on the VSAN datastores
	gomega.Expect(strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:")).To(gomega.BeTrue(), "Volume is not provisioned on vSan datastore. Instead volume is provisioned on %q", queryResult.Volumes[0].DatastoreUrl)

	if checkDatastoreBelongToDatacenter {
		// Verify if VolumeID is created on the datastore from list of datacenters provided in vsphere.conf
		gomega.Expect(isDatastoreBelongsToDatacenterSpecifiedInConfig(queryResult.Volumes[0].DatastoreUrl)).To(gomega.BeTrue(), "Volume is not provisioned on the datastore specified on config file. Instead the volume is provisioned on : %q", queryResult.Volumes[0].DatastoreUrl)
	}

	// Verify the volume is provisioned using specified storage policy
	storagePolicyID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
	gomega.Expect(storagePolicyID == queryResult.Volumes[0].StoragePolicyId).To(gomega.BeTrue(), fmt.Sprintf("Storage policy verification failed. Actual storage policy: %q does not match with the Expected storage policy: %q", queryResult.Volumes[0].StoragePolicyId, storagePolicyID))

}
