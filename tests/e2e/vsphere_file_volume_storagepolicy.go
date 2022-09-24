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
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnstypes "github.com/vmware/govmomi/cns/types"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("[csi-file-vanilla] File Volume Provision Testing With Storage Policy", func() {
	f := framework.NewDefaultFramework("file-volume-basic")
	var (
		client       clientset.Interface
		namespace    string
		targetDsURLs []string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		targetDsURLs = getTargetvSANFileShareDatastoreURLsFromConfig()
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	// Verify dynamic volume provisioning works with storage policy having
	// compliant VSAN datastores only.
	// 1. Create a storage policy having compliant VSAN datastores with VSANFS
	//    enabled from list of datacenters where K8s nodes are deployed on.
	// 2. Create StorageClass with fsType as "nfs4" and storagePolicy created
	//    in step1.
	// 3. Create a PVC with "ReadWriteMany" using the SC from above.
	// 4. Wait for PVC to be Bound.
	// 5. Get the VolumeID from PV.
	// 6. Verify using CNS Query API if VolumeID retrieved from PV is present.
	//    Also verify if Name, Capacity, VolumeType, Health, Policy matches.
	//    Verify if VolumeID is created on one of the VSAN datastores (compliant
	//    with storage policy) from list of datacenters provided in vsphere.conf.
	//    Also verify if VolumeID is created with expected storage policy.
	// 7. Delete PVC.
	// 8. Delete storage policy.
	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode, "+
		"when storage policy is offered", func() {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		testHelperForCreateFileVolumeWithNoDatastoreURLInSCWithStoragePolicy(f, client,
			namespace, v1.ReadWriteMany, storagePolicyNameForSharedDatastores, true)
	})

	// Verify dynamic volume provisioning works with storage policy having
	// compliant VSAN datastores only.
	// 1. Create a storage policy having compliant VSAN datastores with VSANFS
	//    enabled from list of datacenters where K8s nodes are deployed on.
	// 2. Create StorageClass with fsType as "nfs4" and storagePolicy created
	//    in step1
	// 3. Create a PVC with "ReadWriteMany" using the SC from above.
	// 4. Wait for PVC to be Bound.
	// 5. Get the VolumeID from PV.
	// 6. Verify using CNS Query API if VolumeID retrieved from PV is present.
	//    Also verify if Name, Capacity, VolumeType, Health, Policy matches.
	//    Also verify if VolumeID is created with expected storage policy.
	// 7. Delete PVC.
	// 8. Delete storage policy.

	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode, "+
		"when storage policy is offered and datacenters is not specified in conf file", func() {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		testHelperForCreateFileVolumeWithNoDatastoreURLInSCWithStoragePolicy(f, client,
			namespace, v1.ReadWriteMany, storagePolicyNameForSharedDatastores, false)
	})

	// Verify dynamic volume provisioning works with storage policy having
	// compliant VSAN datastores only.
	// 1. Create a storage policy having compliant VSAN datastores with VSANFS
	//    enabled from list of datacenters where K8s nodes are deployed on.
	// 2. Create StorageClass with fsType as "nfs4" and storagePolicy created
	//    in step1.
	// 3. Create a PVC with "ReadOnlyMany" using the SC from above.
	// 4. Wait for PVC to be Bound.
	// 5. Get the VolumeID from PV.
	// 6. Verify using CNS Query API if VolumeID retrieved from PV is present.
	//    Also verify if Name, Capacity, VolumeType, Health, Policy matches.
	//    Verify if VolumeID is created on one of the VSAN datastores (compliant
	//    with storage policy) from list of datacenters provided in vsphere.conf.
	//    Also verify if VolumeID is created with expected storage policy.
	// 7. Delete PVC.
	// 8. Delete storage policy.

	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadOnlyMany access mode, "+
		"when storage policy is offered", func() {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		testHelperForCreateFileVolumeWithNoDatastoreURLInSCWithStoragePolicy(f, client,
			namespace, v1.ReadOnlyMany, storagePolicyNameForSharedDatastores, true)
	})

	// Verify dynamic volume provisioning works with storage policy having
	// compliant VSAN datastores only.
	// 1. Create a storage policy using targetvSANFileShareDatastoreURLs as
	//    the compliant datastores.
	// 2. Create StorageClass with fsType as "nfs4" and storagePolicy created
	//    in step1.
	// 3. Create a PVC with "ReadWriteMany" using the SC from above.
	// 4. Wait for PVC to be Bound.
	// 5. Get the VolumeID from PV.
	// 6. Verify using CNS Query API if VolumeID retrieved from PV is present.
	//    Verify if Name, Capacity, VolumeType, Health, Policy matches.
	//    Verify if VolumeID is created on one of the VSAN datastores from list
	//    of targetvSANFileShareDatastoreURLs provided in vsphere.conf.
	//    Also verify if VolumeID is created with expected storage policy.
	// 7. Delete PVC.
	// 8. Delete storage policy.

	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode "+
		"when storage policy is offered with TargetvSANFileShareDatastoreURLs as the compliant datastores", func() {
		// Verify if test is valid for the given environment
		if len(targetDsURLs) == 0 {
			ginkgo.Skip("TargetvSANFileShareDatastoreURLs is not set in e2eTest.conf, skipping the test")
		}
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		createFileVolumeWithStoragePolicyAndTargetvSANFileShareDatastoreURLs(f, client,
			namespace, v1.ReadWriteMany, storagePolicyNameForSharedDatastores, "")
	})

	// Verify dynamic volume provisioning works with storage policy having
	// compliant VSAN datastores only.
	// 1. Create a storage policy using targetvSANFileShareDatastoreURLs as
	//    the compliant datastores.
	// 2. Create StorageClass with fsType as "nfs4" and storagePolicy created
	//    in step1 and datastoreURL set to anyone from
	//    targetvSANFileShareDatastoreURLs.
	// 3. Create a PVC with "ReadWriteMany" using the SC from above.
	// 4. Wait for PVC to be Bound.
	// 5. Get the VolumeID from PV.
	// 6. Verify using CNS Query API if VolumeID retrieved from PV is present.
	//    Verify if Name, Capacity, VolumeType, Health, Policy matches.
	//    Verify if VolumeID is created on one of the VSAN datastores from list
	//    of targetvSANFileShareDatastoreURLs provided in vsphere.conf.
	//    Verify if VolumeID is created on the datastoreURL mentioned in Storage
	//    class. Also verify if VolumeID is created with expected storage policy.
	// 7. Delete PVC.
	// 8. Delete storage policy.

	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode "+
		"when datastoreURL is mentioned in storage class and storage policy has the vSAN compliant datastores", func() {
		// Verify if test is valid for the given environment
		if len(targetDsURLs) == 0 {
			ginkgo.Skip("TargetvSANFileShareDatastoreURLs is not set in e2eTest.conf, skipping the test")
		}
		sharedDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		createFileVolumeWithStoragePolicyAndTargetvSANFileShareDatastoreURLs(f, client,
			namespace, v1.ReadWriteMany, storagePolicyNameForSharedDatastores, sharedDatastoreURL)
	})

	/*
		Verify dynamic volume provisioning fails with storage policy having non-VSAN compliant datastores
		1. Create a storage policy using non-vSAN compliant datastores
		2. Create StorageClass with fsType as "nfs4" and storagePolicy created in step1
		3. Create a PVC with "ReadWriteMany" using the SC from above
		4. Expect the PVC to fail.
		5. Verify the error message returned on PVC failure is correct.
		6. Delete PVC
		7. Delete Storage class
	*/

	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode "+
		"when storage policy has non-vSAN compliant datastores", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Verify if test is valid for the given environment
		if len(targetDsURLs) == 0 {
			ginkgo.Skip("TargetvSANFileShareDatastoreURLs is not set in e2eTest.conf, skipping the test")
		}
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		accessMode := v1.ReadWriteMany
		storagePolicyNameForNonSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForNonSharedDatastores)
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType
		scParameters[scParamStoragePolicyName] = storagePolicyNameForNonSharedDatastores

		// Create Storage class and PVC
		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q, storage policy %q and fstype %q",
			accessMode, storagePolicyNameForNonSharedDatastores, nfs4FSType))
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", false, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as the storage policy mentioned in Storage class " +
			"has non-vSAN compliant datastores")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "Failed to create volume."
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})
})

func testHelperForCreateFileVolumeWithNoDatastoreURLInSCWithStoragePolicy(f *framework.Framework,
	client clientset.Interface, namespace string, accessMode v1.PersistentVolumeAccessMode,
	storagePolicyName string, checkDatastoreBelongToDatacenter bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	storageclass, pvclaim, err = createPVCAndStorageClass(client,
		namespace, nil, scParameters, "", nil, "", false, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

	ginkgo.By(fmt.Sprintf("volume Name: %q capacity: %d volumeType: %q health: %q storagePolicy: %q",
		queryResult.Volumes[0].Name,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
		queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus, queryResult.Volumes[0].StoragePolicyId))

	ginkgo.By("Verifying disk size specified in PVC is honored")
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb !=
		diskSizeInMb {
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
	gomega.Expect(strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:")).To(gomega.BeTrue(),
		"Volume is not provisioned on vSan datastore. Instead volume is provisioned on %q",
		queryResult.Volumes[0].DatastoreUrl)

	if checkDatastoreBelongToDatacenter {
		// Verify if VolumeID is created on the datastore from list of datacenters provided in vsphere.conf
		gomega.Expect(isDatastoreBelongsToDatacenterSpecifiedInConfig(queryResult.Volumes[0].DatastoreUrl)).To(
			gomega.BeTrue(), "Volume is not provisioned on the datastore specified on config file. "+
				"Instead the volume is provisioned on : %q", queryResult.Volumes[0].DatastoreUrl)
	}

	// Verify the volume is provisioned using specified storage policy
	storagePolicyID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
	gomega.Expect(storagePolicyID == queryResult.Volumes[0].StoragePolicyId).To(gomega.BeTrue(),
		fmt.Sprintf("Storage policy verification failed. Actual storage policy: %q does not match "+
			"with the Expected storage policy: %q", queryResult.Volumes[0].StoragePolicyId, storagePolicyID))

}

func createFileVolumeWithStoragePolicyAndTargetvSANFileShareDatastoreURLs(f *framework.Framework,
	client clientset.Interface, namespace string, accessMode v1.PersistentVolumeAccessMode,
	storagePolicyName string, datastoreURL string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	scParameters := make(map[string]string)
	scParameters[scParamFsType] = nfs4FSType
	if storagePolicyName != "" {
		scParameters[scParamStoragePolicyName] = storagePolicyName
	}
	if datastoreURL != "" {
		scParameters[scParamDatastoreURL] = datastoreURL
	}
	// Create Storage class and PVC
	ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q, storage policy %q and fstype %q",
		accessMode, storagePolicyName, nfs4FSType))
	storageclass, pvclaim, err = createPVCAndStorageClass(client,
		namespace, nil, scParameters, "", nil, "", false, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

	targetQueryVolume := queryResult.Volumes[0]
	ginkgo.By(fmt.Sprintf("volume Name: %q capacity: %d volumeType: %q health: %q storagePolicy: %q",
		targetQueryVolume.Name,
		targetQueryVolume.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
		targetQueryVolume.VolumeType, targetQueryVolume.HealthStatus, targetQueryVolume.StoragePolicyId))

	ginkgo.By("Verifying disk size specified in PVC is honored")
	if targetQueryVolume.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb != diskSizeInMb {
		err = fmt.Errorf("wrong disk size provisioned")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Verifying volume type specified in PVC is honored")
	if targetQueryVolume.VolumeType != testVolumeType {
		err = fmt.Errorf("volume type is not %q", testVolumeType)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// TODO: Verify HealthStatus is shown "Healthy", currently, only after 1 hour, the health status of newly created
	// volume will change from "Unknown" to "Healthy"

	// Verify if VolumeID is created on VSAN datastores
	gomega.Expect(strings.HasPrefix(targetQueryVolume.DatastoreUrl, "ds:///vmfs/volumes/vsan:")).To(gomega.BeTrue(),
		"Volume is not provisioned on vSan datastore. Instead volume is provisioned on %q",
		queryResult.Volumes[0].DatastoreUrl)

	if datastoreURL != "" {
		// Verify if VolumeID is created in the datastore mentioned in the Storage class params
		if targetQueryVolume.DatastoreUrl != datastoreURL {
			err = fmt.Errorf("volume provisioned on datastoreURL %q which is not the datatore "+
				"mentioned in the storage class", targetQueryVolume.DatastoreUrl)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}

	// Verify if VolumeID is created in one of the datastores listed in
	// TargetvSANFileShareDatastoreURLs provided in vsphere.conf.
	errorMsg := fmt.Sprintf("Volume is provisioned on %q which does not match any of the datastores "+
		"specified in TargetvSANFileShareDatastoreURLs in the vSphere config file",
		targetQueryVolume.DatastoreUrl)
	gomega.Expect(isDatastorePresentinTargetvSANFileShareDatastoreURLs(targetQueryVolume.DatastoreUrl)).To(
		gomega.BeTrue(), errorMsg)

	// Verify the volume is provisioned using specified storage policy
	storagePolicyID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
	gomega.Expect(storagePolicyID == queryResult.Volumes[0].StoragePolicyId).To(gomega.BeTrue(),
		fmt.Sprintf("Storage policy verification failed. Actual storage policy: %q does not match "+
			"with the Expected storage policy: %q", queryResult.Volumes[0].StoragePolicyId, storagePolicyID))
}
