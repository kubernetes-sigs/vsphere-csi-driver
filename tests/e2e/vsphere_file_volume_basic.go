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

	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"gitlab.eng.vmware.com/hatchway/govmomi/find"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	testVolumeType = "FILE"
)

var _ = ginkgo.Describe("[csi-vanilla] [csi-file] Basic Testing", func() {
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
				Test to verify dynamic provisioning with ReadWriteMany access mode, when no storage policy is offered

				1. Create StorageClass with fsType as "nfs4"
		    	2. Create a PVC with "ReadWriteMany" using the SC from above
		    	3. Wait for PVC to be Bound
		    	4. Get the VolumeID from PV
		    	5. Verify using CNS Query API if VolumeID retrieved from PV is present. Also verify
		           Name, Capacity, VolumeType, Health matches
		    	6. Verify if VolumeID is created on one of the VSAN datastores from list of datacenters provided in vsphere.conf
		    	7. Delete PVC
		    	8. Delete Storage class
	*/
	ginkgo.It("[csi-file] verify dynamic provisioning with ReadWriteMany access mode, when no storage policy is offered", func() {
		testHelperForCreateFileVolumeWithNoDatastoreUrlInSC(f, client, namespace, v1.ReadWriteMany)
	})

	/*
			    Test to verify dynamic provisioning with ReadWriteMany access mode with datastoreURL is set in storage class, when no storage policy is offered

				1. Create StorageClass with fsType as "nfs4"
		        2. Create a PVC with "ReadWriteMany" using the SC from above
		        3. Wait for PVC to be Bound
		        4. Get the VolumeID from PV
		        5. Verify using CNS Query API if VolumeID retrieved from PV is present. Also verify Name, Capacity, VolumeType, Health matches
		        6. Verify if VolumeID is created on "datastoreUrl" specified in storage class
		        7. Delete PVC
		        8. Delete Storage class
	*/
	ginkgo.It("[csi-file] verify dynamic provisioning with ReadWriteMany access mode with datastoreURL is set in storage class, when no storage policy is offered", func() {
		datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		testHelperForCreateFileVolumeWithDatastoreUrlInSC(f, client, namespace, v1.ReadWriteMany, datastoreURL)
	})

	/*
			    Test to verify dynamic provisioning with ReadWriteMany access mode with datastoreURL specified in storage class is a non-VSAN datastore
				1. Create StorageClass with fsType as "nfs4" and "datastoreUrl"
		        2. Create a PVC with "ReadWriteMany" using the SC from above
		        3. Expect the PVC to fail
		        4. Verify the error returned on PVC failure is as expected
		        5. Delete PVC
		        6. Delete Storage class
	*/
	ginkgo.It("[csi-file] verify dynamic provisioning with ReadWriteMany access mode with datastoreURL specified in storage class is a non-VSAN datastore, when no storage policy is offered", func() {
		nonVSANDatastoreURL := GetAndExpectStringEnvVar(envNonSharedStorageClassDatastoreURL)
		testHelperForCreateFileVolumeWithoutValidVSANDatastoreUrlInSC(f, client, namespace, v1.ReadWriteMany, nonVSANDatastoreURL)
	})

	/*
			Test to verify dynamic provisioning with ReadOnlyMany access mode, when no storage policy is offered

			1. Create StorageClass with fsType as "nfs4"
		    	2. Create a PVC with "ReadOnlyMany" using the SC from above
		    	3. Wait for PVC to be Bound
		    	4. Get the VolumeID from PV
		    	5. Verify using CNS Query API if VolumeID retrieved from PV is present. Also verify
		           Name, Capacity, VolumeType, Health matches
		    	6. Verify if VolumeID is created on one of the VSAN datastores from list of datacenters provided in vsphere.conf
		    	7. Delete PVC
		    	8. Delete Storage class
	*/
	ginkgo.It("[csi-file] verify dynamic provisioning with ReadOnlyMany access mode, when no storage policy is offered", func() {
		testHelperForCreateFileVolumeWithNoDatastoreUrlInSC(f, client, namespace, v1.ReadOnlyMany)
	})
})

func testHelperForCreateFileVolumeWithNoDatastoreUrlInSC(f *framework.Framework, client clientset.Interface, namespace string, accessMode v1.PersistentVolumeAccessMode) {
	ginkgo.By(fmt.Sprintf("Invoking Test for accessMode: %s", accessMode))
	scParameters := make(map[string]string)
	scParameters[scParamsFsType] = nfs4FSType
	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class With nfs4")
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
		err := framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

	ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s", queryResult.Volumes[0].Name, queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb, queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus))

	ginkgo.By("Verifying disk size specified in PVC is honored")
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb != diskSizeInMb {
		err = fmt.Errorf("Wrong disk size provisioned")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Verifying volume type specified in PVC is honored")
	if queryResult.Volumes[0].VolumeType != testVolumeType {
		err = fmt.Errorf("Volume type is not %q", testVolumeType)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// TODO: Verify HealthStauts is shown "Healthy", currently, only after 1 hour, the health status of newly created
	// volume will change from "Unknown" to "Healthy"

	// Verify if VolumeID is created on the VSAN datastores
	gomega.Expect(strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:")).To(gomega.BeTrue(), "Volume is not provisioned on vSan datastore")

	// Verify if VolumeID is created on the datastore from list of datacenters provided in vsphere.conf
	gomega.Expect(isDatastoreBelongsToDatacenterSpecifiedInConfig(queryResult.Volumes[0].DatastoreUrl)).To(gomega.BeTrue(), "Volume is not provisioned on the datastore specified on config file")
}

// isDatastoreBelongsToDatacenterSpecifiedInConfig checks whether the given datastoreURL belongs to the datacenter specified in the vSphere.conf file
func isDatastoreBelongsToDatacenterSpecifiedInConfig(datastoreURL string) bool {
	var datacenters []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finder := find.NewFinder(e2eVSphere.Client.Client, false)
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// TODO: handle empty datacenter field in vsphere.conf
	dcList := strings.Split(cfg.Global.Datacenters, ",")
	for _, dc := range dcList {
		dcName := strings.TrimSpace(dc)
		if dcName != "" {
			datacenters = append(datacenters, dcName)
		}
	}
	for _, dc := range datacenters {
		defaultDatacenter, _ := finder.Datacenter(ctx, dc)
		finder.SetDatacenter(defaultDatacenter)
		defaultDatastore, err := getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
		if defaultDatastore != nil && err == nil {
			return true
		}
	}

	// loop through all datacenters specified in conf file, and cannot find this given datastore
	return false

}

func testHelperForCreateFileVolumeWithDatastoreUrlInSC(f *framework.Framework, client clientset.Interface, namespace string, accessMode v1.PersistentVolumeAccessMode, datastoreURL string) {
	ginkgo.By(fmt.Sprintf("Invoking Test for accessMode: %s", accessMode))
	scParameters := make(map[string]string)
	scParameters[scParamsFsType] = nfs4FSType
	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class With nfs4")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	if datastoreURL != "" {
		scParameters[scParamDatastoreURL] = datastoreURL
	}
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
		err := framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

	ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s", queryResult.Volumes[0].Name, queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb, queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus))

	ginkgo.By("Verifying disk size specified in PVC is honored")
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb != diskSizeInMb {
		err = fmt.Errorf("Wrong disk size provisioned")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Verifying volume type specified in PVC is honored")
	if queryResult.Volumes[0].VolumeType != testVolumeType {
		err = fmt.Errorf("Volume type is not %q", testVolumeType)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// TODO: Verify HealthStauts is shown "Healthy", currently, only after 1 hour, the health status of newly created
	// volume will change from "Unknown" to "Healthy"

	// Verify if VolumeID is created on the VSAN datastores
	gomega.Expect(strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:")).To(gomega.BeTrue(), "Volume is not provisioned on vSan datastore")

	// Verify if VolumeID is created on the datastore from list of datacenters provided in vsphere.conf
	gomega.Expect(isDatastoreBelongsToDatacenterSpecifiedInConfig(queryResult.Volumes[0].DatastoreUrl)).To(gomega.BeTrue(), "Volume is not provisioned on the datastore specified on config file")
}

func testHelperForCreateFileVolumeWithoutValidVSANDatastoreUrlInSC(f *framework.Framework, client clientset.Interface, namespace string, accessMode v1.PersistentVolumeAccessMode, datastoreURL string) {
	ginkgo.By(fmt.Sprintf("Invoking Test for accessMode: %s", accessMode))
	scParameters := make(map[string]string)
	scParameters[scParamsFsType] = nfs4FSType
	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class With nfs4")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	if datastoreURL != "" {
		scParameters[scParamDatastoreURL] = datastoreURL
	}
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

	ginkgo.By("Expect claim to fail provisioning volume without valid VSAN datastore specified in storage class")
	err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
	gomega.Expect(err).To(gomega.HaveOccurred())
	expectedErrMsg := "failed to provision volume with StorageClass \"" + storageclass.Name + "\""
	fmt.Println(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
	isFailureFound := checkEventsforError(client, namespace, metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
	gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")

}
