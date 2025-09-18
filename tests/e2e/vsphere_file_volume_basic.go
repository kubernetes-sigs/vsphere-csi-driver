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
	"os"
	"strings"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

const (
	testVolumeType = "FILE"
)

var _ = ginkgo.Describe("[csi-file-vanilla] Basic Testing", func() {
	f := framework.NewDefaultFramework("file-volume-basic")
	var (
		client    clientset.Interface
		namespace string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
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
	ginkgo.It("[cf-vanilla-file][csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode, "+
		"when no storage policy is offered", ginkgo.Label(p0, file, vanilla, vc70), func() {
		testHelperForCreateFileVolumeWithNoDatastoreURLInSC(f, client, namespace, v1.ReadWriteMany)
	})

	/*
		Test to verify dynamic provisioning with ReadWriteMany access mode with
		datastoreURL is set in storage class, when no storage policy is offered.
		1. Create StorageClass with fsType as "nfs4"
		2. Create a PVC with "ReadWriteMany" using the SC from above
		3. Wait for PVC to be Bound
		4. Get the VolumeID from PV
		5. Verify using CNS Query API if VolumeID retrieved from PV is present.
		   Also verify Name, Capacity, VolumeType, Health matches.
		6. Verify if VolumeID is created on "datastoreUrl" specified in storage class
		7. Delete PVC
		8. Delete Storage class
	*/
	ginkgo.It("[csi-file-vanilla] [ef-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode with "+
		"datastoreURL is set in storage class, when no storage policy is offered", func() {
		datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		testHelperForCreateFileVolumeWithDatastoreURLInSC(f, client, namespace, v1.ReadWriteMany, datastoreURL, false)
	})

	/*
		Test to verify dynamic provisioning with ReadWriteMany access mode with
		datastoreURL specified in storage class is a non-VSAN datastore.
		1. Create StorageClass with fsType as "nfs4" and "datastoreUrl"
		2. Create a PVC with "ReadWriteMany" using the SC from above
		3. Expect the PVC to fail
		4. Verify the error returned on PVC failure is as expected
		5. Delete PVC
		6. Delete Storage class
	*/
	ginkgo.It("[csi-file-vanilla] [ef-file-vanilla]verify dynamic provisioning with ReadWriteMany access mode with "+
		"datastoreURL specified in storage class is a non-VSAN datastore, when no storage policy is "+
		"offered", ginkgo.Label(p1, negative, file, vanilla, vc70), func() {
		nonVSANDatastoreURL := GetAndExpectStringEnvVar(envNonSharedStorageClassDatastoreURL)
		testHelperForCreateFileVolumeWithoutValidVSANDatastoreURLInSC(
			f, client, namespace, v1.ReadWriteMany, nonVSANDatastoreURL)
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
	ginkgo.It("[cf-vanilla-file][csi-file-vanilla] verify dynamic provisioning with ReadOnlyMany access mode, "+
		"when no storage policy is offered", ginkgo.Label(p0, file, vanilla, vc70), func() {
		testHelperForCreateFileVolumeWithNoDatastoreURLInSC(f, client, namespace, v1.ReadOnlyMany)
	})

	/*
		Verify dynamic volume provisioning fails for VSAN datastore specified in
		"sc.datastoreUrl" but doesn't VSAN FS enabled.
		1. Create StorageClass with fsType as "nfs4" and "datastoreUrl"
		2. Create a PVC with "ReadWriteMany" using the SC from above
		3. Expect the PVC to fail.
		4. Verify the error message returned on PVC failure is correct.
		5. Delete PVC
		6. Delete Storage class
	*/
	ginkgo.It("[csi-file-vanilla] [ef-file-vanilla]verify dynamic volume provisioning fails for VSAN datastore "+
		"specified in sc.datastoreUrl but doesn't have VSAN FS enabled", ginkgo.Label(p1,
		negative, file, vanilla, vc70), func() {
		datastoreURL := os.Getenv(envFileServiceDisabledSharedDatastoreURL)
		if datastoreURL == "" {
			ginkgo.Skip("env variable FILE_SERVICE_DISABLED_SHARED_VSPHERE_DATASTORE_URL is not set, skip the test")
		}
		testHelperForCreateFileVolumeFailWhenFileServiceIsDisabled(f, client, namespace, v1.ReadWriteMany, datastoreURL)
	})

	/*
		Verify dynamic volume provisioning fails for VSAN datastore in the datacenter but doesn't VSAN FS enabled.
		1. Create StorageClass with fsType as "nfs4"
		2. Create a PVC with "ReadWriteMany" using the SC from above
		3. Expect the PVC to fail.
		4. Verify the error message returned on PVC failure is correct.
		5. Delete PVC
		6. Delete Storage class
	*/
	ginkgo.It("[csi-file-vanilla] [ef-file-vanilla]verify dynamic volume provisioning fails for VSAN datastore "+
		"in datacenter doesn't have VSAN FS enabled", ginkgo.Label(p1, negative, file, vanilla, vc70), func() {
		testHelperForCreateFileVolumeFailWhenFileServiceIsDisabled(f, client, namespace, v1.ReadWriteMany, "")
	})

})

func testHelperForCreateFileVolumeWithNoDatastoreURLInSC(f *framework.Framework,
	client clientset.Interface, namespace string, accessMode v1.PersistentVolumeAccessMode) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scParameters := make(map[string]string)
	scParameters[scParamFsType] = nfs4FSType
	// Create Storage class and PVC
	ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessMode, nfs4FSType))
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
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
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

	ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s",
		queryResult.Volumes[0].Name,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
		queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus))

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
	gomega.Expect(strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:")).To(
		gomega.BeTrue(), "Volume is not provisioned on vSan datastore")

	// Verify if VolumeID is created on the datastore from list of datacenters provided in vsphere.conf
	gomega.Expect(isDatastoreBelongsToDatacenterSpecifiedInConfig(queryResult.Volumes[0].DatastoreUrl)).To(
		gomega.BeTrue(), "Volume is not provisioned on the datastore specified on config file")
}

func testHelperForCreateFileVolumeWithDatastoreURLInSC(f *framework.Framework, client clientset.Interface,
	namespace string, accessMode v1.PersistentVolumeAccessMode, datastoreURL string, isDCEmpty bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scParameters := make(map[string]string)
	scParameters[scParamFsType] = nfs4FSType
	// Create Storage class and PVC
	ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessMode, nfs4FSType))
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	if datastoreURL != "" {
		scParameters[scParamDatastoreURL] = datastoreURL
	}
	storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
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
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

	ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s",
		queryResult.Volumes[0].Name,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
		queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus))

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
	gomega.Expect(strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:")).To(
		gomega.BeTrue(), "Volume is not provisioned on vSan datastore")

	// Verify if VolumeID is created on the datastore from list of datacenters
	// provided in vsphere.conf, only if datacenter list is not empty.
	if !isDCEmpty {
		gomega.Expect(isDatastoreBelongsToDatacenterSpecifiedInConfig(queryResult.Volumes[0].DatastoreUrl)).To(
			gomega.BeTrue(), "Volume is not provisioned on the datastore specified on config file")
	} else {
		secret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		originalConf := string(secret.Data[vSphereCSIConf])
		vsphereCfg, err := readConfigFromSecretString(originalConf)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(vsphereCfg.Global.Datacenters == "").To(gomega.BeTrue(),
			"Volume is provisioned on the datastore not specified on config file")
	}
}

func testHelperForCreateFileVolumeWithoutValidVSANDatastoreURLInSC(f *framework.Framework,
	client clientset.Interface, namespace string, accessMode v1.PersistentVolumeAccessMode, datastoreURL string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scParameters := make(map[string]string)
	scParameters[scParamFsType] = nfs4FSType
	// Create Storage class and PVC
	ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessMode, nfs4FSType))
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	if datastoreURL != "" {
		scParameters[scParamDatastoreURL] = datastoreURL
	}
	storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
		namespace, nil, scParameters, "", nil, "", false, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By("Expect claim to fail provisioning volume without valid VSAN datastore specified in storage class")
	err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
		pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
	gomega.Expect(err).To(gomega.HaveOccurred())
	expectedErrMsg := "failed to provision volume with StorageClass \"" + storageclass.Name + "\""
	ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
	isFailureFound := checkEventsforError(client, namespace,
		metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
	gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
}

func testHelperForCreateFileVolumeFailWhenFileServiceIsDisabled(f *framework.Framework,
	client clientset.Interface, namespace string, accessMode v1.PersistentVolumeAccessMode, datastoreURL string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scParameters := make(map[string]string)
	scParameters["fstype"] = nfs4FSType
	// Create Storage class and PVC
	ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q, fstype %q and datastoreURL:%q",
		accessMode, nfs4FSType, datastoreURL))
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	if datastoreURL != "" {
		scParameters[scParamDatastoreURL] = datastoreURL
	}
	storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
		namespace, nil, scParameters, "", nil, "", false, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By("Expect claim fails to provision volume when file service is disabled")
	err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
		pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
	gomega.Expect(err).To(gomega.HaveOccurred())
	expectedErrMsg := "failed to provision volume with StorageClass \"" + storageclass.Name + "\""
	ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
	isFailureFound := checkEventsforError(client, namespace,
		metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
	gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
}
