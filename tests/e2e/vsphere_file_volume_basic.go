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

	ginkgo "github.com/onsi/ginkgo/v2"
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
	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode, "+
		"when no storage policy is offered", func() {
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
	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode with "+
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
	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode with "+
		"datastoreURL specified in storage class is a non-VSAN datastore, when no storage policy is offered", func() {
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
	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadOnlyMany access mode, "+
		"when no storage policy is offered", func() {
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
	ginkgo.It("[csi-file-vanilla] verify dynamic volume provisioning fails for VSAN datastore "+
		"specified in sc.datastoreUrl but doesn't have VSAN FS enabled", func() {
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
	ginkgo.It("[csi-file-vanilla] verify dynamic volume provisioning fails for VSAN datastore "+
		"in datacenter doesn't have VSAN FS enabled", func() {
		testHelperForCreateFileVolumeFailWhenFileServiceIsDisabled(f, client, namespace, v1.ReadWriteMany, "")
	})

	/*
		Test to verify dynamic provisioning with ReadWriteMany access mode with
		datastoreURL specified in TargetvSANFileShareDatastoreURLs of vsphere
		config.
		1. Create StorageClass with fsType as "nfs4" and no datastoreUrl specified
		2. Create a PVC with "ReadWriteMany" using the SC from above
		3. Wait for PVC to be Bound
		4. Get the VolumeID from PV
		5. Verify using CNS Query API if VolumeID retrieved from PV is present.
		   Also verify if Name, Capacity, VolumeType, Health match.
		6. Verify if VolumeID is created on one of the datastores listed in
		   TargetvSANFileShareDatastoreURLs provided in vsphere.conf.
		7. Delete PVC
		8. Delete Storage class
	*/
	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with ReadWriteMany access mode "+
		"with datastoreURL specified in TargetvSANFileShareDatastoreURLs", func() {
		// Verify if test is valid for the given environment
		if len(targetDsURLs) == 0 {
			ginkgo.Skip("TargetvSANFileShareDatastoreURLs is not set in e2eTest.conf, skipping the test")
		}
		createFileVolumeUsingDatastoreFromVsphereConf(f, client, namespace, v1.ReadWriteMany)
	})

	/*
		Test to verify dynamic provisioning with ReadWriteMany access mode with
		datastoreURL specified in storage class matching the one specified in
		TargetvSANFileShareDatastoreURLs of vsphere config.
		(NOTE: test makes sense when TargetvSANFileShareDatastoreURLs has more
		 than one URL)
		1. Create StorageClass with fsType as "nfs4" and "datastoreUrl" specified
		   from TargetvSANFileShareDatastoreURLs.
		2. Create a PVC with "ReadWriteMany" using the SC from above
		3. Wait for PVC to be Bound
		4. Get the VolumeID from PV
		5. Verify using CNS Query API if VolumeID retrieved from PV is present.
		   Also verify if Name, Capacity, VolumeType, Health match.
		6. Verify if VolumeID is created on one of the datastores listed in
		   "datastoreUrl" specified in storage class.
		7. Delete PVC
		8. Delete Storage class
	*/
	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning using datastoreURL specified "+
		"in storage class matching one of the URLs specified in TargetvSANFileShareDatastoreURLs", func() {
		// Verify if test is valid for the given environment
		if len(targetDsURLs) == 0 {
			ginkgo.Skip("TargetvSANFileShareDatastoreURLs is not set in e2eTest.conf, skipping the test")
		}
		createFileVolumeUsingDatastoreMatchingWithTargetURLs(f, client, namespace, v1.ReadWriteMany, targetDsURLs[0])
	})

	/*
		Test to verify dynamic provisioning with ReadWriteOnce access mode with
		datastoreURL specified in storage class matching the one specified in
		TargetvSANFileShareDatastoreURLs of vsphere config.
		1. Create StorageClass with fsType as "nfs4" and "datastoreUrl" specified
		   from TargetvSANFileShareDatastoreURLs.
		2. Create a PVC with "ReadWriteOnce" using the SC from above
		3. Expect the PVC to fail.
		4. Verify the error message returned on PVC failure is correct.
		5. Delete PVC
		6. Delete Storage class
	*/
	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning with access mode ReadWriteOnce "+
		"using datastoreURL specified in storage class matching one of the URLs specified in "+
		"TargetvSANFileShareDatastoreURLs", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Verify if test is valid for the given environment
		if len(targetDsURLs) == 0 {
			ginkgo.Skip("TargetvSANFileShareDatastoreURLs is not set in e2eTest.conf, skipping the test")
		}

		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType
		accessMode := v1.ReadWriteOnce
		datastoreURL := targetDsURLs[0]
		if datastoreURL != "" {
			scParameters[scParamDatastoreURL] = datastoreURL
		}
		// Create Storage class and PVC
		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessMode, nfs4FSType))
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

		ginkgo.By(fmt.Sprintf("Expect claim to fail as the access mode %q is not supported for File volumes", accessMode))
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "fstype nfs4 not supported for ReadWriteOnce volume creation"
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
		Test to verify dynamic provisioning with ReadWriteMany access mode fails
		when datastoreURL specified in storage class does not match the one
		specified in TargetvSANFileShareDatastoreURLs of vsphere config.
		1. Create StorageClass with fsType as "nfs4" and "datastoreUrl" not
		   specified in TargetvSANFileShareDatastoreURLs.
		2. Create a PVC with "ReadWriteMany" using the SC from above
		3. Expect the PVC to fail.
		4. Delete PVC
		5. Delete Storage class
	*/
	ginkgo.It("[csi-file-vanilla] verify dynamic provisioning fails using datastoreURL specified "+
		"in storage class not matching the ones specified in TargetvSANFileShareDatastoreURLs", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Verify if test is valid for the given environment
		if len(targetDsURLs) == 0 {
			ginkgo.Skip("TargetvSANFileShareDatastoreURLs is not set in e2eTest.conf, skipping the test")
		}

		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType
		accessMode := v1.ReadWriteMany
		nonVSANDatastoreURL := GetAndExpectStringEnvVar(envNonSharedStorageClassDatastoreURL)
		if nonVSANDatastoreURL != "" {
			scParameters[scParamDatastoreURL] = nonVSANDatastoreURL
		}
		// Create Storage class and PVC
		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessMode, nfs4FSType))
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

		ginkgo.By("Expect claim to fail as the datastore URL mentioned in Storage class " +
			"does not match any of the URLs mentioned in TargetvSANFileShareDatastoreURLs")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		eventList, _ := client.CoreV1().Events(namespace).List(ctx,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)})
		for _, item := range eventList.Items {
			framework.Logf(fmt.Sprintf(item.Message))
		}

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

	ginkgo.By("Expect claim to fail provisioning volume without valid VSAN datastore specified in storage class")
	err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
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

	ginkgo.By("Expect claim fails to provision volume when file service is disabled")
	err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
		pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
	gomega.Expect(err).To(gomega.HaveOccurred())
	expectedErrMsg := "failed to provision volume with StorageClass \"" + storageclass.Name + "\""
	ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
	isFailureFound := checkEventsforError(client, namespace,
		metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
	gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
}

func createFileVolumeUsingDatastoreFromVsphereConf(f *framework.Framework,
	client clientset.Interface, namespace string, accessMode v1.PersistentVolumeAccessMode) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By("Invoking test to check if the TargetvSANFileShareDatastoreURLs in vSphere config is applied " +
		"when no datastore is specified in Storage Class")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	scParameters := make(map[string]string)
	scParameters[scParamFsType] = nfs4FSType

	// Create Storage class and PVC
	ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %s", accessMode, nfs4FSType))
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
	persistentVolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Test if volumeID has corresponding CNS volume
	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

	// Test if the attributes match
	targetQueryVolume := queryResult.Volumes[0]
	ginkgo.By(fmt.Sprintf("Volume Name:%s capacity:%d volumeType:%s health:%s",
		targetQueryVolume.Name,
		targetQueryVolume.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
		targetQueryVolume.VolumeType,
		targetQueryVolume.HealthStatus))

	// Test if the spec was honored
	ginkgo.By("Verifying disk size specified in PVC is honored")
	if targetQueryVolume.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb != diskSizeInMb {
		err = fmt.Errorf("disk size expected to be %d. Actual size %d", diskSizeInMb,
			targetQueryVolume.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Verifying volume type specified in PVC is honored")
	if targetQueryVolume.VolumeType != testVolumeType {
		err = fmt.Errorf("volume type expected to be %q, found %q", testVolumeType, targetQueryVolume.VolumeType)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// TODO: Verify HealthStatus is shown "Healthy". Currently the health status of newly created volumes
	// is shown as "Unknown" and gets reflected as "Healthy" only after 1 hour

	// Verify if VolumeID is created on the VSAN datastores
	gomega.Expect(strings.HasPrefix(targetQueryVolume.DatastoreUrl, "ds:///vmfs/volumes/vsan:")).To(gomega.BeTrue(),
		"Volume is provisioned on %q which is not a vSan datastore", targetQueryVolume.DatastoreUrl)

	// Verify if VolumeID is created in one of the datastores listed in
	// TargetvSANFileShareDatastoreURLs provided in vsphere.conf.
	errorMsg := fmt.Sprintf("Volume is provisioned on %q which does not match any of the datastores "+
		"specified in TargetvSANFileShareDatastoreURLs in the vSphere config file",
		targetQueryVolume.DatastoreUrl)
	gomega.Expect(isDatastorePresentinTargetvSANFileShareDatastoreURLs(targetQueryVolume.DatastoreUrl)).To(
		gomega.BeTrue(), errorMsg)
}

func createFileVolumeUsingDatastoreMatchingWithTargetURLs(f *framework.Framework,
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

	// Verify if VolumeID is created in the datastore mentioned in the Storage class params
	if queryResult.Volumes[0].DatastoreUrl != datastoreURL {
		err = fmt.Errorf("volume provisioned on datastoreURL %q which is not the datatore mentioned in the storage class",
			queryResult.Volumes[0].DatastoreUrl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}
