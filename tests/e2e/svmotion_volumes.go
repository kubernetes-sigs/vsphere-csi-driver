/*
Copyright 2019-2023 The Kubernetes Authors.

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
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

/*
	Test to verify sVmotion works fine for volumes in detached state

	Steps
	1. Create StorageClass.
	2. Create PVC.
	3. Expect PVC to pass and verified it is created correctly.
	4. Relocate detached volume
	5. Invoke CNS Query API and validate datastore URL value changed correctly
*/

var _ = ginkgo.Describe("[csi-block-vanilla] [csi-block-vanilla-parallelized] Relocate detached volume ", func() {
	f := framework.NewDefaultFramework("svmotion-disk")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client              clientset.Interface
		namespace           string
		scParameters        map[string]string
		datastoreURL        string
		sourceDatastore     *object.Datastore
		destDatastore       *object.Datastore
		datacenter          *object.Datacenter
		destDsURL           string
		pvclaims            []*v1.PersistentVolumeClaim
		fcdID               string
		labelKey            string
		labelValue          string
		pvc10g              string
		pandoraSyncWaitTime int
	)
	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = f.Namespace.Name
		scParameters = make(map[string]string)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		destDsURL = GetAndExpectStringEnvVar(destinationDatastoreURL)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		labelKey = "app"
		labelValue = "e2e-labels"
		pvc10g = "10Gi"

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
	})

	// Test for relocating volume being detached state
	ginkgo.It("Verify relocating detached volume works fine", ginkgo.Label(p0, vanilla, block, core, vc70), func() {
		ginkgo.By("Invoking Test for relocating detached volume")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		var datacenters []string
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters,
			",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}

		for _, dc := range datacenters {
			datacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(datacenter)
			sourceDatastore, err = getDatastoreByURL(ctx, datastoreURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			destDatastore, err = getDatastoreByURL(ctx, destDsURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create storageclass and PVC from that storageclass")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete Storageclass and PVC")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		fcdID = persistentvolumes[0].Spec.CSI.VolumeHandle

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s",
			persistentvolumes[0].Spec.CSI.VolumeHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(persistentvolumes[0].Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("error: QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk is created on the specified datastore")
		if queryResult.Volumes[0].DatastoreUrl != datastoreURL {
			err = fmt.Errorf("disk is created on the wrong datastore")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Relocate volume
		ginkgo.By("Relocating FCD to different datastore")
		err = e2eVSphere.relocateFCD(ctx, fcdID, sourceDatastore.Reference(), destDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to finish FCD relocation:%s to sync with pandora",
			defaultPandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(defaultPandoraSyncWaitTime) * time.Second)

		// verify disk is relocated to the specified destination datastore
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s after relocating the disk",
			persistentvolumes[0].Spec.CSI.VolumeHandle))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(persistentvolumes[0].Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("error: QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk is relocated to the specified datastore")
		if queryResult.Volumes[0].DatastoreUrl != destDsURL {
			err = fmt.Errorf("disk is relocated on the wrong datastore")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* Online relocation of volume using cnsRelocate Volume API
	STEPS:
	1. Create a tag based policy with 2 shared vmfs datastores(sharedVmfs-0 and sharedVmfs-1).
	2. Create SC with storage policy created in step 1.
	3. Create a PVC with sc created in step 2 and wait for it to come to bound state.
	4. Create a pod with the pvc created in step 3 and wait for it to come to Running state.
	5. Relocate volume from one shared datastore to another datastore using
	   CnsRelocateVolume API and verify the datastore of fcd after migration and volume compliance.
	6. Delete pod,pvc and sc.
	*/
	ginkgo.It("[cf-vanilla-block] Online relocation of volume using cnsRelocate Volume API", ginkgo.Label(p0,
		vanilla, block, core, vc70), func() {
		ginkgo.By("Invoking Test for offline relocation")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedvmfs2URL := os.Getenv(envSharedVMFSDatastore2URL)
		if sharedvmfs2URL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastore2URL))
		}
		datastoreUrls := []string{sharedvmfsURL, sharedvmfs2URL}

		govmomiClient := newClient(ctx, &e2eVSphere)
		pc := newPbmClient(ctx, govmomiClient)
		scParameters := make(map[string]string)
		pvcs := []*v1.PersistentVolumeClaim{}

		ginkgo.By("Creating tag and category to tag datastore")

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		ginkgo.By("Attaching tag to shared vmfs datastores")

		attachTagToDS(ctx, tagID, sharedvmfsURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfsURL)
		}()

		attachTagToDS(ctx, tagID, sharedvmfs2URL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfs2URL)
		}()

		ginkgo.By("Create Tag Based policy with shared datstores")
		policyID, policyName := createTagBasedPolicy(
			ctx, pc, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		ginkgo.By("Create Storageclass and a PVC from storageclass created")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcs = append(pvcs, pvclaim)

		defer func() {
			ginkgo.By("Delete the SCs created")
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Deleted the SPBM polices created")
			_, err = pc.DeleteProfile(ctx, []pbmtypes.PbmProfileId{*policyID})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID := pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			ginkgo.By("Delete the PVCs created in test")
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")

		ginkgo.By("Creating a pod")
		pod, err := createPod(ctx, client, namespace, nil, pvcs, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete the pod created")
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		filePath := "/mnt/volume1/file1.txt"
		filePath2 := "/mnt/volume1/file2.txt"

		//Write data on file.txt on Pod
		data := "This file is written by Pod"
		ginkgo.By("write to a file in pod")
		writeDataOnFileFromPod(namespace, pod.Name, filePath, data)

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
		framework.Logf("Volume: %s is present on %s", volumeID, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
				break
			}
		}

		ginkgo.By("Relocate volume from one shared datastore to another datastore using" +
			"CnsRelocateVolume API")
		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)
		_, err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

		ginkgo.By("Verify that the relocated CNS volumes are compliant and have correct policy id")
		storagePolicyMatches, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")

		//Read file1.txt created from Pod
		ginkgo.By("Read file.txt from Pod created by Pod")
		output := readFileFromPod(namespace, pod.Name, filePath)
		ginkgo.By(fmt.Sprintf("File contents from file.txt are: %s", output))
		data = data + "\n"
		gomega.Expect(output == data).To(gomega.BeTrue(), "data verification failed after relocation")

		data = "Writing some data to pod post relocation"
		ginkgo.By("writing to a file in pod post relocation")
		writeDataOnFileFromPod(namespace, pod.Name, filePath2, data)

		ginkgo.By("Read file.txt created by Pod")
		output = readFileFromPod(namespace, pod.Name, filePath2)
		ginkgo.By(fmt.Sprintf("File contents from file.txt are: %s", output))
		data = data + "\n"
		gomega.Expect(output == data).To(gomega.BeTrue(), "data verification failed after relocation")

	})

	/* Offline relocation of volume using cnsRelocate Volume API
	STEPS:
	1. Create a tag based policy with 2 shared vmfs datastores(sharedVmfs-0 and sharedVmfs-1).
	2. Create SC with storage policy created in step 1.
	3. Create a PVC with sc created in step 2 and wait for it to come to bound state.
	4. Relocate volume from one shared datastore to another datastore
	   using CnsRelocateVolume API and verify the datastore of fcd after migration and volume compliance.
	5. Delete pvc and sc.
	*/
	ginkgo.It("[cf-vanilla-block] Offline relocation of volume using cnsRelocate Volume API", ginkgo.Label(p0,
		vanilla, block, core, vc70), func() {
		ginkgo.By("Invoking Test for offline relocation")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedvmfs2URL := os.Getenv(envSharedVMFSDatastore2URL)
		if sharedvmfs2URL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastore2URL))
		}
		datastoreUrls := []string{sharedvmfsURL, sharedvmfs2URL}

		govmomiClient := newClient(ctx, &e2eVSphere)
		pc := newPbmClient(ctx, govmomiClient)
		scParameters := make(map[string]string)
		pvcs := []*v1.PersistentVolumeClaim{}

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		ginkgo.By("Creating tag and category to tag datastore")

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		ginkgo.By("Attaching tag to shared vmfs datastores")

		attachTagToDS(ctx, tagID, sharedvmfsURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfsURL)
		}()

		attachTagToDS(ctx, tagID, sharedvmfs2URL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfs2URL)
		}()

		ginkgo.By("Create Tag Based policy with shared datstores")
		policyID, policyName := createTagBasedPolicy(
			ctx, pc, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		ginkgo.By("Create Storageclass and a PVC from storageclass created")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcs = append(pvcs, pvclaim)

		defer func() {
			ginkgo.By("Delete the SCs created")
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID := pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			ginkgo.By("Delete the PVCs created in test")
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
		framework.Logf("Volume: %s is present on %s", volumeID, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
				break
			}
		}

		ginkgo.By("Relocate volume from one shared datastore to another datastore using" +
			"CnsRelocateVolume API")
		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)
		_, err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

		ginkgo.By("Verify that the relocated CNS volumes are compliant and have correct policy id")
		storagePolicyMatches, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")

	})

	/*
		Start attached volume's relocation and then expand it
		Steps:
			1.  Create a SPBM policy with tag based rules.
			2.  Create SC using policy created in step 1.
			3.  Create PVC using SC created in step 2 and and wait for it to be bound.
			4.  Create a pod with the pod created in step 3.
			5.  Start writing some IO to pod which run in parallel to steps 6-7.
			6.  Relocate CNS volume corresponding to pvc from step 3 to a different datastore.
			7.  While relocation is running resize the volume.
			8.  Verify the IO written so far.
			9.  Verify relocation was successful.
			10. Verify online volume expansion is successful.
			11. Delete all the objects created during the test.
	*/
	ginkgo.It("[csi-block-vanilla][csi-block-vanilla-parallelized] Start attached volume's relocation and then "+
		"expand it", ginkgo.Label(p0, vanilla, block, core, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, sharedVsanDatastoreURL := "", ""
		var datastoreUrls []string
		var policyName string
		pc := newPbmClient(ctx, e2eVSphere.Client)

		sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedVsanDatastoreURL = os.Getenv(envSharedDatastoreURL)
		if sharedVsanDatastoreURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedDatastoreURL))
		}
		datastoreUrls = append(datastoreUrls, sharedvmfsURL, sharedVsanDatastoreURL)

		scParameters := make(map[string]string)
		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		attachTagToDS(ctx, tagID, sharedvmfsURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfsURL)
		}()

		attachTagToDS(ctx, tagID, sharedVsanDatastoreURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedVsanDatastoreURL)
		}()

		ginkgo.By("create SPBM policy with tag based rules")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyID *pbmtypes.PbmProfileId

		policyID, policyName = createTagBasedPolicy(
			ctx, pc, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, pvc10g, nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcs = append(pvcs, pvclaim)
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		volumeID := pvs[0].Spec.CSI.VolumeHandle
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
		ginkgo.By("verify we can read and write on the PVCs")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)
		defer func() {
			ginkgo.By("Delete the pod created")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
		framework.Logf("Volume: %s is present on %s", volumeID, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
				break
			}
		}

		framework.Logf("dest url: %s", destDsUrl)
		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)

		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 100mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=100").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		currentPvcSize := pvcs[0].Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse(diskSize))

		originalSizeInMb, err := getFSSizeMb(f, pods[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Start relocation of volume to a different datastore")
		task, err := e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Waiting for a few seconds for relocation to be started properly on VC")
		time.Sleep(time.Duration(10) * time.Second)
		data := "This file is written by Pod"

		ginkgo.By("Resize volume and writing IO to pod while relocating volume")
		var wg sync.WaitGroup
		wg.Add(2)
		go writeDataToMultipleFilesOnPodInParallel(namespace, pods[0].Name, data, &wg)
		go resize(client, pvcs[0], pvcs[0].Spec.Resources.Requests[v1.ResourceStorage], newSize, &wg)
		wg.Wait()

		ginkgo.By("Wait for relocation task to complete")

		cnsFault := waitForCNSTaskToComplete(ctx, task)
		if cnsFault != nil {
			err = fmt.Errorf("failed to relocate volume=%+v", cnsFault)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify relocation of volume is successful")
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

		ginkgo.By("Wait and verify the file system resize on pvcs")
		framework.Logf("Waiting for file system resize to finish for pvc %v", pvcs[0].Name)
		pvcs[0], err = waitForFSResize(pvcs[0], client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvcs[0].Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc %v should not have status conditions", pvcs[0].Name)

		var fsSize int64
		framework.Logf("Verify filesystem size for mount point /mnt/volume1 for pod %v", pods[0].Name)
		fsSize, err = getFSSizeMb(f, pods[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %v", fsSize)
		gomega.Expect(fsSize > originalSizeInMb).To(gomega.BeTrue(),
			fmt.Sprintf(
				"filesystem size %v is not > than before expansion %v for pvc %q",
				fsSize, originalSizeInMb, pvcs[0].Name))

		framework.Logf("File system resize finished successfully for pvc %v", pvcs[0].Name)

		ginkgo.By("Verify the data on the PVCs match what was written in step 7")
		for i := 0; i < 10; i++ {
			filePath := fmt.Sprintf("/mnt/volume1/file%v.txt", i)
			output := readFileFromPod(namespace, pods[0].Name, filePath)
			ginkgo.By(fmt.Sprintf("File contents from file%v.txt are: %s", i, output))
			dataToVerify := data + "\n"
			gomega.Expect(output == dataToVerify).To(gomega.BeTrue(), "data verification failed after relocation")
		}

		storagePolicyMatches, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)

		ginkgo.By("Delete the pod created")
		deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podsNew := createMultiplePods(ctx, client, pvclaims2d, true)
		deletePodsAndWaitForVolsToDetach(ctx, client, podsNew, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Start attached volume's expansion and then relocate it
		Steps:
			1.  Create a SPBM policy with tag based rules.
			2.  Create SC using policy created in step 1.
			3.  Create PVC using SC created in step 2 and and wait for it to be bound.
			4.  Create a pod with the pod created in step 3.
			5.  Start writing some IO to pod which run in parallel to steps 6-7.
			6.  Resize the volume.
			7.  While expansion is running relocate the volume allocation to different datastore.
			8.  Verify the IO written so far.
			9.  Verify relocation was successful.
			10. Verify online volume expansion is successful.
			11. Delete all the objects created during the test.
	*/
	ginkgo.It("[csi-block-vanilla][csi-block-vanilla-parallelized] Start attached volume's expansion and then relocate"+
		"it", ginkgo.Label(p1, vanilla, block, core, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, sharedVsanDatastoreURL := "", ""
		var datastoreUrls []string
		var policyName string
		pc := newPbmClient(ctx, e2eVSphere.Client)

		sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedVsanDatastoreURL = os.Getenv(envSharedDatastoreURL)
		if sharedVsanDatastoreURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedDatastoreURL))
		}
		datastoreUrls = append(datastoreUrls, sharedvmfsURL, sharedVsanDatastoreURL)

		scParameters := make(map[string]string)
		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		attachTagToDS(ctx, tagID, sharedvmfsURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfsURL)
		}()

		attachTagToDS(ctx, tagID, sharedVsanDatastoreURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedVsanDatastoreURL)
		}()

		ginkgo.By("create SPBM policy with tag based rules")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyID *pbmtypes.PbmProfileId

		policyID, policyName = createTagBasedPolicy(
			ctx, pc, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, pvc10g, nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcs = append(pvcs, pvclaim)
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		volumeID := pvs[0].Spec.CSI.VolumeHandle
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
		ginkgo.By("verify we can read and write on the PVCs")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)
		defer func() {
			ginkgo.By("Delete the pod created")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
		framework.Logf("Volume: %s is present on %s", volumeID, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
				break
			}
		}

		framework.Logf("dest url: %s", destDsUrl)
		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)

		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 100mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=100").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		currentPvcSize := pvcs[0].Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse(diskSize))

		originalSizeInMb, err := getFSSizeMb(f, pods[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Start online expansion of volume")
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		_, err = expandPVCSize(pvcs[0], newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		data := "This file is written by Pod"

		ginkgo.By("Relocate volume and writing IO to pod while resizing volume")
		var wg sync.WaitGroup
		wg.Add(2)
		go writeDataToMultipleFilesOnPodInParallel(namespace, pods[0].Name, data, &wg)
		go cnsRelocateVolumeInParallel(e2eVSphere, ctx, volumeID, dsRefDest, true, &wg)
		wg.Wait()

		ginkgo.By("Verify relocation of volume is successful")
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

		ginkgo.By("Wait and verify the file system resize on pvcs")
		framework.Logf("Waiting for file system resize to finish for pvc %v", pvcs[0].Name)
		pvcs[0], err = waitForFSResize(pvcs[0], client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvcs[0].Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc %v should not have status conditions", pvcs[0].Name)

		var fsSize int64
		framework.Logf("Verify filesystem size for mount point /mnt/volume1 for pod %v", pods[0].Name)
		fsSize, err = getFSSizeMb(f, pods[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %v", fsSize)
		gomega.Expect(fsSize > originalSizeInMb).To(gomega.BeTrue(),
			fmt.Sprintf(
				"filesystem size %v is not > than before expansion %v for pvc %q",
				fsSize, originalSizeInMb, pvcs[0].Name))

		framework.Logf("File system resize finished successfully for pvc %v", pvcs[0].Name)

		ginkgo.By("Verify the data on the PVCs match what was written in step 7")
		for i := 0; i < 10; i++ {
			filePath := fmt.Sprintf("/mnt/volume1/file%v.txt", i)
			output := readFileFromPod(namespace, pods[0].Name, filePath)
			ginkgo.By(fmt.Sprintf("File contents from file%v.txt are: %s", i, output))
			dataToVerify := data + "\n"
			gomega.Expect(output == dataToVerify).To(gomega.BeTrue(), "data verification failed after relocation")
		}

		storagePolicyMatches, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)

		ginkgo.By("Delete the pod created")
		deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podsNew := createMultiplePods(ctx, client, pvclaims2d, true)
		deletePodsAndWaitForVolsToDetach(ctx, client, podsNew, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Start volume relocation and then update its metadata
		Steps:
			1.  Create a SPBM policy with tag based rules.
			2.  Create SC using policy created in step 1.
			3.  Create PVC using SC created in step 2 and and wait for it to be bound.
			4.  Create a pod with the pod created in step 3.
			5.  Start writing some IO to pod which run in parallel to steps 6-7.
			6.  Relocate CNS volume corresponding to pvc from step 3 to a different datastore.
			7.  While relocation is running add labels to PV and PVC.
			8.  Verify the IO written so far.
			9.  Verify relocation was successful.
			10. Verify the labels in cnsvolume metadata post relocation.
			11. Delete all the objects created during the test.
	*/
	ginkgo.It("[csi-block-vanilla][csi-block-vanilla-parallelized] Start volume relocation and then "+
		"update its metadata", ginkgo.Label(p0, vanilla, block, core, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, sharedNfsURL := "", ""
		var datastoreUrls []string
		var policyName string
		pc := newPbmClient(ctx, e2eVSphere.Client)

		sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedNfsURL = os.Getenv(envSharedNFSDatastoreURL)
		if sharedNfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", sharedNfsURL))
		}
		datastoreUrls = append(datastoreUrls, sharedvmfsURL, sharedNfsURL)

		scParameters := make(map[string]string)
		policyNames := []string{}
		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		attachTagToDS(ctx, tagID, sharedvmfsURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfsURL)
		}()

		attachTagToDS(ctx, tagID, sharedNfsURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedNfsURL)
		}()

		ginkgo.By("create SPBM policy with tag based rules")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyID *pbmtypes.PbmProfileId

		policyID, policyName = createTagBasedPolicy(
			ctx, pc, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()
		policyNames = append(policyNames, policyName)

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, pvc10g, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcs = append(pvcs, pvclaim)
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")

		volumeID := pvs[0].Spec.CSI.VolumeHandle
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
		ginkgo.By("verify we can read and write on the PVCs")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)
		defer func() {
			ginkgo.By("Delete the pod created")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
		framework.Logf("Volume: %s is present on %s", volumeID, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
				break
			}
		}

		framework.Logf("dest url: %s", destDsUrl)
		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)

		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 100mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=100").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By("Start relocation of volume to a different datastore")
		task, err := e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Waiting for a few seconds for relocation to be started properly on VC")
		time.Sleep(time.Duration(10) * time.Second)

		ginkgo.By("Adding labels to volume and writing IO to pod whle relocating volume")
		var wg sync.WaitGroup
		wg.Add(3)
		go writeKnownData2PodInParallel(f, client, pods[0], testdataFile, &wg)
		go updatePvcLabelsInParallel(ctx, client, namespace, labels, pvcs, &wg)
		go updatePvLabelsInParallel(ctx, client, namespace, labels, pvs, &wg)
		wg.Wait()

		ginkgo.By("Wait for relocation task to complete")
		cnsFault := waitForCNSTaskToComplete(ctx, task)
		if cnsFault != nil {
			err = fmt.Errorf("failed to relocate volume=%+v", cnsFault)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify relocation of volume is successful")
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvclaim.Name, namespace))
		pv := getPvFromClaim(client, namespace, pvclaim.Name)
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s",
			labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the data on the PVCs match what was written in step 7")
		verifyKnownDataInPod(f, client, pods[0], testdataFile)

		storagePolicyMatches, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)

		ginkgo.By("Delete the pod created")
		deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podsNew := createMultiplePods(ctx, client, pvclaims2d, true)
		deletePodsAndWaitForVolsToDetach(ctx, client, podsNew, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Start volume relocation and take a snapshot
		Steps:
			1.  Create a SPBM policy with tag based rules.
			2.  Create SC using policy created in step 1.
			3.  Create PVC using SC created in step 2 and and wait for it to be bound.
			4.  Create a pod with the pod created in step 3.
			5.  Start writing some IO to pod which run in parallel to steps 6-7.
			6.  Relocate CNS volume corresponding to pvc from step 3 to a different datastore.
			7.  While relocation is running create a CSI snapshot.
			8.  Verify the IO written so far.
			9.  Verify relocation was successful
			10. Verify snapshot creation was successful.
			11. Delete all the objects created during the test.
	*/
	ginkgo.It("[csi-block-vanilla][csi-block-vanilla-parallelized] Start volume relocation and take a "+
		"snapshot", ginkgo.Label(p0, vanilla, block, core, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedNfsUrl, sharedVsanDsurl := "", ""
		var datastoreUrls []string
		var policyName string
		pc := newPbmClient(ctx, e2eVSphere.Client)

		sharedVsanDsurl = os.Getenv(envSharedDatastoreURL)
		if sharedVsanDsurl == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedDatastoreURL))
		}

		sharedNfsUrl = os.Getenv(envSharedNFSDatastoreURL)
		if sharedNfsUrl == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedNFSDatastoreURL))
		}
		datastoreUrls = append(datastoreUrls, sharedNfsUrl, sharedVsanDsurl)

		scParameters := make(map[string]string)
		policyNames := []string{}
		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		attachTagToDS(ctx, tagID, sharedVsanDsurl)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedVsanDsurl)
		}()

		attachTagToDS(ctx, tagID, sharedNfsUrl)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedNfsUrl)
		}()

		ginkgo.By("create SPBM policy with tag based rules")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyID *pbmtypes.PbmProfileId

		policyID, policyName = createTagBasedPolicy(
			ctx, pc, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		policyNames = append(policyNames, policyName)

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, pvc10g, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcs = append(pvcs, pvclaim)
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})

		defer func() {
			if vanillaCluster {
				ginkgo.By("Delete the SCs created in step 2")
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		volumeID := pvs[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volumeID = getVolumeIDFromSupervisorCluster(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
		ginkgo.By("verify we can read and write on the PVCs")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)
		defer func() {
			ginkgo.By("Delete the pod created")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
		framework.Logf("Volume: %s is present on %s", volumeID, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
				break
			}
		}

		framework.Logf("dest url: %s", destDsUrl)
		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)

		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 100mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=100").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		snaps := []*snapV1.VolumeSnapshot{}
		snapIDs := []string{}

		//Get snapshot client using the rest config
		restConfig := getRestConfigClient()
		snapc, err := snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot class with name %q created", volumeSnapshotClass.Name)

		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(
				ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Start relocation of volume to a different datastore")
		task, err := e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Waiting for a few seconds for relocation to be started properly on VC")
		time.Sleep(time.Duration(10) * time.Second)

		ginkgo.By("Create a snapshot of volume and writing IO to pod while relocating volume")
		ch := make(chan *snapV1.VolumeSnapshot)
		lock := &sync.Mutex{}
		var wg sync.WaitGroup
		wg.Add(2)
		go writeKnownData2PodInParallel(f, client, pods[0], testdataFile, &wg)
		go createSnapshotInParallel(ctx, namespace, snapc, pvclaim.Name, volumeSnapshotClass.Name,
			ch, lock, &wg)
		go func() {
			for v := range ch {
				snaps = append(snaps, v)
			}
		}()
		wg.Wait()

		ginkgo.By("Wait for relocation task to complete")
		cnsFault := waitForCNSTaskToComplete(ctx, task)
		if cnsFault != nil {
			err = fmt.Errorf("failed to relocate volume=%+v", cnsFault)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify relocation of volume is successful")
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

		volumeSnapshot := snaps[0]
		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("snapshot restore size is : %s", volumeSnapshot.Status.RestoreSize.String())
		gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(pvclaim.Spec.Resources.Requests[v1.ResourceStorage])).To(
			gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(*snapshotContent.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle := *snapshotContent.Status.SnapshotHandle
		snapshotId := strings.Split(snapshothandle, "+")[1]
		snapIDs = append(snapIDs, snapshotId)

		defer func() {
			if len(snaps) > 0 {
				framework.Logf("Delete volume snapshot %v", volumeSnapshot.Name)
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				framework.Logf("Verify snapshot entry %v is deleted from CNS for volume %v", snapIDs[0], volumeID)
				err = waitForCNSSnapshotToBeDeleted(volumeID, snapIDs[0])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			}
		}()

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volumeID, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the data on the PVCs match what was written in step 7")
		verifyKnownDataInPod(f, client, pods[0], testdataFile)

		storagePolicyMatches, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)

		ginkgo.By("Delete the pod created")
		deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podsNew := createMultiplePods(ctx, client, pvclaims2d, true)
		deletePodsAndWaitForVolsToDetach(ctx, client, podsNew, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})
})
