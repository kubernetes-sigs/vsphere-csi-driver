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
	"math/rand"
	"os"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	v1 "k8s.io/api/core/v1"
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
	f := framework.NewDefaultFramework("svmotion-detached-disk")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client          clientset.Interface
		namespace       string
		scParameters    map[string]string
		datastoreURL    string
		sourceDatastore *object.Datastore
		destDatastore   *object.Datastore
		datacenter      *object.Datacenter
		destDsURL       string
		pvclaims        []*v1.PersistentVolumeClaim
		fcdID           string
	)
	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = f.Namespace.Name
		scParameters = make(map[string]string)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		destDsURL = GetAndExpectStringEnvVar(destinationDatastoreURL)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	// Test for relocating volume being detached state
	ginkgo.It("Verify relocating detached volume works fine", func() {
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

		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
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
	ginkgo.It("Online relocation of volume using cnsRelocate Volume API", func() {
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

		policyID, policyName := createTagBasedPolicy(
			ctx, pc, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
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
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID := pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")

		ginkgo.By("Creating a pod")
		pod, err := createPod(client, namespace, nil, pvcs, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		filePath := "/mnt/volume1/file1.txt"
		filePath2 := "/mnt/volume1/file2.txt"

		//Write data on file.txt on Pod
		data := "This file is written by Pod"
		ginkgo.By("write to a file in pod")
		writeDataOnFileFromPod(namespace, pod.Name, filePath, data)

		defer func() {
			ginkgo.By("Delete the pod created")
			err := fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
		framework.Logf("Volume is present on %s", dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
			}
		}

		ginkgo.By("Relocate volume from one shared datastore to another datastore using" +
			"CnsRelocateVolume API")
		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)
		err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

		ginkgo.By("Verify that the relocated CNS volumes are compliant and have correct policy id")
		storagePolicyExists, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")

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
	ginkgo.It("Offline relocation of volume using cnsRelocate Volume API", func() {
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

		policyID, policyName := createTagBasedPolicy(
			ctx, pc, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcs = append(pvcs, pvclaim)

		defer func() {
			ginkgo.By("Delete the SCs created")
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID := pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
		framework.Logf("Volume is present on %s", dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
			}
		}

		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)
		err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

		ginkgo.By("Verify that the relocated CNS volumes are compliant and have correct policy id")
		storagePolicyExists, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")

	})
})
