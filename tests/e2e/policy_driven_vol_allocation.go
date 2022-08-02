/*
Copyright 2022 The Kubernetes Authors.

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
	"strconv"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/pbm"
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
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[vol-allocation] Policy driven volume space allocation tests", func() {

	f := framework.NewDefaultFramework("e2e-spbm-policy")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string
		pc        *pbm.Client
	)
	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		govmomiClient := newClient(ctx, &e2eVSphere)
		pc = newPbmClient(ctx, govmomiClient)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		setVpxdTaskTimeout(ctx, 0) // reset vpxd timeout to default
	})

	/*
		Verify Thin, EZT, LZT volume creation via SPBM policies
		Steps:
			1	create 3 SPBM policies with thin, LZT, EZT volume allocation respectively
			2	create 3 storage classes, each with a SPBM policy created from step 1
			3	create a PVC each using the storage policies created from step 2
			4	Verify the PVCs created in step 3 are bound
			5	Create pods with using the PVCs created in step 3 and wait for them to be ready
			6	verify we can read and write on the PVCs
			7	Delete pods created in step 5
			8	Delete the PVCs created in step 3
			9	Delete the SCs created in step 2
			10	Deleted the SPBM polices created in step 1
	*/
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] Verify Thin, EZT, LZT volume creation via "+
		"SPBM policies", func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		scParameters := make(map[string]string)
		policyNames := []string{}
		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		scs := []*storagev1.StorageClass{}

		rand.Seed(time.Now().UnixNano())
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

		allocationTypes := []string{
			thinAllocType,
			eztAllocType,
			lztAllocType,
		}

		ginkgo.By("create 3 SPBM policies with thin, LZT, EZT volume allocation respectively")
		ginkgo.By("create 3 storage classes, each with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policies created from step 2")
		for _, at := range allocationTypes {
			policyID, policyName := createVmfsStoragePolicy(
				ctx, pc, at, map[string]string{categoryName: tagName})
			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			scParameters[scParamStoragePolicyName] = policyName
			policyNames = append(policyNames, policyName)
			storageclass, pvclaim, err := createPVCAndStorageClass(client,
				namespace, nil, scParameters, "", nil, "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim)
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			scs = append(scs, storageclass)
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			for _, sc := range scs {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i])
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			gomega.Expect(e2eVSphere.verifyDatastoreMatch(volumeID, sharedvmfsURL)).To(
				gomega.BeTrue(), "volume %v was created on wrong ds", sharedvmfsURL)
		}

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[i].Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
		ginkgo.By("verify we can read and write on the PVCs")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		ginkgo.By("Delete pods created in step 5")
		deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
	})

	/*
		Fill LZT/EZT volume
		Steps:
		1. create SPBM policies with LZT, EZT volume allocation
		2. Create SCs using policies created in step 1
		3. Create a large PVCs using SCs created in step 2
		4. Wait and verify the PVCs created in step 3 is bound
		5. Create a pod using PVCs created in step 3 and wait for it to be Ready.
		6. Fill the PVCs by running IO within the pod created in step 5
		7. Delete pod created in step 5
		8. Delete the PVCs created in step 3
		9. Delete the SCs created in step 2
		10. Deleted the SPBM policies created in step 1
	*/
	ginkgo.It("[csi-block-vanilla][csi-block-vanilla-parallelized] Fill LZT/EZT volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		largeSize := os.Getenv(envDiskSizeLarge)
		if largeSize == "" {
			largeSize = diskSizeLarge
		}

		scParameters := make(map[string]string)
		policyNames := []string{}
		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		scs := []*storagev1.StorageClass{}

		rand.Seed(time.Now().UnixNano())
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

		allocationTypes := []string{
			eztAllocType,
			lztAllocType,
		}

		ginkgo.By("create SPBM policies with LZT, EZT volume allocation respectively")
		ginkgo.By("Create SCs using policies created in step 1")
		ginkgo.By("create a large PVC each using the storage policies created from step 2")
		for _, at := range allocationTypes {
			policyID, policyName := createVmfsStoragePolicy(
				ctx, pc, at, map[string]string{categoryName: tagName})
			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			scParameters[scParamStoragePolicyName] = policyName
			policyNames = append(policyNames, policyName)
			storageclass, pvclaim, err := createPVCAndStorageClass(client,
				namespace, nil, scParameters, largeSize, nil, "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim)
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			scs = append(scs, storageclass)
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			for _, sc := range scs {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i])
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			gomega.Expect(e2eVSphere.verifyDatastoreMatch(volumeID, sharedvmfsURL)).To(
				gomega.BeTrue(), "volume %v was created on wrong ds", sharedvmfsURL)
		}

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[i].Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
		ginkgo.By("verify we can read and write on the PVCs")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)
		defer func() {
			ginkgo.By("Delete pods")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		fillVolumeInPods(f, pods)
	})

	/*
		Verify large EZT volume creation (should take >vpxd task timeout)
		Steps:
		1. Create a SPBM policy with EZT volume allocation
		2. Create a SC using policy created in step 1
		3. Create a large PVC using SC created in step 2, this should take more than vpxd task timeout
		4. Wait and verify the PVC created in step 3 is bound
		5. Verify no orphan volumes are created
		6. Delete the PVC created in step 3
		7. Delete the SC created in step 2
		8. Deleted the SPBM policy created in step 1
	*/
	ginkgo.It("[csi-block-vanilla] Verify large EZT volume creation which takes longer than vpxd timeout", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		largeSize := os.Getenv(envDiskSizeLarge)
		if largeSize == "" {
			largeSize = diskSizeLarge
		}

		scParameters := make(map[string]string)

		rand.Seed(time.Now().UnixNano())
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

		ginkgo.By("Create a SPBM policy with EZT volume allocation")
		policyID, policyName := createVmfsStoragePolicy(
			ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		setVpxdTaskTimeout(ctx, vpxdReducedTaskTimeoutSecsInt)
		defer func() {
			setVpxdTaskTimeout(ctx, 0)
		}()

		ginkgo.By("Create SC using policy created in step 1")
		ginkgo.By("Create a large PVC using SC created in step 2, this should take more than vpxd task timeout")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, largeSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete the SC created in step 2")
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		start := time.Now()
		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(
			client, []*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		elapsed := time.Since(start)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC creation took longer than vpxd timeout")
		gomega.Expect(elapsed > time.Second*time.Duration(vpxdReducedTaskTimeoutSecsInt)).To(
			gomega.BeTrue(), "PVC creation was faster than vpxd timeout")

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		volumeID := pvs[0].Spec.CSI.VolumeHandle
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		gomega.Expect(e2eVSphere.verifyDatastoreMatch(volumeID, sharedvmfsURL)).To(
			gomega.BeTrue(), "volume %v was created on wrong ds", sharedvmfsURL)

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()
		// TODO: Verify no orphan volumes are created
	})

	/*
		Verify EZT offline volume expansion
		Steps:
		1.	Create a SPBM policy with EZT volume allocation
		2.	Create a SC using policy created in step 1 and allowVolumeExpansion set to true
		3.	Create a 2g PVC using SC created in step 2, say pvc1
		4.	Wait and verify for pvc1 to be bound
		5.	Create a pod using pvc1 say pod1 and wait for it to be ready
		6.  Delete pod1
		7.	Expand pvc1
		8.	Wait for the PVC created in step 3 to reach FileSystemResizePending state
		9.	Create pod using pvc1 say pod2 and wait for it to be ready
		10. Wait and verify the file system resize on pvc1
		11.	Delete pod2,pvc1
		12.	Delete the SC created in step 2
		13.	Deleted the SPBM policy created in step 1
	*/
	t := "[csi-block-vanilla] Verify EZT offline volume expansion"
	ginkgo.It(t, func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		largeSize := os.Getenv(envDiskSizeLarge)
		if largeSize == "" {
			largeSize = diskSizeLarge
		}

		scParameters := make(map[string]string)
		rand.Seed(time.Now().UnixNano())
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

		ginkgo.By("Create a SPBM policy with EZT volume allocation")
		policyID, policyName := createVmfsStoragePolicy(
			ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		ginkgo.By("Create SC using policy created in step 1")
		ginkgo.By("Create a 2g PVC using SC created in step 2, say pvc1")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete the SC created in step 2")
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(
			client, []*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volHandle := pvs[0].Spec.CSI.VolumeHandle
		svcPVCName := pvs[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		ginkgo.By("Verify that the created CNS volume is compliant and has correct policy id")
		volumeID := pvs[0].Spec.CSI.VolumeHandle
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(e2eVSphere.verifyDatastoreMatch(volumeID, sharedvmfsURL)).To(
			gomega.BeTrue(), "volume %v was created on wrong ds", sharedvmfsURL)

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		pvcs2d := [][]*v1.PersistentVolumeClaim{}
		pvcs2d = append(pvcs2d, []*v1.PersistentVolumeClaim{pvclaim})
		ginkgo.By("Create a pod using pvc1 say pod1 and wait for it to be ready")
		pods := createMultiplePods(ctx, client, pvcs2d, true) // only 1 will be created here
		defer func() {
			ginkgo.By("Delete pod")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("Delete pod")
		deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)

		// Modify PVC spec to trigger volume expansion
		// We expand the PVC while no pod is using it to ensure offline expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}
		if guestCluster {
			ginkgo.By("Checking for PVC request size change on SVC PVC")
			b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(b).To(gomega.BeTrue())
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if guestCluster {
			ginkgo.By("Checking for resize on SVC PV")
			verifyPVSizeinSupervisor(svcPVCName, newSize)
		}

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if guestCluster {
			ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
			_, err = checkSvcPvcHasGivenStatusCondition(svcPVCName,
				true, v1.PersistentVolumeClaimFileSystemResizePending)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		newPods := createMultiplePods(ctx, client, pvcs2d, true)

		defer func() {
			ginkgo.By("Deleting the pod")
			err = fpod.DeletePodWithWait(client, newPods[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
		_, err = framework.LookForStringInPodExec(namespace, newPods[0].Name,
			[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		var fsSize int64

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFSSizeMb(f, newPods[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %s", fsSize)

		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time after pod creation
		if fsSize < diskSizeInMb {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}
		ginkgo.By("File system resize finished successfully")

		if guestCluster {
			ginkgo.By("Checking for PVC resize completion on SVC PVC")
			_, err = waitForFSResizeInSvc(svcPVCName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		framework.Logf("File system resize finished successfully")
		ginkgo.By("Verify that the expanded CNS volume is compliant")
		storagePolicyMatches, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", newPods[0].Name, namespace))
		deletePodsAndWaitForVolsToDetach(ctx, client, newPods, true)

	})

	/*
		Verify expansion during Thin to EZT, LZT to EZT conversion takes longer than vpxd timeout
		1. Create 2 SPBM policies with thin, and LZT volume allocation respectively, say pol1, pol2
		2. Create 2 SCs each with a SPBM policy created from step 1
		3. Create a PVC of 10g using each of the SCs created from step 2
		4. Wait for PVCs created in step 3 to be bound
		5. Change volume allocation of pol1 and pol2 to EZT and opt for immediate updation
		6. While updation in step 5 is still running, expand all PVCs created in step 3 such that
		   resize should take more than 40mins for PVCs updated to EZT allocation
		7. Wait for all PVCs created in step 3 to reach FileSystemResizePending state
		8. Delete the PVCs created in step 3
		9. Delete the SCs created in step 2
		10.Deleted the SPBM policies created in step 1
	*/
	ginkgo.It("[csi-block-vanilla] Verify expansion during Thin to EZT, LZT to EZT conversion takes longer than vpxd timeout", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		//largeSize := os.Getenv(envDiskSizeLarge)
		//if largeSize == "" {
		//	largeSize = diskSizeLarge
		//}

		scParameters := make(map[string]string)
		policyNames, svcPVCNames := []string{}, []string{}
		pvcs := []*v1.PersistentVolumeClaim{}
		scs := []*storagev1.StorageClass{}
		policyIDs := []*pbmtypes.PbmProfileId{}

		rand.Seed(time.Now().UnixNano())
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

		allocationTypes := []string{
			thinAllocType,
			lztAllocType,
		}

		setVpxdTaskTimeout(ctx, vpxdReducedTaskTimeoutSecsInt)
		defer func() {
			setVpxdTaskTimeout(ctx, 0)
		}()

		ginkgo.By("Create 2 SPBM policies with thin, and LZT volume allocation respectively")
		ginkgo.By("Create storageclasses using policies created in step 1")
		ginkgo.By("create a PVCs each using the storage policies created from step 2")
		for _, at := range allocationTypes {
			policyID, policyName := createVmfsStoragePolicy(
				ctx, pc, at, map[string]string{categoryName: tagName})
			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			scParameters[scParamStoragePolicyName] = policyName
			policyNames = append(policyNames, policyName)
			storageclass, pvclaim, err := createPVCAndStorageClass(client,
				namespace, nil, scParameters, "", nil, "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim)
			scs = append(scs, storageclass)
			policyIDs = append(policyIDs, policyID)

		}

		defer func() {
			ginkgo.By("Delete the SC created in step 2")
			for _, sc := range scs {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(
			client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvclaim := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[i].Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		for i, pv := range pvs {
			ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
			volumeID := pv.Spec.CSI.VolumeHandle
			svcPVCName := pvs[0].Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(volumeID)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			svcPVCNames = append(svcPVCNames, svcPVCName)
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i])
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			gomega.Expect(e2eVSphere.verifyDatastoreMatch(volumeID, sharedvmfsURL)).To(
				gomega.BeTrue(), "volume %v was created on wrong ds", sharedvmfsURL)
		}

		start := time.Now()
		ginkgo.By("Expand pvcs while changing volume allocation of policies created to EZT")
		var wg sync.WaitGroup
		currentPvcSize := pvcs[0].Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse(diskSize))

		wg.Add(len(pvcs) * 2)
		for i, pvc := range pvcs {
			go changeVolAllocationOfPolicyInParallel(ctx, pc, *policyIDs[i], eztAllocType, policyNames[i], &wg)
			go resize(client, pvc, pvc.Spec.Resources.Requests[v1.ResourceStorage], newSize, &wg)
		}
		wg.Wait()

		for i := range pvcs {
			framework.Logf("Waiting for controller volume resize to finish")
			err = waitForPvResizeForGivenPvc(pvcs[i], client, totalResizeWaitPeriod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if guestCluster {
				framework.Logf("Checking for resize on SVC PV")
				verifyPVSizeinSupervisor(svcPVCNames[i], newSize)
			}
		}

		ginkgo.By("Verify PVC expansion took longer than vpxd timeout")
		elapsed := time.Since(start)
		gomega.Expect(elapsed > time.Second*time.Duration(vpxdReducedTaskTimeoutSecsInt)).To(
			gomega.BeTrue(), "PVC creation was faster than vpxd timeout")

		for i := range pvcs {
			framework.Logf("Checking for conditions on pvc")
			pvcs[i], err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvcs[i].Name, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if guestCluster {
				framework.Logf("Checking for 'FileSystemResizePending' status condition on SVC PVC")
				_, err = checkSvcPvcHasGivenStatusCondition(svcPVCNames[i],
					true, v1.PersistentVolumeClaimFileSystemResizePending)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			/*var fsSize int64
			framework.Logf("Verify filesystem size for mount point /mnt/volume1 for pod %v", pods[i].Name)
			fsSize, err = getFSSizeMb(f, pods[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("File system size after expansion : %v", fsSize)
			gomega.Expect(fsSize > fsSizes[i]).To(gomega.BeTrue(),
				fmt.Sprintf(
					"filesystem size %v is not > than before expansion %v for pvc %q",
					fsSize, fsSizes[i], pvcs[i].Name))

			framework.Logf("File system resize finished successfully for pvc %v", pvcs[i].Name)*/
		}

		ginkgo.By("Verify that the expanded CNS volume is compliant")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
		}

		// TODO: Verify no orphan volumes are created
	})

})

// fillVolumesInPods fills the volumes in pods upto given percentage
func fillVolumeInPods(f *framework.Framework, pods []*v1.Pod) {
	for _, pod := range pods {
		size, err := getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		writeRandomDataOnPod(pod, size-100) // leaving 100m for FS metadata
	}
}

// writeRandomDataOnPod runs dd on the given pod and write count in Mib
func writeRandomDataOnPod(pod *v1.Pod, count int64) {
	cmd := []string{"--namespace=" + pod.Namespace, "-c", pod.Spec.Containers[0].Name, "exec", pod.Name, "--",
		"/bin/sh", "-c", "dd if=/dev/urandom of=/mnt/volume1/f1 bs=1M count=" + strconv.FormatInt(count, 10)}
	_ = framework.RunKubectlOrDie(pod.Namespace, cmd...)
}

// setVpxdTaskTimeout sets vpxd task timeout to given number of seconds
// Following cases will be handled here
//  1. Timeout is not set, and we want to set it
//  2. Timeout is set we want to clear it
//  3. different timeout is set, we want to change it
//  4. timeout is not set/set to a number, and that is what we want
//
// default task timeout is 40 mins
// if taskTimeout param is 0 we will remove the timeout entry in cfg file and default timeout will kick-in
func setVpxdTaskTimeout(ctx context.Context, taskTimeout int) {
	vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
	timeoutMatches := false
	diffTimeoutExists := false

	grepCmd := "grep '<timeout>' /etc/vmware-vpx/vpxd.cfg"
	framework.Logf("Invoking command '%v' on vCenter host %v", grepCmd, vcAddress)
	result, err := fssh.SSH(grepCmd, vcAddress, framework.TestContext.Provider)
	if err != nil {
		fssh.LogResult(result)
		err = fmt.Errorf("couldn't execute command: %s on vCenter host %v: %v", grepCmd, vcAddress, err)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// cmd to add the timeout value to the file
	sshCmd := fmt.Sprintf(
		"sed -i 's/<task>/<task>\\n    <timeout>%v<\\/timeout>/' /etc/vmware-vpx/vpxd.cfg", taskTimeout)
	if result.Code == 0 {
		grepCmd2 := fmt.Sprintf("grep '<timeout>%v</timeout>' /etc/vmware-vpx/vpxd.cfg", taskTimeout)
		framework.Logf("Invoking command '%v' on vCenter host %v", grepCmd2, vcAddress)
		result2, err := fssh.SSH(grepCmd2, vcAddress, framework.TestContext.Provider)
		if err != nil {
			fssh.LogResult(result)
			err = fmt.Errorf("couldn't execute command: %s on vCenter host %v: %v", grepCmd2, vcAddress, err)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if result2.Code == 0 {
			timeoutMatches = true
		} else {
			diffTimeoutExists = true
		}
	} else {
		if taskTimeout == 0 {
			timeoutMatches = true
		}
	}
	if timeoutMatches {
		framework.Logf("vpxd timeout already matches, nothing to do ...")
		return
	}
	if diffTimeoutExists {
		sshCmd = fmt.Sprintf(
			"sed -i 's/<timeout>[0-9]*<\\/timeout>/<timeout>%v<\\/timeout>/' /etc/vmware-vpx/vpxd.cfg", taskTimeout)
	}
	if taskTimeout == 0 {
		sshCmd = "sed -i '/<timeout>[0-9]*<\\/timeout>/d' /etc/vmware-vpx/vpxd.cfg"
	}

	framework.Logf("Invoking command '%v' on vCenter host %v", sshCmd, vcAddress)
	result, err = fssh.SSH(sshCmd, vcAddress, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		err = fmt.Errorf("couldn't execute command: %s on vCenter host %v: %v", sshCmd, vcAddress, err)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// restart vpxd after changing the timeout
	err = invokeVCenterServiceControl(restartOperation, vpxdServiceName, vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = waitVCenterServiceToBeInState(vpxdServiceName, vcAddress, svcRunningMessage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	connect(ctx, &e2eVSphere)
}

//
func changeVolAllocationOfPolicyInParallel(ctx context.Context, pc *pbm.Client, profileID pbmtypes.PbmProfileId,
	allocType string, profileName string, wg *sync.WaitGroup) {
	defer wg.Done()
	changeVolumeAllocationOfPolicy(ctx, pc, profileID, allocType, profileName)
}
