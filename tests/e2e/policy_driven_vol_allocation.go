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
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/pbm"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
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
