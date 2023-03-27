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
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
	ginkgo "github.com/onsi/ginkgo/v2"
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

var pc *pbm.Client
var spareSpace int64 = 200

var _ = ginkgo.Describe("[vol-allocation] Policy driven volume space allocation tests", func() {

	f := framework.NewDefaultFramework("e2e-spbm-policy")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimitScaleTest)
		}
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
	ginkgo.It("[csi-block-vanilla][csi-block-vanilla-parallelized][csi-guest][csi-supervisor]"+
		" Verify Thin, EZT, LZT volume creation via SPBM policies", func() {

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

		sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

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

		allocationTypes := []string{
			thinAllocType,
			eztAllocType,
			lztAllocType,
		}

		ginkgo.By("create 3 SPBM policies with thin, LZT, EZT volume allocation respectively")
		ginkgo.By("create 3 storage classes, each with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policies created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyName string
		var policyID *pbmtypes.PbmProfileId
		for _, at := range allocationTypes {
			policyID, policyName = createVmfsStoragePolicy(
				ctx, pc, at, map[string]string{categoryName: tagName})

			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			policyNames = append(policyNames, policyName)
		}

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, policyNames)
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, policyNames)
		}

		for _, policyName := range policyNames {
			if vanillaCluster {
				ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
				scParameters[scParamStoragePolicyName] = policyName
				storageclass, pvclaim, err = createPVCAndStorageClass(client,
					namespace, nil, scParameters, "", nil, "", false, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else if supervisorCluster {
				ginkgo.By("CNS_TEST: Running for WCP setup")
				// create resource quota
				createResourceQuota(client, namespace, rqLimit, policyName)
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				ginkgo.By("CNS_TEST: Running for GC setup")
				createResourceQuota(client, namespace, rqLimit, policyName)
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			pvcs = append(pvcs, pvclaim)
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			scs = append(scs, storageclass)
		}

		defer func() {
			if vanillaCluster {
				ginkgo.By("Delete the SCs created in step 2")
				for _, sc := range scs {
					err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		var volumeID string
		for i, pv := range pvs {
			volumeID = pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, sharedvmfsURL)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})

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
	ginkgo.It("[csi-block-vanilla][csi-guest][csi-supervisor] Fill LZT/EZT volume", func() {

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
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyName string
		var policyID *pbmtypes.PbmProfileId

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

		allocationTypes := []string{
			eztAllocType,
			lztAllocType,
		}

		ginkgo.By("create SPBM policies with LZT, EZT volume allocation respectively")
		ginkgo.By("Create SCs using policies created in step 1")
		ginkgo.By("create a large PVC each using the storage policies created from step 2")
		for _, at := range allocationTypes {
			policyID, policyName = createVmfsStoragePolicy(
				ctx, pc, at, map[string]string{categoryName: tagName})

			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			policyNames = append(policyNames, policyName)
		}

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, policyNames)
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, policyNames)
		}

		for _, policyName := range policyNames {
			if vanillaCluster {
				ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
				scParameters[scParamStoragePolicyName] = policyName
				storageclass, pvclaim, err = createPVCAndStorageClass(client,
					namespace, nil, scParameters, largeSize, nil, "", false, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else if supervisorCluster {
				ginkgo.By("CNS_TEST: Running for WCP setup")
				// create resource quota
				createResourceQuota(client, namespace, rqLimit, policyName)
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(client, namespace, nil, largeSize, storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				ginkgo.By("CNS_TEST: Running for GC setup")
				createResourceQuota(client, namespace, rqLimit, policyName)
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(client, namespace, nil, largeSize, storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			pvcs = append(pvcs, pvclaim)
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			scs = append(scs, storageclass)
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			if vanillaCluster {
				for _, sc := range scs {
					err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})
		}

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				volumeID := pvs[i].Spec.CSI.VolumeHandle
				if guestCluster {
					volumeID = getVolumeIDFromSupervisorCluster(pvs[i].Spec.CSI.VolumeHandle)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
				}
				err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
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
	ginkgo.It("[csi-block-vanilla][csi-guest][csi-supervisor] Verify large EZT volume creation which takes "+
		"longer than vpxd timeout", func() {

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

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

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

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, []string{policyName})
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, []string{policyName})
		}

		setVpxdTaskTimeout(ctx, vpxdReducedTaskTimeoutSecsInt)
		defer func() {
			setVpxdTaskTimeout(ctx, 0)
		}()

		ginkgo.By("Create SC using policy created in step 1")
		ginkgo.By("Create a large PVC using SC created in step 2, this should take more than vpxd task timeout")
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = policyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client,
				namespace, nil, scParameters, largeSize, nil, "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, policyName)
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(client, namespace, nil, largeSize, storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			createResourceQuota(client, namespace, rqLimit, policyName)
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(client, namespace, nil, largeSize, storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			if vanillaCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
		if guestCluster {
			volumeID = getVolumeIDFromSupervisorCluster(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})

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
		Verify EZT online volume expansion to a large size (should take >vpxd task timeout)
		Steps:
		1	Create a SPBM policy with EZT volume allocation
		2	Create a SC using policy created in step 1 and allowVolumeExpansion set to true
		3	Create a 2g PVC using SC created in step 2, say pvc1
		4	Wait and verify for pvc1 to be bound
		5	Create a pod using pvc1 say pod1 and wait for it to be ready
		6	Expand pvc1 to a large size this should take more than vpxd timeout
		7	Wait and verify the file system resize on pvc1
		8	Delete pod1
		9	Delete pvc1
		10	Delete the SC created in step 2
		11	Deleted the SPBM policy created in step 1
	*/
	ginkgo.It("[csi-block-vanilla][csi-guest][csi-supervisor] Verify EZT online volume expansion to a large size "+
		"which takes longer than vpxd timeout", func() {

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

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

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

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, []string{policyName})
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, []string{policyName})
		}

		setVpxdTaskTimeout(ctx, vpxdReducedTaskTimeoutSecsInt)
		defer func() {
			setVpxdTaskTimeout(ctx, 0)
		}()

		ginkgo.By("Create SC using policy created in step 1")
		ginkgo.By("Create a 2g PVC using SC created in step 2, say pvc1")
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = policyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client,
				namespace, nil, scParameters, "", nil, "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, policyName)
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			createResourceQuota(client, namespace, rqLimit, policyName)
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			if vanillaCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(
			client, []*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volume is compliant and has correct policy id")
		volumeID := pvs[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volumeID = getVolumeIDFromSupervisorCluster(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})

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

		ginkgo.By("Get filesystem size for mount point /mnt/volume1 before expansion")
		originalFsSize, err := getFSSizeMb(f, pods[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expand pvc1 to a large size this should take more than vpxd timeout")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse(largeSize))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		start := time.Now()
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())
		if !guestCluster {
			err = waitForPvResize(pvs[0], client, newSize, totalResizeWaitPeriod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			svcPVCName := pvs[0].Spec.CSI.VolumeHandle
			err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
			framework.ExpectNoError(err, "While waiting for pvc resize to finish")

			ginkgo.By("Checking for resize on SVC PV")
			verifyPVSizeinSupervisor(svcPVCName, newSize)
			_, err = waitForFSResizeInSvc(svcPVCName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		elapsed := time.Since(start)

		ginkgo.By("Verify PVC expansion took longer than vpxd timeout")
		gomega.Expect(elapsed > time.Second*time.Duration(vpxdReducedTaskTimeoutSecsInt)).To(
			gomega.BeTrue(), "PVC expansion was faster than vpxd timeout")

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(pvclaim.Status.Conditions).To(
			gomega.BeEmpty(), "pvc should not have conditions but it has: %v", pvclaim.Status.Conditions)

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err := getFSSizeMb(f, pods[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %v", fsSize)
		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time
		gomega.Expect(fsSize > originalFsSize).To(gomega.BeTrue(),
			fmt.Sprintf(
				"filesystem size %v is not > than before expansion %v for pvc %q",
				fsSize, originalFsSize, pvclaim.Name))

		framework.Logf("File system resize finished successfully")
		ginkgo.By("Verify that the expanded CNS volume is compliant")
		storagePolicyMatches, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)

	})

	/*
		Verify online LZT/EZT volume expansion of attached volumes with IO
		Steps:
		1	Create a SPBM policies with EZT and LZT volume allocation
		2	Create SCs using policies created in step 1 and allowVolumeExpansion set to true
		3	Create 2g PVCs using each of the SCs created in step 2
		4	Wait and verify for pvcs to be bound
		5	Create a pod using pvcs say pod1 and wait for it to be ready
		6	Expand pvcs while writing some data on them
		7	Wait and verify the file system resize on pvcs
		8	Verify the data on the PVCs match what was written in step 7
		9	Delete pod1
		10	Delete pvc1
		11	Delete the SC created in step 2
		12	Deleted the SPBM policy created in step 1
	*/
	ginkgo.It("[csi-block-vanilla][csi-guest][csi-supervisor] Verify online LZT/EZT volume expansion of "+
		"attached volumes with IO", func() {

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
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyName string
		var policyID *pbmtypes.PbmProfileId

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

		allocationTypes := []string{
			eztAllocType,
			lztAllocType,
		}

		ginkgo.By("create SPBM policies with LZT, EZT volume allocation respectively")
		ginkgo.By("Create SCs using policies created in step 1")
		ginkgo.By("create a PVC each using the storage policies created from step 2")
		for _, at := range allocationTypes {
			policyID, policyName = createVmfsStoragePolicy(
				ctx, pc, at, map[string]string{categoryName: tagName})

			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			policyNames = append(policyNames, policyName)
		}

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, policyNames)
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, policyNames)
		}

		for _, policyName := range policyNames {
			if vanillaCluster {
				ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
				scParameters[scParamStoragePolicyName] = policyName
				storageclass, pvclaim, err = createPVCAndStorageClass(client,
					namespace, nil, scParameters, "", nil, "", true, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else if supervisorCluster {
				ginkgo.By("CNS_TEST: Running for WCP setup")
				// create resource quota
				createResourceQuota(client, namespace, rqLimit, policyName)
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				ginkgo.By("CNS_TEST: Running for GC setup")
				createResourceQuota(client, namespace, rqLimit, policyName)
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			pvcs = append(pvcs, pvclaim)
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			scs = append(scs, storageclass)
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			if vanillaCluster {
				for _, sc := range scs {
					err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})
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

		ginkgo.By("Expand pvcs while writing some data on them")
		var wg sync.WaitGroup
		currentPvcSize := pvcs[0].Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse(diskSize))
		fsSizes := []int64{}

		for _, pod := range pods {
			originalSizeInMb, err := getFSSizeMb(f, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			fsSizes = append(fsSizes, originalSizeInMb)
		}
		wg.Add(len(pods) * 2)
		for i, pod := range pods {
			go writeKnownData2PodInParallel(f, pod, testdataFile, &wg, fsSizes[i]-spareSpace)
			go resize(client, pvcs[i], pvcs[i].Spec.Resources.Requests[v1.ResourceStorage], newSize, &wg)
		}
		wg.Wait()

		ginkgo.By("Wait and verify the file system resize on pvcs")
		for i := range pvcs {
			framework.Logf("Waiting for file system resize to finish for pvc %v", pvcs[i].Name)
			pvcs[i], err = waitForFSResize(pvcs[i], client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pvcConditions := pvcs[i].Status.Conditions
			expectEqual(len(pvcConditions), 0, "pvc %v should not have status conditions", pvcs[i].Name)

			var fsSize int64
			framework.Logf("Verify filesystem size for mount point /mnt/volume1 for pod %v", pods[i].Name)
			fsSize, err = getFSSizeMb(f, pods[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("File system size after expansion : %v", fsSize)
			gomega.Expect(fsSize > fsSizes[i]).To(gomega.BeTrue(),
				fmt.Sprintf(
					"filesystem size %v is not > than before expansion %v for pvc %q",
					fsSize, fsSizes[i], pvcs[i].Name))

			framework.Logf("File system resize finished successfully for pvc %v", pvcs[i].Name)
		}
		ginkgo.By("Verify that the expanded CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
		}

		ginkgo.By("Verify the data on the PVCs match what was written in step 7")
		for i, pod := range pods {
			verifyKnownDataInPod(f, pod, testdataFile, fsSizes[i]-spareSpace)
		}
	})

	/*
		Relocate volume to another same type datastore
		Steps for offline volumes:
		1	Create a vmfs SPBM policy with thin volume allocation
		2	Create a SC using policy created in step 1
		3	Create a PVC using SC created in step 2
		4	Verify that pvc created in step 3 is bound
		5	Create a pod, say pod1 using pvc created in step 4.
		6	write some data to the volume.
		7	delete pod1.
		8	Relocate volume to another vmfs datastore.
		9	Recreate pod1
		10	Verify pod1 is running and pvc1 is accessible and verify the data written in step 6
		11	Delete pod1
		12	Delete pvc, sc and SPBM policy created for this test

		Steps for online volumes:
		1	Create a vmfs SPBM policies with thin volume allocation
		2	Create a SC using policy created in step 1
		3	Create a PVC using SC created in step 2
		4	Verify that pvc created in step 3 is bound
		5	Create a pod, say pod1 using pvc created in step 4.
		6	write some data to the volume.
		7	Relocate volume to another vmfs datastore.
		8	Verify the data written in step 6
		9	Delete pod1
		10	Delete pvc, sc and SPBM policy created for this test
	*/
	ginkgo.It("[csi-supervisor][csi-block-vanilla][csi-guest]"+
		" Relocate volume to another same type datastore", func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, sharedvmfs2URL := "", ""
		var datastoreUrls []string
		var policyName string

		sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedvmfs2URL = os.Getenv(envSharedVMFSDatastore2URL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastore2URL))
		}
		datastoreUrls = append(datastoreUrls, sharedvmfsURL)
		datastoreUrls = append(datastoreUrls, sharedvmfs2URL)

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

		attachTagToDS(ctx, tagID, sharedvmfs2URL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfs2URL)
		}()

		ginkgo.By("create SPBM policy with thin/ezt volume allocation")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyID *pbmtypes.PbmProfileId

		policyID, policyName = createVmfsStoragePolicy(
			ctx, pc, thinAllocType, map[string]string{categoryName: tagName})

		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		policyNames = append(policyNames, policyName)

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, policyNames)
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, policyNames)
		}

		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = policyName
			storageclass, err = createStorageClass(client,
				scParameters, nil, "", "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			createResourceQuota(client, namespace, rqLimit, policyName)
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			createResourceQuota(client, namespace, rqLimit, policyName)
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcs = append(pvcs, pvclaim, pvclaim2)
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim2})

		defer func() {
			if vanillaCluster {
				ginkgo.By("Delete the SCs created in step 2")
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")

		volIds := []string{}
		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for _, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pvs[0].Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			volIds = append(volIds, volumeID)
		}

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volIds[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
		ginkgo.By("verify we can read and write on the PVCs")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		defer func() {
			ginkgo.By("Delete the pod created")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

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

		ginkgo.By("Writing known data to pods")
		for _, pod := range pods {
			writeKnownData2Pod(f, pod, testdataFile)
		}

		ginkgo.By("delete pod1")
		pvcName := pods[1].Spec.Volumes[0].VolumeSource.PersistentVolumeClaim.ClaimName // for recreating pod1
		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pods[1]}, true)

		ginkgo.By("Relocate volume to another vmfs datastore")
		for _, volumeID := range volIds {
			dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
			framework.Logf("Volume is present on %s", dsUrlWhereVolumeIsPresent)
			e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)
			destDsUrl := ""
			for _, dsurl := range datastoreUrls {
				if dsurl != dsUrlWhereVolumeIsPresent {
					destDsUrl = dsurl
					break
				}
			}

			framework.Logf("dest url for volume id %s is %s", volumeID, destDsUrl)
			dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)
			err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
		}

		ginkgo.By("Recreate pod1")
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcs2d2 := [][]*v1.PersistentVolumeClaim{{pvc}}
		podsNew := createMultiplePods(ctx, client, pvcs2d2, true)
		pods[1] = podsNew[0]

		ginkgo.By("Verify the data written to the pods")
		for _, pod := range pods {
			verifyKnownDataInPod(f, pod, testdataFile)
		}

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
	ginkgo.It("[csi-guest][csi-supervisor][csi-block-vanilla] Verify EZT offline volume expansion", func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		scParameters := make(map[string]string)
		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

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

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, []string{policyName})
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, []string{policyName})
		}

		ginkgo.By("Create SC using policy created in step 1")
		ginkgo.By("Create a 2g PVC using SC created in step 2, say pvc1")

		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = policyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client,
				namespace, nil, scParameters, "", nil, "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, policyName)
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			createResourceQuota(client, namespace, rqLimit, policyName)
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			if vanillaCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(
			client, []*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volumeID := pvs[0].Spec.CSI.VolumeHandle
		svcPVCName := pvs[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volumeID = getVolumeIDFromSupervisorCluster(volumeID)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}

		ginkgo.By("Verify that the created CNS volume is compliant and has correct policy id")
		storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})

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
		newSize.Add(resource.MustParse(diskSize))
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
		verify volume allocation change post snapshot deletion works fine
		Steps for detached volumes:
		1	Create 2 SPBM policies with thin and LZT volume allocation respectively, say pol1, pol2
		2	Create 2 SCs each with a SPBM policy created from step 1
		3	Create a PVC of 10g using each of the SCs created from step 2
		4	Wait for PVCs created in step 3 to be bound
		5	Create a snapshot class
		6	Create a snapshots of all PVCs created in step 3
		7	Change volume allocation of pol1 and pol2 to EZT and apply the changes
		8	Verify the volume allocation type changes fail
		9	Delete snapshots created in step 6
		10	Apply volume allocation type changes again and verify that it is successful this time
		11	Delete the PVCs created in step 3
		12	Delete the SCs created in step 2
		13	Delete the SPBM policies created in step 1

		Steps for attached volumes:
		1	Create 2 SPBM policies with thin, and LZT volume allocation respectively, say pol1, pol2
		2	Create 2 SCs each with a SPBM policy created from step 1
		3	Create a PVC of 10g using each of the SCs created from step 2
		4	Wait for PVCs created in step 3 to be bound
		5	Create pods using PVCs created in step 3
		6	Create a snapshot class
		7	Create a snapshots of all PVCs created in step 3
		8	Change volume allocation of pol1 and pol2 to EZT and apply the changes
		9	Verify the volume allocation type changes fail
		10	Delete snapshots created in step 6
		11	Apply volume allocation type changes again and verify that it is successful this time
		12	Delete pods created in step 5
		13	Delete the PVCs created in step 3
		14	Delete the SCs created in step 2
		15	Delete the SPBM policies created in step 1
	*/
	ginkgo.It("[csi-block-vanilla] verify volume allocation change post snapshot deletion works fine", func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		scParameters := make(map[string]string)
		policyNames := []string{}
		policyIds := []*pbmtypes.PbmProfileId{}
		pvcs := []*v1.PersistentVolumeClaim{}
		scs := []*storagev1.StorageClass{}
		pvcs2d := [][]*v1.PersistentVolumeClaim{}

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

		allocationTypes := []string{
			thinAllocType,
			lztAllocType,
		}

		ginkgo.By("create 2 SPBM policies with thin, LZT volume allocation respectively")
		ginkgo.By("Create 2 SCs each with a SPBM policy created from step 1")
		ginkgo.By("Create two PVCs of 10g using each of the SCs created from step 2")
		for _, at := range allocationTypes {
			policyID, policyName := createVmfsStoragePolicy(
				ctx, pc, at, map[string]string{categoryName: tagName})
			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			scParameters[scParamStoragePolicyName] = policyName
			policyNames = append(policyNames, policyName)
			policyIds = append(policyIds, policyID)
			storageclass, pvclaim, err := createPVCAndStorageClass(client,
				namespace, nil, scParameters, "", nil, "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim)
			scs = append(scs, storageclass)
			pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim2)
			pvcs2d = append(pvcs2d, []*v1.PersistentVolumeClaim{pvclaim})
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

		volIds := []string{}
		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i/2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})
			volIds = append(volIds, volumeID)
		}

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for _, pvc := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			for i := range pvcs {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[i].Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating 2 pods to attach 2 PVCs")
		pods := createMultiplePods(ctx, client, pvcs2d, true)

		defer func() {
			ginkgo.By("Delete pods")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

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

		snaps := []*snapV1.VolumeSnapshot{}
		snapIDs := []string{}
		ginkgo.By("Create a snapshots of all PVCs created in step 3")
		for i, pvc := range pvcs {
			volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvc.Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)
			snaps = append(snaps, volumeSnapshot)
			ginkgo.By("Verify volume snapshot is created")
			volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("snapshot restore size is : %s", volumeSnapshot.Status.RestoreSize.String())
			gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(pvc.Spec.Resources.Requests[v1.ResourceStorage])).To(
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

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = verifySnapshotIsCreatedInCNS(volIds[i], snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			if len(snaps) > 0 {
				for _, snap := range snaps {
					framework.Logf("Delete volume snapshot %v", snap.Name)
					err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(
						ctx, snap.Name, metav1.DeleteOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				for i, snapshotId := range snapIDs {
					framework.Logf("Verify snapshot entry %v is deleted from CNS for volume %v", snapshotId, volIds[i])
					err = waitForCNSSnapshotToBeDeleted(volIds[i], snapshotId)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		ginkgo.By("Change volume allocation of pol1 and pol2 to EZT and apply the changes")
		ginkgo.By("Verify the volume allocation type changes fail")
		for i, volId := range volIds {
			framework.Logf("updating policy %v with %v allocation type", policyNames[i/2], eztAllocType)
			err = updateVmfsPolicyAlloctype(ctx, pc, eztAllocType, policyNames[i/2], policyIds[i/2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf(
				"trying to reconfigure volume %v with policy %v which is expected to fail", volId, policyNames[i/2])
			err = e2eVSphere.reconfigPolicy(ctx, volId, policyIds[i/2].UniqueId)
			framework.Logf("reconfigure volume %v with policy %v errored out with:\n%v", volId, policyNames[i/2], err)
			gomega.Expect(err).To(gomega.HaveOccurred())
		}

		ginkgo.By("Delete snapshots created in step 6")
		for _, snap := range snaps {
			framework.Logf("Delete volume snapshot %v", snap.Name)
			err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(
				ctx, snap.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		for i, snapshotId := range snapIDs {
			framework.Logf("Verify snapshot entry %v is deleted from CNS for volume %v", snapshotId, volIds[i])
			err = waitForCNSSnapshotToBeDeleted(volIds[i], snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		snaps = []*snapV1.VolumeSnapshot{}
		ginkgo.By("Apply volume allocation type changes again and verify that it is successful this time")
		for i, volId := range volIds {
			framework.Logf("trying to reconfigure volume %v with policy %v", volId, policyNames[i/2])
			err = e2eVSphere.reconfigPolicy(ctx, volIds[i], policyIds[i/2].UniqueId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

	/*
		Verify expansion during Thin -> EZT, LZT -> EZT conversion (should take >vpxd task timeout)
		Steps for offline volumes:
		1	Create 2 SPBM policies with thin, and LZT volume allocation respectively, say pol1, pol2
		2	Create 2 SCs each with a SPBM policy created from step 1
		3	Create a PVC of 10g using each of the SCs created from step 2
		4	Wait for PVCs created in step 3 to be bound
		5	Change volume allocation of pol1 and pol2 to EZT and opt for immediate updation
		6	While updation in step 5 is still running, expand all PVCs created in step 3 such that resize should take
		    more than vpxd task timeout for PVCs updated to EZT allocation
		7	Wait for all PVCs created in step 3 to reach FileSystemResizePending state
		8	Delete the PVCs created in step 3
		9	Delete the SCs created in step 2
		10	Deleted the SPBM policies created in step 1

		Steps for online volumes:
		1	Create 2 SPBM policies with thin, and LZT volume allocation respectively, say pol1, pol2
		2	Create 2 SCs each with a SPBM policy created from step 1
		3	Create a PVC of 10g using each of the SCs created from step 2
		4	Wait for PVCs created in step 3 to be bound
		5	Create pods using PVCs created in step 4
		6	Change volume allocation of pol1 and pol2 to EZT and opt for immediate updation
		7	While updation in step 5 is still running, expand all PVCs created in step 3 such that resize should take
		    more than vpxd task timeout for PVCs updated to EZT allocation
		8	Wait for file system resize to complete on all PVCs created in step 3
		9	Delete pods created in step 4
		10	Delete the PVCs created in step 3
		11	Delete the SCs created in step 2
		12	Deleted the SPBM policies created in step 1
	*/
	ginkgo.It("[csi-block-vanilla][csi-guest][csi-supervisor] Verify expansion during Thin -> EZT, LZT -> EZT"+
		" conversion (should take >vpxd task timeout)", func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		scParameters := make(map[string]string)
		policyNames := []string{}
		policyIds := []*pbmtypes.PbmProfileId{}
		pvcs := []*v1.PersistentVolumeClaim{}
		scs := []*storagev1.StorageClass{}
		pvcs2d := [][]*v1.PersistentVolumeClaim{}
		largeSize := os.Getenv(envDiskSizeLarge)
		pvc10g := "10Gi"
		if largeSize == "" {
			largeSize = diskSizeLarge
		}

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

		allocationTypes := []string{
			thinAllocType,
			lztAllocType,
		}

		ginkgo.By("create 2 SPBM policies with thin, LZT volume allocation respectively")
		for _, at := range allocationTypes {
			policyID, policyName := createVmfsStoragePolicy(
				ctx, pc, at, map[string]string{categoryName: tagName})
			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			scParameters[scParamStoragePolicyName] = policyName
			policyNames = append(policyNames, policyName)
			policyIds = append(policyIds, policyID)
		}

		defer func() {
			for _, policyID := range policyIds {
				deleteStoragePolicy(ctx, pc, policyID)
			}
		}()

		ginkgo.By("Create 2 SCs each with a SPBM policy created from step 1")
		if vanillaCluster {
			for i, policyName := range policyNames {
				scParameters[scParamStoragePolicyName] = policyName
				policyNames = append(policyNames, policyName)
				policyIds = append(policyIds, policyIds[i])
				storageclass, err := createStorageClass(client,
					scParameters, nil, "", "", true, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				scs = append(scs, storageclass)
			}
		} else if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, policyNames)
			for _, policyName := range policyNames {
				createResourceQuota(client, namespace, rqLimit, policyName)
				storageclass, err := client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				scs = append(scs, storageclass)
			}
		} else if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(svcClient, ctx, svNamespace, policyNames)
			for _, policyName := range policyNames {
				createResourceQuota(svcClient, svNamespace, rqLimit, policyName)
				storageclass, err := svcClient.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				scs = append(scs, storageclass)
			}
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			if vanillaCluster {
				for _, sc := range scs {
					err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		ginkgo.By("Create two PVCs of 10g using each of the SCs created from step 2")
		for _, sc := range scs {
			pvclaim, err := createPVC(client, namespace, nil, pvc10g, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim)
			pvclaim2, err := createPVC(client, namespace, nil, pvc10g, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim2)
			// one pvc for online case and one more pvc for offline case
			pvcs2d = append(pvcs2d, []*v1.PersistentVolumeClaim{pvclaim})
		}

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volIds := []string{}
		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pvs[0].Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i/2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})
			volIds = append(volIds, volumeID)
		}

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for _, pvc := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			for _, volId := range volIds {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volId)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating 2 pods to attach 2 PVCs")
		pods := createMultiplePods(ctx, client, pvcs2d, true)

		defer func() {
			ginkgo.By("Delete pods")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		fsSizes := []int64{}
		for _, pod := range pods {
			originalSizeInMb, err := getFSSizeMb(f, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			fsSizes = append(fsSizes, originalSizeInMb)
		}

		ginkgo.By("Change volume allocation of pol1 and pol2 to EZT")
		for i, policyId := range policyIds {
			err = updateVmfsPolicyAlloctype(ctx, pc, eztAllocType, policyNames[i], policyId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Apply the volume allocation changes and while it is still running, expand PVCs created to a" +
			" large size such that the resize takes more than vpxd task timeout")
		var wg sync.WaitGroup
		wg.Add(len(volIds))
		n := resource.MustParse(largeSize)
		sizeInInt, b := n.AsInt64()
		gomega.Expect(b).To(gomega.BeTrue())
		// since we are creating 4 volumes reducing the size of each expansion by half to start with
		newSize := *(resource.NewQuantity(sizeInInt/2, resource.BinarySI))
		start := time.Now()
		for i, volId := range volIds {
			go reconfigPolicyParallel(ctx, volId, policyIds[i/2].UniqueId, &wg)
		}
		wg.Wait()

		wg.Add(len(volIds))
		for _, pvc := range pvcs {
			go resize(client, pvc, pvc.Spec.Resources.Requests[v1.ResourceStorage], newSize, &wg)
		}
		wg.Wait()
		for i := range pvcs {
			err = waitForPvResize(pvs[i], client, newSize, totalResizeWaitPeriod*2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "resize exceeded timeout")
		}
		elapsed := time.Since(start)

		ginkgo.By("Verify PVC expansion took longer than vpxd timeout")
		gomega.Expect(elapsed > time.Second*time.Duration(vpxdReducedTaskTimeoutSecsInt)).To(
			gomega.BeTrue(), "PVC expansion was faster than vpxd timeout")

		ginkgo.By("Wait and verify the file system resize on pvcs")
		for i := range pods {
			framework.Logf("Waiting for file system resize to finish for pvc %v", pvcs[i*2].Name)
			pvcs[i*2], err = waitForFSResize(pvcs[i*2], client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			fsSize, err := getFSSizeMb(f, pods[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("File system size after expansion : %v", fsSize)
			gomega.Expect(fsSize > fsSizes[i]).To(gomega.BeTrue(),
				fmt.Sprintf(
					"filesystem size %v is not > than before expansion %v for pvc %q",
					fsSize, fsSizes[i], pvcs[i*2].Name))

			framework.Logf("File system resize finished successfully for pvc %v", pvcs[i*2].Name)
		}

	})

})

// fillVolumesInPods fills the volumes in pods after leaving 100m for FS metadata
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

	govmomiClient := newClient(ctx, &e2eVSphere)
	pc = newPbmClient(ctx, govmomiClient)
}

// writeKnownData2PodInParallel writes known 1mb data to a file in given pod's volume until 200mb is left in the volume
// in parallel
func writeKnownData2PodInParallel(
	f *framework.Framework, pod *v1.Pod, testdataFile string, wg *sync.WaitGroup, size ...int64) {

	defer wg.Done()
	writeKnownData2Pod(f, pod, testdataFile, size...)
}

// writeKnownData2Pod writes known 1mb data to a file in given pod's volume until 200mb is left in the volume
func writeKnownData2Pod(f *framework.Framework, pod *v1.Pod, testdataFile string, size ...int64) {
	_ = framework.RunKubectlOrDie(pod.Namespace, "cp", testdataFile, fmt.Sprintf(
		"%v/%v:/mnt/volume1/testdata", pod.Namespace, pod.Name))
	fsSize, err := getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	iosize := fsSize - spareSpace
	if len(size) != 0 {
		iosize = size[0]
	}
	iosize = iosize / 100 * 100 // will keep it as multiple of 100
	framework.Logf("Total IO size: %v", iosize)
	for i := int64(0); i < iosize; i = i + 100 {
		seek := fmt.Sprintf("%v", i)
		cmd := []string{"--namespace=" + pod.Namespace, "-c", pod.Spec.Containers[0].Name, "exec", pod.Name, "--",
			"/bin/sh", "-c", "dd if=/mnt/volume1/testdata of=/mnt/volume1/f1 bs=1M count=100 seek=" + seek}
		_ = framework.RunKubectlOrDie(pod.Namespace, cmd...)
	}
	cmd := []string{"--namespace=" + pod.Namespace, "-c", pod.Spec.Containers[0].Name, "exec", pod.Name, "--",
		"/bin/sh", "-c", "rm /mnt/volume1/testdata"}
	_ = framework.RunKubectlOrDie(pod.Namespace, cmd...)
}

// verifyKnownDataInPod verify known data on a file in given pod's volume in 100mb loop
func verifyKnownDataInPod(f *framework.Framework, pod *v1.Pod, testdataFile string, size ...int64) {
	fsSize, err := getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	iosize := fsSize - spareSpace
	if len(size) != 0 {
		iosize = size[0]
	}
	iosize = iosize / 100 * 100 // will keep it as multiple of 100
	framework.Logf("Total IO size: %v", iosize)
	for i := int64(0); i < iosize; i = i + 100 {
		skip := fmt.Sprintf("%v", i)
		cmd := []string{"--namespace=" + pod.Namespace, "-c", pod.Spec.Containers[0].Name, "exec", pod.Name, "--",
			"/bin/sh", "-c", "dd if=/mnt/volume1/f1 of=/mnt/volume1/testdata bs=1M count=100 skip=" + skip}
		_ = framework.RunKubectlOrDie(pod.Namespace, cmd...)
		_ = framework.RunKubectlOrDie(pod.Namespace, "cp",
			fmt.Sprintf("%v/%v:/mnt/volume1/testdata", pod.Namespace, pod.Name),
			testdataFile+pod.Name)
		framework.Logf("Running diff with source file and file from pod %v for 100M starting %vM", pod.Name, skip)
		op, err := exec.Command("diff", testdataFile, testdataFile+pod.Name).Output()
		framework.Logf("diff: ", op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(op)).To(gomega.BeZero())
	}
}

func reconfigPolicyParallel(ctx context.Context, volID string, policyId string, wg *sync.WaitGroup) {
	defer wg.Done()
	err := e2eVSphere.reconfigPolicy(ctx, volID, policyId)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}
