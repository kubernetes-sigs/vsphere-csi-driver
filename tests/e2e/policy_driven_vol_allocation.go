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

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/pbm"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	"golang.org/x/crypto/ssh"
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

var pc *pbm.Client
var spareSpace int64 = 200

var _ = ginkgo.Describe("[vol-allocation] Policy driven volume space allocation tests", func() {
	f := framework.NewDefaultFramework("e2e-spbm-policy")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	var (
		client              clientset.Interface
		namespace           string
		labelKey            string
		labelValue          string
		eztVsandPvcName     = "pvc-vsand-ezt-"
		lztVsandPvcName     = "pvc-vsand-lzt-"
		eztVsandPodName     = "pod-vsand-ezt-"
		lztVsandPodName     = "pod-vsand-lzt-"
		resourceQuotaLimit  = "300Gi"
		svcMasterIp         string
		sshWcpConfig        *ssh.ClientConfig
		svcNamespace        string
		pandoraSyncWaitTime int
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)

		bootstrap()

		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		govmomiClient := newClient(ctx, &e2eVSphere)
		pc = newPbmClient(ctx, govmomiClient)

		vsanDirectSetup := os.Getenv(envVsanDirectSetup)
		if vsanDirectSetup == "VSAN_DIRECT" {
			wcpVsanDirectCluster = true
		}
		labelKey = "app"
		labelValue = "e2e-labels"

		if wcpVsanDirectCluster {
			svcNamespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
			svcMasterIp = GetAndExpectStringEnvVar(svcMasterIP)
			svcMasterPwd := GetAndExpectStringEnvVar(svcMasterPassword)
			framework.Logf("svc master ip: %s", svcMasterIp)
			sshWcpConfig = &ssh.ClientConfig{
				User: rootUser,
				Auth: []ssh.AuthMethod{
					ssh.Password(svcMasterPwd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}
		}

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if supervisorCluster {
			dumpSvcNsEventsOnTestFailure(client, namespace)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}
		if wcpVsanDirectCluster && supervisorCluster {
			framework.Logf("Cleaning pods and pvc from namespace: %s", namespace)
			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err := client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("Deleting pod: %s", pod.Name)
				err = fpod.DeletePodWithWait(ctx, client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			pvcList := getAllPVCFromNamespace(client, namespace)
			for _, pvc := range pvcList.Items {
				framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace),
					"Failed to delete PVC", pvc.Name)
			}

		}
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
		"[csi-wcp-vsan-direct][ef-vks-thickthin][cf-vanilla-block] Verify Thin,"+
		"EZT, LZT volume creation via SPBM "+
		"policies", ginkgo.Label(p0, vanilla, block, thickThin, wcp, tkg, windows, stable, vsanDirect, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, vsanDDatstoreURL := "", ""
		var allocationTypes []string
		scParameters := make(map[string]string)
		policyNames := []string{}
		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		scs := []*storagev1.StorageClass{}

		if wcpVsanDirectCluster && supervisorCluster {
			vsanDDatstoreURL = os.Getenv(envVsanDDatastoreURL)
			if vsanDDatstoreURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envVsanDDatastoreURL))
			}

		} else {
			sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
			if sharedvmfsURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
			}
		}

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		randomStr := strconv.Itoa(rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		if wcpVsanDirectCluster && supervisorCluster {
			attachTagToDS(ctx, tagID, vsanDDatstoreURL)
			defer func() {
				detachTagFromDS(ctx, tagID, vsanDDatstoreURL)
			}()
		} else {
			attachTagToDS(ctx, tagID, sharedvmfsURL)
			defer func() {
				detachTagFromDS(ctx, tagID, sharedvmfsURL)
			}()
		}
		if wcpVsanDirectCluster && supervisorCluster {
			allocationTypes = []string{
				eztAllocType,
				lztAllocType,
			}
		} else {
			allocationTypes = []string{
				thinAllocType,
				eztAllocType,
				lztAllocType,
			}
		}

		pvcVsandNames := []string{
			eztVsandPvcName + randomStr,
			lztVsandPvcName + randomStr,
		}

		podVsandNames := []string{
			eztVsandPodName + randomStr,
			lztVsandPodName + randomStr,
		}

		// Vsan direct only supports ezt and lzt volume allocation
		if wcpVsanDirectCluster && supervisorCluster {
			ginkgo.By("create 2 SPBM policies with LZT, EZT volume allocation respectively")
		} else {
			ginkgo.By("create 3 SPBM policies with thin, LZT, EZT volume allocation respectively")
		}
		ginkgo.By("create a storage class with each SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pod *v1.Pod
		var policyName string
		var policyID *pbmtypes.PbmProfileId
		pods := []*v1.Pod{}

		for _, at := range allocationTypes {
			if wcpVsanDirectCluster && supervisorCluster {
				policyID, policyName = createVsanDStoragePolicy(
					ctx, pc, at, map[string]string{categoryName: tagName})
			} else {
				policyID, policyName = createVmfsStoragePolicy(
					ctx, pc, at, map[string]string{categoryName: tagName})
			}
			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()

			policyNames = append(policyNames, policyName)
		}

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, policyNames, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			for _, policyName := range policyNames {
				setStoragePolicyQuota(ctx, restClientConfig, policyName, namespace, resourceQuotaLimit)
			}
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, policyNames, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			for _, policyName := range policyNames {
				setStoragePolicyQuota(ctx, restClientConfig, policyName, svNamespace, resourceQuotaLimit)
			}
		}

		for i, policyName := range policyNames {
			if vanillaCluster {
				ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
				scParameters[scParamStoragePolicyName] = policyName
				storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
					namespace, nil, scParameters, "", nil, "", false, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvcs = append(pvcs, pvclaim)
				pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			} else if supervisorCluster {
				ginkgo.By("CNS_TEST: Running for WCP setup")

				if wcpVsanDirectCluster {
					storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = createVsanDPvcAndPod(sshWcpConfig, svcMasterIp, svcNamespace,
						pvcVsandNames[i], podVsandNames[i], policyName, "")
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else {
					storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvcs = append(pvcs, pvclaim)
					pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
				}
			} else {
				ginkgo.By("CNS_TEST: Running for GC setup")

				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvcs = append(pvcs, pvclaim)
				pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			}

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

		if wcpVsanDirectCluster && supervisorCluster {
			pvcList := getAllPVCFromNamespace(client, namespace)
			for _, pvc := range pvcList.Items {
				pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvcs = append(pvcs, pvclaim)
				pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			}
			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpod.WaitTimeoutForPodRunningInNamespace(ctx, client, pod.Name, namespace, pollTimeout*2)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pods = append(pods, pod)
			}
		}

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
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
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")
			if wcpVsanDirectCluster && supervisorCluster {
				framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, vsanDDatstoreURL)
				e2eVSphere.verifyDatastoreMatch(volumeID, []string{vsanDDatstoreURL})

			} else {
				framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, sharedvmfsURL)
				e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})
			}
		}

		defer func() {
			ginkgo.By("Delete pods created before terminating the test")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)

			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				volumeID = pvs[i].Spec.CSI.VolumeHandle
				if guestCluster {
					volumeID = getVolumeIDFromSupervisorCluster(pvs[i].Spec.CSI.VolumeHandle)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
				}
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		if !wcpVsanDirectCluster {
			ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
			ginkgo.By("verify we can read and write on the PVCs")
			pods = createMultiplePods(ctx, client, pvclaims2d, true)
		}

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
	ginkgo.It("[csi-block-vanilla][csi-guest][csi-supervisor][csi-wcp-vsan-direct]"+
		" Fill LZT/EZT "+
		"volume", ginkgo.Label(p0, vanilla, block, thickThin, wcp, tkg, windows, stable, vsanDirect, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, vsanDDatstoreURL := "", ""

		if wcpVsanDirectCluster && supervisorCluster {
			vsanDDatstoreURL = os.Getenv(envVsanDDatastoreURL)
			if vsanDDatstoreURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envVsanDDatastoreURL))
			}

		} else {
			sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
			if sharedvmfsURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
			}
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
		var pod *v1.Pod
		var policyID *pbmtypes.PbmProfileId
		pods := []*v1.Pod{}

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		if wcpVsanDirectCluster && supervisorCluster {
			attachTagToDS(ctx, tagID, vsanDDatstoreURL)
			defer func() {
				detachTagFromDS(ctx, tagID, vsanDDatstoreURL)
			}()
		} else {
			attachTagToDS(ctx, tagID, sharedvmfsURL)
			defer func() {
				detachTagFromDS(ctx, tagID, sharedvmfsURL)
			}()
		}

		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		randomStr := strconv.Itoa(r1.Intn(1000))

		pvcVsandNames := []string{
			eztVsandPvcName + randomStr,
			lztVsandPvcName + randomStr,
		}

		podVsandNames := []string{
			eztVsandPodName + randomStr,
			lztVsandPodName + randomStr,
		}

		allocationTypes := []string{
			eztAllocType,
			lztAllocType,
		}

		ginkgo.By("create SPBM policies with LZT, EZT volume allocation respectively")
		ginkgo.By("Create SCs using policies created in step 1")
		ginkgo.By("create a large PVC each using the storage policies created from step 2")
		for _, at := range allocationTypes {
			if wcpVsanDirectCluster && supervisorCluster {
				policyID, policyName = createVsanDStoragePolicy(
					ctx, pc, at, map[string]string{categoryName: tagName})
			} else {
				policyID, policyName = createVmfsStoragePolicy(
					ctx, pc, at, map[string]string{categoryName: tagName})
			}

			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			policyNames = append(policyNames, policyName)
		}

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, policyNames, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			for _, policyName := range policyNames {
				setStoragePolicyQuota(ctx, restClientConfig, policyName, namespace, resourceQuotaLimit)
			}
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, policyNames, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			for _, policyName := range policyNames {
				setStoragePolicyQuota(ctx, restClientConfig, policyName, svNamespace, resourceQuotaLimit)
			}
		}

		for i, policyName := range policyNames {
			if vanillaCluster {
				ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
				scParameters[scParamStoragePolicyName] = policyName
				storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
					namespace, nil, scParameters, largeSize, nil, "", false, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvcs = append(pvcs, pvclaim)
				pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			} else if supervisorCluster {
				ginkgo.By("CNS_TEST: Running for WCP setup")
				if wcpVsanDirectCluster {
					storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = createVsanDPvcAndPod(sshWcpConfig, svcMasterIp, svcNamespace,
						pvcVsandNames[i], podVsandNames[i], policyName, "")
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else {
					storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvclaim, err = createPVC(ctx, client, namespace, nil, largeSize, storageclass, "")
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvcs = append(pvcs, pvclaim)
					pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
				}
			} else {
				ginkgo.By("CNS_TEST: Running for GC setup")
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(ctx, client, namespace, nil, largeSize, storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvcs = append(pvcs, pvclaim)
				pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			}
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

		if wcpVsanDirectCluster && supervisorCluster {
			pvcList := getAllPVCFromNamespace(client, namespace)
			for _, pvc := range pvcList.Items {
				pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvcs = append(pvcs, pvclaim)
				pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			}
			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpod.WaitTimeoutForPodRunningInNamespace(ctx, client, pod.Name, namespace, pollTimeout*2)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pods = append(pods, pod)
			}
		}

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete pods created before terminating the test")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)

			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				volumeID := pvs[i].Spec.CSI.VolumeHandle
				if guestCluster {
					volumeID = getVolumeIDFromSupervisorCluster(pvs[i].Spec.CSI.VolumeHandle)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
				}
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

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
			if wcpVsanDirectCluster && supervisorCluster {
				framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, vsanDDatstoreURL)
				e2eVSphere.verifyDatastoreMatch(volumeID, []string{vsanDDatstoreURL})

			} else {
				framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, sharedvmfsURL)
				e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})
			}
		}

		if !wcpVsanDirectCluster {
			ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
			ginkgo.By("verify we can read and write on the PVCs")
			pods = createMultiplePods(ctx, client, pvclaims2d, true)
		}
		if wcpVsanDirectCluster && supervisorCluster {
			for _, pod := range pods {
				framework.Logf("svnamespace: %s", namespace)
				cmd := fmt.Sprintf("kubectl exec %s -n %s -- dd if=/dev/urandom"+
					" of=/mnt/file1 bs=64k count=800", pod.Name, namespace)
				for i := 0; i < 5; i++ {
					writeDataOnPodInSupervisor(sshWcpConfig, svcMasterIp, cmd)
				}

			}
		} else {
			fillVolumeInPods(f, client, pods)
		}

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
	ginkgo.It("[csi-block-vanilla][csi-guest][csi-supervisor]"+
		"[csi-wcp-vsan-direct] Verify large EZT volume creation which takes longer than vpxd "+
		"timeout", ginkgo.Label(p0, vanilla, block, thickThin, wcp, tkg, windows, stable, vsanDirect, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, vsanDDatstoreURL := "", ""

		if wcpVsanDirectCluster && supervisorCluster {
			vsanDDatstoreURL = os.Getenv(envVsanDDatastoreURL)
			if vsanDDatstoreURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envVsanDDatastoreURL))
			}

		} else {
			sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
			if sharedvmfsURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
			}
		}

		largeSize := os.Getenv(envDiskSizeLarge)
		if largeSize == "" {
			largeSize = diskSizeLarge
		}

		scParameters := make(map[string]string)
		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		randomStr := strconv.Itoa(rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyName string
		var policyID *pbmtypes.PbmProfileId
		var pod *v1.Pod

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		if wcpVsanDirectCluster && supervisorCluster {
			attachTagToDS(ctx, tagID, vsanDDatstoreURL)
			defer func() {
				detachTagFromDS(ctx, tagID, vsanDDatstoreURL)
			}()
		} else {
			attachTagToDS(ctx, tagID, sharedvmfsURL)
			defer func() {
				detachTagFromDS(ctx, tagID, sharedvmfsURL)
			}()
		}

		ginkgo.By("Create a SPBM policy with EZT volume allocation")
		if wcpVsanDirectCluster {
			policyID, policyName = createVsanDStoragePolicy(
				ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		} else {
			policyID, policyName = createVmfsStoragePolicy(
				ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		}

		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, []string{policyName}, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			setStoragePolicyQuota(ctx, restClientConfig, policyName, namespace, resourceQuotaLimit)

		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, []string{policyName}, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			setStoragePolicyQuota(ctx, restClientConfig, policyName, svNamespace, resourceQuotaLimit)

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
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, largeSize, nil, "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			if wcpVsanDirectCluster {
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = createVsanDPvcAndPod(sshWcpConfig, svcMasterIp, svcNamespace,
					eztVsandPvcName+randomStr, eztVsandPodName+randomStr, policyName, largeSize)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(ctx, client, namespace, nil, largeSize, storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(ctx, client, namespace, nil, largeSize, storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("Delete pods")
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Delete the PVCs created in step 3")
			pv := getPvFromClaim(client, namespace, pvclaim.Name)
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		if wcpVsanDirectCluster && supervisorCluster {
			pvcList := getAllPVCFromNamespace(client, namespace)
			for _, pvc := range pvcList.Items {
				pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
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
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx,
			client, []*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout*4)
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
		if wcpVsanDirectCluster && supervisorCluster {
			framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, vsanDDatstoreURL)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{vsanDDatstoreURL})

		} else {
			framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, sharedvmfsURL)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})
		}

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
	ginkgo.It("[csi-block-vanilla][csi-guest][csi-supervisor]"+
		"[csi-wcp-vsan-direct] Verify EZT online volume expansion to a large size which takes longer than vpxd "+
		"timeout", ginkgo.Label(p0, vanilla, block, thickThin, wcp, windows, tkg, stable, vsanDirect, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, vsanDDatstoreURL := "", ""

		if wcpVsanDirectCluster && supervisorCluster {
			vsanDDatstoreURL = os.Getenv(envVsanDDatastoreURL)
			if vsanDDatstoreURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envVsanDDatastoreURL))
			}

		} else {
			sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
			if sharedvmfsURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
			}
		}

		largeSize := os.Getenv(envDiskSizeLarge)
		if largeSize == "" {
			largeSize = diskSizeLarge
		}

		scParameters := make(map[string]string)

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		randomStr := strconv.Itoa(rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyName string
		var policyID *pbmtypes.PbmProfileId
		var pod *v1.Pod
		pods := []*v1.Pod{}

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		if wcpVsanDirectCluster {
			attachTagToDS(ctx, tagID, vsanDDatstoreURL)
			defer func() {
				detachTagFromDS(ctx, tagID, vsanDDatstoreURL)
			}()
		} else {
			attachTagToDS(ctx, tagID, sharedvmfsURL)
			defer func() {
				detachTagFromDS(ctx, tagID, sharedvmfsURL)
			}()
		}

		ginkgo.By("Create a SPBM policy with EZT volume allocation")
		if wcpVsanDirectCluster {
			policyID, policyName = createVsanDStoragePolicy(
				ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		} else {
			policyID, policyName = createVmfsStoragePolicy(
				ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		}
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, []string{policyName}, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			setStoragePolicyQuota(ctx, restClientConfig, policyName, namespace, resourceQuotaLimit)

		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, []string{policyName}, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			setStoragePolicyQuota(ctx, restClientConfig, policyName, svNamespace, resourceQuotaLimit)

		}

		framework.Logf("namespace: %s", f.Namespace.Name)

		setVpxdTaskTimeout(ctx, vpxdReducedTaskTimeoutSecsInt)
		defer func() {
			setVpxdTaskTimeout(ctx, 0)
		}()

		ginkgo.By("Create SC using policy created in step 1")
		ginkgo.By("Create a 2g PVC using SC created in step 2, say pvc1")
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = policyName
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, "", nil, "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			// create resource quota
			if wcpVsanDirectCluster {
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = createVsanDPvcAndPod(sshWcpConfig, svcMasterIp, svcNamespace,
					eztVsandPvcName+randomStr, eztVsandPodName+randomStr, policyName, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			if vanillaCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		if wcpVsanDirectCluster && supervisorCluster {
			pvcList := getAllPVCFromNamespace(client, namespace)
			for _, pvc := range pvcList.Items {
				pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpod.WaitTimeoutForPodRunningInNamespace(ctx, client, pod.Name, namespace, pollTimeout*3)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pods = append(pods, pod)
			}
		}

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx,
			client, []*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete pods")
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Delete the PVCs created in step 3")
			pv := getPvFromClaim(client, namespace, pvclaim.Name)
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

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
		if wcpVsanDirectCluster && supervisorCluster {
			framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, vsanDDatstoreURL)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{vsanDDatstoreURL})

		} else {
			framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, sharedvmfsURL)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})
		}

		pvcs2d := [][]*v1.PersistentVolumeClaim{}
		pvcs2d = append(pvcs2d, []*v1.PersistentVolumeClaim{pvclaim})
		if !wcpVsanDirectCluster {
			ginkgo.By("Create a pod using pvc1 say pod1 and wait for it to be ready")
			pods = createMultiplePods(ctx, client, pvcs2d, true) // only 1 will be created here
			defer func() {
				ginkgo.By("Delete pod")
				deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
			}()
		}
		ginkgo.By("Get filesystem size for mount point /mnt/volume1 before expansion")
		originalFsSize, err := getFileSystemSizeForOsType(f, client, pods[0])
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
			err = waitForPvResize(pvs[0], client, newSize, totalResizeWaitPeriod*2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			svcPVCName := pvs[0].Spec.CSI.VolumeHandle
			err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod*2)
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
		fsSize, err := getFileSystemSizeForOsType(f, client, pods[0])
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
	ginkgo.It("[csi-block-vanilla][csi-guest][csi-supervisor][ef-vks-thickthin]"+
		"[csi-wcp-vsan-direct] Verify online LZT/EZT volume expansion of attached volumes with "+
		"IO", ginkgo.Label(p0, vanilla, block, thickThin, wcp, tkg, stable, vsanDirect, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, vsanDDatstoreURL := "", ""

		if wcpVsanDirectCluster && supervisorCluster {
			vsanDDatstoreURL = os.Getenv(envVsanDDatastoreURL)
			if vsanDDatstoreURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envVsanDDatastoreURL))
			}

		} else {
			sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
			if sharedvmfsURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
			}
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
		var pod *v1.Pod
		var policyID *pbmtypes.PbmProfileId
		pods := []*v1.Pod{}
		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		randomStr := strconv.Itoa(rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		if wcpVsanDirectCluster && supervisorCluster {
			attachTagToDS(ctx, tagID, vsanDDatstoreURL)
			defer func() {
				detachTagFromDS(ctx, tagID, vsanDDatstoreURL)
			}()
		} else {
			attachTagToDS(ctx, tagID, sharedvmfsURL)
			defer func() {
				detachTagFromDS(ctx, tagID, sharedvmfsURL)
			}()
		}

		allocationTypes := []string{
			eztAllocType,
			lztAllocType,
		}

		pvcVsandNames := []string{
			eztVsandPvcName + randomStr,
			lztVsandPvcName + randomStr,
		}

		podVsandNames := []string{
			eztVsandPodName + randomStr,
			lztVsandPodName + randomStr,
		}

		ginkgo.By("create SPBM policies with LZT, EZT volume allocation respectively")
		ginkgo.By("Create SCs using policies created in step 1")
		ginkgo.By("create a PVC each using the storage policies created from step 2")
		for _, at := range allocationTypes {
			if wcpVsanDirectCluster && supervisorCluster {
				policyID, policyName = createVsanDStoragePolicy(
					ctx, pc, at, map[string]string{categoryName: tagName})
			} else {
				policyID, policyName = createVmfsStoragePolicy(
					ctx, pc, at, map[string]string{categoryName: tagName})
			}

			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			policyNames = append(policyNames, policyName)
		}

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, policyNames, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			for _, policyName := range policyNames {
				setStoragePolicyQuota(ctx, restClientConfig, policyName, namespace, resourceQuotaLimit)
			}
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, policyNames, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			for _, policyName := range policyNames {
				setStoragePolicyQuota(ctx, restClientConfig, policyName, svNamespace, resourceQuotaLimit)
			}
		}

		for i, policyName := range policyNames {
			if vanillaCluster {
				ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
				scParameters[scParamStoragePolicyName] = policyName
				storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
					namespace, nil, scParameters, "", nil, "", true, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvcs = append(pvcs, pvclaim)
				pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			} else if supervisorCluster {
				ginkgo.By("CNS_TEST: Running for WCP setup")
				if wcpVsanDirectCluster {
					storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = createVsanDPvcAndPod(sshWcpConfig, svcMasterIp, svcNamespace,
						pvcVsandNames[i], podVsandNames[i], policyName, "")
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else {
					storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvcs = append(pvcs, pvclaim)
					pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
				}
			} else {
				ginkgo.By("CNS_TEST: Running for GC setup")
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvcs = append(pvcs, pvclaim)
				pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			}
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

		if wcpVsanDirectCluster && supervisorCluster {
			pvcList := getAllPVCFromNamespace(client, namespace)
			for _, pvc := range pvcList.Items {
				pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvcs = append(pvcs, pvclaim)
				pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			}
			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpod.WaitTimeoutForPodRunningInNamespace(ctx, client, pod.Name, namespace, pollTimeout*2)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pods = append(pods, pod)
			}

		}

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete pods created before terminating the test")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)

			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				volumeID := pvs[i].Spec.CSI.VolumeHandle
				if guestCluster {
					volumeID = getVolumeIDFromSupervisorCluster(pvs[i].Spec.CSI.VolumeHandle)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
				}
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

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
			if wcpVsanDirectCluster && supervisorCluster {
				framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, vsanDDatstoreURL)
				e2eVSphere.verifyDatastoreMatch(volumeID, []string{vsanDDatstoreURL})

			} else {
				framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, sharedvmfsURL)
				e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})
			}
		}

		if !wcpVsanDirectCluster {
			ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
			ginkgo.By("verify we can read and write on the PVCs")
			pods = createMultiplePods(ctx, client, pvclaims2d, true)
		}
		var testdataFile string
		var op []byte
		if !windowsEnv {
			rand.New(rand.NewSource(time.Now().Unix()))
			testdataFile = fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
			ginkgo.By(fmt.Sprintf("Creating a 100mb test data file %v", testdataFile))
			op, err = exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
				"bs=1M", "count=100").Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				op, err = exec.Command("rm", "-f", testdataFile).Output()
				fmt.Println(op)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}

		ginkgo.By("Expand pvcs while writing some data on them")
		var wg sync.WaitGroup
		currentPvcSize := pvcs[0].Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse(diskSize))
		fsSizes := []int64{}

		for _, pod := range pods {
			originalSizeInMb, err := getFileSystemSizeForOsType(f, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			fsSizes = append(fsSizes, originalSizeInMb)
		}
		wg.Add(len(pods) * 2)
		for i, pod := range pods {
			go writeKnownData2PodInParallel(f, client, pod, testdataFile, &wg, fsSizes[i]-spareSpace)
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
			fsSize, err := getFileSystemSizeForOsType(f, client, pods[i])
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
			verifyKnownDataInPod(f, client, pod, testdataFile, fsSizes[i]-spareSpace)
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
	ginkgo.It("[cflater-wcp][csi-supervisor][csi-block-vanilla][csi-block-vanilla-parallelized][csi-guest]"+
		"[csi-wcp-vsan-direct] Relocate volume to another same type "+
		"datastore", ginkgo.Label(p0, vanilla, block, thickThin, wcp, tkg, stable, vsanDirect, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, sharedvmfs2URL := "", ""
		vsanDDatstoreURL, vsanDDatstore2URL := "", ""
		var datastoreUrls []string
		var policyName string

		if wcpVsanDirectCluster && supervisorCluster {
			vsanDDatstoreURL = os.Getenv(envVsanDDatastoreURL)
			if vsanDDatstoreURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envVsanDDatastoreURL))
			}

			vsanDDatstore2URL = os.Getenv(envVsanDDatastore2URL)
			if vsanDDatstore2URL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envVsanDDatastore2URL))
			}
			datastoreUrls = append(datastoreUrls, vsanDDatstoreURL, vsanDDatstore2URL)
		} else {
			sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
			if sharedvmfsURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
			}

			sharedvmfs2URL = os.Getenv(envSharedVMFSDatastore2URL)
			if sharedvmfsURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastore2URL))
			}
			datastoreUrls = append(datastoreUrls, sharedvmfsURL, sharedvmfs2URL)
		}

		scParameters := make(map[string]string)
		policyNames := []string{}
		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		pods := []*v1.Pod{}

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		randomStr := strconv.Itoa(rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		if wcpVsanDirectCluster && supervisorCluster {
			attachTagToDS(ctx, tagID, vsanDDatstoreURL)
			defer func() {
				detachTagFromDS(ctx, tagID, vsanDDatstoreURL)
			}()

			attachTagToDS(ctx, tagID, vsanDDatstore2URL)
			defer func() {
				detachTagFromDS(ctx, tagID, vsanDDatstore2URL)
			}()
		} else {
			attachTagToDS(ctx, tagID, sharedvmfsURL)
			defer func() {
				detachTagFromDS(ctx, tagID, sharedvmfsURL)
			}()

			attachTagToDS(ctx, tagID, sharedvmfs2URL)
			defer func() {
				detachTagFromDS(ctx, tagID, sharedvmfs2URL)
			}()
		}

		ginkgo.By("create SPBM policy with thin/ezt volume allocation")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pod *v1.Pod
		var policyID *pbmtypes.PbmProfileId

		if wcpVsanDirectCluster && supervisorCluster {
			policyID, policyName = createVsanDStoragePolicy(
				ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		} else {
			policyID, policyName = createVmfsStoragePolicy(
				ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		}
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		policyNames = append(policyNames, policyName)

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, policyNames, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			for _, policyName := range policyNames {
				setStoragePolicyQuota(ctx, restClientConfig, policyName, namespace, resourceQuotaLimit)
			}
		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, policyNames, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			for _, policyName := range policyNames {
				setStoragePolicyQuota(ctx, restClientConfig, policyName, svNamespace, resourceQuotaLimit)
			}
		}

		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = policyName
			storageclass, err = createStorageClass(client,
				scParameters, nil, "", "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if wcpVsanDirectCluster && supervisorCluster {
			err = createVsanDPvcAndPod(sshWcpConfig, svcMasterIp, svcNamespace,
				eztVsandPvcName+randomStr, eztVsandPodName+randomStr, policyName, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim2, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim, pvclaim2)
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim2})
		}

		defer func() {
			if vanillaCluster {
				ginkgo.By("Delete the SCs created in step 2")
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		if wcpVsanDirectCluster && supervisorCluster {
			pvcList := getAllPVCFromNamespace(client, namespace)
			for _, pvc := range pvcList.Items {
				pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvcs = append(pvcs, pvclaim)
				pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			}
			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpod.WaitTimeoutForPodRunningInNamespace(ctx, client, pod.Name, namespace, pollTimeout*2)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pods = append(pods, pod)
			}
		}

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volIds[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		if !wcpVsanDirectCluster {
			ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
			ginkgo.By("verify we can read and write on the PVCs")
			pods = createMultiplePods(ctx, client, pvclaims2d, true)
		}

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

		if !wcpVsanDirectCluster {
			ginkgo.By("Writing known data to pods")
			for _, pod := range pods {
				writeKnownData2Pod(f, client, pod, testdataFile)
			}
		}

		var pvcName string
		if wcpVsanDirectCluster && supervisorCluster {
			ginkgo.By("Delete the pod created")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		} else {
			ginkgo.By("delete pod1")
			pvcName = pods[1].Spec.Volumes[0].VolumeSource.PersistentVolumeClaim.ClaimName // for recreating pod1
			deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pods[1]}, true)

		}

		ginkgo.By("Relocate volume to another vmfs/vsand datastore")
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
			_, err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
		}

		if wcpVsanDirectCluster && supervisorCluster {
			ginkgo.By("Creating a pod")
			err := applyVsanDirectPodYaml(sshWcpConfig, svcMasterIp, svcNamespace, pvclaim.Name, eztVsandPodName+randomStr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pods = append(pods, pod)
		} else {
			ginkgo.By("Recreate pod1")
			pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs2d2 := [][]*v1.PersistentVolumeClaim{{pvc}}
			podsNew := createMultiplePods(ctx, client, pvcs2d2, true)
			pods[1] = podsNew[0]
		}

		ginkgo.By("Verify the data written to the pods")
		if !wcpVsanDirectCluster {
			for _, pod := range pods {
				verifyKnownDataInPod(f, client, pod, testdataFile)
			}
		}

		if !(wcpVsanDirectCluster && supervisorCluster) {
			ginkgo.By("Delete pods created in step 5")
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		} else {
			ginkgo.By("Delete the pod created")
			err = fpod.DeletePodWithWait(ctx, client, pods[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	ginkgo.It("[csi-guest][csi-supervisor][csi-block-vanilla][csi-wcp-vsan-direct] [ef-vks-thickthin] Verify "+
		"EZT offline volume "+
		"expansion", ginkgo.Label(p0, vanilla, block, thickThin, wcp, tkg, windows, stable, vsanDirect, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, vsanDDatstoreURL := "", ""

		if wcpVsanDirectCluster && supervisorCluster {
			vsanDDatstoreURL = os.Getenv(envVsanDDatastoreURL)
			if vsanDDatstoreURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envVsanDDatastoreURL))
			}

		} else {
			sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
			if sharedvmfsURL == "" {
				ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
			}
		}

		scParameters := make(map[string]string)
		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		randomStr := strconv.Itoa(rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyName string
		var pod *v1.Pod
		var policyID *pbmtypes.PbmProfileId
		pods := []*v1.Pod{}

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		if wcpVsanDirectCluster && supervisorCluster {
			attachTagToDS(ctx, tagID, vsanDDatstoreURL)
			defer func() {
				detachTagFromDS(ctx, tagID, vsanDDatstoreURL)
			}()
		} else {
			attachTagToDS(ctx, tagID, sharedvmfsURL)
			defer func() {
				detachTagFromDS(ctx, tagID, sharedvmfsURL)
			}()
		}

		ginkgo.By("Create a SPBM policy with EZT volume allocation")
		if wcpVsanDirectCluster && supervisorCluster {
			policyID, policyName = createVsanDStoragePolicy(
				ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		} else {
			policyID, policyName = createVmfsStoragePolicy(
				ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		}
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		if supervisorCluster {
			assignPolicyToWcpNamespace(client, ctx, namespace, []string{policyName}, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			setStoragePolicyQuota(ctx, restClientConfig, policyName, namespace, resourceQuotaLimit)

		} else if guestCluster {
			_, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(client, ctx, svNamespace, []string{policyName}, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)

			restClientConfig := getRestConfigClient()
			setStoragePolicyQuota(ctx, restClientConfig, policyName, svNamespace, resourceQuotaLimit)

		}

		ginkgo.By("Create SC using policy created in step 1")
		ginkgo.By("Create a 2g PVC using SC created in step 2, say pvc1")

		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = policyName
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, "", nil, "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			if wcpVsanDirectCluster {
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = createVsanDPvcAndPod(sshWcpConfig, svcMasterIp, svcNamespace,
					eztVsandPvcName+randomStr, eztVsandPodName+randomStr, policyName, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			if vanillaCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		if wcpVsanDirectCluster && supervisorCluster {
			pvcList := getAllPVCFromNamespace(client, namespace)
			for _, pvc := range pvcList.Items {
				pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpod.WaitTimeoutForPodRunningInNamespace(ctx, client, pod.Name, namespace, pollTimeout*2)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pods = append(pods, pod)
			}
		}

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx,
			client, []*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete pods")
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Delete the PVCs created in step 3")
			pv := getPvFromClaim(client, namespace, pvclaim.Name)
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

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
		if wcpVsanDirectCluster && supervisorCluster {
			framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, vsanDDatstoreURL)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{vsanDDatstoreURL})

		} else {
			framework.Logf("Verify if VolumeID: %s is created on the datastore: %s", volumeID, sharedvmfsURL)
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{sharedvmfsURL})
		}

		pvcs2d := [][]*v1.PersistentVolumeClaim{}
		pvcs2d = append(pvcs2d, []*v1.PersistentVolumeClaim{pvclaim})
		if !wcpVsanDirectCluster {
			ginkgo.By("Create a pod using pvc1 say pod1 and wait for it to be ready")
			pods = createMultiplePods(ctx, client, pvcs2d, true) // only 1 will be created here
			defer func() {
				ginkgo.By("Delete pod")
				deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
			}()
		}
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
		var newPods []*v1.Pod
		if wcpVsanDirectCluster && supervisorCluster {
			err := applyVsanDirectPodYaml(sshWcpConfig, svcMasterIp, svcNamespace, pvclaim.Name, eztVsandPodName+randomStr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			newPods = append(newPods, pod)
		} else {
			newPods = createMultiplePods(ctx, client, pvcs2d, true)
		}

		defer func() {
			ginkgo.By("Deleting the pod")
			deletePodsAndWaitForVolsToDetach(ctx, client, newPods, true)
		}()

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		var fsSize int64

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFileSystemSizeForOsType(f, client, newPods[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %d", fsSize)

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
	ginkgo.It("[csi-block-vanilla] verify volume allocation change post snapshot deletion works"+
		" fine", ginkgo.Label(p0, vanilla, block, thickThin, stable, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		var pandoraSyncWaitTime int
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

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
			storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, "", nil, "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim)
			scs = append(scs, storageclass)
			pvclaim2, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
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
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
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
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
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
			err = waitForCNSSnapshotToBeCreated(volIds[i], snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			if len(snaps) > 0 {
				for _, snap := range snaps {
					framework.Logf("Delete volume snapshot %v", snap.Name)
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snap.Name, pandoraSyncWaitTime)
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
			deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snap.Name, pandoraSyncWaitTime)
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
		" conversion (should take >vpxd task timeout)", ginkgo.Label(p0, vanilla, block, thickThin, wcp, tkg,
		windows, stable, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		setVpxdTaskTimeout(ctx, vpxdReducedTaskTimeoutSecsInt)
		defer func() {
			setVpxdTaskTimeout(ctx, 0)
		}()
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
			assignPolicyToWcpNamespace(client, ctx, namespace, policyNames, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)

			for _, policyName := range policyNames {
				storageclass, err := client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				scs = append(scs, storageclass)
			}
			restClientConfig := getRestConfigClient()
			for _, policyName := range policyNames {
				setStoragePolicyQuota(ctx, restClientConfig, policyName, namespace, resourceQuotaLimit)
			}
		} else if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			assignPolicyToWcpNamespace(svcClient, ctx, svNamespace, policyNames, resourceQuotaLimit)
			time.Sleep(5 * time.Minute)
			restClientConfig := getRestConfigClient()
			for _, policyName := range policyNames {
				setStoragePolicyQuota(ctx, restClientConfig, policyName, svNamespace, resourceQuotaLimit)
			}
			for _, policyName := range policyNames {
				restClientConfig := getRestConfigClient()
				setStoragePolicyQuota(ctx, restClientConfig, policyName, svNamespace, resourceQuotaLimit)
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
			pvclaim, err := createPVC(ctx, client, namespace, nil, pvc10g, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim)
			pvclaim2, err := createPVC(ctx, client, namespace, nil, pvc10g, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim2)
			// one pvc for online case and one more pvc for offline case
			pvcs2d = append(pvcs2d, []*v1.PersistentVolumeClaim{pvclaim})
		}

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volIds := []string{}
		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pvs[i].Spec.CSI.VolumeHandle)
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
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
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
			originalSizeInMb, err := getFileSystemSizeForOsType(f, client, pod)
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
			fsSize, err := getFileSystemSizeForOsType(f, client, pods[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("File system size after expansion : %v", fsSize)
			gomega.Expect(fsSize > fsSizes[i]).To(gomega.BeTrue(),
				fmt.Sprintf(
					"filesystem size %v is not > than before expansion %v for pvc %q",
					fsSize, fsSizes[i], pvcs[i*2].Name))

			framework.Logf("File system resize finished successfully for pvc %v", pvcs[i*2].Name)
		}

	})

	/*
		Relocate from vsand datastore to vmfs datastore and vice versa
		Steps:
			1. Create a SPBM policy with EZT volume allocation on vmfs datastore.
			2. Create a SC, say sc1
			3. Create a large PVC pvc1 using SC created in step 2, this should take more than 40 mins
			4. Verify that pvc1 is bound and backend fcd is on vsand datastore.
			5. Create a pod, say pod1 using pvc1.
			6. write some data to the volume.
			7. delete pod1.
			8. Relocate fcd to vmfs ds.
			9. Recreate pod1
			10.Verify pod1 is running and pvc1 is accessible and verify the data written in step 6
			11.Delete pod1
			12.	Relocate fcd to vsanDirect ds.
			13. Recreate pod1
			14.Verify pod1 is running and pvc1 is accessible and verify the data written in step 6
			15. Delete pod1
			16. Delete pvc1, sc1
			17. Delete SPBM policies created
	*/
	ginkgo.It("[csi-wcp-vsan-direct] Relocate from vmfs datastore to vsand datastore "+
		"and vice versa", ginkgo.Label(p0, wcp, thickThin, stable, vsanDirect, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		vsanDDatstoreURL := os.Getenv(envVsanDDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envVsanDDatastoreURL))
		}

		datastoreUrls := []string{sharedvmfsURL, vsanDDatstoreURL}
		policyNames := []string{}
		pvcs := []*v1.PersistentVolumeClaim{}
		scs := []*storagev1.StorageClass{}

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		randomStr := strconv.Itoa(rand.Intn(10000))
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

		attachTagToDS(ctx, tagID, vsanDDatstoreURL)
		defer func() {
			detachTagFromDS(ctx, tagID, vsanDDatstoreURL)
		}()

		ginkgo.By("create SPBM policy with EZT volume allocation")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var pvclaim *v1.PersistentVolumeClaim
		var pod *v1.Pod

		policyID, policyName := createStoragePolicyWithSharedVmfsNVsand(
			ctx, pc, eztAllocType, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()
		policyNames = append(policyNames, policyName)

		assignPolicyToWcpNamespace(client, ctx, namespace, policyNames, resourceQuotaLimit)
		time.Sleep(5 * time.Minute)
		restClientConfig := getRestConfigClient()
		for _, policyName := range policyNames {
			setStoragePolicyQuota(ctx, restClientConfig, policyName, namespace, resourceQuotaLimit)
		}

		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = createVsanDPvcAndPod(sshWcpConfig, svcMasterIp, svcNamespace,
			eztVsandPvcName+randomStr, eztVsandPodName+randomStr, policyName, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		scs = append(scs, storageclass)

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			for _, sc := range scs {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		if wcpVsanDirectCluster && supervisorCluster {
			pvcList := getAllPVCFromNamespace(client, namespace)
			for _, pvc := range pvcList.Items {
				pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				pvcs = append(pvcs, pvclaim)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			podList := getAllPodsFromNamespace(ctx, client, namespace)
			for _, p := range podList.Items {
				pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpod.WaitTimeoutForPodRunningInNamespace(ctx, client, pod.Name, namespace, pollTimeout*2)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")

		volumeID := pvs[0].Spec.CSI.VolumeHandle
		storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[0])
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")

		defer func() {
			ginkgo.By("Delete the pod created")
			deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pod}, true)

			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[i].Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pod}, true)

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
		framework.Logf("Volume is present on %s", dsUrlWhereVolumeIsPresent)
		srcVsandDsUrl := dsUrlWhereVolumeIsPresent
		e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
			}
		}

		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)
		_, err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})

		storagePolicyExists, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[0])
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")

		ginkgo.By("Creating a pod")
		err = applyVsanDirectPodYaml(sshWcpConfig, svcMasterIp, svcNamespace, pvclaim.Name, eztVsandPodName+randomStr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		podList := getAllPodsFromNamespace(ctx, client, namespace)
		for _, p := range podList.Items {
			pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Relocate back to vsand datstore")
		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pod}, true)

		dsRefDest = getDsMoRefFromURL(ctx, srcVsandDsUrl)
		_, err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{srcVsandDsUrl})

		storagePolicyExists, err = e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[0])
		e2eVSphere.verifyVolumeCompliance(volumeID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")

		ginkgo.By("Creating a pod")
		err = applyVsanDirectPodYaml(sshWcpConfig, svcMasterIp, svcNamespace, pvclaim.Name, eztVsandPodName+randomStr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		podList = getAllPodsFromNamespace(ctx, client, namespace)
		for _, p := range podList.Items {
			pod, err = client.CoreV1().Pods(namespace).Get(ctx, p.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete pods created in step 5")
		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pod}, true)

	})

	/*
		Start attached volume's conversion and relocation in parallel
		Steps for offline volumes:
			1.  Create a SPBM policy with lzt volume allocation for vmfs datastore.
			2.  Create SC using policy created in step 1
			3.  Create PVC using SC created in step 2
			4.  Verify that pvc created in step 3 are bound
			5.  Create a pod, say pod1 using pvc created in step 4.
			6.  Start writing some IO to pod.
			7.  Delete pod1.
			8.  Relocate CNS volume corresponding to pvc from step 3 to a different datastore.
			9.  While relocation is running perform volume conversion.
			10. Verify relocation was successful.
			11. Verify online volume conversion is successful.
			12. Delete all the objects created during the test.

		Steps for online volumes:
			1.  Create a SPBM policy with lzt volume allocation for vmfs datastore.
			2.  Create SC using policy created in step 1
			3.  Create PVC using SC created in step 2
			4.  Verify that pvc created in step 3 are bound
			5.  Create a pod, say pod1 using pvc created in step 4.
			6.  Start writing some IO to pod which run in parallel to steps 6-7.
			7.  Relocate CNS volume corresponding to pvc from step 3 to a different datastore.
			8.  While relocation is running perform volume conversion.
			9.  Verify the IO written so far.
			10. Verify relocation was successful.
			11. Verify online volume conversion is successful.
			12. Delete all the objects created during the test.
	*/
	ginkgo.It("[csi-block-vanilla][csi-block-vanilla-parallelized] Start attached volume's conversion and "+
		"relocation in parallel", ginkgo.Label(p0, vanilla, block, thickThin, stable, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, sharedvmfs2URL := "", ""
		var datastoreUrls []string
		var policyName string
		volIdToDsUrlMap := make(map[string]string)
		volIdToCnsRelocateVolTask := make(map[string]*object.Task)

		sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedvmfs2URL = os.Getenv(envSharedVMFSDatastore2URL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastore2URL))
		}
		datastoreUrls = append(datastoreUrls, sharedvmfsURL, sharedvmfs2URL)

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

		ginkgo.By("create SPBM policy with lzt volume allocation")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyID *pbmtypes.PbmProfileId

		policyID, policyName = createVmfsStoragePolicy(
			ctx, pc, lztAllocType, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()
		policyNames = append(policyNames, policyName)

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim2, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
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
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volIds := []string{}
		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i/2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			volIds = append(volIds, volumeID)
		}

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
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

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, volId := range volIds {
			dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volId)
			framework.Logf("Volume is present on %s for volume: %s", dsUrlWhereVolumeIsPresent, volId)
			e2eVSphere.verifyDatastoreMatch(volId, datastoreUrls)
			for _, dsurl := range datastoreUrls {
				if dsurl != dsUrlWhereVolumeIsPresent {
					destDsUrl = dsurl
				}
			}
			framework.Logf("dest url: %s", destDsUrl)
			volIdToDsUrlMap[volId] = destDsUrl
		}

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

		ginkgo.By("delete pod1")
		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pods[1]}, true)

		ginkgo.By("Updating policy volume allocation from lzt -> ezt")
		err = updateVmfsPolicyAlloctype(ctx, pc, eztAllocType, policyName, policyID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Start relocation of volume to a different datastore")
		for _, volId := range volIds {
			dsRefDest := getDsMoRefFromURL(ctx, volIdToDsUrlMap[volId])
			task, err := e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volId, dsRefDest, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volIdToCnsRelocateVolTask[volId] = task
			framework.Logf("Waiting for a few seconds for relocation to be started properly on VC")
			time.Sleep(time.Duration(10) * time.Second)
		}

		ginkgo.By("Perform volume conversion and write IO to pod while relocate volume to different datastore")
		var wg sync.WaitGroup
		wg.Add(1 + len(volIds))
		go writeKnownData2PodInParallel(f, client, pods[0], testdataFile, &wg)
		for _, volId := range volIds {
			go reconfigPolicyParallel(ctx, volId, policyID.UniqueId, &wg)
		}
		wg.Wait()

		ginkgo.By("Verify the data on the PVCs match what was written in step 7")
		verifyKnownDataInPod(f, client, pods[0], testdataFile)

		for _, volId := range volIds {
			ginkgo.By(fmt.Sprintf("Wait for relocation task to complete for volumeID: %s", volId))
			waitForCNSTaskToComplete(ctx, volIdToCnsRelocateVolTask[volId])
			ginkgo.By("Verify relocation of volume is successful")
			e2eVSphere.verifyDatastoreMatch(volId, []string{volIdToDsUrlMap[volId]})
			storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volId, policyNames[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volId, true)
		}

		ginkgo.By("Delete the pod created")
		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pods[0]}, true)

	})

	/*
		Start attached volume's conversion and relocation of volume with updation of its metadata in parallel
		Steps for offline volumes:
			1.  Create a SPBM policy with lzt volume allocation for vmfs datastore.
			2.  Create SC using policy created in step 1
			3.  Create PVC using SC created in step 2
			4.  Verify that pvc created in step 3 are bound
			5.  Create a pod, say pod1 using pvc created in step 4.
			6.  Start writing some IO to pod.
			7.  Delete pod1.
			8.  Relocate CNS volume corresponding to pvc from step 3 to a different datastore.
			9.  While relocation is running add labels to PV and PVC
			    in parallel with volume conversion.
			10. Verify relocation was successful.
			11. Verify online volume conversion is successful.
			12. Delete all the objects created during the test.

		Steps for online volumes:
			1.  Create a SPBM policy with lzt volume allocation for vmfs datastore.
			2.  Create SC using policy created in step 1
			3.  Create PVC using SC created in step 2
			4.  Verify that pvc created in step 3 are bound
			5.  Create a pod, say pod1 using pvc created in step 4.
			6.  Start writing some IO to pod which run in parallel to steps 6-7.
			7.  Relocate CNS volume corresponding to pvc from step 3 to a different datastore.
			8.  While relocation is running add labels to PV and PVC
			    in parallel with volume conversion.
			9.  Verify the IO written so far.
			10. Verify relocation was successful.
			11. Verify online volume conversion is successful.
			12. Delete all the objects created during the test.
	*/
	ginkgo.It("[csi-block-vanilla][csi-block-vanilla-parallelized]"+
		" Start attached volume's conversion and relocation of volume with updation of "+
		"its metadata in parallel", ginkgo.Label(p0, vanilla, block, thickThin, stable, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, sharedvmfs2URL := "", ""
		var datastoreUrls []string
		var policyName string
		volIdToDsUrlMap := make(map[string]string)
		labels := make(map[string]string)
		labels[labelKey] = labelValue
		volIdToCnsRelocateVolTask := make(map[string]*object.Task)

		sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedvmfs2URL = os.Getenv(envSharedVMFSDatastore2URL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastore2URL))
		}
		datastoreUrls = append(datastoreUrls, sharedvmfsURL, sharedvmfs2URL)

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

		ginkgo.By("create SPBM policy with lzt volume allocation")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyID *pbmtypes.PbmProfileId

		policyID, policyName = createVmfsStoragePolicy(
			ctx, pc, lztAllocType, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()
		policyNames = append(policyNames, policyName)

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim2, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
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
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volIds := []string{}
		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i/2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			volIds = append(volIds, volumeID)
		}

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
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

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, volId := range volIds {
			dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volId)
			framework.Logf("Volume is present on %s for volume: %s", dsUrlWhereVolumeIsPresent, volId)
			e2eVSphere.verifyDatastoreMatch(volId, datastoreUrls)
			for _, dsurl := range datastoreUrls {
				if dsurl != dsUrlWhereVolumeIsPresent {
					destDsUrl = dsurl
				}
			}
			framework.Logf("dest url: %s", destDsUrl)
			volIdToDsUrlMap[volId] = destDsUrl
		}

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

		ginkgo.By("delete pod1")
		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pods[1]}, true)

		ginkgo.By("Start relocation of volume to a different datastore")
		for _, volId := range volIds {
			dsRefDest := getDsMoRefFromURL(ctx, volIdToDsUrlMap[volId])
			task, err := e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volId, dsRefDest, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volIdToCnsRelocateVolTask[volId] = task
			framework.Logf("Waiting for a few seconds for relocation to be started properly on VC")
			time.Sleep(time.Duration(10) * time.Second)
		}

		ginkgo.By("Add labels to volumes and write IO to pod while relocating volume to different datastore")
		err = updateVmfsPolicyAlloctype(ctx, pc, eztAllocType, policyName, policyID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var wg sync.WaitGroup
		wg.Add(3 + len(volIds))
		go writeKnownData2PodInParallel(f, client, pods[0], testdataFile, &wg)
		go updatePvcLabelsInParallel(ctx, client, namespace, labels, pvcs, &wg)
		go updatePvLabelsInParallel(ctx, client, namespace, labels, pvs, &wg)
		for _, volId := range volIds {
			go reconfigPolicyParallel(ctx, volId, policyID.UniqueId, &wg)

		}
		wg.Wait()

		for _, pvclaim := range pvcs {
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
		}

		ginkgo.By("Verify the data on the PVCs match what was written in step 7")
		verifyKnownDataInPod(f, client, pods[0], testdataFile)

		for _, volId := range volIds {
			ginkgo.By(fmt.Sprintf("Wait for relocation task to complete for volumeID: %s", volId))
			waitForCNSTaskToComplete(ctx, volIdToCnsRelocateVolTask[volId])
			ginkgo.By("Verify relocation of volume is successful")
			e2eVSphere.verifyDatastoreMatch(volId, []string{volIdToDsUrlMap[volId]})
			storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volId, policyNames[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volId, true)
		}

		ginkgo.By("Delete the pod created")
		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pods[0]}, true)
	})

	/*
		Start attached volume's conversion while creation of snapshot and
		relocation of volume in parallel
		Steps for offline volumes:
			1.  Create a SPBM policy with lzt volume allocation for vmfs datastore.
			2.  Create SC using policy created in step 1
			3.  Create PVC using SC created in step 2
			4.  Verify that pvc created in step 3 are bound
			5.  Create a pod, say pod1 using pvc created in step 4.
			6.  Start writing some IO to pod.
			7.  Delete pod1.
			8.  Relocate CNS volume corresponding to pvc from step 3 to a different datastore.
			9.  While relocation is running perform volume conversion
			    and create a snapshot in parallel.
			10. Verify relocation was successful.
			11. Verify online volume conversion is successful.
			12. Delete all the objects created during the test.

		Steps for online volumes:
			1.  Create a SPBM policy with lzt volume allocation for vmfs datastore.
			2.  Create SC using policy created in step 1
			3.  Create PVC using SC created in step 2
			4.  Verify that pvc created in step 3 are bound
			5.  Create a pod, say pod1 using pvc created in step 4.
			6.  Start writing some IO to pod which run in parallel to steps 6-7.
			7.  Relocate CNS volume corresponding to pvc from step 3 to a different datastore.
			8.  While relocation is running perform volume conversion
			    and create a snapshot in parallel.
			9.  Verify the IO written so far.
			10. Verify relocation was successful.
			11. Verify online volume conversion is successful.
			12. Delete all the objects created during the test.
	*/
	ginkgo.It("[csi-block-vanilla][csi-block-vanilla-parallelized]"+
		" Start attached volume's conversion while creation of snapshot and"+
		" relocation of volume in parallel", ginkgo.Label(p0, vanilla, block, thickThin, stable, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedvmfsURL, sharedvmfs2URL := "", ""
		var datastoreUrls []string
		var policyName string
		volIdToDsUrlMap := make(map[string]string)
		volIdToCnsRelocateVolTask := make(map[string]*object.Task)
		snapToVolIdMap := make(map[string]string)

		sharedvmfsURL = os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedvmfs2URL = os.Getenv(envSharedVMFSDatastore2URL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastore2URL))
		}
		datastoreUrls = append(datastoreUrls, sharedvmfsURL, sharedvmfs2URL)

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

		ginkgo.By("create SPBM policy with lzt volume allocation")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var policyID *pbmtypes.PbmProfileId

		policyID, policyName = createVmfsStoragePolicy(
			ctx, pc, lztAllocType, map[string]string{categoryName: tagName})
		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()
		policyNames = append(policyNames, policyName)

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")
		scParameters[scParamStoragePolicyName] = policyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim2, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
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
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volIds := []string{}
		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
			}
			storagePolicyMatches, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i/2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyMatches).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			volIds = append(volIds, volumeID)
		}

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
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

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, volId := range volIds {
			dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volId)
			framework.Logf("Volume is present on %s for volume: %s", dsUrlWhereVolumeIsPresent, volId)
			e2eVSphere.verifyDatastoreMatch(volId, datastoreUrls)
			for _, dsurl := range datastoreUrls {
				if dsurl != dsUrlWhereVolumeIsPresent {
					destDsUrl = dsurl
				}
			}
			framework.Logf("dest url: %s", destDsUrl)
			volIdToDsUrlMap[volId] = destDsUrl
		}

		snaps := []*snapV1.VolumeSnapshot{}
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

		ginkgo.By("delete pod1")
		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pods[1]}, true)

		ginkgo.By("Start relocation of volume to a different datastore")
		for _, volId := range volIds {
			dsRefDest := getDsMoRefFromURL(ctx, volIdToDsUrlMap[volId])
			task, err := e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volId, dsRefDest, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volIdToCnsRelocateVolTask[volId] = task
			framework.Logf("Waiting for a few seconds for relocation to be started properly on VC")
			time.Sleep(time.Duration(10) * time.Second)
		}

		ginkgo.By("Perform volume conversion and write IO to pod and" +
			" create a snapshot while relocating volume to different datastore")
		err = updateVmfsPolicyAlloctype(ctx, pc, eztAllocType, policyName, policyID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var wg sync.WaitGroup
		ch := make(chan *snapV1.VolumeSnapshot)
		lock := &sync.Mutex{}
		wg.Add(1 + 2*len(volIds))
		go writeKnownData2PodInParallel(f, client, pods[0], testdataFile, &wg)
		for i := range volIds {
			go reconfigPolicyParallel(ctx, volIds[i], policyID.UniqueId, &wg)
			go createSnapshotInParallel(ctx, namespace, snapc, pvcs[i].Name, volumeSnapshotClass.Name,
				ch, lock, &wg)
			go func(volID string) {
				for v := range ch {
					snaps = append(snaps, v)
					snapToVolIdMap[v.Name] = volID
				}
			}(volIds[i])
		}
		wg.Wait()

		ginkgo.By("Verify the data on the PVCs match what was written in step 7")
		verifyKnownDataInPod(f, client, pods[0], testdataFile)

		for _, snap := range snaps {
			volumeSnapshot := snap
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

			defer func() {
				framework.Logf("Delete volume snapshot %v", volumeSnapshot.Name)
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				framework.Logf("Verify snapshot entry %v is deleted from CNS for volume %v", snapshotId,
					snapToVolIdMap[volumeSnapshot.Name])
				err = waitForCNSSnapshotToBeDeleted(snapToVolIdMap[volumeSnapshot.Name], snapshotId)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = waitForCNSSnapshotToBeCreated(snapToVolIdMap[volumeSnapshot.Name], snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, volId := range volIds {
			ginkgo.By(fmt.Sprintf("Wait for relocation task to complete for volumeID: %s", volId))
			waitForCNSTaskToComplete(ctx, volIdToCnsRelocateVolTask[volId])
			ginkgo.By("Verify relocation of volume is successful")
			e2eVSphere.verifyDatastoreMatch(volId, []string{volIdToDsUrlMap[volId]})
			storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volId, policyNames[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")
			e2eVSphere.verifyVolumeCompliance(volId, true)
		}

		ginkgo.By("Delete the pod created")
		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pods[0]}, true)

	})

})
