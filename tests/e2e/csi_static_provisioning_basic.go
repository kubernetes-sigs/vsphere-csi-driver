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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var _ = ginkgo.Describe("Basic Static Provisioning", func() {
	f := framework.NewDefaultFramework("e2e-csistaticprovision")

	var (
		client              clientset.Interface
		namespace           string
		fcdID               string
		pv                  *v1.PersistentVolume
		pvc                 *v1.PersistentVolumeClaim
		defaultDatacenter   *object.Datacenter
		defaultDatastore    *object.Datastore
		deleteFCDRequired   bool
		pandoraSyncWaitTime int
		err                 error
		datastoreURL        string
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = f.Namespace.Name
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		deleteFCDRequired = false
		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}

		for _, dc := range datacenters {
			defaultDatacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	ginkgo.AfterEach(func() {
		ginkgo.By("Performing test cleanup")
		if deleteFCDRequired {
			ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := e2eVSphere.deleteFCD(ctx, fcdID, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if pvc != nil {
			framework.ExpectNoError(framework.DeletePersistentVolumeClaim(client, pvc.Name, namespace), "Failed to delete PVC ", pvc.Name)
		}

		if pv != nil {
			framework.ExpectNoError(framework.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
			framework.ExpectNoError(e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle))
		}
	})

	/*
		This test verifies the static provisioning workflow.

		Test Steps:
		1. Create FCD and wait for fcd to allow syncing with pandora.
		2. Create PV Spec with volumeID set to FCDID created in Step-1, and PersistentVolumeReclaimPolicy is set to Delete.
		3. Create PVC with the storage request set to PV's storage capacity.
		4. Wait for PV and PVC to bound.
		5. Create a POD.
		6. Verify volume is attached to the node and volume is accessible in the pod.
		7. Verify container volume metadata is present in CNS cache.
		8. Delete POD.
		9. Verify volume is detached from the node.
		10. Delete PVC.
		11. Verify PV is deleted automatically.
	*/
	ginkgo.It("[csi-block-vanilla] Verify basic static provisioning workflow", func() {
		var err error

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Creating FCD Disk")
		fcdID, err := e2eVSphere.createFCD(ctx, "BasicStaticFCD", diskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		deleteFCDRequired = true

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora", pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		// Creating label for PV.
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID

		ginkgo.By("Creating the PV")
		pv = getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimDelete, staticPVLabels)
		pv, err = client.CoreV1().PersistentVolumes().Create(pv)
		if err != nil {
			return
		}
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC")
		pvc = getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(framework.WaitOnPVandPVC(client, namespace, pv, pvc))

		// Set deleteFCDRequired to false.
		// After PV, PVC is in the bind state, Deleting PVC should delete container volume.
		// So no need to delete FCD directly using vSphere API call.
		deleteFCDRequired = false

		ginkgo.By("Verifying CNS entry is present in cache")
		_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the Pod")
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		pod, err := framework.CreatePod(client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")

		ginkgo.By("Verify the volume is accessible and available to the pod by creating an empty file")
		filepath := filepath.Join("/mnt/volume1", "/emptyFile.txt")
		_, err = framework.LookForStringInPodExec(namespace, pod.Name, []string{"/bin/touch", filepath}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify container volume metadata is present in CNS cache")
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolume with VolumeID: %s", pv.Spec.CSI.VolumeHandle))
		_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := []types.KeyValue{{Key: "fcd-id", Value: fcdID}}
		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, pvc.Name, pv.ObjectMeta.Name, pod.Name, labels...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the Pod")
		framework.ExpectNoError(framework.DeletePodWithWait(f, client, pod), "Failed to delete pod ", pod.Name)

		ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", pod.Spec.NodeName))
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")

		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(framework.DeletePersistentVolumeClaim(client, pvc.Name, namespace), "Failed to delete PVC ", pvc.Name)
		pvc = nil

		ginkgo.By("Verify PV should be deleted automatically")
		framework.ExpectNoError(framework.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeout))
		pv = nil
	})
	/*
		This test verifies the static provisioning workflow in guest cluster.

		Test Steps:
		1. Create a PVC using the existing SC in GC
		2. Wait for PVC to be Bound in GC
		3. Verifying if the mapping PVC is bound in SC using the volume handler
		4. Verify volume is created on CNS and check the spbm health
		5. Change the reclaim policy of the PV from delete to retain in GC
		6. Delete PVC in GC
		7. Delete PV in GC
		8. Verifying if PVC and PV still persists in SV cluster
		9. Create a PV with reclaim policy=delete in GC using the bound PVC in SV cluster as the volume id
		10. Create a PVC in GC using the above PV
		11. Verify the PVC is bound in GC.
		12. Delete the PVC in GC
		13. Verifying if PVC and PV also deleted in the SV cluster
		14. Verify volume is deleted on CNS
	*/
	ginkgo.It("[csi-guest] Static provisioning workflow in guest cluster", func() {
		var err error

		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		scParameters := make(map[string]string)
		scParameters[svStorageClassName] = storagePolicyNameForSharedDatastores
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for claim to be in bound phase")
		ginkgo.By("Expect claim to pass provisioning volume as shared datastore")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		volumeID := pv.Spec.CSI.VolumeHandle
		// svcPVCName refers to PVC Name in the supervisor cluster
		svcPVCName := volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

		pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
		pv, err = client.CoreV1().PersistentVolumes().Update(pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete the PVC")
		err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Delete the PV %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(pv.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying if volume still exists in the Supervisor Cluster")
		// svcPVCName refers to PVC Name in the supervisor cluster
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		// Creating label for PV.
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = volumeID

		ginkgo.By("Creating the PV")
		pv = getPersistentVolumeSpec(svcPVCName, v1.PersistentVolumeReclaimDelete, staticPVLabels)
		pv, err = client.CoreV1().PersistentVolumes().Create(pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC")
		pvc = getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(framework.WaitOnPVandPVC(client, namespace, pv, pvc))

		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(framework.DeletePersistentVolumeClaim(client, pvc.Name, namespace), "Failed to delete PVC ", pvc.Name)
		pvc = nil

		ginkgo.By("Verify PV should be deleted automatically")
		framework.ExpectNoError(framework.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
		pv = nil

		ginkgo.By("Verify volume is deleted in Supervisor Cluster")
		volumeExists := verifyVolumeExistInSupervisorCluster(svcPVCName)
		gomega.Expect(volumeExists).To(gomega.BeFalse())
	})

	/*
		This test verifies the static provisioning workflow II in guest cluster.

		Test Steps:
		1. Create a PVC using the existing SC in SV Cluster
		2. Wait for PVC to be Bound in SV cluster
		3. Create a PV with reclaim policy=delete in GC using the bound PVC in SV cluster as the volume id
		4. Create a PVC in GC using the above PV
		5. Verify the PVC is bound in GC.
		6. Delete the PVC in GC
		7. Verifying if PVC and PV also deleted in the SV cluster
		8. Verify volume is deleted on CNS
	*/
	ginkgo.It("[csi-guest] Static provisioning workflow II in guest cluster", func() {
		var err error
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		// create supvervisor cluster client
		var svcClient clientset.Interface
		if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
			svcClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)

		ginkgo.By("Creating PVC in supervisor cluster")
		// get storageclass from the supervisor cluster
		storageclass, err := svcClient.StorageV1().StorageClasses().Get(storagePolicyNameForSharedDatastores, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim, err := createPVC(svcClient, svNamespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := framework.DeletePersistentVolumeClaim(svcClient, pvclaim.Name, svNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for claim to be in bound phase")
		ginkgo.By("Expect claim to pass provisioning volume as shared datastore")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, svcClient, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv := getPvFromClaim(svcClient, pvclaim.Namespace, pvclaim.Name)
		volumeID := pv.Spec.CSI.VolumeHandle

		// Creating label for PV.
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = volumeID

		svcPVCName := volumeID
		ginkgo.By("Creating the PV in guest cluster")
		pv = getPersistentVolumeSpec(svcPVCName, v1.PersistentVolumeReclaimDelete, staticPVLabels)
		pv, err = client.CoreV1().PersistentVolumes().Create(pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC in guest cluster")
		pvc = getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(framework.WaitOnPVandPVC(client, namespace, pv, pvc))

		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(framework.DeletePersistentVolumeClaim(client, pvc.Name, namespace), "Failed to delete PVC ", pvc.Name)
		pvc = nil

		ginkgo.By("Verify PV should be deleted automatically")
		framework.ExpectNoError(framework.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
		pv = nil

		ginkgo.By("Verify volume is deleted in Supervisor Cluster")
		volumeExists := verifyVolumeExistInSupervisorCluster(svcPVCName)
		gomega.Expect(volumeExists).To(gomega.BeFalse())
	})
})
