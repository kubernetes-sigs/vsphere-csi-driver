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
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	admissionapi "k8s.io/pod-security-admission/api"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"

	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

/*
   Tests to verify label updates .

   Steps
   - Create StorageClass, PVC and PV with valid parameters
   Test 1) Modify labels on PVC and PV and verify labels are getting updated by metadata syncer.
   Test 2) Delete labels on PVC and PV and verify labels are getting removed by metadata syncer.
           Verify Pod name label is added when Pod is created with container volume.
           Verify Pod name label associated with volume is removed when Pod is deleted.
   Cleanup
   - Delete PVC and StorageClass and verify volume is deleted from CNS.
*/

var _ bool = ginkgo.Describe("[csi-block-vanilla] [csi-block-vanilla-parallelized] label-updates", func() {

	f := framework.NewDefaultFramework("e2e-volume-label-updates")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client              clientset.Interface
		namespace           string
		labelKey            string
		labelValue          string
		pvclabelKey         string
		pvclabelValue       string
		pvlabelKey          string
		pvlabelValue        string
		pandoraSyncWaitTime int
		datacenter          *object.Datacenter
		datastoreURL        string
		datastore           *object.Datastore
		fcdID               string
		storagePolicyName   string
		scParameters        map[string]string
		storageClassName    string
	)
	const (
		fcdName = "BasicStaticFCD"
	)
	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		labelKey = "app"
		labelValue = "e2e-labels"

		pvclabelKey = "app-pvc"
		pvclabelValue = "e2e-labels-pvc"

		pvlabelKey = "app-pv"
		pvlabelValue = "e2e-labels-pv"
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})

	ginkgo.AfterEach(func() {
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}
	})

	ginkgo.It("[csi-supervisor] verify labels are created in CNS after updating pvc and/or pv with new labels", func() {
		ginkgo.By("Invoking test to verify labels creation")
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, "", storagePolicyName)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels

		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.It("[csi-supervisor] verify labels are removed in CNS after removing them from pvc and/or pv", func() {
		ginkgo.By("Invoking test to verify labels deletion")
		labels := make(map[string]string)
		labels[labelKey] = labelValue

		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, "", storagePolicyName)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels
		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Fetching updated pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc.Labels = make(map[string]string)
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be deleted for pvc %s in namespace %s",
			labels, pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle,
			pvc.Labels, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Fetching updated pv %s", pv.Name))
		pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting labels %+v for pv %s", labels, pv.Name))
		pv.Labels = make(map[string]string)
		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be deleted for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle,
			pv.Labels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.It("[csi-supervisor] verify podname label is created/deleted "+
		"when pod with cns volume is created/deleted.", func() {
		ginkgo.By("Invoking test to verify pod name label updates")
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, "", storagePolicyName)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		var vmUUID string
		var exists bool
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else {
			annotations := pod.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
			_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By(fmt.Sprintf("Waiting for pod name to be updated for volume %s by metadata-syncer",
			pv.Spec.CSI.VolumeHandle))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, nil,
			string(cnstypes.CnsKubernetesEntityTypePOD), pod.Name, pod.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if vanillaCluster {
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
				client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		} else {
			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(),
				fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
					vmUUID, pod.Spec.NodeName))
		}

		ginkgo.By(fmt.Sprintf("Waiting for pod name to be deleted for volume %s by metadata-syncer",
			pv.Spec.CSI.VolumeHandle))
		err = waitForPodNameLabelRemoval(pv.Spec.CSI.VolumeHandle, pod.Name, pod.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Test to verify PVC name is removed from PV entry on CNS when PVC is deleted when Reclaim Policy is set to retain

		Steps
		1. Create a Storage Class with ReclaimPolicy=Retain
		2. Create a PVC using above SC
		3. Wait for PVC to be in Bound phase
		4. Delete PVC
		5. Verify PVC name is removed from PV entry on CNS
		6. Delete PV
		7. Verify PV entry is deleted from CNS
		8. Delete SC
	*/

	ginkgo.It("[csi-block-vanilla] Verify PVC name is removed from PV entry on CNS after PVC is deleted "+
		"when Reclaim Policy is set to retain.", func() {

		var err error

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)

		cfg, err := getConfig()
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
			datastore, err = getDatastoreByURL(ctx, datastoreURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating storage class")
		sc, err := createStorageClass(client, nil, nil, v1.PersistentVolumeReclaimRetain, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating PVC")
		pvc, err := createPVC(client, namespace, nil, "", sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())

		var pv = pvs[0]
		fcdID = pv.Spec.CSI.VolumeHandle

		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0)

		if len(queryResult.Volumes) > 0 {
			// Find datastore from datastoreURL
			finder := find.NewFinder(e2eVSphere.Client.Client, false)

			for _, dc := range datacenters {
				datacenter, err = finder.Datacenter(ctx, dc)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				finder.SetDatacenter(datacenter)
				datastore, err = getDatastoreByURL(ctx, queryResult.Volumes[0].DatastoreUrl, datacenter)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if datastore != nil {
					break
				}
			}
		}
		gomega.Expect(datastore).NotTo(gomega.BeNil())

		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Waiting for some time for PVC to be deleted correctly
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow PVC deletion", oneMinuteWaitTimeInSeconds))
		time.Sleep(time.Duration(oneMinuteWaitTimeInSeconds) * time.Second)

		_, err = e2eVSphere.getLabelsForCNSVolume(pv.Spec.CSI.VolumeHandle,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, namespace)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting pv %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be deleted", pv.Spec.CSI.VolumeHandle))
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
		err = deleteFcdWithRetriesForSpecificErr(ctx, fcdID, datastore.Reference(),
			[]string{disklibUnlinkErr}, []string{objOrItemNotFoundErr})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the Storage Class")
		err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Test to Create/Delete statically provisioned volume and perform label updates.

		Steps
		1. Create an FCD disk
		2. Wait for FCD to sync with Pandora
		3. Create a PV with above FCD
		4. Create a PVC
		5. Wait for PVC to be in Bound phase
		6. Verify PVC name is added to PV entry on CNS
		7. Update PV and PVC labels
		8. Verify updates labels are reflected on CNS
		9. Delete PVC
		10. Verify PVC name is removed from PV entry on CNS
		11. Delete PV
		12. Verify PV entry is deleted from CNS
		13. Delete SC
	*/

	ginkgo.It("Verify label updates on statically provisioned volume.", func() {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)

		cfg, err := getConfig()
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
			datastore, err = getDatastoreByURL(ctx, datastoreURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating FCD Disk")
		fcdID, err = e2eVSphere.createFCD(ctx, fcdName, diskSizeInMb, datastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Creating the PV with the fcdID %s", fcdID))
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID
		pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimRetain, staticPVLabels, ext4FSType)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		if err != nil {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC")
		pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, pv, pvc))

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv = getPvFromClaim(client, pvc.Namespace, pvc.Name)
		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels

		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting PVC %s in namespace %s", pvc.Name, pvc.Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcLabel, err := e2eVSphere.getLabelsForCNSVolume(pv.Spec.CSI.VolumeHandle,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, namespace)
		if pvcLabel == nil {
			framework.Logf("PVC name is successfully removed")
		} else {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Deleting the PV %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be deleted", fcdID))
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
		err = deleteFcdWithRetriesForSpecificErr(ctx, fcdID, datastore.Reference(),
			[]string{disklibUnlinkErr}, []string{objOrItemNotFoundErr})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Test to create/delete stateful set with label updates

		Steps
		1. Create a Storage Class
		2. Create a statefulset with 3 replicas
		3. Wait for all PVCs to be in Bound phase and Pods are Ready state
		4. Update PVC labels
		5. Verify PVC labels are updated on CNS
		6. Scale up number of replicas to 5
		7. Update PV labels
		8. Verify PV labels are updated on CNS
		9. Scale down statefulsets to 0 replicas and delete all pods.
		10. Delete PVCs
		11. Delete SC
	*/
	ginkgo.It("[csi-supervisor] Verify label updates on PVC and PV attached to a stateful set.", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters = nil
			storageClassName = "nginx-sc-label-updates"
		} else {
			storageClassName = defaultNginxStorageClassName
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storageClassName)
		}
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = storageClassName
		CreateStatefulSet(namespace, statefulset, client)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(f.ClientSet, statefulset, replicas)
		gomega.Expect(fss.CheckMount(f.ClientSet, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleup := fss.GetPodList(f.ClientSet, statefulset)
		gomega.Expect(ssPodsBeforeScaleup.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleup.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		pvclabels := make(map[string]string)
		pvclabels[pvclabelKey] = pvclabelValue

		for _, sspod := range ssPodsBeforeScaleup.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

					ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s",
						pvclabels, volumespec.PersistentVolumeClaim.ClaimName, namespace))
					pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
						volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvc.Labels = pvclabels
					_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
						pvclabels, volumespec.PersistentVolumeClaim.ClaimName, namespace))
					err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle,
						pvclabels, string(cnstypes.CnsKubernetesEntityTypePVC),
						volumespec.PersistentVolumeClaim.ClaimName, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas+2))
		_, scaleupErr := fss.Scale(f.ClientSet, statefulset, replicas+2)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(f.ClientSet, statefulset, replicas+2)
		fss.WaitForStatusReadyReplicas(f.ClientSet, statefulset, replicas+2)
		pvlabels := make(map[string]string)
		pvlabels[pvlabelKey] = pvlabelValue

		ssPodsAfterScaleUp := fss.GetPodList(f.ClientSet, statefulset)

		for _, spod := range ssPodsAfterScaleUp.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, spod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, vspec := range spod.Spec.Volumes {
				if vspec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, vspec.PersistentVolumeClaim.ClaimName)

					ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", pvlabels, pv.Name))
					pv.Labels = pvlabels
					_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", pvlabels, pv.Name))
					err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle,
						pvlabels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", 0))
		_, scaledownErr := fss.Scale(f.ClientSet, statefulset, 0)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(f.ClientSet, statefulset, 0)
		ssPodsAfterScaleDown := fss.GetPodList(f.ClientSet, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(0)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
	})

})

func waitForPodNameLabelRemoval(volumeID string, podname string, namespace string) error {
	err := wait.Poll(poll, pollTimeout, func() (bool, error) {
		_, err := e2eVSphere.getLabelsForCNSVolume(volumeID,
			string(cnstypes.CnsKubernetesEntityTypePOD), podname, namespace)
		if err != nil {
			framework.Logf("pod name label is successfully removed")
			return true, err
		}
		framework.Logf("waiting for pod name label to be removed by metadata-syncer for volume: %q", volumeID)
		return false, nil
	})
	// unable to retrieve pod name label from vCenter
	if err != nil {
		return nil
	}
	return fmt.Errorf("pod name label is not removed from cns")
}
