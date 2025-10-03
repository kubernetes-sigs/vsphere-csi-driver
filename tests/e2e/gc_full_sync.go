/*
Copyright 2020 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-guest] Guest cluster fullsync tests", func() {
	f := framework.NewDefaultFramework("e2e-full-sync")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		scParameters      map[string]string
		storagePolicyName string
		svcPVCName        string // PVC Name in the Supervisor Cluster.
		labelKey          string
		labelValue        string
		fullSyncWaitTime  int
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		labelKey = "app"
		labelValue = "e2e-labels"

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Full sync interval can be 1 min at minimum so full sync wait time
			// has to be more than 120s.
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, rqLimit)
	})

	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
	})

	// Steps:
	// Create a PVC using any replicated storage class from the SV.
	// Wait for PVC to be in Bound phase.
	// Bring down csi-controller pod in GC.
	// Update PVC labels.
	// Verify CnsVolumeMetadata CRD is not updated.
	// Bring up csi-controller pod in SV.
	// Sleep double the Full Sync interval.
	// Verify CnsVolumeMetadata CRD is updated.
	// Verify entry is updated in CNS.
	// Delete PVC.
	ginkgo.It("[pq-vks][pq-vks-n1][pq-vks-n2] Verify CNS volume is synced with updated GC PV & PVC labels "+
		"when GC csi-controller pod is down", ginkgo.Label(p0, block, tkg, vc70), func() {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil, scParameters, "", nil, "", false, "")

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		svcPVCName = volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		var vmUUID string
		ginkgo.By("Verifying CNSNodeVMAttachment in supervisor")
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifyCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		ginkgo.By("Scaling down the csi driver to zero replica")
		deployment := updateDeploymentReplica(client, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		ginkgo.By(fmt.Sprintf("Successfully scaled down the csi driver deployment:%s to zero replicas", deployment.Name))

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scaling up the csi driver to one replica")
		deployment = updateDeploymentReplica(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		ginkgo.By(fmt.Sprintf("Successfully scaled up the csi driver deployment:%s to one replica", deployment.Name))

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

	})

	// Create a PVC using any replicated storage class from the SV.
	// Wait for PVC to be in Bound phase.
	// Break connection to SV.
	// Update PV and PVC labels.
	// Re-establish connection to SV.
	// Sleep double the Full Sync interval.
	// Verify CnsVolumeMetadata CRD is updated.
	// Verify entry is updated in CNS.
	// Delete PVC.
	ginkgo.It("[pq-vks][pq-vks-n1][pq-vks-n2] Verify CNS volume is synced with updated GC PV & PVC labels after "+
		"SVC connection is restored", ginkgo.Label(p0, block, tkg, vc70), func() {
		var err error
		var sc *storagev1.StorageClass
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("CNS_TEST: Running for GC setup")
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err := createPVCAndStorageClass(ctx, client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		svcVolumeID := getVolumeIDFromSupervisorCluster(volumeID)
		gomega.Expect(svcVolumeID).NotTo(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("SVC volume ID for claim %s is %s", volumeID, svcVolumeID))
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		fysncInterval := fmt.Sprintf("%v", int((fullSyncWaitTime-20)/120))
		ginkgo.By(fmt.Sprintf("Reducing full sync interval to %v mins for the test...", fysncInterval))
		_ = updateCSIDeploymentTemplateFullSyncInterval(client, fysncInterval, csiSystemNamespace)
		defer func() {
			ginkgo.By(fmt.Sprintf("Resetting full sync interval to %v mins", defaultFullSyncIntervalInMin))
			_ = updateCSIDeploymentTemplateFullSyncInterval(client, defaultFullSyncIntervalInMin, csiSystemNamespace)
		}()

		// Break connection with SVC.
		ginkgo.By("Scaling down the csi driver to zero replica")
		deployment := updateDeploymentReplica(client, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		ginkgo.By(fmt.Sprintf("Successfully scaled down the csi driver deployment:%s to zero replicas", deployment.Name))

		pvLabels := make(map[string]string)
		pvLabels["pvLabelKeyFsync"] = "pv-label-Value-fsync"

		pvcLabels := make(map[string]string)
		pvcLabels["pvcLabelKeyFsync"] = "pvc-label-Value-fsync"

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", pvcLabels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.GetName(), metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = pvcLabels
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s in namespace %s", pvLabels, pv.Name, namespace))
		pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.GetName(), metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv.Labels = pvLabels
		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Re-establish connection with SVC.
		ginkgo.By("Scaling up the csi driver to one replica")
		deployment = updateDeploymentReplica(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		ginkgo.By(fmt.Sprintf("Successfully scaled up the csi driver deployment:%s to one replica", deployment.Name))

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync to finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		fmt.Println("PVC name in SV", volumeID)
		pvcUID := string(pvc.GetUID())
		fmt.Println("PVC UUID in GC", pvcUID)
		gcClusterID := strings.Replace(volumeID, pvcUID, "", -1)

		fmt.Println("gcClusterId", gcClusterID)
		pvUID := string(pv.UID)
		fmt.Println("PV uuid", pvUID)

		// Check pvc label update in CRD.
		verifyEntityReferenceInCRDInSupervisor(ctx, f, volumeID, crdCNSVolumeMetadatas,
			crdVersion, crdGroup, true, volumeID, true, pvcLabels, true)

		// Check pv label update in CRD.
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID, crdCNSVolumeMetadatas,
			crdVersion, crdGroup, true, volumeID, true, pvLabels, true)

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			pvcLabels, pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(svcVolumeID, pvcLabels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", pvLabels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(svcVolumeID, pvLabels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

})
