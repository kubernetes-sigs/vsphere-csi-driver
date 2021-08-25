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
	"strings"
	"time"

	"github.com/onsi/ginkgo"
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
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

var _ = ginkgo.Describe("[csi-guest] pvCSI metadata syncer tests", func() {
	f := framework.NewDefaultFramework("e2e-guest-cluster-cnsvolumemetadata")
	var (
		client                     clientset.Interface
		namespace                  string
		svNamespace                string
		scParameters               map[string]string
		storagePolicyName          string
		svcPVCName                 string // PVC Name in the Supervisor Cluster
		labelKey                   string
		labelValue                 string
		gcClusterID                string
		pvcUID                     string
		manifestPath               = "tests/e2e/testing-manifests/statefulset/nginx"
		pvclabelKey                string
		pvclabelValue              string
		pvlabelKey                 string
		pvlabelValue               string
		pvclaim                    *v1.PersistentVolumeClaim
		clientNewGc                clientset.Interface
		pvc                        *v1.PersistentVolumeClaim
		isVsanhealthServiceStopped bool
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		svcClient, svNamespace = getSvcClientAndNamespace()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		setResourceQuota(svcClient, svNamespace, rqLimit)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		labelKey = "app"
		labelValue = "e2e-labels"
		pvclabelKey = "pvcapp"
		pvclabelValue = "e2e-labels-pvc"
		pvlabelKey = "pvapp"
		pvlabelValue = "e2e-labels-pv"
	})

	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		if isVsanhealthServiceStopped {
			ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
			err := invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again",
				vsanHealthServiceWaitTime))
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}
	})

	// Steps:
	// 1. Create a PVC using any replicated storage class from the SV.
	// 2. Wait for PVC to be in Bound phase.
	// 3. Verify CnsVolumeMetadata CRD in SV is created.
	// 4. Create a Pod with this PVC mounted as a volume.
	// 5. Verify entityReference for this volume on CNS contains entries for
	//    PV/PVC/Pod in GC and PVC in SV.
	// 6. Delete Pod.
	// 7. Delete PVC.
	ginkgo.It("Verify CnsVolumeMetadata's entityReference for the volume on CNS", func() {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		// svcPVCName refers to PVC Name in the supervisor cluster.
		svcPVCName = volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
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
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		podUID := string(pod.UID)
		framework.Logf("Pod uuid : " + podUID)
		framework.Logf("PVC name in SV " + svcPVCName)
		pvcUID = string(pvc.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)
		gcClusterID = strings.Replace(svcPVCName, pvcUID, "", -1)

		framework.Logf("gcClusterId " + gcClusterID)
		pvUID := string(pv.UID)
		framework.Logf("PV uuid " + pvUID)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle, crdCNSVolumeMetadatas,
			crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+podUID, crdCNSVolumeMetadatas,
			crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID, crdCNSVolumeMetadatas,
			crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	})

	// Steps:
	// 1. Create a PVC using any replicated storage class from the SV.
	// 2. Wait for PVC to be in Bound phase.
	// 3. Verify entityReference for this volume on CNS contains entries for
	//    PV/PVC in GC and PVC in SV.
	// 4. Update PVC Labels.
	// 5. Verify CnsVolumeMetadata CRD in SV is updated.
	// 6. Wait for labels to be present in CNS.
	// 7. Delete PVC Labels.
	// 8. Verify CnsVolumeMetadata CRD in SV is updated.
	// 9. Wait for labels to be deleted in CNS.
	// 10. Delete PVC.
	ginkgo.It("Validate PVC labels are updated/deleted on CNS", func() {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		// svcPVCName refers to PVC Name in the supervisor cluster.
		svcPVCName = volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("PVC name in SV " + svcPVCName)
		pvcUID = string(pvc.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)
		gcClusterID = strings.Replace(svcPVCName, pvcUID, "", -1)

		framework.Logf("gcClusterId " + gcClusterID)
		pvUID := string(pv.UID)
		framework.Logf("PV uuid " + pvUID)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// TODO: Replace sleep with polling mechanism.
		framework.Logf("Sleeping for 20 seconds for the labels to be updated")
		time.Sleep(20 * time.Second)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, true, labels, true)

	})

	// Steps:
	// 1. Create a PVC using any replicated storage class from the SV.
	// 2. Wait for PVC to be in Bound phase.
	// 3. Create a Pod attached to above PV.
	// 4. Verify CnsVolumeMetadata CRD in SV is created.
	// 5. Wait for Pod name to be present in CNS.
	// 6. Verify entityReference for this volume on CNS contains entries for
	//    PV/PVC/Pod in GC and PVC in SV.
	// 7. Delete Pod and wait for disk to be detached.
	// 8. Verify CnsVolumeMetadata CRD in SV is deleted.
	// 9. Wait for Pod name to be deleted in CNS.
	// 10. Delete PVC.
	ginkgo.It("Verify Pod Name is updated/deleted on CNS", func() {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		// svcPVCName refers to PVC Name in the supervisor cluster.
		svcPVCName = volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
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
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		podUID := string(pod.UID)
		framework.Logf("Pod uuid : " + podUID)
		framework.Logf("PVC name in SV " + svcPVCName)
		pvcUID = string(pvc.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)
		gcClusterID = strings.Replace(svcPVCName, pvcUID, "", -1)

		framework.Logf("gcClusterId " + gcClusterID)
		pvUID := string(pv.UID)
		framework.Logf("PV uuid " + pvUID)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+podUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		// TODO: Replace sleep with polling mechanism.
		ginkgo.By("Sleeping for 20s for update...")
		time.Sleep(20 * time.Second)
		// Verifying the CnsVolumeMetadata CRD for Pod in SV is deleted.
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+podUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, false, pv.Spec.CSI.VolumeHandle, false, nil, false)
	})

	// Steps:
	// 1. Create a Storage Class.
	// 2. Create a statefulset with 3 replicas.
	// 3. Wait for all PVCs to be in Bound phase and Pods are Ready state.
	// 4. Update PVC labels.
	// 5. Verify PVC labels are updated on CNS.
	// 6. Scale up number of replicas to 5.
	// 7. Update PV labels.
	// 8. Verify PV labels are updated on CNS.
	// 9. Scale down statefulsets to 0 replicas and delete all pods.
	// 10. Delete PVCs.
	// 11. Delete SC.

	ginkgo.It("Statefulset tests with label updates", func() {
		var sc *storagev1.StorageClass
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")

		ginkgo.By("Creating StorageClass for Statefulset")
		scParameters[svStorageClassName] = storagePolicyName
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating statefulset")
		statefulset := fss.CreateStatefulSet(client, manifestPath, namespace)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
				err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleup := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleup.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleup.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		pvclabels := make(map[string]string)
		pvclabels[pvclabelKey] = pvclabelValue
		var volumeID string

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
					volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
					framework.Logf("value of volumeID " + volumeID)
					ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
						pvclabels, volumespec.PersistentVolumeClaim.ClaimName,
						GetAndExpectStringEnvVar(envSupervisorClusterNamespace)))
					err = e2eVSphere.waitForLabelsToBeUpdated(volumeID, pvclabels,
						string(cnstypes.CnsKubernetesEntityTypePVC), volumespec.PersistentVolumeClaim.ClaimName, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas+2))
		_, scaleupErr := fss.Scale(client, statefulset, replicas+2)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, replicas+2)
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas+2)
		pvlabels := make(map[string]string)
		pvlabels[pvlabelKey] = pvlabelValue

		ssPodsAfterScaleUp := fss.GetPodList(client, statefulset)

		for _, spod := range ssPodsAfterScaleUp.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, spod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range spod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", pvlabels, pv.Name))
					pv.Labels = pvlabels
					pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
					framework.Logf("value of volumeID " + volumeID)
					ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", pvlabels, pv.Name))
					err = e2eVSphere.waitForLabelsToBeUpdated(volumeID, pvlabels,
						string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, "")
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", 0))
		_, scaledownErr := fss.Scale(client, statefulset, 0)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, 0)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(0)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
	})

	// Steps:
	// 1. Create a PVC using any replicated storage class from the SV.
	// 2. Wait for PVC to be in Bound phase.
	// 3. Bring down csi-controller pod in SV.
	// 4. Update PV/PVC labels.
	// 5. Verify CnsVolumeMetadata CRDs are updated.
	// 6. Bring up csi-controller pod in SV.
	// 7. Verify PV and PVC entry is updated in CNS.
	// 8. Delete PVC.
	ginkgo.It("Verify CNS Operator receives callbacks on all objects when csi-controller was brought back up", func() {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		// svcPVCName refers to PVC Name in the supervisor cluster.
		svcPVCName = volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
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
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		podUID := string(pod.UID)
		framework.Logf("Pod uuid : " + podUID)
		framework.Logf("PVC name in SV " + svcPVCName)
		pvcUID = string(pvc.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)
		gcClusterID = strings.Replace(svcPVCName, pvcUID, "", -1)

		framework.Logf("gcClusterId " + gcClusterID)
		pvUID := string(pv.UID)
		framework.Logf("PV uuid " + pvUID)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+podUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)

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

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)

		ginkgo.By("Scaling up the csi driver to one replica")
		deployment = updateDeploymentReplica(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		ginkgo.By(fmt.Sprintf("Successfully scaled up the csi driver deployment:%s to one replica", deployment.Name))

		// TODO: Replace sleep with polling mechanism.
		framework.Logf("Sleeping for 60 seconds")
		time.Sleep(60 * time.Second)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, true, labels, true)

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	})

	// Metadata Syncer - 2
	//
	// Steps:
	// 1. Create a PVC using any replicated storage class from the SV.
	// 2. Wait for PVC to be in Bound phase.
	// 3. Verify entityReference for this volume on CNS contains entries for
	//    PV/PVC in GC and PVC in SV.
	// 4. Update PV Labels.
	// 5. Verify CnsVolumeMetadata CRD in SV is updated.
	// 6. Wait for labels to be present in CNS.
	// 7. Delete PV Labels.
	// 8. Verify CnsVolumeMetadata CRD in SV is updated.
	// 9. Wait for labels to be deleted in CNS.
	// 10. Delete PVC.

	ginkgo.It("Validate PV labels are updated/deleted on CNS.", func() {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		framework.Logf("pv name is : %s", pv.GetName())
		svcPVCName := volumeID
		framework.Logf("volume id  is : %s", volumeID)
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		framework.Logf("SVC PVC volume id  is : %s", volumeID)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		svcPvName := getPVCFromSupervisorCluster(svcPVCName)
		framework.Logf("svcPvName   is : %s", svcPvName)
		pvcUID := string(pvc.GetUID())
		pvUID := string(pv.UID)
		gcClusterID := strings.Replace(svcPVCName, pvcUID, "", -1)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels

		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(volumeID, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Fetching updated pv %s in namespace %s", pv.Name, pv.Namespace))
		pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting labels for pv %s", pv.Name))
		pv.Labels = make(map[string]string)

		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels  to be deleted for pv %s", pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(volumeID, pv.Labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	// Metadata Syncer - 4
	// Steps:
	//
	// 1. Create a PVC using any replicated storage class from the SV.
	// 2. Wait for PVC to be in Bound phase.
	// 3. Verify entityReference for this volume on CNS contains entries for
	//    PV/PVC in GC and PVC in SV.
	// 4. Update PVC Labels.
	// 5. Update PV Labels.
	// 6. Verify CnsVolumeMetadata CRDs in SV are updated.
	// 7. Wait for labels to be present in CNS.
	// 8. Delete PVC Labels.
	// 9. Delete PV Labels.
	// 10. Verify CnsVolumeMetadata CRD in SV are updated.
	// 11. Wait for labels to be deleted in CNS.
	// 12. Delete PVC.
	ginkgo.It("Validate PV and PVC labels are updated/deleted on CNS", func() {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		framework.Logf("pv name is : %s", pv.GetName())
		svcPVCName := volumeID
		framework.Logf("volume id  is : %s", volumeID)
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		framework.Logf("SVC PVC volume id  is : %s", volumeID)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		svcPvName := getPVCFromSupervisorCluster(svcPVCName)
		framework.Logf("svcPvName   is : %s", svcPvName)
		pvcUID := string(pvc.GetUID())
		pvUID := string(pv.UID)
		gcClusterID := strings.Replace(svcPVCName, pvcUID, "", -1)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels

		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(volumeID, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, true, labels, true)

		ginkgo.By(fmt.Sprintf("Fetching updated pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s", labels, pvc.Name))
		pvc.Labels = labels

		_, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s", labels, pvc.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(volumeID, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvcUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, true, labels, true)

		ginkgo.By(fmt.Sprintf("Fetching updated pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Fetching updated pv %s in namespace %s", pv.Name, pv.Namespace))
		pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting labels for pv %s", pv.Name))
		pv.Labels = make(map[string]string)

		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels to be deleted for pv %s", pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(volumeID, pv.Labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, true, pv.Labels, true)

		ginkgo.By(fmt.Sprintf("deleting labels for pvc %s", pvc.Name))
		pvc.Labels = make(map[string]string)

		_, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels to be deleted for pvc %s", pvc.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(volumeID, pvc.Labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, true, pvc.Labels, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	// Metadata Syncer - 6
	// Steps:
	//
	// 1. Create multiple PVCs using any replicated storage class from the SV.
	// 2. Wait for PVCs to be in Bound phase.
	// 3. Create a Pod attached to above PVCs.
	// 4. Verify CnsVolumeMetadata CRD in SV is created.
	// 5. Wait for Pod name to be present in CNS.
	// 6. Verify entityReference for all volumes on CNS contains entries for
	//    PV/PVC/Pod in GC and PVC in SV.
	// 7. Delete Pod and wait for disk to be detached.
	// 8. Verify CnsVolumeMetadata CRD in SV is deleted.
	// 9. Wait for Pod name to be deleted in CNS.
	// 10. Delete PVCs.

	ginkgo.It("Multiple PVCs - Verify Pod Name is updated/deleted on CNS", func() {
		var sc *storagev1.StorageClass
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc1, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		sc2, pvc2, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.StorageV1().StorageClasses().Delete(ctx, sc2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc1.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc1, pvc2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv1 := pvs[0]
		pv2 := pvs[1]

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc1, pvc2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		verifyEntityReferenceForKubEntities(ctx, f, client, pv1, pvc1, pod)
		verifyEntityReferenceForKubEntities(ctx, f, client, pv2, pvc2, pod)

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv1.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv1.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

	})

	// Metadata Syncer Test-10
	// Test Steps:
	//
	// 1. Create a PVC using any replicated storage class from the SV.
	// 2. Wait for PVC to be in Bound phase.
	// 3. Stop vsan-health.
	// 4. Update PV/PVC labels.
	// 5. Verify CnsVolumeMetadata CRD is updated.
	// 6. Start vsan-health.
	// 7. Verify labels are updated on CNS.
	// 8. Delete PVC in GC.
	ginkgo.It("Verify CnsVolumeMetadata updated after vsan health restart", func() {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())

		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		svcPVCName := volumeID
		cnsQueryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("queryResult: %v", cnsQueryResult)
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		isVsanhealthServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels

		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, true, labels, true)

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanhealthServiceStopped = false

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(volumeID, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	// Steps:
	// 1. Create a PVC using any replicated storage class from the SV.
	// 2. Wait for PVC to be in Bound phase.
	// 3. Verify entityReference for this volume on CNS contains entries for
	//    PV/PVC in GC and PVC in SV.
	// 4. Make datastore on which this volume is created inaccessible.
	// 5. Update PVC Labels.
	// 6. Update PV Labels.
	// 7. Verify CnsVolumeMetadata CRDs in SV are updated.
	// 8. Verify labels are not updated on CNS.
	// 9. Make datastore accessible.
	// 10. Verify labels are updated on CNS.
	// 11. Delete PVC.
	ginkgo.It("Verify labels are not updated on inaccessible datastore", func() {
		var err error
		var sc *storagev1.StorageClass
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("CNS_TEST: Running for GC setup")
		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		svcVolumeID := getVolumeIDFromSupervisorCluster(volumeID)
		gomega.Expect(svcVolumeID).NotTo(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("SVC volume ID for claim %s is %s", volumeID, svcVolumeID))
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		isVsanhealthServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		pvLabels := make(map[string]string)
		pvLabels["pvlabelKey"] = pvlabelValue

		pvcLabels := make(map[string]string)
		pvcLabels["pvclabelKey"] = pvclabelValue

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

		fmt.Println("PVC name in SV", volumeID)
		pvcUID := string(pvc.GetUID())
		fmt.Println("PVC UUID in GC", pvcUID)
		gcClusterID := strings.Replace(volumeID, pvcUID, "", -1)

		fmt.Println("gcClusterId", gcClusterID)
		pvUID := string(pv.UID)
		fmt.Println("PV uuid", pvUID)

		// Check pvc label update in CRD.
		verifyEntityReferenceInCRDInSupervisor(ctx, f, volumeID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, volumeID, true, pvcLabels, true)

		// Check pv label update in CRD.
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, volumeID, true, pvLabels, true)

		/*
			TODO:
			Since we are bringing vsan-health service down to simulate data store
			being in-accessible to CNS. It brings down CNS as well.
			Hence commenting the steps below until we have a better way to make
			the datastore inaccessible.

			ginkgo.By(fmt.Sprintf("Sleeping for %v minutes and verifying labels are not updated on CNS", pollTimeout))
			time.Sleep(pollTimeout)
			ginkgo.By("Checking PVC labels via CNS")
			cnsLabels, err := e2eVSphere.getLabelsForCNSVolume(svcVolumeID,
				string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(cnsLabels)).Should(gomega.BeZero())
			ginkgo.By("Checking PV labels via CNS")
			cnsLabels, err = e2eVSphere.getLabelsForCNSVolume(svcVolumeID,
				string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(cnsLabels)).Should(gomega.BeZero())
		*/

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanhealthServiceStopped = false

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

	// Metadata syncer testcases
	// TC-8 - Verify static provisioning across Guest Clusters.
	// 1. Create a PVC using any replicated storage class from the SV.
	// 2. Wait for PVC to be in Bound phase.
	// 3. Verify entityReference for this volume on CNS contains entries for
	//    PV/PVC in GC and PVC in SV.
	// 4. Change reclaimPolicy of PV to Retain.
	// 5. Delete PVC.
	// 6. Verify CnsVolumeMetadata CRD is deleted.
	// 7. Verify PVC name is removed from volume entry on CNS.
	// 8. Verify entityReference for this volume on CNS contains entries for
	//    PV in GC and PVC in SV.
	// 9. Delete PV.
	// 10.Verify CnsVolumeMetadata CRD is deleted.
	// 11.Verify PV entry is deleted from CNS.
	// 12.Verify entityReference for this volume on CNS contains entries for
	//    PVC in SV.
	//
	// From a new guest cluster, do the following:
	// 1. Create PV with VolumeHandle=PVC in SV.
	// 2. Verify CnsVolumeMetadata CRD is created.
	// 3. Wait for PV entry to be present in CNS.
	// 4. Verify entityReference for this volume on CNS contains entries for
	//    PV in GC and PVC in SV.
	// 5. Delete PV on GC.
	// 6. Verify entityReference for this volume on CNS contains entries for
	//    and PVC in SV.
	// 7. Delete the corresponding PVC on SV.
	ginkgo.It("MultipleGC Verify static provisioning across Guest Clusters.", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newGcKubconfigPath := os.Getenv("NEW_GUEST_CLUSTER_KUBE_CONFIG")
		if newGcKubconfigPath == "" {
			ginkgo.Skip("Env NEW_GUEST_CLUSTER_KUBE_CONFIG is missing")
		}

		ginkgo.By("Creating PVC in  GC1")
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		// Create Storage class and PVC.
		ginkgo.By("Creating Storage Class and PVC")

		scParameters[svStorageClassName] = storagePolicyName
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if pvclaim != nil {
				ginkgo.By("Delete the PVC")
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Waiting for PVC to be bound.
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		volumeID := pv.Spec.CSI.VolumeHandle
		// svcPVCName refers to PVC Name in the supervisor cluster.
		svcPVCName := volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		framework.Logf("PVC name in SV " + svcPVCName)
		pvcUID := string(pvclaim.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)
		gcClusterID := strings.Replace(svcPVCName, pvcUID, "", -1)

		framework.Logf("gcClusterId " + gcClusterID)
		pvUID := string(pv.UID)
		framework.Logf("PV uuid " + pvUID)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, gcClusterID+pvUID, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvcUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, gcClusterID+pvcUID, false, nil, false)

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

		// Changing the reclaim policy of the pv to retain.
		ginkgo.By("Changing the volume reclaim policy")
		pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete the PVC")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil

		ginkgo.By("verify crd in supervisor")
		framework.Logf("Expected instance %v", pv.Spec.CSI.VolumeHandle)
		verifyCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, false, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)

		ginkgo.By(fmt.Sprintf("Delete the PV %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, false, gcClusterID+pvUID, false, nil, false)

		ginkgo.By("Verifying if volume still exists in the Supervisor Cluster")
		// svcPVCName refers to PVC Name in the supervisor cluster.
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		pvAnnotations := pv.Annotations
		pvSpec := pv.Spec.CSI
		pvStorageClass := pv.Spec.StorageClassName

		// Create PV in New GC.
		clientNewGc, err = k8s.CreateKubernetesClientFromConfig(newGcKubconfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Error creating k8s client with %v: %v", newGcKubconfigPath, err))
		ginkgo.By("Creating namespace on second GC")
		ns, err := framework.CreateTestingNS(f.BaseName, clientNewGc, map[string]string{
			"e2e-framework": f.BaseName,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on second GC")

		namespaceNewGC := ns.Name
		framework.Logf("Created namespace on second GC %v", namespaceNewGC)
		defer func() {
			err := clientNewGc.CoreV1().Namespaces().Delete(ctx, namespaceNewGC, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var svClient clientset.Interface
		if k8senvsv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senvsv != "" {
			svClient, err = k8s.CreateKubernetesClientFromConfig(k8senvsv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		svcNamespace := os.Getenv("SVC_NAMESPACE")

		ginkgo.By("Creating PVC in New GC with the vol handle from SVC")
		scParameters = make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		scParameters[svStorageClassName] = storagePolicyName
		storageclassNewGC, err := createStorageClass(clientNewGc,
			scParameters, nil, v1.PersistentVolumeReclaimDelete, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcNew, err := createPVC(clientNewGc, namespaceNewGC, nil, "", storageclassNewGC, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvcs []*v1.PersistentVolumeClaim
		pvcs = append(pvcs, pvcNew)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(clientNewGc, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvNewGC := getPvFromClaim(clientNewGc, pvcNew.Namespace, pvcNew.Name)
		volumeIDNewGC := pvNewGC.Spec.CSI.VolumeHandle
		// svcNewPVCName refers to PVC Name in the supervisor cluster.
		svcNewPVCName := volumeIDNewGC
		volumeIDNewGC = getVolumeIDFromSupervisorCluster(svcNewPVCName)
		gomega.Expect(volumeIDNewGC).NotTo(gomega.BeEmpty())

		framework.Logf("PVC name in SV " + svcNewPVCName)
		pvcNewUID := string(pvcNew.GetUID())
		framework.Logf("pvcNewUID in GC " + pvcNewUID)
		gcNewClusterID := strings.Replace(svcNewPVCName, pvcNewUID, "", -1)

		ginkgo.By("Creating PV in new guest cluster with volume handle from SVC")
		pvNew := getPersistentVolumeSpec(svcPVCName, v1.PersistentVolumeReclaimDelete, nil)
		pvNew.Annotations = pvAnnotations
		pvNew.Spec.StorageClassName = pvStorageClass
		pvNew.Spec.CSI = pvSpec
		pvNew.Spec.CSI.VolumeHandle = svcPVCName
		pvNew, err = clientNewGc.CoreV1().PersistentVolumes().Create(ctx, pvNew, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvNewUID := string(pvNew.UID)
		framework.Logf("pvNew uuid " + pvNewUID)

		ginkgo.By("verify crd in supervisor")
		time.Sleep(10 * time.Second)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcNewClusterID+pvNewUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, gcClusterID+pvNewUID, false, nil, false)
		verifyCRDInSupervisor(ctx, f, gcNewClusterID+pvNewUID, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)
		defer func() {
			if pvc != nil {
				ginkgo.By("Delete the PVC in SVC")
				pvc, err := svClient.CoreV1().PersistentVolumeClaims(svcNamespace).Get(ctx, svcPVCName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				err = fpv.DeletePersistentVolumeClaim(svClient, pvc.Name, pvc.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By(fmt.Sprintf("Delete the PV %s", pvNew.Name))
				err = clientNewGc.CoreV1().PersistentVolumes().Delete(ctx, pvNew.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		ginkgo.By(fmt.Sprintf("Delete the PV %s", pvNew.Name))
		err = clientNewGc.CoreV1().PersistentVolumes().Delete(ctx, pvNew.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvNewUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, false, gcClusterID+pvNewUID, false, nil, false)
		// Delete PVC in SVC.
		pvc, err := svClient.CoreV1().PersistentVolumeClaims(svcNamespace).Get(ctx, svcPVCName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete the PVC in SVC")
		err = fpv.DeletePersistentVolumeClaim(svClient, pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc = nil
	})

	// Metadata syncer testcases - TC 7
	// TC-8 - Verify static provisioning across Guest Clusters.
	// 1. Create a PVC using any replicated storage class from the SV.
	// 2. Wait for PVC to be in Bound phase.
	// 3. Verify entityReference for this volume on CNS contains entries for
	//    PV/PVC in GC and PVC in SV.
	// 4. Change reclaimPolicy of PV to Retain.
	// 5. Delete PVC.
	// 6. Verify CnsVolumeMetadata CRD is deleted.
	// 7. Verify PVC name is removed from volume entry on CNS.
	// 8. Verify entityReference for this volume on CNS contains entries for
	//    PV in GC and PVC in SV.
	// 9. Delete PV.
	// 10.Verify CnsVolumeMetadata CRD is deleted.
	// 11.Verify PV entry is deleted from CNS.
	// 12.Verify entityReference for this volume on CNS contains entries for
	//    PVC in SV.
	//
	// 13.Create PV with VolumeHandle=PVC in SV.
	// 14.Verify CnsVolumeMetadata CRD is created.
	// 15.Wait for PV entry to be present in CNS.
	// 16.Verify entityReference for this volume on CNS contains entries for
	//    PV in GC and PVC in SV.
	// 17.Delete PV on GC.
	// 18.Verify entityReference for this volume on CNS contains entries for
	//    and PVC in SV.
	// 19.Delete the corresponding PVC on SV.

	ginkgo.It("Static provisioning across Guest Clusters.", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Creating PVC in  GC")
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		// Create Storage class and PVC.
		ginkgo.By("Creating Storage Class and PVC")

		scParameters[svStorageClassName] = storagePolicyName
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if pvclaim != nil {
				ginkgo.By("Delete the PVC")
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Waiting for PVC to be bound.
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		volumeID := pv.Spec.CSI.VolumeHandle

		pvAnnotations := pv.Annotations
		pvSpec := pv.Spec.CSI

		// svcPVCName refers to PVC Name in the supervisor cluster.
		svcPVCName := volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		framework.Logf("PVC name in SV " + svcPVCName)
		pvcUID := string(pvclaim.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)
		gcClusterID := strings.Replace(svcPVCName, pvcUID, "", -1)

		framework.Logf("gcClusterId " + gcClusterID)
		pvUID := string(pv.UID)
		framework.Logf("PV uuid " + pvUID)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, gcClusterID+pvUID, false, nil, false)

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

		// Changing the reclaim policy of the pv to retain.
		ginkgo.By("Changing the volume reclaim policy")
		pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete the PVC")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil

		ginkgo.By("verify crd in supervisor")
		verifyCRDInSupervisorWithWait(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, false, pv.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)

		ginkgo.By(fmt.Sprintf("Delete the PV %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, false, gcClusterID+pvUID, false, nil, false)

		ginkgo.By("Verifying if volume still exists in the Supervisor Cluster")
		// svcPVCName refers to PVC Name in the supervisor cluster.
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		var svClient clientset.Interface
		if k8senvsv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senvsv != "" {
			svClient, err = k8s.CreateKubernetesClientFromConfig(k8senvsv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		svcNamespace := os.Getenv("SVC_NAMESPACE")

		ginkgo.By("Creating PV in guest cluster with volume handle from SVC")
		pvNew := getPersistentVolumeSpec(svcPVCName, v1.PersistentVolumeReclaimDelete, nil)
		pvNew.Annotations = pvAnnotations
		pvNew.Spec.StorageClassName = "gc-storage-profile"
		pvNew.Spec.CSI = pvSpec
		pvNew.Spec.CSI.VolumeHandle = svcPVCName
		pvNew, err = client.CoreV1().PersistentVolumes().Create(ctx, pvNew, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvNewUID := string(pvNew.UID)
		framework.Logf("pvNew uuid " + pvNewUID)

		defer func() {
			if pvc != nil {
				ginkgo.By("Delete the PVC in SVC")
				pvc, err := svClient.CoreV1().PersistentVolumeClaims(svcNamespace).Get(ctx, svcPVCName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				err = fpv.DeletePersistentVolumeClaim(svClient, pvc.Name, pvc.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("verify crd in supervisor")
		verifyCRDInSupervisorWithWait(ctx, f, gcClusterID+pvNewUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvNewUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, gcClusterID+pvNewUID, false, nil, false)

		ginkgo.By(fmt.Sprintf("Delete the PV %s", pvNew.Name))
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pvNew.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvNewUID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, false, gcClusterID+pvNewUID, false, nil, false)

		ginkgo.By("Delete the PVC in SVC")
		pvc, err := svClient.CoreV1().PersistentVolumeClaims(svcNamespace).Get(ctx, svcPVCName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpv.DeletePersistentVolumeClaim(svClient, pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc = nil
	})
})
