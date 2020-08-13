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
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var _ = ginkgo.Describe("[csi-guest] CnsNodeVmAttachment persistense.", func() {
	f := framework.NewDefaultFramework("e2e-node-vm-attachments")
	var (
		client            clientset.Interface
		namespace         string
		scParameters      map[string]string
		storagePolicyName string
		svcPVCName        string // PVC Name in the Supervisor Cluster
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})

	/*
		Test-1 (Attach/Detach)

		Test Steps

		Create a PVC using any replicated storage class from the SV.
		Wait for PVC to be in Bound phase
		Create a Pod with this PVC mounted as a volume
		Verify CnsNodeVmAttachment CRD is created
		Verify Pod is in Running phase
		Verify volume is attached to VM.
		Verify CnsNodeVmAttachment.Attached is true
		Delete Pod
		Verify CnsNodeVmAttachment CRD is deleted
		Verify Pod is deleted from GC
		Verify volume is detached from VM.
		Delete PVC in GC
	*/

	ginkgo.It("Verify CnsNodeVmAttachements existence in a pod lifecycle", func() {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		// svcPVCName refers to PVC Name in the supervisor cluster
		svcPVCName = pv.Spec.CSI.VolumeHandle
		volumeID := getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Creating pod using pvc %s mounted as volume", pvc.Name))
		pod, err := fpod.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		var vmUUID string

		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		verifyIsAttachedInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdVersion, crdGroup)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
		time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
		verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, false)

	})

	/*
		Test-3 (Attach/Detach)

		Test Steps

		Create 2 PVCs using any replicated storage class from the SV.
		Wait for PVCs to be in Bound phase.
		Create 2 Pods concurrently with these PVCs mounted as volumes.
		Verify CnsNodeVmAttachment CRDs are created.
		Verify Pods are in Running phase.
		Verify volumes are attached to VMs.
		Verify CnsNodeVmAttachment.Attached is true
		Delete Pods.
		Verify CnsNodeVmAttachment CRDs are deleted.
		Verify Pods are deleted from GC.
		Verify volumes are detached from VMs.
		Delete PVCs in GC.
	*/

	ginkgo.It("Verify CnsNodeVmAttachements crd existence when pods are created concurrently", func() {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create 2 PVCs using any replicated storage class from the SV")
		scParameters[svStorageClassName] = storagePolicyName
		sc1, pvc1, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		sc2, pvc2, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for first persistent claim %s to be in bound phase", pvc1.Name))
		pvs1, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs1).NotTo(gomega.BeEmpty())
		pv1 := pvs1[0]
		// svcPVCName refers to PVC Name in the supervisor cluster
		svcPVCName1 := pv1.Spec.CSI.VolumeHandle
		volumeID1 := getVolumeIDFromSupervisorCluster(svcPVCName1)
		gomega.Expect(volumeID1).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Waiting for second persistent claim %s to be in bound phase", pvc2.Name))
		pvs2, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs2).NotTo(gomega.BeEmpty())
		pv2 := pvs2[0]
		// svcPVCName refers to PVC Name in the supervisor cluster
		svcPVCName2 := pv2.Spec.CSI.VolumeHandle
		volumeID2 := getVolumeIDFromSupervisorCluster(svcPVCName2)
		gomega.Expect(volumeID2).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create 2 Pods concurrently with these PVCs mounted as volumes")
		go verifyPodCreation(f, client, namespace, pvc1, pv1)
		verifyPodCreation(f, client, namespace, pvc2, pv2)

	})

	/*
		Test - 5 (Attach/Detach)

		Test Steps

		Create a PVC using any replicated storage class from the SV.
		Wait for PVC to be in Bound phase
		Bring down csi-controller pod in SV
		Create a Pod with this PVC mounted as a volume
		Verify CnsNodeVmAttachment CRD is created
		Verify Pod is still in ContainerCreating phase
		Bring up csi-controller pod in SV
		Verify Pod is in Running phase
		Verify volume is attached to VM.
		Verify CnsNodeVmAttachment.Attached is true
		Delete Pod
		Verify CnsNodeVmAttachment CRD is deleted
		Verify volume is detached from VM.
		Delete PVC in GC
	*/

	ginkgo.It("Verify CnsNodeVmAttachements crd and POD is created after CSI controller comes up", func() {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		var isControllerUp = true

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		// svcPVCName refers to PVC Name in the supervisor cluster
		svcPVCName = pv.Spec.CSI.VolumeHandle
		volumeID := getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var svcClient clientset.Interface
		if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
			svcClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring down csi-controller pod in SV")
		bringDownCsiController(svcClient)
		isControllerUp = false
		defer func() {
			if !isControllerUp {
				bringUpCsiController(svcClient)
			}
		}()

		ginkgo.By("Create a Pod with PVC created in previous step mounted as a volume")

		pod := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		pod, err = client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod, err = client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)

		ginkgo.By("Verify CnsNodeVmAttachment CRD is created")
		verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

		ginkgo.By("Verify Pod is still in ContainerCreating phase")
		gomega.Expect(podContainerCreatingState == pod.Status.ContainerStatuses[0].State.Waiting.Reason).To(gomega.BeTrue())

		ginkgo.By("Bring up csi-controller pod in SV")
		bringUpCsiController(svcClient)
		isControllerUp = true

		err = fpod.WaitForPodRunningInNamespace(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler * 2)

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		var vmUUID string

		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifyIsAttachedInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdVersion, crdGroup)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
		time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
		verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, false)

	})

	/*

			Test case - 9 in Attach/Detach

			Steps
			1 - Create a statefulset with 3 replicas
			2 - Wait for all PVCs to be in Bound phase and Pods are Running state
			3 - Verify CnsNodeVmAttachment CRD are created.
			4 - Verify volumes are attached to VM.
			5 - Scale up number of replicas to 5
			6 - Verify CnsNodeVmAttachment CRD are created.
		    7 - Verify volumes are attached to VM.
			8 - Scale down statefulsets to 2 replicas.
			9 - Verify CnsNodeVmAttachment CRD are deleted.
			10- Verify volumes are detached from VM.
			11- Delete statefulset.
			13- Delete PVCs.
	*/

	ginkgo.It("Detach Statefulset testing with default podManagementPolicy", func() {

		ginkgo.By("Creating StorageClass for Statefulset")
		scParameters[svStorageClassName] = storagePolicyName
		sc, err := createStorageClass(client, scParameters, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		//statefulsetTester := fpod.NewStatefulSetTester(client)

		ginkgo.By("Creating service")
		CreateService(namespace, client)
		statefulset := GetStatefulSetFromManifest(namespace)

		ginkgo.By("Create a statefulset with 3 replicas")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Waiting for pods status to be Ready
		ginkgo.By("Wait for all Pods are Running state")
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		// Get the list of Volumes attached to Pods before scale down
		//var volumesBeforeScaleDown []string
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					//volumesBeforeScaleDown = append(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)
					ginkgo.By("Verify CnsNodeVmAttachment CRD is created")
					volumeID := pv.Spec.CSI.VolumeHandle
					// svcPVCName refers to PVC Name in the supervisor cluster
					svcPVCName := volumeID
					volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
					verifyCRDInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					vmUUID, err = getVMUUIDFromNodeName(sspod.Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					verifyIsAttachedInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+pv.Spec.CSI.VolumeHandle, crdVersion, crdGroup)

				}
			}
		}

		var scaledUpReplicas int32 = 5
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", scaledUpReplicas))
		_, scaleupErr := fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)

		ssPodsAfterScaleUp := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitForPodsReady(client, statefulset.Namespace, sspod.Name, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By("Verify CnsNodeVmAttachment CRD is created")
					volumeID := pv.Spec.CSI.VolumeHandle
					// svcPVCName refers to PVC Name in the supervisor cluster
					svcPVCName := volumeID
					volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

					verifyCRDInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string

					vmUUID, err = getVMUUIDFromNodeName(sspod.Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					verifyIsAttachedInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+pv.Spec.CSI.VolumeHandle, crdVersion, crdGroup)

				}
			}
		}

		var scaledDownReplicas int32 = 2

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", scaledDownReplicas))
		_, scaledownErr := fss.Scale(client, statefulset, replicas-1)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas-1)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas-1)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
		// After scale down, verify vSphere volumes are detached from deleted pods
		ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")

		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			if err != nil {
				gomega.Expect(apierrs.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						verifyIsDetachedInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+pv.Spec.CSI.VolumeHandle, crdVersion, crdGroup)
					}
				}
			}
		}
	})

})
