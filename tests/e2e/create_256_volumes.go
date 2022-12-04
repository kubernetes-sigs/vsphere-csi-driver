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
	"strings"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/drain"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

// path to 256 disk statefulset spec
const (
	manifestPathFor256Disks = "tests/e2e/testing-manifests/statefulset256disk"
)

var _ = ginkgo.Describe("[csi-vanilla-256-disk-support] Volume-Provisioning-With-256-Disk-Support", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                  clientset.Interface
		namespace               string
		statefulSetReplicaCount int32
		pvcCount                int
		csiReplicas             int32
		csiNamespace            string
		bindingMode             storagev1.VolumeBindingMode
		allMasterIps            []string
		masterIp                string
	)

	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer

		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]
		fmt.Println(masterIp)

		enablePvScsiCtrlFor256DiskSupport()
		setMaxVolPerNodeToEnable256disk(ctx, client, masterIp, csiNamespace)

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		TESTCASE-1
		Consider a testbed of 3 worker nodes and each worker node supports 255 volumes attachment.
		so - 253 * 3 = 765 volume attachment we need to create and attach it to Pods and verify.

		Steps//
		1. Create Storage Class with Immediate Binding mode
		2. Trigger StatefulSet-1 with replica count 63 and with 4 PVC's attached to each replica Pod
		 using above created SC (i.e 63 Pods with 252 volume attachment)
		3. Verify all PV's, PVC's are in Bound state and Statefulset Pods are in up and running state.
		4. Trigger StatefulSet-2 with replica count 63 and with 4 PVC's attached to each replica Pod
		 using above created SC (i.e 63 Pods with 252 volume attachment)
		5. Verify all PV's, PVC's are in Bound state and Statefulset Pods are in up and running state.
		6. Trigger StatefulSet-3 with replica count 63 and with 4 PVC's attached to each replica Pod
		 using above created SC (i.e 63 Pods with 252 volume attachment)
		7. Verify all PV's, PVC's are in Bound state and Statefulset Pods are in up and running state.
		8. Create 9 standalone PVC's.
		9. Verify all standalone PVC's are in Bound state.
		10. Create 9 Pods using above created PVC's
		11. Verify standalone Pods are in up and running state.
		12. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Verify volume provisioning when multiple statefulsets are triggered with 63 replicas and "+
		"each replica pod is attached to 4 pvcs", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		statefulSetReplicaCount = 63
		pvcCount = 9
		var podList []*v1.Pod

		ginkgo.By("Create Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create statefulset-1 with 63 replica pods and each pod is attached to 4 pvcs")
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset1, client,
			statefulSetReplicaCount)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			deleteAllStsAndTheirPVCInNSWithLargeTimeout(client, namespace)
		}()

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := GetListOfPodsInSts(client, statefulset1)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-2 with 63 replica pods and each pod is attached to 4 pvcs")
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset2, client, statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset2)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-3 with 63 replica pods and each pod is attached to 4 pvcs")
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset3, client, statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset3)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}
		}()
	})

	/*
		TESTCASE-2
		Perform multiple times scaleup/scaledown operation when creating 765 volume attachment

		Steps//
		1. Create Storage Class with Immediate Binding mode
		2. Trigger StatefulSet-1 with replica count 40, with each replica Pod is attached to 4 PVC's.
		3. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		4. Trigger StatefulSet-2 with replica count 35, with each replica Pod is attached to 4 PVC's.
		5. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		6. Trigger StatefulSet-3 with replica count 50, with each replica Pod is attached to 4 PVC's.
		7. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		8. Perform scaleup operation on StatefulSet-1. Scale up the replica count of sts-1 from 40 to 52.
		9. Verify scaleup operation of StatefulSet-1 went successful.
		10. Perform scaleup operation on StatefulSet-2. Increase the replica count of sts-2 from 35 to 40.
		11. Verify scaleup operation went successful.
		12. Perform scaledown operation on StatefulSet-3. Scale down the replica count of sts-3 from 50 to 30.
		13. Verify scaledown operation went successful.
		14. Perform scaledown operation on StatefulSet-1. Scale down the replica count of sts-1 from 52 to 42.
		15. Verify scaledown operation went successful.
		16. Perform scaleup operation on StatefulSet-2. Scale up the replica count of sts-2 from 40 to 60.
		17. Verify scaleup operation went successful.
		18. Perform scaleup operation on StatefulSet-3. Scale up the replica count of sts-3 from 30 to 50.
		19. Verify scaleup operation went successful.
		20. Scale up all 3 statefulsets (sts-1,sts-2,sts-3) to replica count 63.
		21. Verify scaleup operation on all 3 statefulsets are successful.
		22. Create 9 standalone PVC's.
		23. Verify standalone PVC's are in Bound state.
		24. Create 9 Pods using above created PVC's
		25. Verify standalone Pods are in up and running state.
		26. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Perform multiple times scaleup scaledown operation on multiple triggered statefulsets "+
		"while creating 256 volumes attachment", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pvcCount = 9
		var podList []*v1.Pod

		ginkgo.By("Create Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create statefulset-1 with 40 replica pods and each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 40
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset1, client, statefulSetReplicaCount)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			deleteAllStsAndTheirPVCInNSWithLargeTimeout(client, namespace)
		}()

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := GetListOfPodsInSts(client, statefulset1)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-2 with 35 replica pods and each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 35
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset2, client,
			statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset2)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-3 with 50 replica pods and each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 50
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset3, client,
			statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset3)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Scaleup statefulset-1 replica pod count to 52")
		statefulSetReplicaCount = 52
		scaleUpStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaleup statefulset-2 replica pod count to 40")
		statefulSetReplicaCount = 40
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaledown statefulset-3 replica pod count to 30")
		statefulSetReplicaCount = 30
		scaleDownStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaledown statefulset-1 replica pod count to 42")
		statefulSetReplicaCount = 42
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaleup statefulset-2 replica pod count to 60")
		statefulSetReplicaCount = 60
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaleup statefulset-3 replica pod count to 50")
		statefulSetReplicaCount = 50
		scaleUpStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("ScaleUp statefulset-1, statefulset-2, statefulset-3 replica pod count to 63")
		statefulSetReplicaCount = 63
		scaleUpStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)
		scaleUpStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}

		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}
		}()
	})

	/*
		TESTCASE-3
		Restart node daemonset when multiple times scaleup/scaledown operation is in progress

		Steps//
		1. Create Storage Class with Immediate Binding mode
		2. Trigger StatefulSet-1 using parallel pod management policy with replica count 40,
		with each replica Pod is attached to 4 PVC's.
		3. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		4. Trigger StatefulSet-2 using default pod management policy with replica count 35,
		with each replica Pod is attached to 4 PVC's.
		5. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		6. Trigger StatefulSet-3 using parallel pod management policy with replica count 50,
		with each replica Pod is attached to 4 PVC's.
		7. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		8. Perform scaleup operation on StatefulSet-1. Scale up the replica count of sts-1 from 40 to 52.
		9. Verify scaleup operation of StatefulSet-1 went successful.
		10. Restart node daemon set.
		11. Perform scaleup operation on StatefulSet-2. Increase the replica count of sts-2 from 35 to 40.
		12. Verify scaleup operation went successful.
		13. Perform scaledown operation on StatefulSet-3. Scale down the replica count of sts-3 from 50 to 30.
		14. Verify scaledown operation went successful.
		15. Perform scaledown operation on StatefulSet-1. Scale down the replica count of sts-1 from 52 to 42.
		16. Verify scaledown operation went successful.
		17. Perform scaleup operation on StatefulSet-2. Scale up the replica count of sts-2 from 40 to 60.
		18. Verify scaleup operation went successful.
		19. Perform scaleup operation on StatefulSet-3. Scale up the replica count of sts-3 from 30 to 50.
		20. Verify scaleup operation went successful.
		21. Restart node daemo set.
		22. Scale up all 3 statefulsets (sts-1,sts-2,sts-3) to replica count 63.
		23. Verify scaleup operation on all 3 statefulsets are successful.
		24. Create 9 standalone PVC's.
		25. Verify standalone PVC's are in Bound state.
		26. Create 9 Pods using above created PVC's
		27. Verify standalone Pods are in up and running state.
		28. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Trigger multiple statefulsets using different management policy and restart node "+
		"daemon set when statefulsets scaleup/sclaedown operation is in progress", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pvcCount = 9
		var podList []*v1.Pod
		ignoreLabels := make(map[string]string)

		ginkgo.By("Create Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create statefulset-1 with 40 replica pods and each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 40
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		statefulset1.Spec.PodManagementPolicy = apps.ParallelPodManagement
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset1, client, statefulSetReplicaCount)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			deleteAllStsAndTheirPVCInNSWithLargeTimeout(client, namespace)
		}()

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := GetListOfPodsInSts(client, statefulset1)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-2 with 35 replica pods and each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 35
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset2, client,
			statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset2)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-3 with 50 replica pods and each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 50
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		statefulset3.Spec.PodManagementPolicy = apps.ParallelPodManagement
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset3, client,
			statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset3)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Scaleup statefulset-1 replica pod count to 52")
		statefulSetReplicaCount = 52
		scaleUpStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)

		// Fetch the number of CSI pods running before restart
		list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		num_csi_pods := len(list_of_pods)

		// Collecting and dumping csi pod logs before restrating CSI daemonset
		collectPodLogs(ctx, client, csiSystemNamespace)
		ginkgo.By("Restart Daemonset")
		cmd := []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
		framework.RunKubectlOrDie(csiSystemNamespace, cmd...)
		ginkgo.By("Waiting for daemon set rollout status to finish")
		statusCheck := []string{"rollout", "status", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
		framework.RunKubectlOrDie(csiSystemNamespace, statusCheck...)
		// wait for csi Pods to be in running ready state
		err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout,
			ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scaleup statefulset-2 replica pod count to 40")
		statefulSetReplicaCount = 40
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaledown statefulset-3 replica pod count to 30")
		statefulSetReplicaCount = 30
		scaleDownStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaledown statefulset-1 replica pod count to 42")
		statefulSetReplicaCount = 42
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaleup statefulset-2 replica pod count to 60")
		statefulSetReplicaCount = 60
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaleup statefulset-3 replica pod count to 50")
		statefulSetReplicaCount = 50
		scaleUpStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)

		// Collecting and dumping csi pod logs before restrating CSI daemonset
		collectPodLogs(ctx, client, csiSystemNamespace)
		ginkgo.By("Restart Daemonset")
		cmd = []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
		framework.RunKubectlOrDie(csiSystemNamespace, cmd...)
		ginkgo.By("Waiting for daemon set rollout status to finish")
		statusCheck = []string{"rollout", "status", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
		framework.RunKubectlOrDie(csiSystemNamespace, statusCheck...)
		// wait for csi Pods to be in running ready state
		err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout,
			ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("ScaleUp statefulset-1, statefulset-2, statefulset-3 replica pod count to 63")
		statefulSetReplicaCount = 63
		scaleUpStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)
		scaleUpStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}
		}()
	})

	/*
		TESTCASE-4
		Restart csi driver when multiple times scaleup/scaledown operation is in progress

		Steps//
		1. Create Storage Class with Immediate Binding mode
		2. Trigger StatefulSet-1 using parallel pod management policy with replica count 40,
		with each replica Pod is attached to 4 PVC's.
		3. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		4. Trigger StatefulSet-2 using default pod management policy with replica count 35,
		with each replica Pod is attached to 4 PVC's.
		5. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		6. Trigger StatefulSet-3 using parallel pod management policy with replica count 50,
		with each replica Pod is attached to 4 PVC's.
		7. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		8. Perform scaleup operation on StatefulSet-1. Scale up the replica count of sts-1 from 40 to 52.
		9. Verify scaleup operation of StatefulSet-1 went successful.
		10. Restart csi driver.
		11. Perform scaleup operation on StatefulSet-2. Increase the replica count of sts-2 from 35 to 40.
		12. Verify scaleup operation went successful.
		13. Perform scaledown operation on StatefulSet-3. Scale down the replica count of sts-3 from 50 to 30.
		14. Verify scaledown operation went successful.
		15. Perform scaledown operation on StatefulSet-1. Scale down the replica count of sts-1 from 52 to 42.
		16. Verify scaledown operation went successful.
		17. Perform scaleup operation on StatefulSet-2. Scale up the replica count of sts-2 from 40 to 60.
		18. Verify scaleup operation went successful.
		19. Restart csi driver.
		20. Perform scaleup operation on StatefulSet-3. Scale up the replica count of sts-3 from 30 to 50.
		21. Verify scaleup operation went successful.
		22. Scale up all 3 statefulsets (sts-1,sts-2,sts-3) to replica count 63.
		23. Verify scaleup operation on all 3 statefulsets are successful.
		24. Restart csi driver.
		25. Create 9 standalone PVC's.
		26. Verify standalone PVC's are in Bound state.
		27. Create 9 Pods using above created PVC's
		28. Verify standalone Pods are in up and running state.
		29. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Trigger multiple statefulsets using parallel management policy and restart csi "+
		"driver when statefulsets scaleup/sclaedown operation is in progress", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pvcCount = 9
		var podList []*v1.Pod

		ginkgo.By("Create Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create statefulset-1 with 40 replica pods and each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 40
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		statefulset1.Spec.PodManagementPolicy = apps.ParallelPodManagement
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset1, client, statefulSetReplicaCount)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			deleteAllStsAndTheirPVCInNSWithLargeTimeout(client, namespace)
		}()

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := GetListOfPodsInSts(client, statefulset1)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-2 with 35 replica pods and each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 35
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		statefulset2.Spec.PodManagementPolicy = apps.ParallelPodManagement
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset2, client,
			statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset2)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-3 with 50 replica pods and each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 50
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		statefulset3.Spec.PodManagementPolicy = apps.ParallelPodManagement
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset3, client,
			statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset3)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Scaleup statefulset-1 replica pod count to 52")
		statefulSetReplicaCount = 52
		scaleUpStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)

		restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scaleup statefulset-2 replica pod count to 40")
		statefulSetReplicaCount = 40
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaledown statefulset-3 replica pod count to 30")
		statefulSetReplicaCount = 30
		scaleDownStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaledown statefulset-1 replica pod count to 42")
		statefulSetReplicaCount = 42
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaleup statefulset-2 replica pod count to 60")
		statefulSetReplicaCount = 60
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

		restartSuccess, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scaleup statefulset-3 replica pod count to 50")
		statefulSetReplicaCount = 50
		scaleUpStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("ScaleUp statefulset-1, statefulset-2, statefulset-3 replica pod count to 63")
		statefulSetReplicaCount = 63
		scaleUpStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

		restartSuccess, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleUpStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}
		}()
	})

	/*
		TESTCASE-5
		Scaleup/Scaledown of worker nodes

		Steps//
		1. Scaledown worker node to count 1.
		2. Create Storage Class with Immediate Binding mode.
		3. Trigger StatefulSet-1 with replica count 63, with each replica Pod is attached to 4 PVCs.
		4. Verify all PVs, PVCs to reach Bound state and Statefulset Pods to reach running state.
		5. Trigger StatefulSet-2 with replica count 63 with each replica Pod is attached to 4 PVCs.
		6. Verify all PVs, PVCs to reach Bound state and Statefulset Pods to reach running state.
		7. Trigger 6 standalone PVCs.
		8. Verity PVC should reach Bound state.
		9. Create 6 Pods using above created PVCs.
		10. Create StatefulSet-3 with replica count 1.
		11. PVC should reach Bound state but Pod creation should get stuck in Pending state due to
		max volume count reached.
		12. Scaleup worker node count to 1.
		13. Trigger StatefulSet-4 with replica count 40 with each replica Pod is attached to 4 PVCs.
		14. Verify all PVs, PVCs to reach Bound state and Statefulset Pods to reach running state.
		15. Verify StatefulSet-3 Pods status which was stuck in Pending state. It should now reach to
		running state.
		16. Scaledown worker node count to 1.
		17. Verify StatefulSet-1, StatefulSet-2 Pods status.
		18. Verify StatefulSet-3, StatefulSet-4 Pods status. Pods should get stuck in Pending state and
		should start eviction.
		19. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Perform scaleup scaledown of worker nodes", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		statefulSetReplicaCount = 63
		pvcCount = 6
		var podList []*v1.Pod
		expectedErrMsg := "exceed max volume count"
		isNodeUncordon := true

		dh := drain.Helper{
			Ctx:                 ctx,
			Client:              client,
			Force:               true,
			IgnoreAllDaemonSets: true,
			Out:                 ginkgo.GinkgoWriter,
			ErrOut:              ginkgo.GinkgoWriter,
		}

		framework.Logf("Fetch the Node Details")
		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var nodeToCordon *v1.Node
		for _, node := range nodes.Items {
			if strings.Contains(node.Name, "master") || strings.Contains(node.Name, "control") {
				continue
			} else {
				nodeToCordon = &node
				ginkgo.By("Cordoning of node: " + node.Name)
				err = drain.RunCordonOrUncordon(&dh, &node, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Draining of node: " + node.Name)
				err = drain.RunNodeDrain(&dh, node.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isNodeUncordon = false
				break
			}
		}
		defer func() {
			if !isNodeUncordon {
				ginkgo.By("Uncordoning of node: " + nodeToCordon.Name)
				err = drain.RunCordonOrUncordon(&dh, nodeToCordon, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create statefulset-1 with 63 replica pods and each replica pod is attached to 4 pvcs")
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset1, client,
			statefulSetReplicaCount)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			deleteAllStsAndTheirPVCInNSWithLargeTimeout(client, namespace)
		}()

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := GetListOfPodsInSts(client, statefulset1)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-2 with 63 replica pods and each pod is attached to 4 pvcs")
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset2, client, statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset2)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(client, namespace, nil, pvclaims, false, "")
			if i == 9 && err != nil {
				framework.Logf("Pod is stuck in Pending state due to no space left")
			} else {
				podList = append(podList, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
				vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
			}
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}
		}()

		ginkgo.By("Create statefulset-3 with 1 replica pod and it is attached to 4 pvcs")
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulSetReplicaCount = 1
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		framework.Logf(fmt.Sprintf("Creating statefulset %v/%v with %d replicas and selector %+v",
			statefulset3.Namespace, statefulset3.Name, statefulSetReplicaCount, statefulset3.Spec.Selector))
		_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset3, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		sts3Pods := GetListOfPodsInSts(client, statefulset3)
		for _, pod := range sts3Pods.Items {
			if pod.Status.Phase == v1.PodPending {
				framework.Logf("Pod creation is stuck in Pending state due to max volume count reached")
			}
			err = waitForEvent(ctx, client, namespace, expectedErrMsg, pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
		}

		ginkgo.By("Uncordoning of node: " + nodeToCordon.Name)
		err = drain.RunCordonOrUncordon(&dh, nodeToCordon, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isNodeUncordon = true

		ginkgo.By("Create statefulset-4 with 40 replica pods and each replica pod is attached to 4 pvcs")
		statefulset4 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulSetReplicaCount = 40
		statefulset4.Spec.Replicas = &statefulSetReplicaCount
		statefulset4.Name = "sts4"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset4, client, statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset4, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset4, mountPath)).NotTo(gomega.HaveOccurred())
		sts4Pods := GetListOfPodsInSts(client, statefulset4)
		gomega.Expect(sts4Pods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset4.Name))
		gomega.Expect(len(sts4Pods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Verify statefulset-3 pods status")
		statefulSetReplicaCount = 1
		wait4StsPodsToBeReadyWithLargeTimeout(client, statefulSetReplicaCount, statefulset3)
		fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset3)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// cordon same node
		ginkgo.By("Cordoning of node: " + nodeToCordon.Name)
		err = drain.RunCordonOrUncordon(&dh, nodeToCordon, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Draining of node: " + nodeToCordon.Name)
		err = drain.RunNodeDrain(&dh, nodeToCordon.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isNodeUncordon = false
		defer func() {
			if !isNodeUncordon {
				ginkgo.By("Uncordoning of node: " + nodeToCordon.Name)
				err = drain.RunCordonOrUncordon(&dh, nodeToCordon, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isNodeUncordon = true
			}
		}()

		// verify Pods running state
		ginkgo.By("Verify statefulset-1, statefulset-2 Pods status after node cordon")
		statefulSetReplicaCount = 63
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify statefulset-3, statefulset-4 Pods status after node cordon")
		statefulSetReplicaCount = 0
		fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		for _, pod := range sts3Pods.Items {
			if pod.Status.Phase == v1.PodPending {
				framework.Logf("Pod creation is stuck in Pending state due to max volume count reached")
			}
			err = waitForEvent(ctx, client, namespace, expectedErrMsg, pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
		}

		fss.WaitForStatusReadyReplicas(client, statefulset4, statefulSetReplicaCount)
		for _, pod := range sts4Pods.Items {
			if pod.Status.Phase == v1.PodPending {
				framework.Logf("Pod creation is stuck in Pending state due to max volume count reached")
			}
			err = waitForEvent(ctx, client, namespace, expectedErrMsg, pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
		}
	})
	/*
		TESTCASE-6
		Create delete recreate statefulset pods by scaling down replicas count to 0 and scaling up again

		Steps//
		1. Create Storage Class with WFC Binding mode
		2. Trigger StatefulSet-1 using Parallel Pod management policy with replica count 40,
		with each replica Pod is attached to 4 PVC's.
		3. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		4. Scale down StatefulSet-1 replica count to 0.
		5. Verify scaledown operation went successful.
		6. Trigger StatefulSet-2 using Parallel Pod management policy with replica count 50,
		with each replica Pod is attached to 4 PVC's.
		7. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		8. ScaleUp StatefulSet-1 replica count back to 63.
		9. Verify scaleup operation of StatefulSet-1 went successful.
		10. Scale down StatefulSet-2 replica count to 0.
		11. Verify scale down operation went successful.
		12. Trigger StatefulSet-3 using Parallel Pod management policy with replica count 35,
		with each replica Pod is attached to 4 PVC's.
		13. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		14. Scaledown StatefulSet-3 replica count to 0.
		15. Verify scaledown operation went successful.
		16. Scaleup StatefulSet-2 replica count to 63.
		17. Verify scaling operation went fine.
		18. Scaleup StatefulSet-3 replica count to 63.
		19. Verify scaleup operation went fine.
		20. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Create delete recreate statefulset pods by scaling down replicas count to "+
		"0 and scaling up again", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", bindingMode, false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create statefulset-1 with 40 replica pods with each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 40
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		statefulset1.Spec.PodManagementPolicy = apps.ParallelPodManagement
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset1, client, statefulSetReplicaCount)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			deleteAllStsAndTheirPVCInNSWithLargeTimeout(client, namespace)
		}()

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := GetListOfPodsInSts(client, statefulset1)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Scaledown statefulset-1 replica pod count to 0")
		statefulSetReplicaCount = 0
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Create statefulset-2 with 50 replica pods with each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 50
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		statefulset1.Spec.PodManagementPolicy = apps.ParallelPodManagement
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset2, client,
			statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset2)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Scaleup statefulset-1 replica pod count to 63")
		statefulSetReplicaCount = 63
		scaleUpStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaledown statefulset-2 replica pod count to 0")
		statefulSetReplicaCount = 0
		scaleDownStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Create statefulset-3 with 45 replica pods with each replica pod is attached to 4 pvcs")
		statefulSetReplicaCount = 35
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		statefulset1.Spec.PodManagementPolicy = apps.ParallelPodManagement
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset3, client,
			statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset3)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Scaledown statefulset-3 replica pod count to 0")
		statefulSetReplicaCount = 0
		scaleDownStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaleup statefulset-2 replica pod count to 63")
		statefulSetReplicaCount = 63
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Scaleup statefulset-3 replica pod count to 63")
		statefulSetReplicaCount = 63
		scaleUpStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true, true)
	})

	/*
		TESTCASE-7
		Exceed max volume count of pvc creation

		Steps//
		1. Create Storage Class with Immediate Binding mode
		2. Trigger StatefulSet-1 with replica count 63 and with 4 PVC's attached to each replica Pod.
		3. Verify all PV's, PVC's are in Bound state and Statefulset Pods are in up and running state.
		4. Trigger StatefulSet-2 with replica count 63 and with 4 PVC's attached to each replica Pod.
		5. Verify all PV's, PVC's are in Bound state and Statefulset Pods are in up and running state.
		6. Trigger StatefulSet-3 with replica count 63 and with 4 PVC's attached to each replica Pod.
		7. Verify all PV's, PVC's are in Bound state and Statefulset Pods are in up and running state.
		8. Create 9 standalone PVC's.
		9. Verify all standalone PVC's are in Bound state.
		10. Create 9 Pods using above created PVC's
		11. Verify standalone Pods are in up and running state.
		12. Create StatefulSet-4 with replica count 1 with 4 PVC's attached to it.
		13. StatefulSet-4 Pod creation should be stuck in Pending state due to no disk space left
		on any worker node.
		14. Scale down StatefulSet-2 replica Pod count from 63 to 42.
		15. Verify scaledown operation went successful.
		16. Verify StatefulSet-4 Pod status. It should now reach to Running state.
		12. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Exceed max volume count of pvc creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		statefulSetReplicaCount = 63
		pvcCount = 6
		var podList []*v1.Pod
		expectedErrMsg := "exceed max volume count"

		ginkgo.By("Create Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create statefulset-1 with 63 replica pods with each replica pod is attached to 4 pvcs")
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset1, client,
			statefulSetReplicaCount)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			deleteAllStsAndTheirPVCInNSWithLargeTimeout(client, namespace)
		}()

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := GetListOfPodsInSts(client, statefulset1)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-2 with 63 replica pods with each replica pod is attached to 4 pvcs")
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset2, client, statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset2)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-3 with 63 replica pods with each pod is attached to 4 pvcs")
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset3, client, statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset3)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}
		}()

		ginkgo.By("Create statefulset-4 with 1 replica pod and pod is attached to 4 pvcs")
		statefulset4 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulSetReplicaCount = 1
		statefulset4.Spec.Replicas = &statefulSetReplicaCount
		statefulset4.Name = "sts4"
		framework.Logf(fmt.Sprintf("Creating statefulset %v/%v with %d replicas and selector %+v",
			statefulset4.Namespace, statefulset4.Name, statefulSetReplicaCount, statefulset4.Spec.Selector))
		_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset4, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		sts4Pods := GetListOfPodsInSts(client, statefulset4)
		for _, pod := range sts4Pods.Items {
			if pod.Status.Phase == v1.PodPending {
				framework.Logf("Pod is stuck in Pending state due to volume creation count " +
					"reached to max on worker nodes")
			}
			// check events for the error
			err = waitForEvent(ctx, client, namespace, expectedErrMsg, pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
		}

		ginkgo.By("Scaledown statefulset-2 replica pod count to 42")
		statefulSetReplicaCount = 42
		scaleDownStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

		ginkgo.By("Verify statefulset-4 pods status")
		statefulSetReplicaCount = 1
		sts4Pods = GetListOfPodsInSts(client, statefulset4)
		wait4StsPodsToBeReadyWithLargeTimeout(client, statefulSetReplicaCount, statefulset4)
		fss.WaitForStatusReadyReplicas(client, statefulset4, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset4, mountPath)).NotTo(gomega.HaveOccurred())
		gomega.Expect(sts4Pods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset4.Name))
		gomega.Expect(len(sts4Pods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
	})

	/*
		TESTCASE-8
		Scaleup/Scaledown of worker nodes

		Steps//
		1. Scaledown worker node to count 1.
		2. Create Storage Class with Immediate Binding mode.
		3. Trigger StatefulSet-1 with replica count 63, with each replica Pod is attached to 4 PVCs.
		4. Verify all PVs, PVCs to reach Bound state and Statefulset Pods to reach running state.
		5. Trigger StatefulSet-2 with replica count 63 with each replica Pod is attached to 4 PVCs.
		6. Verify all PVs, PVCs to reach Bound state and Statefulset Pods to reach running state.
		7. Trigger 6 standalone PVCs.
		8. Verity PVC should reach Bound state.
		9. Create 6 Pods using above created PVCs.
		10. Create StatefulSet-3 with replica count 1.
		11. PVC should reach Bound state but Pod creation should get stuck in Pending state due to
		max volume count reached.
		12. Scaleup worker node count to 1.
		13. Trigger StatefulSet-4 with replica count 40 with each replica Pod is attached to 4 PVCs.
		14. Verify all PVs, PVCs to reach Bound state and Statefulset Pods to reach running state.
		15. Verify StatefulSet-3 Pods status which was stuck in Pending state. It should now reach to
		running state.
		16. Scaledown worker node count to 1.
		17. Verify StatefulSet-1, StatefulSet-2 Pods status.
		18. Verify StatefulSet-3, StatefulSet-4 Pods status. Pods should get stuck in Pending state and
		should start eviction.
		19. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Exceed max volume count and perform scaleup of worker nodes", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pvcCount = 6
		var podList []*v1.Pod
		statefulSetReplicaCount = 63

		dh := drain.Helper{
			Ctx:                 ctx,
			Client:              client,
			Force:               true,
			IgnoreAllDaemonSets: true,
			Out:                 ginkgo.GinkgoWriter,
			ErrOut:              ginkgo.GinkgoWriter,
		}

		framework.Logf("Fetch the Node Details")
		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var nodeToCordon *v1.Node
		for _, node := range nodes.Items {
			if strings.Contains(node.Name, "master") || strings.Contains(node.Name, "control") {
				continue
			} else {
				nodeToCordon = &node
				ginkgo.By("Cordoning of node: " + node.Name)
				err = drain.RunCordonOrUncordon(&dh, &node, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Draining of node: " + node.Name)
				err = drain.RunNodeDrain(&dh, node.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				break
			}
		}

		ginkgo.By("Create Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create statefulset-1 with 63 replica pods and each pod is attached to 4 pvcs")
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset1, client,
			statefulSetReplicaCount)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			deleteAllStsAndTheirPVCInNSWithLargeTimeout(client, namespace)
		}()

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := GetListOfPodsInSts(client, statefulset1)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-2 with 63 replica pods and each pod is attached to 4 pvcs")
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset2, client, statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset2)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}

			ginkgo.By("Create statefulset-3 with 1 replica pod and each pod is attached to 4 pvcs")
			statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
			statefulSetReplicaCount = 1
			statefulset3.Spec.Replicas = &statefulSetReplicaCount
			statefulset3.Name = "sts3"
			framework.Logf(fmt.Sprintf("Creating statefulset %v/%v with %d replicas and selector %+v",
				statefulset3.Namespace, statefulset3.Name, statefulSetReplicaCount, statefulset3.Spec.Selector))
			_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset3, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			expectedErrMsg := "exceed max volume count"
			sts3Pods := GetListOfPodsInSts(client, statefulset3)
			for _, pod := range sts3Pods.Items {
				if pod.Status.Phase == v1.PodPending {
					framework.Logf("Pod is in Pending state due to no disk space left")
				}
				// check events for the error
				err = waitForEvent(ctx, client, namespace, expectedErrMsg, pod.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
			}

			ginkgo.By("Uncordoning of node: " + nodeToCordon.Name)
			err = drain.RunCordonOrUncordon(&dh, nodeToCordon, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create statefulset-4 with 40 replica pods and each pod is attached to 4 pvcs")
			statefulset4 := GetStatefulSetFromManifestFor265Disks(namespace)
			statefulSetReplicaCount = 40
			statefulset4.Spec.Replicas = &statefulSetReplicaCount
			statefulset4.Name = "sts4"
			CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset4, client, statefulSetReplicaCount)

			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulset4, statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulset4, mountPath)).NotTo(gomega.HaveOccurred())
			ssPods = GetListOfPodsInSts(client, statefulset4)
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset4.Name))
			gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			ginkgo.By("Scaledown statefulset-4 replica pod count to 25")
			statefulSetReplicaCount = 25
			scaleDownStatefulSetPod(ctx, client, statefulset4, namespace, statefulSetReplicaCount, true, true)
			ginkgo.By("Scaledown statefulset-2 replica pod count to 42")
			statefulSetReplicaCount = 42
			scaleDownStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true, true)

			ginkgo.By("Scaleup statefulset-4 replica pod count to 50")
			statefulSetReplicaCount = 50
			scaleUpStatefulSetPod(ctx, client, statefulset4, namespace, statefulSetReplicaCount, true, true)

			ginkgo.By("Verify statefulset-3 pods status")
			statefulSetReplicaCount = 1
			sts3Pods = GetListOfPodsInSts(client, statefulset3)
			wait4StsPodsToBeReadyWithLargeTimeout(client, statefulSetReplicaCount, statefulset3)
			fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
			gomega.Expect(sts3Pods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
			gomega.Expect(len(sts3Pods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}()
	})

	/*
		TESTCASE-9
		Consider a testbed of 3 worker nodes and each worker node supports 255 volumes attachment.
		so - 253 * 3 = 765 volume attachment we need to create and attach it to Pods and verify.

		Steps//
		1. Create Storage Class with Immediate Binding mode
		2. Trigger StatefulSet-1 with replica count 63 and with 4 PVC's attached to each replica Pod
		 using above created SC (i.e 63 Pods with 252 volume attachment)
		3. Verify all PV's, PVC's are in Bound state and Statefulset Pods are in up and running state.
		4. Trigger StatefulSet-2 with replica count 63 and with 4 PVC's attached to each replica Pod
		 using above created SC (i.e 63 Pods with 252 volume attachment)
		5. Verify all PV's, PVC's are in Bound state and Statefulset Pods are in up and running state.
		6. Trigger StatefulSet-3 with replica count 63 and with 4 PVC's attached to each replica Pod
		 using above created SC (i.e 63 Pods with 252 volume attachment)
		7. Verify all PV's, PVC's are in Bound state and Statefulset Pods are in up and running state.
		8. Create 9 standalone PVC's.
		9. Verify all standalone PVC's are in Bound state.
		10. Create 9 Pods using above created PVC's
		11. Verify standalone Pods are in up and running state.
		12. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Verify volume provisioning when multiple  replicas", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		statefulSetReplicaCount = 63
		pvcCount = 9
		var podList []*v1.Pod

		ginkgo.By("Create Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create statefulset-1 with 63 replica pods and each pod is attached to 4 pvcs")
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset1, client,
			statefulSetReplicaCount)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			deleteAllStsAndTheirPVCInNSWithLargeTimeout(client, namespace)
		}()

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := GetListOfPodsInSts(client, statefulset1)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-2 with 63 replica pods and each pod is attached to 4 pvcs")
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset2, client, statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset2)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Create statefulset-3 with 63 replica pods and each pod is attached to 4 pvcs")
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		CreateMultipleStatefulSetPodsInGivenNamespace(namespace, statefulset3, client, statefulSetReplicaCount)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = GetListOfPodsInSts(client, statefulset3)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}
		}()
	})
})
