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
	"path/filepath"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/manifest"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	manifestPathFor256Disks = "tests/e2e/testing-manifests/statefulset256"
)

var _ = ginkgo.Describe("[csi-256-disk-support-vanilla] Volume-Provisioning-With-256-Disk-Support", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                  clientset.Interface
		namespace               string
		statefulSetReplicaCount int32
		pvclaims                []*v1.PersistentVolumeClaim
		pvclaimsList            []*v1.PersistentVolumeClaim
		podList                 []*v1.Pod
		numberOfPVC             int
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
		Considering a testbed of 3 worker nodes and each worker node supports 255 volume attachment.
		so - 253 * 3 = 765 volume attachment we need to create and attach it to Pods.

		Steps//
		1. Create Storage Class with Immediate Binding mode
		2. Trigger StatefulSet-1 with replica count 63 and with 4 PVC's attached to it
		 using above created SC
		3. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		4. Trigger StatefulSet-2 with replica count 63 and with 4 PVC's attached to it
		 using above created SC
		5. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		6. Trigger StatefulSet-3 with replica count 63 and with 4 PVC's attached to it
		 using above created SC
		7. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		8. Create 9 standalone PVC's.
		9. Verify standalone PVC's are in Bound state.
		10. Create 9 Pods using above created PVC's
		11. Verify standalone Pods are in up and running state.
		12. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Testcase-1", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		statefulSetReplicaCount = 63
		numberOfPVC = 9

		// Create SC
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating StatefulSet service
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset1 with replica count 63 and with 4 PVC's attached to each replica Pod")
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		CreateStatefulSet(namespace, statefulset1, client)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)

			ginkgo.By("Delete all PVC's in a namespace")
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()
		time.Sleep(15 * time.Minute)

		// // verify that the StatefulSets pods are in ready state
		// fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		// gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		// // Get list of Pods in each StatefulSet and verify the replica count
		// ssPods := GetListOfPodsInSts(client, statefulset1)
		// gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
		// 	fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		// gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
		// 	"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Creating statefulset2 with replica count 63 and with 4 PVC's attached to each replica Pod")
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		ginkgo.By("Creating statefulset2")
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		CreateStatefulSet(namespace, statefulset2, client)

		time.Sleep(15 * time.Minute)

		// // verify that the StatefulSets pods are in ready state
		// fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		// gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		// // Get list of Pods in each StatefulSet and verify the replica count
		// ssPods = GetListOfPodsInSts(client, statefulset2)
		// gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
		// 	fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		// gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
		// 	"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Creating statefulset3 with replica count 63 and with 4 PVC's attached to each replica Pod")
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		ginkgo.By("Creating statefulset3")
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		CreateStatefulSet(namespace, statefulset3, client)

		time.Sleep(15 * time.Minute)

		// // verify that the StatefulSets pods are in ready state
		// fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		// gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		// // Get list of Pods in each StatefulSet and verify the replica count
		// ssPods = GetListOfPodsInSts(client, statefulset3)
		// gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
		// 	fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		// gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
		// 	"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Creating 9 standalone PVC and Pods")
		for i := 0; i < numberOfPVC; i++ {
			pvc, err := createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
			pvList, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvList).NotTo(gomega.BeEmpty())
			pvclaims = append(pvclaims, pvc)
			pvclaimsList = append(pvclaimsList, pvc)
			pod, err := createPod(client, namespace, nil, pvclaims, false, execCommand)
			podList = append(podList, pod)
			framework.Logf("Pod %s created successfully", pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaims = nil
		}
		defer func() {
			ginkgo.By("Delete all standalone pods")
			for _, pod := range podList {
				err := fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
	})

	/*
		TESTCASE-2
		Perform multiple times scaleup/scaledown operation when creating 765 volume attachment

		Steps//
		1. Create Storage Class with Immediate Binding mode
		2. Trigger StatefulSet-1 with replica count 63 and with 4 PVC's attached to it
		 using above created SC
		3. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		4. Trigger StatefulSet-2 with replica count 43 and with 4 PVC's attached to it
		 using above created SC
		5. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		6. Trigger StatefulSet-3 with replica count 30 and with 4 PVC's attached to it
		 using above created SC
		7. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		8. Perform scaledown operation of StatefulSet-1. Scale down replica count from 63 to 40.
		9. Verify scaledown operation went successful.
		10. Perform scaleup operation of StatefulSet-2. Increase the replica count from 43 to 50.
		10. Verify scaleup operation went successful.
		11. Perform scaleup operation of StatefulSet-3. Scale up replica count from 30 to 50.
		12. Verify scaleup operation went successful.
		13. Perform scaleup operation of StatefulSet-1. Scale up replica count from 40 to 63.
		14. Verify scaleup operation went successful.
		15. Perform scaleup operation of StatefulSet-1. Scale up replica count from 50 to 63.
		16. Verify scaleup operation went successful.
		17. Perform scaleup operation of StatefulSet-1. Scale up replica count from 50 to 63.
		18. Verify scaleup operation went successful.
		19. Create 9 standalone PVC's.
		20. Verify standalone PVC's are in Bound state.
		21. Create 9 Pods using above created PVC's
		22. Verify standalone Pods are in up and running state.
		23. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Testcase-2", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		numberOfPVC = 9

		// Create SC
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating StatefulSet service
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset1 with replica count 63 and with 4 PVC's attached to each replica Pod")
		statefulSetReplicaCount = 10
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		CreateStatefulSet(namespace, statefulset1, client)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// // verify that the StatefulSets pods are in ready state
		// fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		// gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		// // Get list of Pods in each StatefulSet and verify the replica count
		// ssPods := GetListOfPodsInSts(client, statefulset1)
		// gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
		// 	fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		// gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
		// 	"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Creating statefulset2 with replica count 63 and with 4 PVC's attached to each replica Pod")
		statefulSetReplicaCount = 10
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		ginkgo.By("Creating statefulset2")
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		CreateStatefulSet(namespace, statefulset2, client)

		// // verify that the StatefulSets pods are in ready state
		// fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		// gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		// // Get list of Pods in each StatefulSet and verify the replica count
		// ssPods = GetListOfPodsInSts(client, statefulset2)
		// gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
		// 	fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		// gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
		// 	"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Creating statefulset3 with replica count 63 and with 4 PVC's attached to each replica Pod")
		statefulSetReplicaCount = 10
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		ginkgo.By("Creating statefulset3")
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		CreateStatefulSet(namespace, statefulset3, client)

		// // verify that the StatefulSets pods are in ready state
		// fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		// gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		// // Get list of Pods in each StatefulSet and verify the replica count
		// ssPods = GetListOfPodsInSts(client, statefulset3)
		// gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
		// 	fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		// gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
		// 	"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Scale down StaefulSet-1 replicas to 40")
		statefulSetReplicaCount = 5
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Scale up StaefulSet-2 replicas to 50")
		statefulSetReplicaCount = 15
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Scale up StaefulSet-3 replicas to 50")
		statefulSetReplicaCount = 50
		scaleUpStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Scale up StaefulSet-1 replicas to 50")
		statefulSetReplicaCount = 20
		scaleDownStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Scale up StaefulSet-2 replicas to 50")
		statefulSetReplicaCount = 63
		scaleUpStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Scale up StaefulSet-2 replicas to 50")
		statefulSetReplicaCount = 63
		scaleUpStatefulSetPod(ctx, client, statefulset2, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Scale up StaefulSet-2 replicas to 50")
		statefulSetReplicaCount = 63
		scaleUpStatefulSetPod(ctx, client, statefulset3, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Creating 9 standalone PVC and Pods")
		for i := 0; i < numberOfPVC; i++ {
			pvc, err := createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
			pvList, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvList).NotTo(gomega.BeEmpty())
			pvclaims = append(pvclaims, pvc)
			pvclaimsList = append(pvclaimsList, pvc)
			pod, err := createPod(client, namespace, nil, pvclaims, false, execCommand)
			podList = append(podList, pod)
			framework.Logf("Pod %s created successfully", pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaims = nil
		}
		defer func() {
			ginkgo.By("Delete all standalone pods")
			for _, pod := range podList {
				err := fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
	})

	/*
		TESTCASE-3
		CSI node daemon restart during ScaleUp/ScaleDown operation

		Steps//
		1. Create Storage Class with Immediate Binding mode
		2. Trigger StatefulSet-1 with replica count 40 and with 4 PVC's attached to it
		 using above created SC
		3. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		4. Trigger StatefulSet-2 with replica count 63 and with 4 PVC's attached to it
		 using above created SC
		5. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		6. Restart csi node daemon
		6. Trigger StatefulSet-3 with replica count 63 and with 4 PVC's attached to it
		 using above created SC
		7. Verify PVC's are in Bound state and Statefulset Pods are in up and running state.
		8. Perform scaledown operation of StatefulSet-1. Scale down replica count from 63 to 40.
		9. Verify scaledown operation went successful.
		10. Perform scaleup operation of StatefulSet-2. Increase the replica count from 43 to 53.
		11. Perform scaledown operation of StatefulSet-3. Scale down replica count from 63 to 33.
		12. Verify scaledown operation went successful.
		13. Perform scaleup operation of StatefulSet-1. Scale up replica count from 40 to 63.
		14.
		8. Create 9 standalone PVC's.
		9. Verify standalone PVC's are in Bound state.
		10. Create 9 Pods using above created PVC's
		11. Verify standalone Pods are in up and running state.
		12. Perform cleanup. Delete StatefulSet Pods, PVC's, PV.
	*/

	ginkgo.It("Testcase-3", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		numberOfPVC = 9
		ignoreLabels := make(map[string]string)

		// Create SC
		ginkgo.By("Creating Storage Class")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating StatefulSet service
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset1 with replica count 63 and with 4 PVC's attached to each replica Pod")
		statefulSetReplicaCount = 63
		statefulset1 := GetStatefulSetFromManifestFor265Disks(namespace)
		statefulset1.Spec.Replicas = &statefulSetReplicaCount
		statefulset1.Name = "sts1"
		CreateStatefulSet(namespace, statefulset1, client)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)

			ginkgo.By("Delete all PVC's in a namespace")
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset1, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		// Get list of Pods in each StatefulSet and verify the replica count
		ssPods := GetListOfPodsInSts(client, statefulset1)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Creating statefulset2 with replica count 63 and with 4 PVC's attached to each replica Pod")
		statefulSetReplicaCount = 43
		statefulset2 := GetStatefulSetFromManifestFor265Disks(namespace)
		ginkgo.By("Creating statefulset2")
		statefulset2.Spec.Replicas = &statefulSetReplicaCount
		statefulset2.Name = "sts2"
		CreateStatefulSet(namespace, statefulset2, client)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset2, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		// Get list of Pods in each StatefulSet and verify the replica count
		ssPods = GetListOfPodsInSts(client, statefulset2)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset2.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Restart CSI daemonset
		ginkgo.By("Restart Daemonset")
		list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		num_csi_pods := len(list_of_pods)
		cmd := []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
		framework.RunKubectlOrDie(csiSystemNamespace, cmd...)

		ginkgo.By("Waiting for daemon set rollout status to finish")
		statusCheck := []string{"rollout", "status", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
		framework.RunKubectlOrDie(csiSystemNamespace, statusCheck...)

		// wait for csi Pods to be in running ready state
		err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating statefulset3 with replica count 63 and with 4 PVC's attached to each replica Pod")
		statefulSetReplicaCount = 63
		statefulset3 := GetStatefulSetFromManifestFor265Disks(namespace)
		ginkgo.By("Creating statefulset3")
		statefulset3.Spec.Replicas = &statefulSetReplicaCount
		statefulset3.Name = "sts3"
		CreateStatefulSet(namespace, statefulset3, client)

		// verify that the StatefulSets pods are in ready state
		fss.WaitForStatusReadyReplicas(client, statefulset3, statefulSetReplicaCount)
		gomega.Expect(CheckMountForStsPods(client, statefulset3, mountPath)).NotTo(gomega.HaveOccurred())
		// Get list of Pods in each StatefulSet and verify the replica count
		ssPods = GetListOfPodsInSts(client, statefulset3)
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset3.Name))
		gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Scale down StaefulSet-1 replicas to 40")
		statefulSetReplicaCount = 40
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Scale up StaefulSet-2 replicas to 53")
		statefulSetReplicaCount = 53
		scaleUpStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Scale down StaefulSet-3 replicas to 30")
		statefulSetReplicaCount = 30
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true)

		statefulSetReplicaCount = 63
		ginkgo.By("Perform Scale up of StaefulSet-1 replicas")
		scaleUpStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Perform Scale up of StaefulSet-2 replicas")
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Perform Scale up of StaefulSet-3 replicas")
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true)

		statefulSetReplicaCount = 23
		ginkgo.By("Perform Scale down of StaefulSet-1 replicas")
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true)

		statefulSetReplicaCount = 63
		ginkgo.By("Perform Scale up of StaefulSet-1 replicas")
		scaleDownStatefulSetPod(ctx, client, statefulset1, namespace, statefulSetReplicaCount, true)

		ginkgo.By("Creating 9 standalone PVC and Pods")
		for i := 0; i < numberOfPVC; i++ {
			pvc, err := createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
			pvList, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvList).NotTo(gomega.BeEmpty())
			pvclaims = append(pvclaims, pvc)
			pvclaimsList = append(pvclaimsList, pvc)
			pod, err := createPod(client, namespace, nil, pvclaims, false, execCommand)
			podList = append(podList, pod)
			framework.Logf("Pod %s created successfully", pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaims = nil
		}
		defer func() {
			ginkgo.By("Delete all standalone pods")
			for _, pod := range podList {
				err := fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
	})
})

func GetStatefulSetFromManifestFor265Disks(ns string) *appsv1.StatefulSet {
	ssManifestFilePath := filepath.Join(manifestPathFor256Disks, "statefulset.yaml")
	framework.Logf("Parsing statefulset from %v", ssManifestFilePath)
	ss, err := manifest.StatefulSetFromManifest(ssManifestFilePath, ns)
	framework.ExpectNoError(err)
	return ss
}
