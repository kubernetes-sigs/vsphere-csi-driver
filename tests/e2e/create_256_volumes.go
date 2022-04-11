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
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/manifest"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

const (
	manifestPathFor256Disks = "tests/e2e/yamls-256DiskSupport/statefulset/nginx"
)

var _ = ginkgo.Describe("[csi-256-disk-support-vanilla] vsphere-Aware-Provisioning-With-256-Disk-Support",
	func() {
		f := framework.NewDefaultFramework("e2e-vsphere-aware-provisioning")
		var (
			client                  clientset.Interface
			namespace               string
			sts_count               int
			statefulSetReplicaCount int32
			pvclaims                []*v1.PersistentVolumeClaim
			pvclaimsList            []*v1.PersistentVolumeClaim
			podList                 []*v1.Pod
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
			Considering a testbed of 3 worker nodes and each worker node supports 251 volume attachment.
			so - 253 * 3 = 753 volume attachment we need to create and attach it to Pods.

			Steps//
			1. Create Storage Class with Immediate Binding mode
			2. Trigger 3 parallel StatefulSets using parallel pod management policy with replica count of 60.
			3. Note - Each StatefulSet Pod is connected to 4 PVC's (180 Pods * 4 PVC's = 720 PVC attachment)
			4. Verify PVC's are in Bound state and Pod's are in up and running state.
			5. 720 PVC's used, create remaining 33 PVC's to get a count of 753 PVC's.
			6. Verify PVC's are in Bound state.
			7. Create Single Pod and attach all 33 PVC's to that Pod.
			8. Verify Pod is in up and running state.
			9. Delete StatefulSet Pods, Standalone Pod and all PVC's
			10. Delete Storage Class
		*/

		ginkgo.It("Trigger multiple statefulset pods taking 753 pvcs in account", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sts_count = 3
			statefulSetReplicaCount = 3
			numberOfPVC := 9

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

			// Create multiple StatefulSets Specs in parallel
			ginkgo.By("Trigger multiple StatefulSets")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count)
			var wg sync.WaitGroup
			wg.Add(3)
			for i := 0; i < len(statefulSets); i++ {
				go createParallelStatefulSets(client, namespace, statefulSets[i],
					statefulSetReplicaCount, &wg)

			}
			wg.Wait()

			// Waiting for StatefulSets Pods to be in Ready State
			ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
			waitForPodToBeInRunningState(ctx, client, namespace, statefulSets[1])

			// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
			ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
			for i := 0; i < len(statefulSets); i++ {
				// verify that the StatefulSets pods are in ready state
				fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)

				// Get list of Pods in each StatefulSet and verify the replica count
				ssPods := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
					fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
				gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}

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
				if len(pvclaims) == 1 {
					pod, err := createPod(client, namespace, nil, pvclaims, false, execCommand)
					podList = append(podList, pod)
					framework.Logf("Pod %s created successfully", pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvclaims = nil
				}
			}
			defer func() {
				for _, pod := range podList {
					err := fpod.DeletePodWithWait(client, pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			defer func() {
				for _, pvc := range pvclaimsList {
					pv := getPvFromClaim(client, pvc.Namespace, pvc.Name)
					err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			// Scale down statefulSets replica count to 0
			statefulSetReplicaCount = 0
			ginkgo.By("Scale down statefulset replica count to 0")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
				ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}
		})

		/*
			TESTCASE-2
			Considering a testbed of 3 worker nodes and each worker node supports 251 volume attachment.
			so - 253 * 3 = 753 volume attachment we need to create and attach it to Pods.
			Perform multiple times scale up and scale down operation

			Steps//
			1. Create Storage Class.
			2. Trigger 3 parallel StatefulSets using parallel pod management policy
			with replica count of 30 (i.e. trigger 90 Pods)
			3. Note - Each StatefulSet Pod is connected to 4 PVC's (90 Pods * 4 PVC's = 360 PVC attachment)
			4. Verify PVC's are in Bound state and Pod's are in up and running state.
			5. Scale up 3 StatefulSets to replica count 20. (60 Pods * 4 PVC's = 240 new PVC's attachment)
			6.  Verify Scaling up of StatefulSets went successful.
			7.  Verify PVC's are in Bound phase and StatefulSets Pods are in up and running state.
			8. Again Scale up StatefulSet replica count to 10. (30 Pods * 4 PVC's = 120 new PVC's attachment)
			Note: Total 720 PVC's attachment to 180 statefulSet Pods.
			9. Verify Scaling up of StatefulSets went successful.
			10. Verify newly created PVC's are in Bound phase.
			11. Verify newly created StatefulSets Pods are in up and running state.
			12. Scale down StatefulSets replica count to 40.
			13. Verify Scaling down of StatefulSets went successful.
			14. Further scale down StatefulSet replica count to 30.
			15. Verify Scaling up of StatefulSets went successful.
			16. Further scale up all 3 StatefulSets to 50 replica count.
			17. Verify scaling went successful.
			18. Verify newly created StatefulSets Pods are in up and running state and PVC's are in Bound state.
			19. Further scale down StatefulSets Pods to 70.
			20. Verify Scaling down of StatefulSets went successful.
			21. Create  30 standalone PVC
			22. Create 5 Pods and attach 6 PVC's per pod.
		*/

		ginkgo.It("Trigger multiple times scaleup scaledown operations on statefulset pods", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvclaims []*v1.PersistentVolumeClaim
			sts_count = 3
			statefulSetReplicaCount = 62
			numberOfPVC := 9

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

			// Create multiple StatefulSets Specs in parallel
			ginkgo.By("Trigger multiple StatefulSets")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count)
			var wg sync.WaitGroup
			wg.Add(3)
			for i := 0; i < len(statefulSets); i++ {
				go createParallelStatefulSets(client, namespace, statefulSets[i],
					statefulSetReplicaCount, &wg)

			}
			wg.Wait()

			// Waiting for StatefulSets Pods to be in Ready State
			ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
			time.Sleep(90 * time.Minute)

			// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
			ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
			for i := 0; i < len(statefulSets); i++ {
				// verify that the StatefulSets pods are in ready state
				fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)

				// Get list of Pods in each StatefulSet and verify the replica count
				ssPods := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
					fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
				gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}

			statefulSetReplicaCount = 32 // left count is 62-32 = 30
			ginkgo.By("Scale down statefulset1 replica count to 32")
			scaleDownStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)

			statefulSetReplicaCount = 22 // left count is 62-22 = 40
			ginkgo.By("Scale down statefulset2 replica count to 22")
			scaleDownStatefulSetPod(ctx, client, statefulSets[1], namespace, statefulSetReplicaCount, true)

			statefulSetReplicaCount = 52 // left count is 62-52 = 10
			ginkgo.By("Scale down statefulset3 replica count to 52")
			scaleDownStatefulSetPod(ctx, client, statefulSets[2], namespace, statefulSetReplicaCount, true)

			// Scale up statefulSets replicas count // 30+20 = 50, 40+20 = 60, 10+20 = 30  (12, 2, 32 = 46)
			ginkgo.By("Scale up StaefulSets replicas in parallel")
			statefulSetReplicaCount = 20
			for i := 0; i < len(statefulSets); i++ {
				scaleUpStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			}

			ginkgo.By("Further Scale up StaefulSet3 replicas to 46")
			statefulSetReplicaCount = 46
			scaleUpStatefulSetPod(ctx, client, statefulSets[2], namespace, statefulSetReplicaCount, true)

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
				if len(pvclaims) == 3 {
					pod, err := createPod(client, namespace, nil, pvclaims, false, execCommand)
					framework.Logf("Pod %s created successfully", pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvclaims = nil
				}
			}
			defer func() {
				for _, pvc := range pvclaimsList {
					pv := getPvFromClaim(client, pvc.Namespace, pvc.Name)
					err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			defer func() {
				for _, pod := range podList {
					err := fpod.DeletePodWithWait(client, pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			// Scale down statefulSets replica count to 0
			statefulSetReplicaCount = 0
			ginkgo.By("Scale down statefulset replica count to 0")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
				ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}
		})

		/*
			TESTCASE-3
			Considering a testbed of 3 worker nodes and each worker node supports 251 volume attachment.
			so - 253 * 3 = 753 volume attachment we need to create and attach it to Pods.
			Perform multiple times scale up and scale down operation and in between restart CSI node daemon

			Steps//
			1. Create Storage Class.
			2. Trigger 3 parallel StatefulSets using parallel pod management policy
			with replica count of 30 (i.e. trigger 90 Pods)
			3. Note - Each StatefulSet Pod is connected to 4 PVC's (90 Pods * 4 PVC's = 360 PVC attachment)
			4. Verify PVC's are in Bound state and Pod's are in up and running state.
			5. Scale up 3 StatefulSets to replica count 20. (60 Pods * 4 PVC's = 240 new PVC's attachment)
			6. In between, restart CSI node daemon set
			7.  Verify Scaling up of StatefulSets went successful.
			8.  Verify PVC's are in Bound phase and StatefulSets Pods are in up and running state.
			9. Again Scale up StatefulSet replica count to 10. (30 Pods * 4 PVC's = 120 new PVC's attachment)
			Note: Total 720 PVC's attachment to 180 statefulSet Pods.
			10. Verify Scaling up of StatefulSets went successful.
			11. Verify newly created PVC's are in Bound phase.
			12. Verify newly created StatefulSets Pods are in up and running state.
			13. Scale down StatefulSets replica count to 40.
			14. Restart CSI node daemon set
			15. Verify Scaling down of StatefulSets went successful.
			16. Further scale down StatefulSet replica count to 30.
			17. Verify Scaling up of StatefulSets went successful.
			18. Further scale up all 3 StatefulSets to 50 replica count.
			19. Verify scaling went successful.
			20. Verify newly created StatefulSets Pods are in up and running state and PVC's are in Bound state.
			21. Further scale down StatefulSets Pods to 70.
			22. Verify Scaling down of StatefulSets went successful.
			23. Create  30 standalone PVC
			24. Create 5 Pods and attach 6 PVC's per pod.
		*/

		ginkgo.It("Trigger multiple scaleup scaledown on statefulset pods and in between restart csi node daemonset", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvclaims []*v1.PersistentVolumeClaim
			sts_count = 3
			statefulSetReplicaCount = 62
			numberOfPVC := 9
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

			// Create multiple StatefulSets Specs in parallel
			ginkgo.By("Trigger multiple StatefulSets")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count)
			var wg sync.WaitGroup
			wg.Add(3)
			for i := 0; i < len(statefulSets); i++ {
				go createParallelStatefulSets(client, namespace, statefulSets[i],
					statefulSetReplicaCount, &wg)

			}
			wg.Wait()

			// Waiting for StatefulSets Pods to be in Ready State
			ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
			time.Sleep(45 * time.Minute)

			// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
			ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
			for i := 0; i < len(statefulSets); i++ {
				// verify that the StatefulSets pods are in ready state
				fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)

				// Get list of Pods in each StatefulSet and verify the replica count
				ssPods := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
					fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
				gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}

			statefulSetReplicaCount = 32 // left count is 62-32 = 30
			ginkgo.By("Scale down statefulset1 replica count to 32")
			scaleDownStatefulSetPod(ctx, client, statefulSets[0], namespace, statefulSetReplicaCount, true)

			statefulSetReplicaCount = 22 // left count is 62-22 = 40
			ginkgo.By("Scale down statefulset2 replica count to 22")
			scaleDownStatefulSetPod(ctx, client, statefulSets[1], namespace, statefulSetReplicaCount, true)

			// Fetch the number of CSI pods running before restart
			list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Restart CSI daemonset
			cmd := []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
			framework.RunKubectlOrDie(csiSystemNamespace, cmd...)

			time.Sleep(pollTimeoutShort)
			num_csi_pods := len(list_of_pods)
			err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			statefulSetReplicaCount = 52 // left count is 62-52 = 10
			ginkgo.By("Scale down statefulset3 replica count to 52")
			scaleDownStatefulSetPod(ctx, client, statefulSets[2], namespace, statefulSetReplicaCount, true)

			// Scale up statefulSets replicas count // 30+20 = 50, 40+20 = 60, 10+20 = 30  (12, 2, 32 = 46)
			ginkgo.By("Scale up StaefulSets replicas in parallel")
			statefulSetReplicaCount = 20
			for i := 0; i < len(statefulSets); i++ {
				scaleUpStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			}

			ginkgo.By("Further Scale up StaefulSet3 replicas to 46")
			statefulSetReplicaCount = 46
			scaleUpStatefulSetPod(ctx, client, statefulSets[2], namespace, statefulSetReplicaCount, true)

			// Fetch the number of CSI pods running before restart
			list_of_pods, err = fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Restart CSI daemonset
			cmd = []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
			framework.RunKubectlOrDie(csiSystemNamespace, cmd...)

			time.Sleep(pollTimeoutShort)
			num_csi_pods = len(list_of_pods)
			err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
				if len(pvclaims) == 3 {
					pod, err := createPod(client, namespace, nil, pvclaims, false, execCommand)
					framework.Logf("Pod %s created successfully", pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvclaims = nil
				}
			}
			defer func() {
				for _, pvc := range pvclaimsList {
					pv := getPvFromClaim(client, pvc.Namespace, pvc.Name)
					err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			defer func() {
				for _, pod := range podList {
					err := fpod.DeletePodWithWait(client, pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			// Scale down statefulSets replica count to 0
			statefulSetReplicaCount = 0
			ginkgo.By("Scale down statefulset replica count to 0")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
				ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}
		})

		/*
			TESTCASE-4
			Considering a testbed of 3 worker nodes and each worker node supports 251 volume attachment.
			so - 253 * 3 = 753 volume attachment we need to create and attach it to Pods.
			Perform multiple times scale up and scale down operation and in between restart CSI node daemon

			Steps//
			1. Create Storage Class.
			2. Trigger 3 parallel StatefulSets using parallel pod management policy
			with replica count of 30 (i.e. trigger 90 Pods)
			3. Note - Each StatefulSet Pod is connected to 4 PVC's (90 Pods * 4 PVC's = 360 PVC attachment)
			4. Verify PVC's are in Bound state and Pod's are in up and running state.
			5. Scale up 3 StatefulSets to replica count 20. (60 Pods * 4 PVC's = 240 new PVC's attachment)
			6. In between, restart CSI node daemon set
			7.  Verify Scaling up of StatefulSets went successful.
			8.  Verify PVC's are in Bound phase and StatefulSets Pods are in up and running state.
			9. Again Scale up StatefulSet replica count to 10. (30 Pods * 4 PVC's = 120 new PVC's attachment)
			Note: Total 720 PVC's attachment to 180 statefulSet Pods.
			10. Verify Scaling up of StatefulSets went successful.
			11. Verify newly created PVC's are in Bound phase.
			12. Verify newly created StatefulSets Pods are in up and running state.
			13. Scale down StatefulSets replica count to 40.
			14. Restart CSI node daemon set
			15. Verify Scaling down of StatefulSets went successful.
			16. Further scale down StatefulSet replica count to 30.
			17. Verify Scaling up of StatefulSets went successful.
			18. Further scale up all 3 StatefulSets to 50 replica count.
			19. Verify scaling went successful.
			20. Verify newly created StatefulSets Pods are in up and running state and PVC's are in Bound state.
			21. Further scale down StatefulSets Pods to 70.
			22. Verify Scaling down of StatefulSets went successful.
			23. Create  30 standalone PVC
			24. Create 5 Pods and attach 6 PVC's per pod.
		*/

		ginkgo.It("consider", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvclaims []*v1.PersistentVolumeClaim
			sts_count = 3
			statefulSetReplicaCount = 30
			numberOfPVC := 30
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

			// Create multiple StatefulSets Specs in parallel
			ginkgo.By("Trigger multiple StatefulSets")
			statefulSets := createParallelStatefulSetSpec(namespace, sts_count)
			var wg sync.WaitGroup
			wg.Add(3)
			for i := 0; i < len(statefulSets); i++ {
				go createParallelStatefulSets(client, namespace, statefulSets[i],
					statefulSetReplicaCount, &wg)

			}
			wg.Wait()

			// Waiting for StatefulSets Pods to be in Ready State
			ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
			time.Sleep(45 * time.Minute)

			// Verify that all parallel triggered StatefulSets Pods creation should be in up and running state
			ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
			for i := 0; i < len(statefulSets); i++ {
				// verify that the StatefulSets pods are in ready state
				fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)

				// Get list of Pods in each StatefulSet and verify the replica count
				ssPods := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
					fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
				gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}

			// Scale up statefulSets replicas count
			ginkgo.By("Scale up StaefulSets replicas in parallel")
			statefulSetReplicaCount = 20
			for i := 0; i < len(statefulSets); i++ {
				scaleUpStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			}

			// Fetch the number of CSI pods running before restart
			list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Restart CSI daemonset
			cmd := []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
			framework.RunKubectlOrDie(csiSystemNamespace, cmd...)

			time.Sleep(pollTimeoutShort)
			num_csi_pods := len(list_of_pods)
			err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Scale up statefulSets replicas count
			ginkgo.By("Scale up StaefulSets replicas in parallel")
			statefulSetReplicaCount = 10
			for i := 0; i < len(statefulSets); i++ {
				scaleUpStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			}

			// Scale down statefulset to 40 replicas
			statefulSetReplicaCount = 40
			ginkgo.By("Scale down statefulset replica count to 0")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			}

			// Scale down statefulset to 30 replicas
			statefulSetReplicaCount = 30
			ginkgo.By("Scale down statefulset replica count to 0")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			}

			// Fetch the number of CSI pods running before restart
			list_of_pods, err = fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Restart CSI daemonset
			cmd = []string{"rollout", "restart", "daemonset/vsphere-csi-node", "--namespace=" + csiSystemNamespace}
			framework.RunKubectlOrDie(csiSystemNamespace, cmd...)

			time.Sleep(pollTimeoutShort)
			num_csi_pods = len(list_of_pods)
			err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout, ignoreLabels)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Scale up statefulSets replicas count
			ginkgo.By("Scale up StaefulSets replicas in parallel")
			statefulSetReplicaCount = 70
			for i := 0; i < len(statefulSets); i++ {
				scaleUpStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
			}

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
				if len(pvclaims) == 3 {
					pod, err := createPod(client, namespace, nil, pvclaims, false, execCommand)
					framework.Logf("Pod %s created successfully", pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvclaims = nil
				}
			}
			defer func() {
				for _, pod := range podList {
					err := fpod.DeletePodWithWait(client, pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			defer func() {
				for _, pvc := range pvclaimsList {
					pv := getPvFromClaim(client, pvc.Namespace, pvc.Name)
					err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			// Scale down statefulSets replica count to 0
			statefulSetReplicaCount = 0
			ginkgo.By("Scale down statefulset replica count to 0")
			for i := 0; i < len(statefulSets); i++ {
				scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount, true)
				ssPodsAfterScaleDown := GetListOfPodsInSts(client, statefulSets[i])
				gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
			}
		})

	})

func GetStatefulSetFromManifestFor265Disks(ns string) *appsv1.StatefulSet {
	ssManifestFilePath := filepath.Join(manifestPathFor256Disks, "statefulset.yaml")
	framework.Logf("Parsing statefulset from %v", ssManifestFilePath)
	ss, err := manifest.StatefulSetFromManifest(ssManifestFilePath, ns)
	framework.ExpectNoError(err)
	return ss
}

func waitForPodToBeInRunningState(ctx context.Context, client clientset.Interface, namespace string,
	statefulset *appsv1.StatefulSet) error {
	waitErr := wait.Poll(healthStatusPollInterval, healthStatusPollTimeout, func() (bool, error) {
		framework.Logf("wait for next poll %v", healthStatusPollInterval)
		pods := fss.GetPodList(client, statefulset)
		for _, sspod := range pods.Items {
			err := fpod.WaitForPodNameRunningInNamespace(client, sspod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			return true, nil
		}
		return false, nil
	})
	return waitErr
}
