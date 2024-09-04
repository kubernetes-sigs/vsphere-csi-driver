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
	"os"
	"strconv"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
)

var _ = ginkgo.Describe("Volume Snapshot Basic Test", func() {
	f := framework.NewDefaultFramework("volume-snapshot")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                 clientset.Interface
		namespace              string
		scParameters           map[string]string
		pandoraSyncWaitTime    int
		restConfig             *restclient.Config
		guestClusterRestConfig *restclient.Config
		snapc                  *snapclient.Clientset
		labels_ns              map[string]string
		labelsMap              map[string]string
		zonalPolicy            string
		zonalWffcPolicy        string
		allowedTopologies      []v1.TopologySelectorLabelRequirement
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)

		// fetching node list and checking node status
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		// delete nginx service
		service, err := client.CoreV1().Services(namespace).Get(ctx, servicename, metav1.GetOptions{})
		if err == nil && service != nil {
			deleteService(namespace, client, service)
		}

		// Get snapshot client using the rest config
		if vanillaCluster || supervisorCluster {
			restConfig = getRestConfigClient()
			snapc, err = snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if guestCluster {
			guestClusterRestConfig = getRestConfigClientForGuestCluster(guestClusterRestConfig)
			snapc, err = snapclient.NewForConfig(guestClusterRestConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// reading fullsync wait time
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		// required for pod creation
		labels_ns = map[string]string{}
		labels_ns[admissionapi.EnforceLevelLabel] = string(admissionapi.LevelPrivileged)
		labels_ns["e2e-framework"] = f.BaseName

		//setting map values
		labelsMap = make(map[string]string)
		labelsMap["app"] = "test"

		topologyHaMap := GetAndExpectStringEnvVar(topologyHaMap)
		_, categories := createTopologyMapLevel5(topologyHaMap)
		allowedTopologies = createAllowedTopolgies(topologyHaMap)
		allowedTopologyHAMap := createAllowedTopologiesMap(allowedTopologies)
		framework.Logf("Topology map: %v, categories: %v", allowedTopologyHAMap, categories)

		zonalPolicy = GetAndExpectStringEnvVar(envZonalStoragePolicyName)
		if zonalPolicy == "" {
			ginkgo.Fail(envZonalStoragePolicyName + " env variable not set")
		}
		zonalWffcPolicy = GetAndExpectStringEnvVar(envZonalWffcStoragePolicyName)
		if zonalWffcPolicy == "" {
			ginkgo.Fail(envZonalWffcStoragePolicyName + " env variable not set")
		}
		framework.Logf("zonal policy: %s and zonal wffc policy: %s", zonalPolicy, zonalWffcPolicy)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(ctx, client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if supervisorCluster {
			dumpSvcNsEventsOnTestFailure(client, namespace)
		}

		service, err := client.CoreV1().Services(namespace).Get(ctx, servicename, metav1.GetOptions{})
		if err == nil && service != nil {
			deleteService(namespace, client, service)
		}
	})

	/*
	   Testcase-1
	   ZonalPolicy → immediateBindingMode
	   Workflow Path: PVC → Pod → Snapshot → RestoreVol → Pod

	   1. SVC should list two storage classes
	   a) ZonalPolicy-immediateBindingMode
	   b) ZonalPolicy-lateBinding (WFFC)
	   2. Create PVC using the storage class (ZonalPolicy-immediateBindingMode)
	   3. Wait for PVC to reach the Bound state.
	   4. Create a Pod using the PVC created in step #13
	   5. Wait for Pod to reach the Running state. Write data into the volume.
	   6. Describe PV and verify the node affinity details should show up as Ex: topology.kubernetes.io/zone in [zone-2]
	   7. Make sure Pod is scheduled on appropriate nodes preset in the availability zone
	   8. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   9. Create a volume snapshot for the PVC created in step #3.
	   10. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   11. Create a PVC using the volume snapshot in step #10.
	   12. Wait for PVC to reach the Bound state.
	   13. Create a new Pod and attach it to the volume created in step #12.
	   14. Wait for Pod to reach the Running state. Verify reading/writing data into the volume
	   15. Describe PV and verify the node affinity details should show up.
	   16. Make sure Pod is scheduled on appropriate nodes preset in the availability zone
	   17. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("SS1", ginkgo.Label(p0, wcp, core), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		scParameters[svStorageClassName] = zonalPolicy

		ginkgo.By("Create storage class")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, zonalPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvclaim, pvs, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By("Create a Pod using the volume created above and write data into the volume")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pvs[0].Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		annotations := pod.Annotations
		vmUUID, exists := annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pvs[0].Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		output := readFileFromPod(namespace, pod.Name, filePathPod1)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		writeDataOnFileFromPod(namespace, pod.Name, filePathPod1, "Hello message from test into Pod1")
		output = readFileFromPod(namespace, pod.Name, filePathPod1)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(ctx, volHandle, pvclaim, pvs[0], pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify pv and node affinity")
		err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod, allowedTopologies)

		ginkgo.By("Create or restore a volume using the dynamically created volume snapshot")
		pvclaim2, pvs2, pod2 := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot, diskSize, true)
		volHandle2 := pvs2[0].Spec.CSI.VolumeHandle
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(ctx, volHandle2, pvclaim2, pvs2[0], pod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify pv and node affinity for newly created pod")
		verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod2, allowedTopologies)

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-2
	   ZonalPolicy → Late Binding
	   Workflow Path: PVC → Pod → Snapshot → RestoreVol → Pod

	   1. SVC should list two storage classes
	   a) ZonalPolicy-immediateBindingMode
	   b) ZonalPolicy-lateBinding (WFFC)
	   2. Create PVC using the storage class (ZonalPolicy-lateBinding).
	   3. Create a Pod using the PVC created above.
	   4. Verify PVC reaches the Bound state.
	   5. Wait for Pod to reach the Running state. Write data into the volume.
	   6. Describe PV and verify the node affinity details should show up as Ex: topology.kubernetes.io/zone in [zone-2]
	   7. Make sure Pod is scheduled on appropriate nodes preset in the availability zone
	   8. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster.
	   9. Create a volume snapshot for the PVC created in step #2
	   10. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   11. Create a PVC using the volume snapshot created in step #9.
	   12. Wait for PVC to reach the Bound state.
	   13. Create a new Pod and attach it to the volume created in step #11.
	   14. Wait for Pod to reach the Running state. Verify reading/writing data into the volume
	   15. Describe PV and verify the node affinity details should show up as Ex: topology.kubernetes.io/zone in [zone-2]
	   16. Make sure Pod is scheduled on appropriate nodes preset in the availability zone
	   17. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("SS2", ginkgo.Label(p0, wcp, core), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		scParameters[svStorageClassName] = zonalWffcPolicy

		ginkgo.By("Create storage class")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, zonalWffcPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		_, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, "", false, "", "", storageclass, 1, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a Pod using the volume created above and write data into the volume")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaims[0]}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err := checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[0].Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pvs[0].Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		annotations := pod.Annotations
		vmUUID, exists := annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pvs[0].Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		output := readFileFromPod(namespace, pod.Name, filePathPod1)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		writeDataOnFileFromPod(namespace, pod.Name, filePathPod1, "Hello message from test into Pod1")
		output = readFileFromPod(namespace, pod.Name, filePathPod1)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaims[0], volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(ctx, volHandle, pvclaims[0], pvs[0], pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify pv and node affinity")
		verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod, allowedTopologies)

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)
		pvclaim2, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating pod to attach PV to the node")
		pod2, err := createPod(ctx, client, namespace, nil,
			[]*v1.PersistentVolumeClaim{pvclaim2}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvs2, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := pvs2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		nodeName := pod2.Spec.NodeName
		annotations = pod2.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle2, nodeName))
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volHandle2, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(ctx, volHandle2, pvclaim2, pvs2[0], pod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify pv and node affinity")
		verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod2, allowedTopologies)

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-3
	   ZonalPolicy → Late Binding
	   Workflow Path: Statefulset
	   (with node affinity set and with 3 replicas) → Snapshot of all 3 PVCs → Restore all 3 snapshots -> 3 deployments

	   1. SVC should list two storage classes
	   a) ZonalPolicy-immediateBindingMode
	   b) ZonalPolicy-lateBinding (WFFC)
	   2. Create a Statefulset with node affinity rule set to any particular zone (zone-1) with replica
	   count set to 3 using the storage class (ZonalPolicy-lateBinding).
	   3. Wait for PVC to reach the Bound state and Pods to reach the Running state.
	   4. Write some data to volume
	   5. Describe PV and verify the node affinity details should show up as  topology.kubernetes.io/zone in [zone-1]
	   6. Make sure Pod is scheduled on appropriate nodes preset in the availability zone
	   7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster.
	   8. Create 3 volume snapshots (vols-1, vols-2, vols-3) for all 3 statefulset PVCs created above.
	   9. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   10. Create 3 new PVCs using the volume snapshot created in step #8
	   11. Wait for all new PVCs to reach the Bound state.
	   12. Describe PV and verify the node affinity details should show up as Ex: topology.kubernetes.io/zone in [zone-1]
	   13. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("SS3", ginkgo.Label(p0, vanilla, block, wcp, core), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		scParameters[svStorageClassName] = zonalPolicy

		ginkgo.By("Create storage class")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, zonalPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating StorageClass for Statefulset")
		stsReplicas := 3
		depReplica := 1

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		framework.Logf("Create StatefulSet")
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true,
			int32(stsReplicas), true, allowedTopologies, true, true,
			zonalPolicy, "", storageclass, zonalPolicy)
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		framework.Logf("Verify PV node affinity and that the PODS are running on appropriate node")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("error verifying PV node affinity and POD node details: %v", err))

		ssPodsBeforeScaleDown := fss.GetPodList(ctx, client, statefulset)

		framework.Logf("Fetching pod 1, pvc1 and pv1 details")
		pod1, err := client.CoreV1().Pods(namespace).Get(ctx,
			ssPodsBeforeScaleDown.Items[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc1 := pod1.Spec.Volumes[0].PersistentVolumeClaim
		pvclaim1, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
			pvc1.ClaimName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv1 := getPvFromClaim(client, statefulset.Namespace, pvc1.ClaimName)
		volHandle1 := pv1.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle1).NotTo(gomega.BeEmpty())

		framework.Logf("Fetching pod 2, pvc2 and pv2 details")
		pod2, err := client.CoreV1().Pods(namespace).Get(ctx,
			ssPodsBeforeScaleDown.Items[1].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc2 := pod2.Spec.Volumes[0].PersistentVolumeClaim
		pvclaim2, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
			pvc2.ClaimName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv2 := getPvFromClaim(client, statefulset.Namespace, pvc2.ClaimName)
		volHandle2 := pv2.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		framework.Logf("Fetching pod3, pvc3 and pv3 details")
		pod3, err := client.CoreV1().Pods(namespace).Get(ctx,
			ssPodsBeforeScaleDown.Items[2].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc3 := pod3.Spec.Volumes[0].PersistentVolumeClaim
		pvclaim3, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
			pvc3.ClaimName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv3 := getPvFromClaim(client, statefulset.Namespace, pvc3.ClaimName)
		volHandle3 := pv3.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle3).NotTo(gomega.BeEmpty())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot-1 for pvc-1")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim1, volHandle1, "1Gi", true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create volume snapshot-2 for pvc-2")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim2, volHandle2, "1Gi", true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create volume snapshot-3 for pvc-3")
		volumeSnapshot3, snapshotContent3, snapshotCreated3,
			snapshotContentCreated3, snapshotId3, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim3, volHandle3, "1Gi", true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated3 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent3, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated3 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot3.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot3.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create restorevol1 from snapshot1")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, "1Gi", storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot1.Name, snapshotapigroup)
		restoreVol1, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		restorepv1, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{restoreVol1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		restoreVolHandle1 := restorepv1[0].Spec.CSI.VolumeHandle
		gomega.Expect(restoreVolHandle1).NotTo(gomega.BeEmpty())

		ginkgo.By("Create restorevol2 from snapshot2")
		pvcSpec = getPersistentVolumeClaimSpecWithDatasource(namespace, "1Gi", storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot2.Name, snapshotapigroup)
		restoreVol2, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		restorepv2, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{restoreVol2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		restoreVolHandle2 := restorepv2[0].Spec.CSI.VolumeHandle
		gomega.Expect(restoreVolHandle2).NotTo(gomega.BeEmpty())

		ginkgo.By("Create restorevol3 from snapshot3")
		pvcSpec = getPersistentVolumeClaimSpecWithDatasource(namespace, "1Gi", storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot3.Name, snapshotapigroup)
		restoreVol3, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		restorepv3, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{restoreVol3}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		restoreVolHandle3 := restorepv3[0].Spec.CSI.VolumeHandle
		gomega.Expect(restoreVolHandle3).NotTo(gomega.BeEmpty())

		ginkgo.By("Attach Deployment1 to restorevol1")
		deployment1, err := createDeployment(ctx, client, int32(depReplica), labelsMap,
			nil, namespace, []*v1.PersistentVolumeClaim{restoreVol1}, execRWXCommandPod1, false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment1.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Attach Deployment2 to restorevol2")
		deployment2, err := createDeployment(ctx, client, int32(depReplica), labelsMap,
			nil, namespace, []*v1.PersistentVolumeClaim{restoreVol2}, execRWXCommandPod1, false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment2.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Attach Deployment3 to restorevol3")
		deployment3, err := createDeployment(ctx, client, int32(depReplica), labelsMap,
			nil, namespace, []*v1.PersistentVolumeClaim{restoreVol3}, execRWXCommandPod1, false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment3.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify pv and node affinity")
		err = verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx, client, deployment1, namespace, allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx, client, deployment2, namespace, allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx, client, deployment3, namespace, allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete volume snapshot-1")
		snapshotCreated1, snapshotContentCreated1, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot1, pandoraSyncWaitTime, volHandle1, snapshotId1, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete volume snapshot-2")
		snapshotCreated2, snapshotContentCreated2, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot2, pandoraSyncWaitTime, volHandle2, snapshotId2, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete volume snapshot-3")
		snapshotCreated3, snapshotContentCreated3, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot3, pandoraSyncWaitTime, volHandle3, snapshotId3, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-4
	   ZonalPolicy → immediateBindingMode
	   Workflow Path: PVC → Offline Expansion → Snapshot → Pod → Online Expansion → Snapshot

	   1. SVC should list two storage classes
	   a) ZonalPolicy-immediateBindingMode
	   b) ZonalPolicy-lateBinding (WFFC)
	   2. Create PVC using the storage class (ZonalPolicy-immediateBindingMode).
	   3. Wait for PVC to reach the Bound state.
	   4. Describe PV and verify the node affinity details
	   5. [Offine expansion] Edit PVC and expand the size
	   6. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster.
	   7. Create a volume snapshot for the PVC created in step #2
	   8. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   9. Create a POD using the above PVC and make sure after the POD reaches a running state volume expansion is honoured on the PVC
	   10. [Online expansion] Use the same PVC and expand volume again
	   11. Volume expansion should be successful
	   12. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster.
	   13. Create a volume snapshot for the PVC created in step #2
	   14. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   15. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("SS4", ginkgo.Label(p0, wcp, core), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		scParameters[svStorageClassName] = zonalPolicy

		ginkgo.By("Create storage class")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, zonalPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvclaim, persistentVolumes, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Modify PVC spec to trigger volume expansion
		// We expand the PVC while no pod is using it to ensure offline expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(3072)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot-1 for pvc-1")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the pod")
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))
		annotations := pod.Annotations
		vmUUID, exists := annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

		framework.Logf("VMUUID : %s", vmUUID)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		output := readFileFromPod(namespace, pod.Name, filePathPod1)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		writeDataOnFileFromPod(namespace, pod.Name, filePathPod1, "Hello message from test into Pod1")
		output = readFileFromPod(namespace, pod.Name, filePathPod1)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err := getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %s, before expansion: %s", fsSize, diskSizeInMb)
		if fsSize < diskSizeInMb {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}
		ginkgo.By("File system resize finished successfully")

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)

		ginkgo.By("Create volume snapshot-1 for pvc-1")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete volume snapshot-1")
		snapshotCreated1, snapshotContentCreated1, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot1, pandoraSyncWaitTime, volHandle, snapshotId1, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete volume snapshot-2")
		snapshotCreated2, snapshotContentCreated2, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot2, pandoraSyncWaitTime, volHandle, snapshotId2, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
