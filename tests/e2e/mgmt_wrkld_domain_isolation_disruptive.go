/*
	Copyright 2025 The Kubernetes Authors.

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
	"time"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ bool = ginkgo.Describe("[domain-isolation] Management-Workload-Domain-Isolation", func() {

	f := framework.NewDefaultFramework("domain-isolation")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                  clientset.Interface
		namespace               string
		vcRestSessionId         string
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		storagePolicyName       string
		replicas                int32
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		labelsMap               map[string]string
		labels_ns               map[string]string
		pandoraSyncWaitTime     int
		restConfig              *restclient.Config
		snapc                   *snapclient.Clientset
		err                     error
		zone1                   string
		zone2                   string
		zone3                   string
		zone4                   string
		sharedStoragePolicyName string
		statuscode              int
		clusterName2            string
		clusterName4            string
		sharedStorageProfileId  string
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// making vc connection
		client = f.ClientSet
		bootstrap()

		// reading vc session id
		if vcRestSessionId == "" {
			vcRestSessionId = createVcSession4RestApis(ctx)
		}

		// reading topology map set for management doamin and workload domain
		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap)

		// required for pod creation
		labels_ns = map[string]string{}
		labels_ns[admissionapi.EnforceLevelLabel] = string(admissionapi.LevelPrivileged)
		labels_ns["e2e-framework"] = f.BaseName

		//setting map values
		labelsMap = make(map[string]string)
		labelsMap["app"] = "test"

		// reading fullsync wait time
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		//zones used in the test
		zone1 = topologyAffinityDetails[topologyCategories[0]][0]
		zone2 = topologyAffinityDetails[topologyCategories[0]][1]
		zone3 = topologyAffinityDetails[topologyCategories[0]][2]
		zone4 = topologyAffinityDetails[topologyCategories[0]][3]

		// reading shared storage policy
		sharedStoragePolicyName = GetAndExpectStringEnvVar(envIsolationSharedStoragePolicyName)
		if sharedStoragePolicyName == "" {
			ginkgo.Skip("Skipping the test because WORKLOAD_ISOLATION_SHARED_STORAGE_POLICY is not set")
		} else {
			sharedStorageProfileId = e2eVSphere.GetSpbmPolicyID(sharedStoragePolicyName)
		}

		// reading cluster name tagged to zone
		clusterName2 = getClusterNameFromZone(ctx, zone2)
		clusterName4 = getClusterNameFromZone(ctx, zone4)

		// required for snapshot
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		dumpSvcNsEventsOnTestFailure(client, namespace)

		framework.Logf("Collecting supervisor PVC events before performing PV/PVC cleanup")
		eventList, err := client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, item := range eventList.Items {
			framework.Logf("%q", item.Message)
		}
	})

	/*
		Testcase-1
		Put all ESXi hosts into maintenance mode in the zone-2 workload domain cluster
		Immediate Binding mode
		Block volume

		Test Steps:
		1. Create a WCP namespace and add zones zone-1, zone-2 and zone-3 to it.
		2. Create a storage policy which is shared and compatible across all zones and add it
		to the above namespace.
		3. Create multiple PVCs on a scale in parallel
		4. Wait for PVCs to reach the Bound state.
		5. Create pods and attach them to each PVC created above.
		6. Verify Pod should reach the running state.
		7. Verify Pod node annotation.
		8. Verify PVC annotation and PV affinity details and pod node annotation.
		9. Create statefulset with replica count 3.
		10. Verify Pods and PVCs are created successfully.
		11. Verify annotation and affinity details.
		12. Put all the ESXi hosts of zone-2 in MM mode.
		13. Perform scaling operation. Increase the replica count from 3 to 6.
		14. Verify scaling operation went smoothly.
		15. Create a new PVC and set the requested topology to be zone-2
		16. PVC creation should get stuck in a pending state with an appropriate error message.
		17. Take snapshot of any 1 PVC
		18. Verify snapshot created status.
		19. Exit all the ESXi hosts from MM mode.
		20. Wait for some time for setup to be back to normal state.
		21. Verify the PVC status which was stuck in Pending state. Ideally, it should reach the Bound state.
		22. Mark zone-2 for removal.
		23. Verify workload status, snapshot status and volume status.
		24. Perform scaleup operation
		25. Verify scaling operation goes smoothly.
		26. Restore snapshots to create new volumes.
		27. Create new volumes and attach them to Pods.
		28. Verify newly created workload status.
		29. Perform cleanup: delete pods, snapshots, volumes and namespace
	*/

	ginkgo.It("MM mode", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var podList []*v1.Pod
		var hostsInMM []*object.HostSystem
		pvcCount := 5
		var timeout int32 = 300
		var stsReplicas int32 = 6
		var volhandles []string

		// statefulset replica count
		replicas = 1

		ginkgo.By("Create a WCP namespace and tag zone-1, zone-2 and zone-3 to it using shared storage policy")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId}, getSvcId(vcRestSessionId),
			[]string{zone1, zone2, zone3}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount, nil)

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())

			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
			volhandle := pv.Spec.CSI.VolumeHandle
			volhandles = append(volhandles, volhandle)
			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(
				pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}

			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeoutShort))
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		for i := 0; i < len(podList); i++ {
			err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, podList[i], nil, namespace,
				allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset")
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, false, nil,
			false, true, "", "", storageclass, storageclass.Name)
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity for statefulset")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Put all esxi hosts of Az2 in maintenance mode with ensureAccessibility")
		hostsInCluster2 := getHostsByClusterName(ctx, clusterComputeResource, clusterName2)
		for _, host := range hostsInCluster2 {
			hostsInMM = append(hostsInMM, host)
		}

		for _, host := range hostsInMM {
			enterHostIntoMM(ctx, host, ensureAccessibilityMModeType, timeout, true)
		}
		defer func() {
			framework.Logf("Exit the hosts from MM before terminating the test")
			for _, host := range hostsInMM {
				exitHostMM(ctx, host, timeout)
			}
		}()

		ginkgo.By("Perform sclaeup operation. Increase the replica count from 3 to 6")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		ginkgo.By("Creating pvc with requested topology annotation set to zone2")
		pvcNew, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclass, "", zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to fail provisioning because zone2 cluster hosts are in MM mode")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimPending, client,
			pvcNew.Namespace, pvcNew.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))

		expectedErrMsg := "failed to provision volume with StorageClass"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		errorOccurred := checkEventsforError(client, pvcNew.Namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvcNew.Name)}, expectedErrMsg)
		gomega.Expect(errorOccurred).To(gomega.BeTrue())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot for any 1 PVC")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaimsList[0], volhandles[0], diskSize, true)
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

		framework.Logf("Exit the hosts from MM mode")
		for _, host := range hostsInMM {
			exitHostMM(ctx, host, timeout)
		}
		time.Sleep(5 * time.Minute)

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvcNew, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcNew.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvcNew.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())

		ginkgo.By("Mark zone-2 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace,
			topologyAffinityDetails[topologyCategories[0]][1]) // this will fetch zone-2
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform sclaeup operation. Increase the replica count from 6 to 9")
		stsReplicas = 9
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		ginkgo.By("Restoring a snapshot-1 to create a new volume and attach it to a new Pod")
		restorePvc, restorePv, restorePod := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, true)
		restoreVolHandle := restorePv[0].Spec.CSI.VolumeHandle
		gomega.Expect(restoreVolHandle).NotTo(gomega.BeEmpty())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", restorePod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, restorePod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorePvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restoreVolHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		Testcase-2
				PSOD all hosts in the zone-4 workload domain cluster


			Block Volume

			WFFC Binding mode
			    1. Create a WCP namespace and add zone-1 and zone-4 to the namespace.
			    2. Create a storage policy which is shared and compatible across all zones and add it
			    to the above namespace.
			    3. Create multiple PVCs on a scale
			    4. Wait for PVCs to reach the Bound state.
			    5. Verify PVC annotation and PV affinity details.
			    6. Create pods and attach them to each PVC created above.
			    7. Verify Pod should reach the Bound state.
			    8. Verify Pod node annotation.
			    9. Create statefulset with replica count 3.
			    10. Verify Pods and PVCs are created successfully.
			    11. Verify annotation and affinity details.
			    12. Add a new zone zone-2 to the namespace.
			    13. PSOD all the ESXi hosts in zone-4
			    14. Perform scaling operation. Increase the replica count from 3 to 6.
			    15. Verify scaling operation went smoothly.
			    16. Create a new PVC and set the requested topology to be zone-4
			    17. PVC creation should get stuck in a pending state with an appropriate error message.
			    18. Take snapshots of any 1 volume created above
			    19. Verify snapshot created status.
			    20. ESXI hosts will recover from PSOD timeout
			    21. Wait for some time for setup to be back to normal state.
			    22. Verify the PVC status which was stuck in Pending state. Ideally, it should reach the Bound state.
			    23. Mark zone-1 for removal.
			    24. Verify workload status, snapshot status and volume status.
			    25. Perform scaleup operation
			    26. Verify scaling operation goes smoothly.
			    27. Restore snapshot to create new volume.
			    28. Create new volumes and attach them to Pods.
			    29. Verify newly created workload status.
			    30. Perform cleanup: delete pods, snapshots, volumes and namespace
	*/

	ginkgo.It("psod", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var podList []*v1.Pod
		pvcCount := 5
		var stsReplicas int32 = 6
		var volhandles []string

		// statefulset replica count
		replicas = 1

		ginkgo.By("Create a WCP namespace and tag zone-1 and zone-4 to it using shared storage policy")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId}, getSvcId(vcRestSessionId),
			[]string{zone1, zone4}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount, nil)

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())

			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
			volhandle := pv.Spec.CSI.VolumeHandle
			volhandles = append(volhandles, volhandle)
			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(
				pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}

			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeoutShort))
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		for i := 0; i < len(podList); i++ {
			err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, podList[i], nil, namespace,
				allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset")
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, false, nil,
			false, true, "", "", storageclass, storageclass.Name)
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity for statefulset")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Add zone-2 to wcp namespace")
		err = addZoneToWcpNs(vcRestSessionId, namespace,
			topologyAffinityDetails[topologyCategories[0]][1]) // this will fetch zone-2
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform Psod on all esxi hosts of Az4")
		hostsInCluster4 := getHostsByClusterName(ctx, clusterComputeResource, clusterName4)
		for _, host := range hostsInCluster4 {
			err = psodHost(host.Name(), psodTime)
		}

		ginkgo.By("Perform sclaeup operation. Increase the replica count from 3 to 6")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		ginkgo.By("Creating pvc with requested topology annotation set to zone4")
		pvcNew, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclass, "", zone4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to fail provisioning")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimPending, client,
			pvcNew.Namespace, pvcNew.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))

		expectedErrMsg := "failed to provision volume with StorageClass"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		errorOccurred := checkEventsforError(client, pvcNew.Namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvcNew.Name)}, expectedErrMsg)
		gomega.Expect(errorOccurred).To(gomega.BeTrue())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot for any 1 PVC")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaimsList[0], volhandles[0], diskSize, true)
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

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvcNew, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcNew.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvcNew.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())

		ginkgo.By("Mark zone-1 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace,
			topologyAffinityDetails[topologyCategories[0]][0]) // this will fetch zone-1
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform sclaeup operation. Increase the replica count from 6 to 9")
		stsReplicas = 9
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		ginkgo.By("Restoring a snapshot-1 to create a new volume and attach it to a new Pod")
		restorePvc, restorePv, restorePod := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, true)
		restoreVolHandle := restorePv[0].Spec.CSI.VolumeHandle
		gomega.Expect(restoreVolHandle).NotTo(gomega.BeEmpty())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", restorePod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, restorePod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorePvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restoreVolHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
			vc reboot
			VC reboot with multiple concurrent operations


		Immediate Binding mode

		    Create a WCP namespace and add zones zone-1, zone-2, zone-3  to it.
		    Create a storage policy which is shared and compatible across all zones and add it
		    to the above namespace.
		    Create multiple PVCs on a scale and at the same time bring down vSAN-Health service.
		    PVC should get stuck in the error message. Verify the error.
		    Bring up vsan health service.
		    Verify PVC status. It should come to Bound state.
		    Verify PVC annotation and PV affinity details.
		    Create standalone and deployment Pods and attach them to each PVC created above. While pod creation is going on bring down sps service.
		    Pod creation should get stuck in a Pending state. Verify the error message.
		    Bring up the sps service.
		    Verify Pods status.
		    Verify Pod should reach the running state.
		    Verify Pod node annotation.
		    Create statefulset with replica count 3.
		    Verify Pods and PVCs are created successfully.
		    Verify annotation and affinity details.
		    Mark zone-3 for removal
		    Perform scaling operation. Increase the replica count from 3 to 6. While scaling is going on reboot VC.
		    Wait for VC to come back to running state and later verify scaling operation.
		    Scaleup operation should go smooth.
		    Take snapshots of all the volumes created above so far.
		    Verify snapshot created status.
		    Verify workload status, snapshot status and volume status.
		    Perform scaleup/scaledown operation
		    Restore snapshots to create new volumes.
		    Verify scaling operation goes smoothly.
		    Create new volumes and attach them to Pods.
		    Verify newly created workload status.
		    Perform cleanup: delete pods, snapshots, volumes and namespace
	*/

	ginkgo.It("reboot", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var podList []*v1.Pod
		pvcCount := 5
		var stsReplicas int32 = 6
		var volhandles []string

		// statefulset replica count
		replicas = 1

		ginkgo.By("Create a WCP namespace and tag zone-1 and zone-4 to it using shared storage policy")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId}, getSvcId(vcRestSessionId),
			[]string{zone1, zone4}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount, nil)

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())

			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
			volhandle := pv.Spec.CSI.VolumeHandle
			volhandles = append(volhandles, volhandle)
			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(
				pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}

			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeoutShort))
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		for i := 0; i < len(podList); i++ {
			err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, podList[i], nil, namespace,
				allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset")
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, false, nil,
			false, true, "", "", storageclass, storageclass.Name)
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity for statefulset")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Add zone-2 to wcp namespace")
		err = addZoneToWcpNs(vcRestSessionId, namespace,
			topologyAffinityDetails[topologyCategories[0]][1]) // this will fetch zone-2
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform Psod on all esxi hosts of Az4")
		hostsInCluster4 := getHostsByClusterName(ctx, clusterComputeResource, clusterName4)
		for _, host := range hostsInCluster4 {
			err = psodHost(host.Name(), psodTime)
		}

		ginkgo.By("Perform sclaeup operation. Increase the replica count from 3 to 6")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		ginkgo.By("Creating pvc with requested topology annotation set to zone4")
		pvcNew, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclass, "", zone4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to fail provisioning")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimPending, client,
			pvcNew.Namespace, pvcNew.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))

		expectedErrMsg := "failed to provision volume with StorageClass"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		errorOccurred := checkEventsforError(client, pvcNew.Namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvcNew.Name)}, expectedErrMsg)
		gomega.Expect(errorOccurred).To(gomega.BeTrue())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot for any 1 PVC")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaimsList[0], volhandles[0], diskSize, true)
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

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvcNew, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcNew.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvcNew.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())

		ginkgo.By("Mark zone-1 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace,
			topologyAffinityDetails[topologyCategories[0]][0]) // this will fetch zone-1
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform sclaeup operation. Increase the replica count from 6 to 9")
		stsReplicas = 9
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		ginkgo.By("Restoring a snapshot-1 to create a new volume and attach it to a new Pod")
		restorePvc, restorePv, restorePod := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, true)
		restoreVolHandle := restorePv[0].Spec.CSI.VolumeHandle
		gomega.Expect(restoreVolHandle).NotTo(gomega.BeEmpty())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", restorePod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, restorePod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorePvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restoreVolHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})
})
