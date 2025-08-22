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
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ bool = ginkgo.Describe("[domain-isolation-disruptive] Management-Workload-Domain-Isolation-Disruptive", func() {

	f := framework.NewDefaultFramework("domain-isolation")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                     clientset.Interface
		namespace                  string
		vcRestSessionId            string
		allowedTopologies          []v1.TopologySelectorLabelRequirement
		replicas                   int32
		topologyAffinityDetails    map[string][]string
		topologyCategories         []string
		labelsMap                  map[string]string
		labels_ns                  map[string]string
		restConfig                 *restclient.Config
		snapc                      *snapclient.Clientset
		err                        error
		zone1                      string
		zone2                      string
		zone3                      string
		sharedStoragePolicyName    string
		statuscode                 int
		clusterName2               string
		clusterName3               string
		sharedStorageProfileId     string
		nodeList                   *v1.NodeList
		isVsanHealthServiceStopped bool
		isVcRebooted               bool
		isSPSServiceStopped        bool
		isHostInMaintenanceMode    bool
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

		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
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

		//zones used in the test
		zone1 = topologyAffinityDetails[topologyCategories[0]][0]
		zone2 = topologyAffinityDetails[topologyCategories[0]][1]
		zone3 = topologyAffinityDetails[topologyCategories[0]][2]

		// reading shared storage policy
		sharedStoragePolicyName = GetAndExpectStringEnvVar(envIsolationSharedStoragePolicyName)
		if sharedStoragePolicyName == "" {
			ginkgo.Skip("Skipping the test because WORKLOAD_ISOLATION_SHARED_STORAGE_POLICY is not set")
		} else {
			sharedStorageProfileId = e2eVSphere.GetSpbmPolicyID(sharedStoragePolicyName)
		}

		// reading cluster name tagged to zone
		clusterName2 = getClusterNameFromZone(ctx, zone2)
		clusterName3 = getClusterNameFromZone(ctx, zone3)

		// required for snapshot
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// reading testbedInfo json
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))
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

		if isVsanHealthServiceStopped {
			framework.Logf("Bringing vsanhealth up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}

		// restarting pending and stopped services after vc reboot if any
		if isVcRebooted {
			err := checkVcServicesHealthPostReboot(ctx, vcAddress, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"Setup is not in healthy state, Got timed-out waiting for required VC services to be up and running")
		}

		if isSPSServiceStopped {
			framework.Logf("Bringing sps up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
			isSPSServiceStopped = false
		}
	})

	/*
	   Testcase-1
	   Put all ESXi hosts into maintenance mode in the zone-2 workload domain cluster
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
	   26. Restore snapshot to create new volume.
	   27. Create new volumes and attach them to Pods.
	   28. Verify newly created workload status.
	   29. Perform cleanup: delete pods, snapshots, volumes and namespace
	*/

	ginkgo.It("Workload creation when zone2 host is put in maintenance mode and is marked for removal", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var podList []*v1.Pod
		var hostsInMM []*object.HostSystem
		pvcCount := 5
		var timeout int32 = 300
		var stsReplicas int32 = 6
		var volhandles []string

		// statefulset replica count
		replicas = 3

		ginkgo.By("Create a WCP namespace and tag zone-1, zone-2 and zone-3 to it using shared storage policy")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1, zone2, zone3}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, sharedStoragePolicyName, metav1.GetOptions{})
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

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		for i := 0; i < len(podList); i++ {
			err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, podList[i], nil, namespace,
				allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating service")
		_ = CreateService(namespace, client)

		ginkgo.By("Creating statefulset")
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, false, allowedTopologies,
			false, true, "", "", storageclass, storageclass.Name)

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity for statefulset")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Put all esxi hosts of Az2 in maintenance mode with ensureAccessibility")
		hostsInCluster2 := getHostsByClusterName(ctx, clusterComputeResource, clusterName2)
		hostsInMM = append(hostsInMM, hostsInCluster2...)
		for _, host := range hostsInMM {
			enterHostIntoMM(ctx, host, ensureAccessibilityMModeType, timeout, true)
			isHostInMaintenanceMode = true
		}
		defer func() {
			framework.Logf("Exit the hosts from MM before terminating the test")
			if isHostInMaintenanceMode {
				for _, host := range hostsInMM {
					exitHostMM(ctx, host, timeout)
				}
			}
		}()

		ginkgo.By("Perform scaleup operation. Increase the replica count from 3 to 6")
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

		ginkgo.By("Expect claim to fail provisioning because zone2 cluster hosts are in MM mode")
		expectedErrMsg := "failed to provision volume with StorageClass"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		errorOccurred := checkEventsforError(client, pvcNew.Namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvcNew.Name)}, expectedErrMsg)
		gomega.Expect(errorOccurred).To(gomega.BeTrue())

		ginkgo.By("Read volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot for any 1 PVC")
		volumeSnapshot, _, _,
			_, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaimsList[0], volhandles[0], diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Exit the hosts from MM mode")
		if isHostInMaintenanceMode {
			for _, host := range hostsInMM {
				exitHostMM(ctx, host, timeout)
			}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvcNew, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcNew.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvcNew.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())

		ginkgo.By("Mark zone-2 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace, zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup operation. Increase the replica count from 6 to 9")
		stsReplicas = 9
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		ginkgo.By("Restoring a snapshot-1 to create a new volume and attach it to a new Pod")
		_, _, _ = verifyVolumeRestoreOperation(ctx, client, namespace, storageclass, volumeSnapshot,
			diskSize, true)
	})

	/*
	   Testcase-2
	   PSOD all hosts in the zone-3 workload domain cluster
	   Test Steps:
	   1. Create a WCP namespace and add zone-1 and zone-3 to the namespace.
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
	   13. PSOD all the ESXi hosts in zone-3
	   14. Perform scaling operation. Increase the replica count from 3 to 6.
	   15. Verify scaling operation went smoothly.
	   16. Create a new PVC and set the requested topology to be zone-3
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

	ginkgo.It("Perform psod on Az3 cluster with zones additon and removal", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var podList []*v1.Pod
		pvcCount := 5
		var stsReplicas int32 = 6
		var volhandles []string

		// statefulset replica count
		replicas = 3

		ginkgo.By("Create a WCP namespace and tag zone-1 and zone-3 to it using shared storage policy")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1, zone3}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, sharedStoragePolicyName, metav1.GetOptions{})
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

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		for i := 0; i < len(podList); i++ {
			err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, podList[i], nil, namespace,
				allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating service")
		_ = CreateService(namespace, client)

		ginkgo.By("Creating statefulset")
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, false, nil,
			false, true, "", "", storageclass, storageclass.Name)

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity for statefulset")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Add zone-2 to wcp namespace")
		err = addZoneToWcpNs(vcRestSessionId, namespace, zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform Psod on all esxi hosts of Az3")
		hostsInCluster3 := getHostsByClusterName(ctx, clusterComputeResource, clusterName3)
		for _, host := range hostsInCluster3 {
			err = psodHost(host.Name(), psodTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup operation. Increase the replica count from 3 to 6")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		ginkgo.By("Creating pvc with requested topology annotation set to zone3")
		pvcNew, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclass, "", zone3)
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
		volumeSnapshot, _, _,
			_, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaimsList[0], volhandles[0], diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the PVC which was stuck in Pending state should gets bound eventually")
		pvcNew, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcNew.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvcNew.Status.Phase == v1.ClaimBound).To(gomega.BeTrue())

		ginkgo.By("Mark zone-1 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace, zone1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup operation. Increase the replica count from 6 to 9")
		stsReplicas = 9
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		ginkgo.By("Restoring a snapshot to create a new volume and attach it to a new Pod")
		_, _, _ = verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, true)
	})

	/*
	   Testcase-3
	   VC reboot with multiple concurrent operations
	   Test Steps:
	   1. Create a WCP namespace and add zones zone-1, zone-2, zone-3  to it.
	   2. Create a storage policy which is shared and compatible across all zones and add it
	   to the above namespace.
	   3. Create multiple PVCs on a scale and at the same time bring down vSAN-Health service.
	   4. PVC should get stuck in the error message. Verify the error.
	   5. Bring up vsan health service.
	   6. Verify PVC status. It should come to Bound state.
	   7. Verify PVC annotation and PV affinity details.
	   8. Create pods and attach them to each PVC created above. While pod creation is going on bring down sps service.
	   9. Pod creation should get stuck in a Pending state. Verify the error message.
	   10. Bring up the sps service.
	   11. Verify Pods status.
	   12. Verify Pod should reach the running state.
	   13. Verify Pod node annotation.
	   14. Create statefulset with replica count 3.
	   15. Verify Pods and PVCs are created successfully.
	   16. Verify annotation and affinity details.
	   17. Mark zone-3 for removal
	   18. Perform scaling operation. Increase the replica count from 3 to 6.
	   19. Reboot VC
	   20. Wait for VC to come back to running state and later verify scaling operation.
	   21. Scaleup operation should go smooth.
	   22. Take snapshot of any 1 volume
	   23. Verify snapshot created status.
	   24. Verify workload status, snapshot status and volume status.
	   25. Perform scaledown operation
	   26. Restore snapshot to create new volume.
	   27. Verify scaling operation goes smoothly.
	   28. Create new volumes and attach them to Pod.
	   29. Verify newly created workload status.
	   30. Perform cleanup: delete pods, snapshots, volumes and namespace
	*/

	ginkgo.It("VC reboot with multiple concurrent operations", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var podList []*v1.Pod
		pvcCount := 5
		var stsReplicas int32 = 6
		var volhandles []string

		// statefulset replica count
		replicas = 1

		ginkgo.By("Create a WCP namespace and tag zone-1, zone-2 and zone-3 to it using shared storage policy")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1, zone2, zone3}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, sharedStoragePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount, nil)

		ginkgo.By("Bring down Vsan-health service when pvc creation is in progress")
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanHealthServiceStopped = true
		defer func() {
			if isVsanHealthServiceStopped {
				framework.Logf("Bringing vsanhealth up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
			}
		}()

		ginkgo.By("Check pvc failure error for any 1 pvc")
		expectedErrMsg := "service unavilable"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		errorOccurred := checkEventsforError(client, pvclaimsList[0].Namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaimsList[0].Name)}, expectedErrMsg)
		gomega.Expect(errorOccurred).To(gomega.BeTrue())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

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
			pod, _ := createPod(ctx, client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			if i == 3 {
				ginkgo.By("Bring down SPS service")
				framework.Logf("vcAddress - %s ", vcAddress)
				err = invokeVCenterServiceControl(ctx, stopOperation, spsServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isSPSServiceStopped = true
				err = waitVCenterServiceToBeInState(ctx, spsServiceName, vcAddress, svcStoppedMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer func() {
					if isSPSServiceStopped {
						framework.Logf("Bringing sps up before terminating the test")
						startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
						isSPSServiceStopped = false
					}
				}()
			}
		}

		ginkgo.By("Check pod failure error for any 1 pod")
		expectedErrMsgForPod := "service unavilable"
		framework.Logf("Expected failure message of pod when sps service is down: %+q", expectedErrMsg)
		errorOccurred = checkEventsforError(client, podList[3].Namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", podList[3].Name)}, expectedErrMsgForPod)
		gomega.Expect(errorOccurred).To(gomega.BeTrue())

		ginkgo.By("Bringup SPS service")
		startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)

		ginkgo.By("Verify pod node attachment after sps service bring up")
		for i := 0; i < len(podList); i++ {
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, podList[i].Spec.NodeName)
			isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(
				pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		for i := 0; i < len(podList); i++ {
			err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, podList[i], nil, namespace,
				allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating service")
		_ = CreateService(namespace, client)

		ginkgo.By("Creating statefulset")
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, false, nil,
			false, true, "", "", storageclass, storageclass.Name)

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity for statefulset")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Mark zone-3 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace, zone3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup operation. Increase the replica count from 3 to 6")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		ginkgo.By("Rebooting VC")
		err = invokeVCenterReboot(ctx, vcAddress)
		isVcRebooted = true
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")
		var essentialServices []string
		if vanillaCluster {
			essentialServices = []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		} else {
			essentialServices = []string{spsServiceName, vsanhealthServiceName, vpxdServiceName, wcpServiceName}
		}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		//After reboot
		bootstrap()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot for any 1 PVC")
		volumeSnapshot, _, _,
			_, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaimsList[0], volhandles[0], diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaledown operation. Decrease the replica count from 6 to 1")
		stsReplicas = 1
		ssPods, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset, ssPods, stsReplicas, true, true)

		ginkgo.By("Restoring a snapshot-1 to create a new volume and attach it to a new Pod")
		_, _, _ = verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, true)
	})
})
