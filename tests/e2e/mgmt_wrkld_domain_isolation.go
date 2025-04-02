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
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

/*
Status codes expected when APIs invoked.
If WCP or CSI drivers are up API return 204. If any of WCP or CSI, is down  API fails with 204
*/
const status_code_failure = 500
const status_code_success = 204

var _ bool = ginkgo.Describe("[domain-isolation] Management-Workload-Domain-Isolation", func() {

	f := framework.NewDefaultFramework("domain-isolation")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                  clientset.Interface
		namespace               string
		storageProfileId        string
		vcRestSessionId         string
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		storagePolicyName       string
		replicas                int32
		topkeyStartIndex        int
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
		sharedStorageProfileId  string
		statuscode              int
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
			framework.Logf(item.Message)
		}
	})

	/*
		Testcase-1
		Basic test
		Deploy statefulsets with 1 replica on namespace-1 in the supervisor cluster using vsan-zonal policy with
		immediate volume binding mode storageclass.

		Steps:
		1. Create a wcp namespace and tagged it to zone-2 workload zone.
		2. Read a zonal storage policy which is tagged to wcp namespace created in step #1 using Immediate Binding mode.
		3. Create statefulset with replica count 1.
		4. Wait for PVC and PV to reach Bound state.
		5. Verify PVC has csi.vsphere.volume-accessible-topology annotation with zone-2
		6. Verify PV has node affinity rule for zone-2
		7. Verify statefulset pod is in up and running state.
		8. Veirfy Pod node annoation.
		9. Perform cleanup: Delete Statefulset
		10. Perform cleanup: Delete PVC
	*/

	ginkgo.It("Verifying volume creation and PV affinities with svc namespace tagged to zonal-2 policy, "+
		"zone-2 tag, and immediate binding mode.", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 1

		// reading zonal storage policy of zone-2 workload domain
		storagePolicyName = GetAndExpectStringEnvVar(envZonal2StoragePolicyName)
		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)

		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4
			so topValStartIndex=1 and topValEndIndex=2 will fetch the 1st index value from topology map string
		*/
		topValStartIndex := 1
		topValEndIndex := 2

		ginkgo.By("Create a WCP namespace tagged to zone-2")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		// here fetching zone:zone-2 from topologyAffinityDetails
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId),
			[]string{zone2})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(204))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read zonal-2 storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
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

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-1.1
	   Create a workload with a single zone zone-1 tagged to the namespace using a zonal policy and Immediate Binding mode
	   Brief: NS creation and usage
	   zonal storage policy, compatible with zone-1
	   tagging to zone-1
	   Immediate Binding mode
	   Statefulset creation with zonal storage policy of zone-1

	   Steps:
	   1. Create a WCP Namespace and apply a zonal storage policy(compatible with zone-1), tagging it to zone-1.
	   2. Create a StatefulSet with 3 replicas, using the storage policy from step #1
	   and configuring Immediate Binding mode.
	   3. Wait for the StatefulSet PVCs to reach the "Bound" state and the StatefulSet Pods to reach the "Running" state.
	   4. Verify the StatefulSet PVC annotations and the PVs affinity details.
	   Expected to get the affinity of zone-1 on PV.
	   5. Verify the StatefulSet Pod's node annotation.
	   6. Perform cleanup by deleting the Pods, Volumes, and Namespace.
	*/

	ginkgo.It("Verify workload creation when wcp namespace is tagged to zone-1 mgmt domain and "+
		"zonal policy tagged to wcp ns is compatible only with zone-1 with immediate binding mode", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

		// reading zonal storage policy of zone-1 mgmt domain
		storagePolicyName = GetAndExpectStringEnvVar(envZonal1StoragePolicyName)
		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)

		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4
			so topValStartIndex=0 and topValEndIndex=1 will fetch the 0th index value from topology map string
		*/
		topValStartIndex := 0
		topValEndIndex := 1

		ginkgo.By("Create a WCP namespace tagged to zone-1 mgmt domain and storage policy compatible only to zone-1")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId, []string{storageProfileId},
			getSvcId(vcRestSessionId),
			[]string{zone1})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(204))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Fetch zone-1 storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
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

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Testcase-2
		Create a workload with a single zone zone-2 tagged to the namespace, using a zonal policy and WFFC Binding mode
		Brief: NS creation and usage

		zonal storage policy, compatible with zone-2
		tagging to zone-2
		WFFC Binding mode
		Statefulset creation with zonal storage policy of zone-2

		Test Steps:
		1. Create a WCP Namespace and apply a zonal storage policy(compatible with zone-2), tagging it to zone-2.
		2. Create a StatefulSet with 3 replicas, using the storage policy from step #1 and configuring WFFC Binding mode.
		3. Wait for the StatefulSet PVCs to reach the "Bound" state and the StatefulSet Pods to reach the "Running" state.
		4. Verify the StatefulSet PVC annotations and the PVs affinity details. Expected to get the affinity of zone-2 on PV.
		5. Verify the StatefulSet Pod's node annotation.
		6. Perform cleanup by deleting the Pods, Volumes, and Namespace.
	*/

	ginkgo.It("Verify workload creation when the WCP namespace is tagged to zone-2 workload domain "+
		"and the zonal policy is compatible only with zone-2, "+
		"using WFFC binding mode", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

		// reading zonal storage policy of zone-2 wrkld domain
		storagePolicyNameWffc := GetAndExpectStringEnvVar(envZonal2StoragePolicyNameLateBidning)
		storagePolicyNameImm := GetAndExpectStringEnvVar(envZonal2StoragePolicyName)
		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyNameImm)
		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4
			so topValStartIndex=1 and topValEndIndex=2 will fetch the 1st index value from topology map string
		*/
		topValStartIndex := 1
		topValEndIndex := 2

		ginkgo.By("Create a WCP namespace tagged to zone-2 wrkld domain and storage policy compatible only to zone-2")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId),
			[]string{zone2})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(204))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Fetch zone-2 storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyNameWffc, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
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

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-4
	   Create, restore, and delete dynamic snapshots, along with workload/volume
	   creation, while adding and removing zones from the namespace in between

	   Test Steps:
	   1. Create a WCP Namespace using a shared storage policy and tag it to zone-2 and zone-3.
	   2. Create PVC using the shared storage policy.
	   3. Wait for the PVC to reach the Bound state.
	   4. Verify PVC annotations and affinity details for the PVs.
	   5. Create a StatefulSet with 3 replicas using the storage policy from step #1.
	   6. Wait for the StatefulSet PVCs to reach the Bound state and the StatefulSet Pods to reach the Running state.
	   7. Verify StatefulSet PVC annotations and affinity details for the PV.
	   8. Verify the StatefulSet Pod's node annotation.
	   9. Using the PVCs created in step #2, create Deployment Pod
	   10. Wait for the deployment pod to reach the Running state.
	   11. Verify the Pod's node annotation.
	   12. Add a new zone-4 to the WCP namespace.
	   13. Perform a scaling operation on the StatefulSet, increasing the replica count to 6.
	   14. Wait for the scaling operation to complete successfully.
	   15. Verify pod, pvc, pv affinuty and annotation details for newly created pods and pvcs.
	   16. Take a dynamic snapshot of the volumes created in step #2.
	   17. Verify that the volume snapshots are created successfully and the ReadyToState is set to True.
	   18. Mark zone-2 for removal from the WCP namespace.
	   19. Restore the volume snapshots
	   20. Wait for the restored volumes to reach the Bound state.
	   21. Verify the PVC annotations and affinity details for the PV.
	   22. Create new Pod from the restored volumes created in step #18.
	   23. Verify the status of old and new Workload Pods, snapshots, and volumes—they should all be up and running.
	   24. Verify CNS volume metadata for the Pods and PVCs created.
	   25. Perform cleanup by deleting the Pods, Snapshots, Volumes, and Namespace.
	*/

	ginkgo.It("Create, restore, and delete dynamic snapshot, along with workload/volume creation, "+
		"while adding and removing zones from the namespace in between", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4, zone-5
			so topValStartIndex=1 and topValEndIndex=3 will fetch the 2nd and 3rd index from topology map string
		*/
		topValStartIndex := 0
		topValEndIndex := 5

		ginkgo.By("Fetching allowed topology assigned to all zones")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create a WCP namespace and tag it to zone-2 and zone-3 wrkld " +
			"domains using storage policy compatible to all zones")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId, []string{storageProfileId},
			getSvcId(vcRestSessionId), []string{zone2, zone3})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(204))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Fetch shared storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create PVC")
		pvclaim, persistentVolumes, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating a Deployment using pvc")
		dep, err := createDeployment(ctx, client, 1, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim}, execRWXCommandPod1, false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podList, err := fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Deployment")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		output := readFileFromPod(namespace, podList.Items[0].Name, filePathPod1)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		writeDataOnFileFromPod(namespace, podList.Items[0].Name, filePathPod1, "Hello message from test into Pod1")
		output = readFileFromPod(namespace, podList.Items[0].Name, filePathPod1)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		output2 := readFileFromPod(namespace, podList.Items[0].Name, filePathPod1)
		gomega.Expect(strings.Contains(output2, "Hello message from Pod1")).NotTo(gomega.BeFalse())

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

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, dep, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Add zone-4 to wcp namespace")
		err = addZoneToWcpNs(vcRestSessionId, namespace,
			topologyAffinityDetails[topologyCategories[0]][3]) // this will fetch zone-4
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaling operation on statefulset. Increase the replica count to 9 when zone is marked for removal")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client,
			9, 0, statefulset, true, namespace, allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		ginkgo.By("Mark zone-2 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace,
			topologyAffinityDetails[topologyCategories[0]][1]) // this will fetch zone-2
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restore a volume snapshot")
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

		ginkgo.By("Perform scaling operation on statefulset. Increase the replica count to 9 when zone is marked for removal")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client,
			6, 0, statefulset, true, namespace, allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, pod2, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-3
	   Basic test
	   Deploy statefulsets with 3 replica on namespace-1 in the supervisor cluster.
	   shared policy with immediate volume binding mode storageclass.

	   Steps:
	   1. Create a wcp namespace and tag it to zone-3 workload zone.
	   2. Read a shared storage policy which is tagged to wcp namespace created in step #1 using Immediate Binding mode.
	   3. Create statefulset with replica count 3.
	   4. Wait for PVC and PV to reach Bound state.
	   5. Verify PVC has csi.vsphere.volume-accessible-topology annotation with all zones as its shared policy
	   6. Verify PV has node affinity rule for all zones
	   7. Verify statefulset pod is in up and running state.
	   8. Veirfy Pod node annoation.
	   9. Perform cleanup: Delete Statefulset
	   10. Perform cleanup: Delete PVC
	*/

	ginkgo.It("Verifying volume creation with shared policy on namespace tagged to zone-3", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

		// here fetching zone:zone-3 from topologyAffinityDetails
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId}, getSvcId(vcRestSessionId),
			[]string{zone3})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(204))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, sharedStoragePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
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

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Testcase-5
		Deploy statefulsets with 3 replica on namespace-1 in the supervisor cluster.
		Use vsan-shared policy with CSI and WCP restart in between.
		Steps:
		1. Create a wcp namespace and tagged it to zone-1, zone-2.
		2. Read a shared storage policy which is tagged to wcp namespace created in step #1 using Immediate Binding mode.
		3. Create statefulset with replica count 3.
		4. Wait for PVC and PV to reach Bound state.
		5. Verify PVC has csi.vsphere.volume-accessible-topology annotation with all zones
		6. Verify PV has node affinity rule for all zones
		7. Verify statefulset pod is in up and running state.
		8. Veirfy Pod node annoation.
		9. update zone-3 and zone-4 to the WCP namespace and restart the WCP service at the same time.
		10. Perform a scaling operation on the StatefulSet, increasing the replica count to 6.
		11. Wait for the scaling operation to complete successfully.
		12. Mark zones 1 and 2 for removal from the WCP namespace and restart the CSI while the zone removal is in progress.
		13. Perform a ScaleUp/ScaleDown operation on the StatefulSet.
		14. Verify that the scaling operation is completed successfully.
		15. Verify the StatefulSet PVC annotations and affinity details for the PV.
		16. Verify the StatefulSet Pod node annotation.
		17. Verify CNS volume metadata for the Pods and PVCs created.
		18. Perform cleanup: Delete Statefulset
		19. Perform cleanup: Delete PVC
	*/

	ginkgo.It("CSI and WCP restart while adding and removing zones", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

		// expected status code while add/removing the zones from NS
		expectedStatusCodes := []int{status_code_failure, status_code_success}

		ginkgo.By("Create a WCP namespace tagged to zone-1 & zone-2")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId}, getSvcId(vcRestSessionId),
			[]string{zone1, zone2})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(204))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, sharedStoragePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
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

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Add zone-3 and zone-4 to the WCP namespace and restart the WCP service at the same time")
		var wg sync.WaitGroup
		wg.Add(3)
		go addZoneToWcpNsWithWg(vcRestSessionId, namespace,
			zone3, expectedStatusCodes, &wg)
		go addZoneToWcpNsWithWg(vcRestSessionId, namespace,
			zone4, expectedStatusCodes, &wg)
		go restartWcpWithWg(ctx, vcAddress, &wg)
		wg.Wait()

		ginkgo.By("Check if namespace has new zones added")
		output, _, _ := e2ekubectl.RunKubectlWithFullOutput(namespace, "get", "zones")
		framework.Logf("Check bool %v", !strings.Contains(output, zone3))
		if !strings.Contains(output, zone3) {
			framework.Logf("Adding zone-3 to NS might have failed due to WCP restart, adding it again")
			err = addZoneToWcpNs(vcRestSessionId, namespace, zone3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if !strings.Contains(output, zone4) {
			framework.Logf("Adding zone-4 to NS might have failed due to WCP restart, adding it again")
			err = addZoneToWcpNs(vcRestSessionId, namespace, zone4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform a scaling operation on the StatefulSet, increasing the replica count to 6.")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client,
			6, 0, statefulset, true, namespace, allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Mark zone-1 and zone-2 for removal from wcp namespace and restart the CSI driver at the same time")
		// Get CSI NS name and replica count
		csiNamespace := GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas := *csiDeployment.Spec.Replicas

		wg.Add(3)
		go markZoneForRemovalFromWcpNsWithWg(vcRestSessionId, namespace,
			zone1, expectedStatusCodes, &wg)
		go markZoneForRemovalFromWcpNsWithWg(vcRestSessionId, namespace,
			zone2, expectedStatusCodes, &wg)
		restartstatus, err := restartCSIDriverWithWg(ctx, client, csiNamespace, csiReplicas, &wg)
		gomega.Expect(restartstatus).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		wg.Wait()

		ginkgo.By("Perform a scaling operation on the StatefulSet, decreasing the replica count to 4.")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client,
			4, 0, statefulset, true, namespace, allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Testcase-13
		PVC requested topology is zone-1, but the namespace is tagged to zone-3
		Steps:
		1. Create a WCP namespace and tag it to zone-3.
		2. Create two zonal storage policy compatible with each zone-1 and zone-3  and tag it to the above namespace.
		3. Create PVC using zone-3 storage policy but the request topology for pvc is set to zone-1
		4. PVC creation should get stuck in a pending state with an appropriate error message.
		5. Verify the error message.
		6. Clean up by deleting volume, and Namespace.
	*/

	ginkgo.It("Create pvc with requested topology annotation tagged to one zone but "+
		"namespace is tagged to different zone", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// reading zonal storage policy of zone-1 mgmt domain and zone-3 wrkld domain
		zonalStoragePolicyZone1 := GetAndExpectStringEnvVar(envZonal1StoragePolicyName)
		storageProfileIdZone1 := e2eVSphere.GetSpbmPolicyID(zonalStoragePolicyZone1)
		zonalStoragePolicyZone3 := GetAndExpectStringEnvVar(envZonal3StoragePolicyName)
		storageProfileIdZone3 := e2eVSphere.GetSpbmPolicyID(zonalStoragePolicyZone3)

		ginkgo.By("Creating wcp namespace tagged to zone3 and zonal policies set is of zone1 and zone3")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(
			vcRestSessionId,
			[]string{storageProfileIdZone1, storageProfileIdZone3},
			getSvcId(vcRestSessionId), []string{zone3})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(204))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read zonal storage policy of zone3")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalStoragePolicyZone3, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating pvc with requested topology annotation set to zone1")
		pvclaim, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclass, "", zone1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to fail provisioning because namespace " +
			"is tagged to zone3, pvc is created with storage class of zone3 but pvc ßrequested annotation is set to zone1")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx,
			v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		expectedErrMsg := "failed to provision volume with StorageClass"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		errorOccurred := checkEventsforError(client, pvclaim.Namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(errorOccurred).To(gomega.BeTrue())
	})
})
