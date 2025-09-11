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

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"

	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
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
		vmopC                   ctlrclient.Client
		vmClass                 string
		cnsopC                  ctlrclient.Client
		contentLibId            string
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

		/* Sets up a Kubernetes client with a custom scheme, adds the vmopv1 API types to the scheme,
		and ensures that the client is properly initialized without errors */
		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// get or set vm class required for VM creation
		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}

		// following restconfig used for snapshot creation
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
		"zone-2 tag, and immediate binding mode.", ginkgo.Label(p0, wldi, vc90), func() {
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
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone2}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
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
		"zonal policy tagged to wcp ns is compatible only with zone-1 with immediate binding mode", ginkgo.Label(p0,
		wldi, snapshot, vc90), func() {
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
			getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
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
		"using WFFC binding mode", ginkgo.Label(p0, wldi, snapshot, vc90), func() {
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
			[]string{storageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone2}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
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

	ginkgo.It("Create, restore, and delete dynamic snapshot, along with workload/volume creation, while adding and "+
		"removing zones from the namespace in between", ginkgo.Label(p0, wldi, snapshot, vc90), func() {
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
			getSvcId(vcRestSessionId, &e2eVSphere), []string{zone2, zone3}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
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

		ginkgo.By("Perform scaling operation on statefulset. Increase the replica count to 9" +
			" when zone is marked for removal")
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
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvclaim, volHandle, diskSize, true)
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

		ginkgo.By("Perform scaling operation on statefulset. Increase the replica count to 9 when zone is marked" +
			" for removal")
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

	ginkgo.It("Verifying volume creation with shared policy on namespace tagged to zone-3", ginkgo.Label(p0, wldi,
		snapshot, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

		// here fetching zone:zone-3 from topologyAffinityDetails
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone3}, "", "")
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

	ginkgo.It("CSI and WCP restart while adding and removing zones", ginkgo.Label(p0, wldi, snapshot, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

		// expected status code while add/removing the zones from NS
		expectedStatusCodes := []int{status_code_failure, status_code_success}

		ginkgo.By("Create a WCP namespace tagged to zone-1 & zone-2")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1, zone2}, "", "")
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
		"namespace is tagged to different zone", ginkgo.Label(p1, wldi, snapshot, vc90), func() {
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
			getSvcId(vcRestSessionId, &e2eVSphere), []string{zone3}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
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

	/*
		Testcase-3 from Negative scenario
		Add tests for all supported operations on PVC
		Ex: Snapshot create/delete, restore snapshot, expand, migrate, attach/detach with zone marked for delete.
		Use both zonal policy and shared datastore policy
	*/

	ginkgo.It("Run volume expansion, create and restore operations", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// fetch shared vsphere datatsore url
		datastoreURL := os.Getenv("SHARED_VSPHERE_DATASTORE_URL")
		if datastoreURL == "" {
			ginkgo.Skip("Skipping the test because SHARED_VSPHERE_DATASTORE_URL is not set")
		} else {
			dsRef := getDsMoRefFromURL(ctx, datastoreURL)
			framework.Logf("dsmoId: %v", dsRef.Value)

			// read or create content library if it is empty
			if contentLibId == "" {
				contentLibId, err = createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
					dsRef.Value, &e2eVSphere)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		// statefulset replica count
		replicas = 3

		// reading zonal storage policy of zone-1 and zone-2
		storagePolicyNameZ1 := GetAndExpectStringEnvVar(envZonal1StoragePolicyName)
		storageProfileIdZ1 := e2eVSphere.GetSpbmPolicyID(storagePolicyNameZ1)
		storagePolicyNameZ2 := GetAndExpectStringEnvVar(envZonal2StoragePolicyNameLateBidning)
		storageProfileIdZ2 := e2eVSphere.GetSpbmPolicyID(storagePolicyNameZ2)

		// append late-binding now as it knowns to k8s and not to vc
		storagePolicyNameZ2 = storagePolicyNameZ2 + "-latebinding"

		// read datastore url
		zonal2DsUrl := os.Getenv(envZonal2DatastoreUrl)

		ginkgo.By("Create a WCP namespace tagged to zone-1 & zone-2")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId, storageProfileIdZ1, storageProfileIdZ2}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1, zone2}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Fetch storage class tagged to wcp namespace")
		storageClassNames := []string{sharedStoragePolicyName, storagePolicyNameZ1, storagePolicyNameZ2}
		storageClasses := make([]*storagev1.StorageClass, len(storageClassNames))
		for i, name := range storageClassNames {
			sc, err := client.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			storageClasses[i] = sc
		}
		sharedStorageclass := storageClasses[0]
		zonal1Sc := storageClasses[1]
		zonal2Sc := storageClasses[2]

		ginkgo.By("Create 3 PVCs with different binding modes")
		storageClassList := []*storagev1.StorageClass{sharedStorageclass, zonal1Sc, zonal2Sc}
		pvcList := []*v1.PersistentVolumeClaim{}
		for _, sc := range storageClassList {
			pvc, err := createPVC(ctx, client, namespace, labelsMap, "", sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcList = append(pvcList, pvc)
		}

		pvclaim1 := pvcList[0]
		pvclaim2 := pvcList[1]
		pvclaim3 := pvcList[2]

		ginkgo.By("Creating a Deployment using pvc-1")
		dep, err := createDeployment(ctx, client, 1, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim1}, execRWXCommandPod1, false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify pod node attachment")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, nil, dep, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Pod to attach to Pvc-2")
		pod2, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim2}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		vmUUID := getNodeUUID(ctx, client, pod2.Spec.NodeName)
		isDiskAttached2, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached2).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify pod node attachment")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, 0,
			1)
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, pod2, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vm service vm on pvc-3")
		_, vm, _, err := createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim3}, vmClass, storagePolicyNameZ2, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// fetching nodes list
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		ginkgo.By("Verify volume affinity annotation state")
		pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim3}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv = pvs[0]
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, 2,
			3)
		allowedTopologiesMap := convertToTopologyMap(allowedTopologies)
		err = verifyVolumeAnnotationAffinity(pvclaim3, pv, allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify vm affinity annotation state")
		err = verifyVmServiceVmAnnotationAffinity(vm, allowedTopologiesMap, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Start online expansion of volume")
		currentPvcSize := pvclaim2.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("4Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		_, err = expandPVCSize(pvclaim2, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		currentPvcSize = pvclaim3.Spec.Resources.Requests[v1.ResourceStorage]
		newSize = currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("4Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		_, err = expandPVCSize(pvclaim3, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Waiting for file system resize to finish")
		pvclaim3, err = waitForFSResize(pvclaim3, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcConditions := pvclaim3.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot of a PVC created on shared sc")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle1 := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle1).NotTo(gomega.BeEmpty())
		volumeSnapshot1, _, _, _, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim1, volHandle1, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Mark zone-2 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace,
			zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating service")
		_ = CreateService(namespace, client)

		ginkgo.By("Creating statefulset")
		_ = createCustomisedStatefulSets(ctx, client, namespace, true, replicas, false, nil,
			false, true, "", "", sharedStorageclass, sharedStorageclass.Name)

		ginkgo.By("Restore a volume snapshot")
		restoredPvc, restoredPv, _ := verifyVolumeRestoreOperation(ctx, client, namespace, sharedStorageclass,
			volumeSnapshot1, diskSize, false)

		ginkgo.By("Trigger offline expansion of restored PVC")
		currentPvcSize = restoredPvc.Spec.Resources.Requests[v1.ResourceStorage]
		newSize = currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim2, err = expandPVCSize(restoredPvc, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim2).NotTo(gomega.BeNil())
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(pvclaim2.Name, newSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(b).To(gomega.BeTrue())

		ginkgo.By("Relocate volume from one datastore to another datastore using" +
			"CnsRelocateVolume API")
		dsRefDest := getDsMoRefFromURL(ctx, zonal2DsUrl)
		volumeID := restoredPv[0].Spec.CSI.VolumeHandle
		_, err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		e2eVSphere.verifyDatastoreMatch(volumeID, []string{zonal2DsUrl})

		ginkgo.By("Power off vm")
		vm = setVmPowerState(ctx, vmopC, vm, vmopv1.VirtualMachinePoweredOff)
		vm, err = wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("edit vm spec and remove pvc")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm.Spec.Volumes = vm.Spec.Volumes[:1]
		err = vmopC.Update(ctx, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Pod to attach using PVC from which VM is detached")
		_, err = createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Testcase-5 from Negative scenario
		Perform out of band operation on VMservice VM with attached PVCs.

		Relocate VM using VM API to the zone (both in the same namespace
		and with/without marked for removal of zone in the same namespace)
	*/

	ginkgo.It("Migrate VM with VM service vm", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// fetch shared vsphere datatsore url
		datastoreURL := os.Getenv("SHARED_VSPHERE_DATASTORE_URL")
		if datastoreURL == "" {
			ginkgo.Skip("Skipping the test because SHARED_VSPHERE_DATASTORE_URL is not set")
		} else {
			dsRef := getDsMoRefFromURL(ctx, datastoreURL)
			framework.Logf("dsmoId: %v", dsRef.Value)

			// read or create content library if it is empty
			if contentLibId == "" {
				contentLibId, err = createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
					dsRef.Value, &e2eVSphere)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		//Reading supervisor master credentials
		svcMasterIp := GetAndExpectStringEnvVar(svcMasterIP)
		svcMasterPwd := GetAndExpectStringEnvVar(svcMasterPassword)
		sshClientConfig := &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(svcMasterPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		// reading zonal storage policy of zone-1 and zone2
		storagePolicyNameZ1 := GetAndExpectStringEnvVar(envZonal1StoragePolicyName)
		storageProfileIdZ1 := e2eVSphere.GetSpbmPolicyID(storagePolicyNameZ1)
		storagePolicyNameZ2 := GetAndExpectStringEnvVar(envZonal2StoragePolicyName)
		storageProfileIdZ2 := e2eVSphere.GetSpbmPolicyID(storagePolicyNameZ2)

		ginkgo.By("Create a WCP namespace tagged to zone-1, zone-2 & zone-3")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedStorageProfileId, storageProfileIdZ1, storageProfileIdZ2}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1, zone2, zone3}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		storageClassNames := []string{storagePolicyNameZ1, sharedStoragePolicyName}
		storageClasses := make([]*storagev1.StorageClass, len(storageClassNames))
		for i, name := range storageClassNames {
			sc, err := client.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			storageClasses[i] = sc
		}
		zonal1Sc := storageClasses[0]
		sharedSc := storageClasses[1]

		ginkgo.By("Creating pvc with requested topology annotation set to zone2")
		pvclaim, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", sharedSc, "", zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume affinity annotation state")
		allowedTopologiesMap := convertToTopologyMap(allowedTopologies)
		err = verifyVolumeAnnotationAffinity(pvclaim, pvs[0], allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		ginkgo.By("Create vm service vm")
		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi := waitNGetVmiForImageName(ctx, vmopC, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())
		vm := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvclaim}, vmi, sharedSc.Name, secretName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Power on vm")
		vm = setVmPowerState(ctx, vmopC, vm, vmopv1.VirtualMachinePoweredOn)
		vm, err = wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Migrate VM to zone-3")
		zone3DsName := GetAndExpectStringEnvVar(envZone3DatastoreName)
		isMigrateSuccess, _ := migrateVmsFromDatastore(svcMasterIp, sshClientConfig, zone3DsName, []string{vm.Name},
			0)
		gomega.Expect(isMigrateSuccess).NotTo(gomega.BeTrue(), "Migration of vms Passed!")

		ginkgo.By("Mark zone-2 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace,
			zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating pvc with requested topology annotation set to zone1")
		pvclaim2, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", zonal1Sc, "", zone1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to be in bound state")
		pvs2, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim2}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume affinity annotation state")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, 0,
			1)
		allowedTopologiesMap = convertToTopologyMap(allowedTopologies)
		err = verifyVolumeAnnotationAffinity(pvclaim2, pvs2[0], allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vm service vm")
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvclaim2}, vmi, zonal1Sc.Name, secretName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Migrate VM to zone-2")
		zone2DsName := GetAndExpectStringEnvVar(envZone2DatastoreName)
		isMigrateSuccess2, _ := migrateVmsFromDatastore(svcMasterIp, sshClientConfig, zone2DsName, []string{vm2.Name},
			0)
		gomega.Expect(isMigrateSuccess2).NotTo(gomega.BeTrue(), "Migration of vms failed")
	})

	/*
		Testcase-21
		snapshot creation with zones addition and removal in between
		Test Steps:
		1. On a WCP namespace which is tagged to zone-1 and has zonal and shared storage policy added.
		2. Create multiple PVCs on a scale with RWO access modes and use both binding modes WFFC
		and Immediate for each different PVC
		3. Add new zone zone-2 to the namespace
		4. Create deployment pods using the PVC created above.
		4. Verify PVC reaches to Bound state.
		6. Verify PVC annotation and PV affinity details.
		7. Verify Pods reach the running state.
		8. Verify Pod node annotation.
		9. Mark zone-2 tag for removal.
		10. Add new zone zone-3 to the namespace.
		11. Take snapshot of few PVCs created above.
		12. Wait for the snapshot to get created successfully.
		13. Restore the snapshot created above to create new volumes
		and attach it to a new pods.
		14. Wait for restored volumes to reach the Bound state.
		15. Verify PVC annotation and PV affinity details.
		16. Wait for Pods to reach running state.
		17. Verify pod node annotation.
		18. Perform cleanup - delete pods, volumes and namespace.
	*/

	ginkgo.It("Snapshot creation with zones addition and removal", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		restoredPVCs := []*v1.PersistentVolumeClaim{}
		restoredPods := []*v1.Pod{}
		restoredVolHandles := []string{}
		pvs := []*v1.PersistentVolume{}

		// Read zone-1 storage policy "immediate and latebinding" for pvc creation
		zonalPolicyName := GetAndExpectStringEnvVar(envZonal1StoragePolicyName)
		zonalStorageProfileId := e2eVSphere.GetSpbmPolicyID(zonalPolicyName)
		zonalPolicyNameWffc := zonalPolicyName + "-latebinding"

		// Read shared storage policy for pvc creation
		sharedPolicyName := GetAndExpectStringEnvVar(envIsolationSharedStoragePolicyName)
		sharedStorageProfileId := e2eVSphere.GetSpbmPolicyID(sharedPolicyName)

		// Get corresponding storage classes
		ginkgo.By("Read storage policies tagged to WCP namespace")
		storageClassNames := []string{zonalPolicyNameWffc, sharedPolicyName}
		storageClasses := make([]*storagev1.StorageClass, len(storageClassNames))
		for i, name := range storageClassNames {
			sc, err := client.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			storageClasses[i] = sc
		}
		zonalStorageClass := storageClasses[0]
		sharedStorageClass := storageClasses[1]

		// Create WCP namespace
		ginkgo.By("Create a WCP namespace and tag it to zone-1")
		namespace, statuscode, err := createtWcpNsWithZonesAndPolicies(
			vcRestSessionId,
			[]string{zonalStorageProfileId, sharedStorageProfileId},
			getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1}, "", "",
		)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		// Create PVCs
		ginkgo.By("Create 2 PVCs with different binding modes")
		storageClassList := []*storagev1.StorageClass{zonalStorageClass, sharedStorageClass}
		pvcList := []*v1.PersistentVolumeClaim{}
		for _, sc := range storageClassList {
			pvc, err := createPVC(ctx, client, namespace, labelsMap, "", sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcList = append(pvcList, pvc)
		}
		defer func() {
			ginkgo.By("Delete PVCs")
			for _, pvc := range pvcList {
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Waiting for CNS volumes to be deleted")
			for _, pv := range pvs {
				err := e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Extend namespace to zone-2
		ginkgo.By("Add zone-2 to WCP namespace")
		err = addZoneToWcpNs(vcRestSessionId, namespace, topologyAffinityDetails[topologyCategories[0]][1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create deployments using PVCs
		ginkgo.By("Creating Deployments using PVCs")
		deployments := []*appsv1.Deployment{}
		depSpecs := [][]*v1.PersistentVolumeClaim{
			{pvcList[0], pvcList[1]},
			{pvcList[2]},
		}
		for _, pvcSet := range depSpecs {
			dep, err := createDeployment(ctx, client, 1, labelsMap, nil, namespace,
				pvcSet, execRWXCommandPod1, false, busyBoxImageOnGcr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			deployments = append(deployments, dep)
		}
		defer func() {
			ginkgo.By("Delete Deployments")
			for _, dep := range deployments {
				err := client.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Wait for PVCs to bound
		ginkgo.By("Wait for PVCs to be in bound state")
		pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvcList, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())

		// Verify deployment-based affinity annotations
		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity for Deployments")
		for _, dep := range deployments {
			err := verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, nil, dep, namespace, allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Modify namespace topology
		ginkgo.By("Mark zone-2 for removal from WCP namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace, topologyAffinityDetails[topologyCategories[0]][1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Add zone-3 to WCP namespace")
		err = addZoneToWcpNs(vcRestSessionId, namespace, topologyAffinityDetails[topologyCategories[0]][2])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create VolumeSnapshotClass
		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create snapshots
		var snapshots []*snapV1.VolumeSnapshot
		var snapshotContents []*snapV1.VolumeSnapshotContent
		var snapshotCreatedList, snapshotContentCreatedList []bool

		for i, pvc := range pvcList {
			ginkgo.By(fmt.Sprintf("Create dynamic volume snapshot for PVC %d", i+1))
			volHandle := pvs[i].Spec.CSI.VolumeHandle
			vs, vsc, snapCreated, vscCreated, _, _, err := createDynamicVolumeSnapshot(
				ctx, namespace, snapc, volumeSnapshotClass, pvc, volHandle, diskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			snapshots = append(snapshots, vs)
			snapshotContents = append(snapshotContents, vsc)
			snapshotCreatedList = append(snapshotCreatedList, snapCreated)
			snapshotContentCreatedList = append(snapshotContentCreatedList, vscCreated)
		}
		defer func() {
			ginkgo.By("Perform cleanup of snapshots")
			ginkgo.By("Delete Snapshots")
			for i := range snapshots {
				if snapshotCreatedList[i] {
					framework.Logf("Deleting volume snapshot %s", snapshots[i].Name)
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshots[i].Name, pandoraSyncWaitTime)

					framework.Logf("Waiting for snapshot content to be deleted: %s", snapshots[i].Name)
					err := waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(
						ctx, snapc, *snapshots[i].Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				if snapshotContentCreatedList[i] {
					err := deleteVolumeSnapshotContent(ctx, snapshotContents[i], snapc, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

			ginkgo.By("Delete restored volume snapshots")
			for i := range restoredPVCs {
				err := fpv.DeletePersistentVolumeClaim(ctx, client, restoredPVCs[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				err = e2eVSphere.waitForCNSVolumeToBeDeleted(restoredVolHandles[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Restore snapshots
		ginkgo.By("Restore volume snapshots and create pods from restored PVCs")
		for i := 1; i < len(snapshots); i++ {
			snapshot := snapshots[i]
			ginkgo.By(fmt.Sprintf("Restoring from snapshot %d: %s", i, snapshot.Name))
			pvc, pvs, pod := verifyVolumeRestoreOperation(ctx, client,
				namespace, storageClassList[i], snapshot, diskSize, true)
			gomega.Expect(len(pvs)).To(gomega.BeNumerically(">", 0))
			gomega.Expect(pod).NotTo(gomega.BeNil())

			volHandle := pvs[0].Spec.CSI.VolumeHandle
			if guestCluster {
				volHandle = getVolumeIDFromSupervisorCluster(volHandle)
			}
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

			restoredPVCs = append(restoredPVCs, pvc)
			restoredPods = append(restoredPods, pod)
			restoredVolHandles = append(restoredVolHandles, volHandle)
		}
		defer func() {
			ginkgo.By("Cleanup restored Pods")
			for i := range restoredPVCs {
				if restoredPods[i] != nil {
					err := fpod.DeletePodWithWait(ctx, client, restoredPods[i])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		// Verify pod-based affinity annotations
		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity for restored Pods")
		for _, pod := range restoredPods {
			err := verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, pod, nil, namespace, allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/* Testcase - 22
	   static volume
	   Test Steps:
	   1. Create a namespace add zone-2 to it and tag a zonal policy of zone-2
	   2. Create FCD with valid zonal policy. Make sure the storage policy has sufficient quota and note the FCD ID
	   3. Call CNSRegisterVolume API by specifying VolumeID, AccessMode set to "ReadWriteOnce” and PVC Name
	   4. Wait for some time to get the status of CRD Verify the CRD status should be successful.
	   5. CNS operator creates PV and PVC.
	   6. Verify Bidirectional reference between PV and PVC - validate volumeName,
	   storage class, PVC name, namespace and the size.
	   7. Verify PV and PVC’s are bound
	   8. Verify node affinity on PV.
	   9. Invoke CNS query API, to validate volume is registered in CNS and volume shows the PV PVC information
	   10. Create a Pod using the PVC created above.
	   11. Wait for Pod to reach running state.
	   12. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster.
	   13. Create a volume snapshot for the static PVC.
	   14. Verify snapshot created successfully.
	   15. Restore snapshot and create a pod
	   16. verify pod pvc affinty
	   17. Perform cleanup: delete pods, snapshot, volume and namespace
	*/

	ginkgo.It("Static volume creation using zonal2 policy", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// reading zonal storage policy of zone-2 workload domain
		storagePolicyName = GetAndExpectStringEnvVar(envZonal2StoragePolicyName)
		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)
		storageDatastoreUrlZone2 := GetAndExpectStringEnvVar(envZone2DatastoreUrl)

		ginkgo.By("Read zonal storage class of zone2")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4
			so topValStartIndex=1 and topValEndIndex=2 will fetch the 1st index value from topology map string
		*/
		topValStartIndex := 1
		topValEndIndex := 2

		ginkgo.By("Create a WCP namespace tagged to zone-2")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone2}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Create static volume")
		_, _, staticPvc, staticPv, err := createStaticVolumeOnSvc(ctx, client,
			namespace, storageDatastoreUrlZone2, storagePolicyName)
		defer func() {
			ginkgo.By("Delete PVC")
			err = fpv.DeletePersistentVolumeClaim(ctx, client, staticPvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(staticPv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating a Deployment using pvc")
		dep, err := createDeployment(ctx, client, 1, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{staticPvc}, execRWXCommandPod1, false, busyBoxImageOnGcr)
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

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, nil, dep, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			staticPvc, staticPv.Spec.CSI.VolumeHandle, diskSize, true)
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

		ginkgo.By("Restore volume snapshot and create pod using it")
		pvclaim2, persistentVolumes2, pod2 := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, true)
		volHandle2 := persistentVolumes2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Delete Dynamic snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, staticPv.Spec.CSI.VolumeHandle, snapshotId, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Verify workloads with same policy applicable to multiple zonal datastore.
		Test steps:
		1. Create a wcp namespace and tag zone-2 and zone-3 to it.
		Use storage policy which has all 3 zonal datastores of zone-2, zone-3 and zone-4
		2. Create multiple PVCs using storage policy tagged to wcp namespace.
		(Verify it using Immediate and WFFC Binding mode)
		3. Wait for PVC to reach Bound state.
		4. Verify PVC annotation and PV affinity details. It shouls show affinity of either zone-2, zone-3
		5. Create deployment Pods using the pvc created above.
		6. Verify Pods should reach running state.
		7. Verify Pod node annotation.
		8. Perform cleanup. Delete Pods, PVcs and ns.
	*/
	ginkgo.It("Workloads with all-zone zonal storage policy", ginkgo.Label(p0, wldi, snapshot, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// reading zonal storage policy
		zonalPolicy := GetAndExpectStringEnvVar(envZonalStoragePolicyName)
		zonalProfileId := e2eVSphere.GetSpbmPolicyID(zonalPolicy)

		// append late-binding now as it knowns to k8s and not to vc
		zonalPolicyWffc := zonalPolicy + "-latebinding"

		ginkgo.By("Creating wcp namespace tagged to any 2 zones")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(
			vcRestSessionId,
			[]string{zonalProfileId},
			getSvcId(vcRestSessionId, &e2eVSphere), []string{zone1, zone2}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read zonal class")
		storageclassImm, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		storageclassWffc, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicyWffc, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating pvc using WFFC and Immediate binding")
		pvclaim1, _, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclassImm, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a PVC using zonal policy-WFFC binding")
		pvclaim2, err := createPVC(ctx, client, namespace, labelsMap, diskSize, storageclassWffc, v1.ReadWriteOnce)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating a Deployment")
		dep, err := createDeployment(ctx, client, 1, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim1}, execRWXCommandPod1, false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		// startIndex=0 & endIndex=2 to set allowedTopologies to zone-1 & zone-2
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, 0,
			2)
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, nil, dep, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		dep, err = createDeployment(ctx, client, 1, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim2}, execRWXCommandPod1, false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC is in Bound state.")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, nil, dep, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})
})
