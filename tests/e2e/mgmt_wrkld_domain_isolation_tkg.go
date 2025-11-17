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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

var _ bool = ginkgo.Describe("[tkg-domain-isolation] TKG-Management-Workload-Domain-Isolation", func() {

	f := framework.NewDefaultFramework("tkg-domain-isolation")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		vcRestSessionId             string
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		replicas                    int32
		topologyAffinityDetails     map[string][]string
		topologyCategories          []string
		labelsMap                   map[string]string
		labels_ns                   map[string]string
		pandoraSyncWaitTime         int
		snapc                       *snapclient.Clientset
		err                         error
		zone2                       string
		zone3                       string
		zone1                       string
		sharedStoragePolicyName     string
		zonal2StroragePolicyName    string
		sharedStoragePolicyNameWffc string
		svcNamespace                string
		guestClusterRestConfig      *restclient.Config
		topkeyStartIndex            int
	)

	ginkgo.BeforeEach(func() {
		namespace = getNamespaceToRunTests(f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// making vc connection
		client = f.ClientSet
		bootstrap()

		// reading vc session id
		if vcRestSessionId == "" {
			vcRestSessionId = createVcSession4RestApis(ctx)
		}

		// reading topology map set for management domain and workload domain
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

		// reading shared storage policy
		sharedStoragePolicyName = GetAndExpectStringEnvVar(envIsolationSharedStoragePolicyName)
		sharedStoragePolicyNameWffc = GetAndExpectStringEnvVar(envIsolationSharedStoragePolicyNameLateBidning)

		// reading zonal storage policy
		zonal2StroragePolicyName = GetAndExpectStringEnvVar(envZonal2StoragePolicyName)

		svcNamespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)

		// Get snapshot client using the rest config
		guestClusterRestConfig = getRestConfigClientForGuestCluster(guestClusterRestConfig)
		snapc, err = snapclient.NewForConfig(guestClusterRestConfig)
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
		TKG - Testcase-4
		Dynamic and Pre-Provisioned snapshot creation with removal of zones from the namespace

		Test Steps:
		1. The expectation is that TKG worker nodes are spread across zones (zone-2, zone-3 and zone-4)
		2. Create statefulset with replica count 3 and affinity set
		such that each volume,
		pod should comeup on each worker node.
		3. Now, Mark zone-3 for removal
		4. Increase the replica count from 3 to 6.
		5. Verify if newly created pvcs,pod reach Bound or running state.
		6. Now, take a volume snaphot of any 2 statefulset volumes.
		7. Verify snapshot created successfully.
		8. Create a static snapshot of any 1 snapshot created above.
		9. Verify static snapshot on tkg created successfully.
		10. Perform scaling operation. Increase replica count to 8
		11. Verify scaling operation went smooth.
		12. Perfrom cleanup: Delete Pods, volumes.
	*/

	ginkgo.It("[pq-vks-wldi] Dynamic and Pre-Provisioned snapshot creation with removal of zones from the "+
		"namespace", ginkgo.Label(p0, wldi, snapshot, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

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
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, true, allowedTopologies,
			true, true, "", "", storageclass, storageclass.Name)
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Mark zone-2 for removal SVC namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, svcNamespace,
			zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Increase the replica count from 3 to 6.
		replicas = 6
		ginkgo.By("Increase the replica count to 6 when a zone is marked for removal")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, replicas,
			0, statefulset, true, namespace,
			allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Now, take a volume snaphot of any 2 statefulset volumes.
		framework.Logf("Fetching pod 1, pvc1 and pv1 details")
		ssPods, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPods.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// pod1 details
		pod1, err := client.CoreV1().Pods(namespace).Get(ctx,
			ssPods.Items[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// pvc1 details
		pvc1 := pod1.Spec.Volumes[0].PersistentVolumeClaim
		pvclaim1, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
			pvc1.ClaimName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// pv1 details
		pv1 := getPvFromClaim(client, statefulset.Namespace, pvc1.ClaimName)
		volHandle1 := pv1.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle1).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle1 = getVolumeIDFromSupervisorCluster(volHandle1)
		}

		framework.Logf("Fetching pod 2, pvc2 and pv2 details")
		// pod2 details
		pod2, err := client.CoreV1().Pods(namespace).Get(ctx,
			ssPods.Items[1].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// pvc2 details
		pvc2 := pod2.Spec.Volumes[0].PersistentVolumeClaim
		pvclaim2, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
			pvc2.ClaimName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// pv2 details
		pv2 := getPvFromClaim(client, statefulset.Namespace, pvc2.ClaimName)
		volHandle2 := pv2.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a volume snapshot - 1")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvclaim1, volHandle1, diskSize1GB, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot - 2")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvclaim2, volHandle2, diskSize1GB, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Now mark zone-3 also for removal.
		statusCode := markZoneForRemovalFromNs(svcNamespace, zone3, vcRestSessionId)
		gomega.Expect(statusCode).Should(gomega.BeNumerically("==", status_code_failure))

		framework.Logf("Get volume snapshot handle from Supervisor Cluster")
		_, _, svcVolumeSnapshotName, err := getSnapshotHandleFromSupervisorCluster(ctx,
			*snapshotContent2.Status.SnapshotHandle)

		// Create a static snapshot of volumesnapshot2.
		ginkgo.By("Create a static volume snapshot by snapshotcontent2")
		ginkgo.By("Create pre-provisioned snapshot")
		_, staticSnapshot, staticSnapshotContentCreated,
			staticSnapshotCreated, err := createPreProvisionedSnapshotInGuestCluster(ctx, volumeSnapshot2, snapshotContent2,
			snapc, namespace, pandoraSyncWaitTime, svcVolumeSnapshotName, diskSize1GB)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if staticSnapshotCreated {
				framework.Logf("Deleting static volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, staticSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*staticSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if staticSnapshotContentCreated {
				framework.Logf("Deleting static volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*staticSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *staticSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Increase the replica count from 3 to 8.
		ginkgo.By("Increase the replica count to 8")
		replicas = 8
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, replicas,
			0, statefulset, true, namespace,
			allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TKG - Testcase-1
		Create a workload using a zonal policy of zone-1 and Immediate Binding mode

		Test Steps:
		1. Create a STS with 3 replicas, using the zonal SP which is compatible only with zone-2 with Immediate Binding mode.
		2. Wait for the StatefulSet PVCs to reach the "Bound" state and the StatefulSet Pods to reach the "Running" state.
		3. Verify the StatefulSet PVC annotations and the PVs affinity details.
		4. Verify the StatefulSet Pod's node annotation.
		5. Perform cleanup by deleting the Pods, Volumes, and Namespace.
	*/

	ginkgo.It("[pq-vks-wldi] Statefulset creation with zonal policy", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

		ginkgo.By("Read zonal storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonal2StroragePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset")
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, true, allowedTopologies,
			true, true, "", "", storageclass, storageclass.Name)
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TKG - Testcase-2
		Create a workload by setting requested allowed topology.

		Test Steps:
		1. Create a PVC using a shared storage policy and set the requested allowed topology to zone-4 & WFFC binding mode.
		2. Wait for PVC to reach Bound state.
		3. Create a new PVC and set the requested allowed topology to zone-3.
		4. Wait for PVC to reach Bound state.
		5. Verify PVCs annotation and PV affinity. It should show the requested allowed topology details.
		6. Create standalone Pods for each created PVC.
		7. Verify Pod node annotation.
		8. Perform cleanup by deleting the Pods, Volumes, and Namespace.
	*/

	ginkgo.It("[pq-vks-wldi] Workload creation by setting requested allowed topology", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Read shared-latebinding storage policy tagged to wcp namespace")
		spWffc := sharedStoragePolicyNameWffc
		storageclassWffc, err := client.StorageV1().StorageClasses().Get(ctx, spWffc, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, sharedStoragePolicyNameWffc, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating pvc with requested topology annotation set to zone4")
		pvclaim1, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclassWffc, "", zone3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating another pvc with requested topology annotation set to zone3")
		pvclaim2, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclass, "", zone3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to reach Bound state.")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Pod to attach to Pvc-1")
		pod1, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, pod1, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Pod to attach to Pvc-2")
		pod2, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, pod2, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TKG - Testcase-6
		Create a statefulset with Node Selector Terms.

		Test Steps:
		1. Create a StatefulSet with 3 replicas, using the storage policy
		2. Specify node selector term specific to zone-3 for Pod creation.
		3. Wait for the StatefulSet PVCs to reach the "Bound" state and the StatefulSet Pods to reach the "Running" state.
		4. Verify the StatefulSet PVC annotations and the PVs affinity details. 5. It should show zone-3 topology
		6. Verify the StatefulSet Pod's node annotation. All Pods should come up on zone 3
		7. Perform cleanup by deleting the Pods, Volumes, and Namespace.
	*/

	ginkgo.It("[pq-vks-wldi] Create a statefulset with Node Selector Terms.", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

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

		framework.Logf("Create StatefulSet with node selector set to zone-2")
		topValStartIndex := 1
		topValEndIndex := 2
		allowedTopologiesZ2 := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Creating statefulset")
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, true, allowedTopologiesZ2,
			false, true, "", "", storageclass, sharedStoragePolicyName)
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		// PV will have all 3 zones, but pod will be on zone-2
		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulset, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		TKG - Testcase-7
		Create a statefulset with Node Selector Terms

		Test Steps:
		1. Create a StatefulSet with 3 replicas, using the storage policy and configuring WFFC Binding mode.
		2. Specify node selector term specific to zone-3 for Pod creation.
		3. Wait for the StatefulSet PVCs to reach the "Bound" state and the StatefulSet Pods to reach the "Running" state.
		4. Verify the StatefulSet PVC annotations and the PVs affinity details. It should show zone-3 topology
		5. Verify the StatefulSet Pod's node annotation. All Pods should come up on zone-3
		6. Perform cleanup by deleting the Pods, Volumes, and Namespace.
	*/
	ginkgo.It("[pq-vks-wldi] Create a statefulset with Node Selector Terms and WFFC binding", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

		ginkgo.By("Read shared storage policy tagged to wcp namespace")
		spWffc := zonal2StroragePolicyName + "-latebinding"
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, spWffc, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		framework.Logf("Create StatefulSet with node selector set to zone-2")
		topValStartIndex := 1
		topValEndIndex := 2
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Creating statefulset")
		statefulset := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, true, allowedTopologies,
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
		TKG - Testcase-8
		Create a RWX volume

		Test Steps:
		1. Assign the zonal policy of zone-1 and zone-2 to tkg namespace.
		2. Create RWX PVC by specifying the requested allowed topology to be zone-1
		3. Wait for PVC to reach Bound state.
		4. Verify PVCs annotation and PV topology affinity details.
		5. Create deployment pods with replica count 3 using pvc created above.
		6. Verify Pod node annotation.
		7. Perform cleanup by deleting the Pods, Volumes, and Namespace.
	*/
	ginkgo.It("Create a RWX volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		zonal1And2StroragePolicyName := GetAndExpectStringEnvVar(envZonal1And2PolicyName)
		zonal1And2Sc, err := client.StorageV1().StorageClasses().Get(ctx, zonal1And2StroragePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating RWX- pvc with requested topology annotation set to zone2")
		pvclaim1, err := createPvcWithRequestedTopology(
			ctx,
			client,
			namespace,
			nil,
			"",
			zonal1And2Sc,
			v1.ReadWriteMany,
			zone1,
		)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to reach Bound state.")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating a Deployment using pvc-1")
		dep, err := createDeployment(ctx, client, 3, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim1}, execRWXCommandPod1, false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		topValStartIndex := 0
		topValEndIndex := 1
		allowedTopologiesZ2 := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Verify pod node attachment")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, nil, dep, namespace,
			allowedTopologiesZ2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		TKG - Testcase-15
		Verify dynamic volume provisioning works and pods can use the file volume

		Test Steps:
		1. On a GC namespace where each worker node is spread across 3 zones.
		(zone1, zone2, zone3 ex), add a shared vSAN storage policy.
		where File share is enabled on all 3 zones.
		2. Create a PVC-1 with "ReadWriteMany" access mode using the storage policy tagged to gc ns
		3. Create a PVC-2 with "ReadOnlyMany" access mode using the storage policy tagged to gc ns
		4. Wait for PVC-1 and PVC-2 to be bound in GC
		5. Verify PVC annotation and PV affinity
		6. Create a deployment Pod using PVC-1 and PVC-2.
		created above at a mount paths path1 and path2 + {readOnly : true} specified in PodSpec
		7. Verify Pod is in the Running phase
		8. Create a file (file1.txt) at the mount path path1. Check if the creation is successful
		9. Read the file (file1.txt) created above from the mount path path1. Check if the read is successful
		10. Create a file (file2.txt) at the mount path path2. Check if the creation is unsuccessful
		11. Delete the Pod
		12. Verify if Pod is successfully deleted from GC
		13. Delete PVCs in GC
		14. Verify if PVCs and PVs also deleted in the SV cluster and GC
	*/
	ginkgo.It("Verify dynamic volume provisioning works and pods can use the file volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedVsanPolicy := GetAndExpectStringEnvVar(envSharedZone1Zone2Zone3StoragePolicyName)
		sharedVsanStorageClass, err := client.StorageV1().StorageClasses().Get(ctx, sharedVsanPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create a PVC-1 with RWX access mode using the storage policy tagged to gc ns")
		pvc1, err := createPVC(ctx, client, namespace, labelsMap, "", sharedVsanStorageClass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to reach Bound state.")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a PVC-2 with ReadOnlyMany access mode using the storage policy tagged to gc ns")
		pvc2, err := createPVC(ctx, client, namespace, labelsMap, "", sharedVsanStorageClass, v1.ReadOnlyMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to reach Bound state.")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a Pod to use the PVCs created above
		ginkgo.By("Creating pod to attach PV to the node")
		pod := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvc1, pvc2},
			admissionapi.LevelBaseline, execRWXCommandPod1)

		pod.Spec.Containers[0].VolumeMounts[0] = v1.VolumeMount{Name: "volume1", MountPath: "/mnt/" + "volume1"}
		pod.Spec.Volumes[0] = v1.Volume{Name: "volume1", VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc1.Name, ReadOnly: false}}}

		pod.Spec.Containers[0].VolumeMounts[1] = v1.VolumeMount{Name: "volume2", MountPath: "/mnt/" + "volume2"}
		pod.Spec.Volumes[1] = v1.Volume{Name: "volume2", VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc2.Name, ReadOnly: true}}}

		pod, err = client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify pod node attachment")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, pod, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

})
