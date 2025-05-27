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
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

var _ bool = ginkgo.Describe("[tkg-domain-isolation] TKG-Management-Workload-Domain-Isolation", func() {

	f := framework.NewDefaultFramework("tkg-domain-isolation")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                  clientset.Interface
		namespace               string
		vcRestSessionId         string
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		replicas                int32
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		labelsMap               map[string]string
		labels_ns               map[string]string
		pandoraSyncWaitTime     int
		snapc                   *snapclient.Clientset
		err                     error
		zone2                   string
		zone3                   string
		sharedStoragePolicyName string
		svcNamespace            string
		guestClusterRestConfig  *restclient.Config
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
		zone2 = topologyAffinityDetails[topologyCategories[0]][1]
		zone3 = topologyAffinityDetails[topologyCategories[0]][2]

		// reading shared storage policy
		sharedStoragePolicyName = GetAndExpectStringEnvVar(envIsolationSharedStoragePolicyName)

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

	ginkgo.It("Dynamic and Pre-Provisioned snapshot creation with removal of zones from the namespace", ginkgo.Label(
		p0, wldi, snapshot, vc90), func() {
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
			volumeSnapshotClass, pvclaim1, volHandle1, diskSize, false)
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
			volumeSnapshotClass, pvclaim2, volHandle2, diskSize, false)
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
			snapc, namespace, pandoraSyncWaitTime, svcVolumeSnapshotName, diskSize)
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
})
