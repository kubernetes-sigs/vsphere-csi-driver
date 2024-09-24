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
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
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

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
)

var _ = ginkgo.Describe("[topology-snapshot] Topology-Snapshot", func() {
	f := framework.NewDefaultFramework("topology-snapshot")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                  clientset.Interface
		namespace               string
		scParameters            map[string]string
		datastoreURL            string
		defaultDatacenter       *object.Datacenter
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		restConfig              *restclient.Config
		snapc                   *snapclient.Clientset
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		topologyLength          int
		leafNode                int
		leafNodeTag1            int
		leafNodeTag2            int
		pandoraSyncWaitTime     int
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		//Get snapshot client using the rest config
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		topologyLength, leafNode, _, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2

		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)

		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)

		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		for _, dc := range datacenters {
			defaultDatacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
	})

	/*
	   On a Topology enabled testbed , create snapshot on dynamic PVC

	   1. Create SC with Immediate binding mode and allowed topologies set to
	      region1 > zone1 > building1 > level1 > rack > rack3 in the SC
	   2. Create PVC-1 with the above SC
	   3. Wait for PVC-1 to reach Bound state.
	   4. Create a VolumeSnapshot class.
	   5. Create a volume-snapshot with labels, using the above snapshot-class and pvc-1
	   6. Use the same snapshot to create PVC-2
	   7. Wait for PVC-2 to reach Bound state.
	   8. Create POD using PVC-2
	   9. Describe PV-2 and verify node affinity details, and verify POD should come up on same node as mentioned in PV2
	   10. Perform Cleanup. Delete Snapshot, Pod, PVC, SC, volume-snapshot and VolumeSnapshot class.
	*/
	ginkgo.It("On a topology enabled testbed , create snapshot on dynamic PVC", ginkgo.Label(p0,
		block, vanilla, level5, snapshot, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false

		/* Get allowed topologies for Storage Class
		(region1 > zone1 > building1 > level1 > rack > rack3) */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)

		// create SC and PVC
		storagePolicyName := GetAndExpectStringEnvVar(storagePolicyForDatastoreSpecificToCluster)
		scParameters["storagepolicyname"] = storagePolicyName

		ginkgo.By("Create StorageClass and PVC")
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace, nil,
			scParameters, diskSize, allowedTopologyForSC, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PVC to be in Bound phase
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)
		snapshotCreated := true

		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		gomega.Expect(*snapshotContent.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle := *snapshotContent.Status.SnapshotHandle
		snapshotId := strings.Split(snapshothandle, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotContent.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podName := pod.GetName
		framework.Logf("podName: %s", podName)

		/* Verify PV node affinity and that the PODS are running on appropriate node as
		specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				volHandle2, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle2,
					pod.Spec.NodeName))
		}()

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Deleting volume snapshot Again to check Not found error")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
	})

	/*
		Topology Snapshot workflow for statefulset

		1. Create a statefulset with 3 replicas using a storageclass with
			 volumeBindingMode set to Immediate and multiple labels
		2. Wait for pvcs to be in Bound state
		3. Wait for pods to be in Running state
		4. Create snapshot on 3rd replica's pvc (pvc3 as source)
		5. Create PVC from the snapshot, use the name similar to PVC that get created with the statefulset
		6. Scale up the statefulset to 4 and wait for the scaleup to complete
		7. Verify if the new pod attaches to the PV created in step-6
		8. Verify the node affinity details of all PV's and validate POD is up on appropriate node
		9. Cleanup the sts and the snapshot  PVC's
	*/
	ginkgo.It("Topology Snapshot workflow for statefulset", ginkgo.Label(p0,
		block, vanilla, level5, snapshot, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		var snapshotContentCreated = false
		var snapshotCreated = false

		ginkgo.By("Create storage class and PVC")
		sharedDataStoreUrlBetweenClusters := GetAndExpectStringEnvVar(datstoreSharedBetweenClusters)
		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > (rack2 and rack3)
		Taking Shared datstore between Rack2 and Rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1, leafNodeTag2)

		scParameters["datastoreurl"] = sharedDataStoreUrlBetweenClusters
		storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC,
			"", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Creating StatefulSet with replica count 2 using default pod management policy
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Wait for StatefulSet pods to be in up and running state
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Verify PV node affinity and that the PODS are running on appropriate nodes
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Fetching pod 3, pvc3 and pv3 details")
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

		// Verify the attached volume match the one in CNS cache
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv3.Spec.CSI.VolumeHandle,
			pvc3.ClaimName, pv3.ObjectMeta.Name, pod3.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a volume snapshot - 3")
		volumeSnapshot3, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim3.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot3.Name)

		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot3.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot3.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot3.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot3, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot3.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = true
		gomega.Expect(volumeSnapshot3.Status.RestoreSize.Cmp(resource.MustParse("1Gi"))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent3, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot3.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		gomega.Expect(*snapshotContent3.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle3 := *snapshotContent3.Status.SnapshotHandle
		snapshotId2 := strings.Split(snapshothandle3, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle3, snapshotId2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, "1Gi", storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot3.Name, snapshotapigroup)
		pvcSpec.Name = "www-web-3"
		pvclaimNew, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expecting the volume to bound")
		newPV, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaimNew},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volHandleOfNewPV := newPV[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandleOfNewPV).NotTo(gomega.BeEmpty())

		replicas += 1
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		time.Sleep(5 * time.Minute)
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)

		/* Verify newly created PV node affinity and that the new PODS are running on
		appropriate node as specified in the allowed topologies of SC */
		ginkgo.By("Verify newly created PV node affinity and that the new PODS are running " +
			"on appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot3.Name, pandoraSyncWaitTime)
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot content  is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot3.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot  entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle3, snapshotId2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

})
