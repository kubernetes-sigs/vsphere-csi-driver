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
	"github.com/vmware/govmomi/object"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
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

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
)

var _ = ginkgo.Describe("[Preferential-Topology-Snapshot] Preferential Topology Volume Snapshot tests", func() {
	f := framework.NewDefaultFramework("preferential-topology-volume-snapshot")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                         clientset.Interface
		namespace                      string
		allowedTopologies              []v1.TopologySelectorLabelRequirement
		restConfig                     *restclient.Config
		snapc                          *snapclient.Clientset
		topologyAffinityDetails        map[string][]string
		topologyCategories             []string
		topologyLength                 int
		leafNode                       int
		leafNodeTag1                   int
		leafNodeTag2                   int
		leafNodeTag0                   int
		preferredDatastoreChosen       int
		shareddatastoreListMap         map[string]string
		nonShareddatastoreListMapRack1 map[string]string
		nonShareddatastoreListMapRack2 map[string]string
		nonShareddatastoreListMapRack3 map[string]string
		allMasterIps                   []string
		masterIp                       string
		dataCenters                    []*object.Datacenter
		clusters                       []string
		preferredDatastorePaths        []string
		allowedTopologyRacks           []string
		allowedTopologyForRack1        []v1.TopologySelectorLabelRequirement
		allowedTopologyForRack2        []v1.TopologySelectorLabelRequirement
		allowedTopologyForRack3        []v1.TopologySelectorLabelRequirement
		rack1DatastoreListMap          map[string]string
		rack2DatastoreListMap          map[string]string
		rack3DatastoreListMap          map[string]string
		allDatastoresListMap           map[string]string
		storageclass                   *storagev1.StorageClass
		err                            error
		snapshotContentCreated         bool
		snapshotCreated                bool
		pandoraSyncWaitTime            int
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		//Get snapshot client using the rest config
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		topologyLength, leafNode, leafNodeTag0, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2

		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap,
			topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)

		// fetching k8s master ip
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]

		// fetching datacenter details
		dataCenters, err = e2eVSphere.getAllDatacenters(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching cluster details
		clusters, err = getTopologyLevel5ClusterGroupNames(masterIp, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap,
			topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)

		// fetching list of datatstores shared between vm's
		shareddatastoreListMap, err = getListOfSharedDatastoresBetweenVMs(masterIp, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Fetching list of datastores available in different racks")
		rack1DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rack2DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rack3DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, clusters[2])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Fetching list of datastores which is specific to each rack")
		nonShareddatastoreListMapRack1 = getNonSharedDatastoresInCluster(rack1DatastoreListMap,
			shareddatastoreListMap)
		nonShareddatastoreListMapRack2 = getNonSharedDatastoresInCluster(rack2DatastoreListMap,
			shareddatastoreListMap)
		nonShareddatastoreListMapRack3 = getNonSharedDatastoresInCluster(rack3DatastoreListMap,
			shareddatastoreListMap)

		// Get different allowed topologies required for creating Storage Class
		allowedTopologyForRack1 = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)
		allowedTopologyForRack2 = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1)
		allowedTopologyForRack3 = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)

		allowedTopologyRacks = nil
		// fetching cluster topology racks level
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)
		for i := 4; i < len(allowedTopologyForSC); i++ {
			for j := 0; j < len(allowedTopologyForSC[i].Values); j++ {
				allowedTopologyRacks = append(allowedTopologyRacks, allowedTopologyForSC[i].Values[j])
			}
		}
		// fetching list of datatstores available in a testbed
		allDatastoresListMap, err = getListOfAvailableDatastores(masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// // set preferred datatsore time interval
		// setPreferredDatastoreTimeInterval(client, ctx, csiNamespace, namespace, csiReplicas)
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
		framework.Logf("Perform preferred datastore tags cleanup after test completion")
		err = deleteTagCreatedForPreferredDatastore(masterIp, allowedTopologyRacks)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Recreate preferred datastore tags post cleanup")
		err = createTagForPreferredDatastore(masterIp, allowedTopologyRacks)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

	})

	/*
		Snapshot Testcase-1
		Create/Restore Snapshot of PVC single datastore preference

		Steps
		1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
		specific to rack-1 (ex- NFS-1)
		2. Create SC with Immediate binding mode and allowed topologies set to
			region1 > zone1 > building1 > level1 > rack > rack1 in the SC
		3. Create PVC-1 with the above SC
		4. Wait for PVC-1 to reach Bound state.
		5. Create a VolumeSnapshot class.
		6. Create a volume-snapshot with labels, using the above snapshot-class and pvc-1
		7. Use the same snapshot to create PVC-2
		8. Wait for PVC-2 to reach Bound state.
		9. Create POD using PVC-2
		10. Verify volume should be provisioned on the preferred datastore.
		11. Describe PV-2 and verify node affinity details, and verify POD should come up on same node as
		mentioned in PV2
		12. Perform Cleanup. Delete Snapshot, Pod, PVC, SC, volume-snapshot and VolumeSnapshot class.
		13.Remove datastore preference tags as part of cleanup.
	*/

	ginkgo.It("Create restore snapshot of pvc using single datastore preference", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvclaim *v1.PersistentVolumeClaim
		var snapshotContentCreated = false
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in rack-1(cluster-1))")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
				allowedTopologyRacks[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Create StorageClass and PVC")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
			nil, diskSize, allowedTopologyForRack1, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PVC to be in Bound phase
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
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
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podName := pod.GetName
		framework.Logf("podName: %s", podName)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				volHandle2, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle2,
					pod.Spec.NodeName))
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack1)

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies)

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Deleting volume snapshot Again to check Not found error")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		time.Sleep(40 * time.Second)
	})

	/*
		Snapshot Testcase-2
		Create/Restore Snapshot of PVC when datastore preference gets changed

		Steps
		1. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore specific
		to rack-2 (ex- NFS-2)
		2. Create SC with Immediate binding mode and allowed topologies set to rack-2 in the SC
		3. Create PVC-1 with the above SC
		4. Wait for PVC-1 to reach Bound state.
		5. Create SnapshotClass, Snapshot of PVC-1
		6. Verify snapshot state. It should be in ready-to-use state.
		7. Change datastore preference(ex- from NFS-2 to vSAN-2)
		8. Restore snapshot to create PVC-2
		9. PVC-2 should get stuck in Pending state and proper error message should be displayed.
		10. Perform Cleanup. Delete Snapshot, Pod, PVC, SC, volume-snapshot and VolumeSnapshot class.
		11. Remove datastore preference tags as part of cleanup.
	*/

	ginkgo.It("Create restore snapshot of pvc when datastore preference gets changed", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvclaim *v1.PersistentVolumeClaim
		snapshotContentCreated = false
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in rack-2(cluster-2))")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Create StorageClass and PVC")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
			nil, diskSize, allowedTopologyForRack2, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PVC to be in Bound phase
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
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

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		defer func() {
			ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
			err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Wait till the volume snapshot is deleted")
			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify snapshot entry is deleted from CNS")
			err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Deleting volume snapshot Again to check Not found error")
			err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			time.Sleep(40 * time.Second)
		}()

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0], allowedTopologyRacks[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tag new datastore chosen for volume provisioning")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, preferredDatastorePaths)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
				allowedTopologyRacks[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)
		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to fail provisioning volume within the topology")
		framework.ExpectError(fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound,
			client, pvclaim2.Namespace, pvclaim2.Name, pollTimeoutShort, framework.PollShortTimeout))
		// Get the event list and verify if it contains expected error message
		eventList, _ := client.CoreV1().Events(pvclaim2.Namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(eventList.Items).NotTo(gomega.BeEmpty())
		expectedErrMsg := "failed to get the compatible datastore for create volume from snapshot"
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		Snapshot Testcase-3
		Create/Restore Snapshot of PVC when multiple datastores are tagged and datastore preference is changed

		Steps
		1. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore specific
		to rack-1
		2. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore specific
		to rack-2
		3. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to datastore specific
		to rack-3
		4. Assign Tag rack-1, rack-2, rack-3, Category "cns.vmware.topology-preferred-datastores" to datastore
		shared across all racks (ex- sharedVMFS-12)
		5. Create SC with Immediate binding mode and allowed topologies set to all racks in the SC
		6. Create PVC-1, PVC-2 with the above SC
		7. Wait for all PVC's to reach Bound state.
		8. Describe PV's and verify node affinity details should contain proper node affinity details.
		9. Verify volume should be provisioned on the selected preferred datastores.
		10. Create 2 Pods from above created PVC's.
		11. Make sure POD is running on the same node as mentioned in the node affinity details.
		12. Create SnapshotClass, Snapshot of PVC-1
		13. Verify snapshot state. It should be in ready-to-use state.
		14. Snapshot should get created on the preferred datastore.
		15. Restore snapshot to create new PVC PVC-3.
		16. Wait for PVC-3 to reach Bound state.
		17. Describe PV-3 and verify node affinity details, it should show proper node affnity details.
		18. Verify volume should be provisioned on the preferred datastore where PVC-2 was created.
		19. Create Pod from restored PVC-3.
		20. Make sure POD is running on the same node as mentioned in the node affinity details.
		21. Remove previous tags and Change datatsore preference.
		22. Assign Tag rack-1, Category "cns.vmware.topology-preferred-datastores" to datastore
		specific to rack-1
		23. Assign Tag rack-2, Category "cns.vmware.topology-preferred-datastores" to datastore specific
		to rack-2
		24. Assign Tag rack-3', Category "cns.vmware.topology-preferred-datastores" to datastore specific to
		rack-3
		24. Create new PVC-4 with the above SC
		25. Wait for PVC to reach Bound state.
		26. Describe PV's and verify node affinity details should contain proper node affinity details.
		27. Verify volume should be provisioned on the selected preferred datastores.
		28. Create Pod from above created PVC-4.
		29. Make sure POD is running on the same node as mentioned in the node affinity details.
		30. Create SnapshotClass, Snapshot of PVC-4
		31. Verify snapshot state. It should be in ready-to-use state.
		32. Restore snapshot to create PVC-5.
		33. Wait for PVC-5 to reach Bound state.
		34. Describe PV and verify node affinity details, it should contain proper node affinity details.
		35. Verify volume should be provisioned on the preferred datastore.
		36. Create Pod from restored PVC-5.
		37. Make sure POD is running on the same node as mentioned in the node affinity details.
		38. Perform Cleanup. Delete PVC, Pod, SC
		39. Remove datastore preference tags as part of cleanup.
	*/

	ginkgo.It("Create restore snapshot of pvc when multiple preferred datastores are tagged "+
		"and datastore preference is changed", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning")
		preferredDatastorePath1, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePath1[0],
				allowedTopologyRacks[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		preferredDatastorePath2, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePath2[0],
				allowedTopologyRacks[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		preferredDatastorePath3, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, nonShareddatastoreListMapRack3, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePath3[0],
				allowedTopologyRacks[2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		sharedPreferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, shareddatastoreListMap, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 1; i < len(allowedTopologyRacks); i++ {
			err = tagSameDatastoreAsPreferenceToDifferentRacks(masterIp, allowedTopologyRacks[i],
				preferredDatastoreChosen, sharedPreferredDatastorePaths)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastorePath1...)
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastorePath2...)
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastorePath3...)
		preferredDatastorePaths = append(preferredDatastorePaths, sharedPreferredDatastorePaths...)
		defer func() {
			for i := 0; i < len(allowedTopologyRacks); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, sharedPreferredDatastorePaths[0],
					allowedTopologyRacks[i])
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v time for preferred datatsore to get refreshed in the"+
			"environment", preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating Storage Class")
		storageclass, err = createStorageClass(client, nil, allowedTopologies, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating PVC-1")
		pvclaim1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim1)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs1, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims1, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs1).NotTo(gomega.BeEmpty())
		pv1 := pvs1[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", pv1.Spec.CSI.VolumeHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(pv1.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(pv1.Spec.CSI.VolumeHandle))

		ginkgo.By("Creating Pod-1")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv1.Spec.CSI.VolumeHandle, pod1.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv1.Spec.CSI.VolumeHandle,
					pod1.Spec.NodeName))
		}()

		ginkgo.By("Creating PVC-2")
		pvclaim2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvclaim2)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs2, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs2).NotTo(gomega.BeEmpty())
		pv2 := pvs2[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			err = fpv.WaitForPersistentVolumeDeleted(client, pv2.Name, poll, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv2.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", pv2.Spec.CSI.VolumeHandle))
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", pv2.Spec.CSI.VolumeHandle))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(pv2.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(pv2.Spec.CSI.VolumeHandle))

		ginkgo.By("Creating Pod-2")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv2.Spec.CSI.VolumeHandle, pod2.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv2.Spec.CSI.VolumeHandle,
					pod2.Spec.NodeName))
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod1, namespace, preferredDatastorePaths,
			allDatastoresListMap)

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod1, namespace,
			allowedTopologies)

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod2, namespace, preferredDatastorePaths,
			allDatastoresListMap)

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod2, namespace,
			allowedTopologies)

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
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim1.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(*snapshotContent.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle := *snapshotContent.Status.SnapshotHandle
		snapshotId := strings.Split(snapshothandle, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(pv1.Spec.CSI.VolumeHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotContent.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
			err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Wait till the volume snapshot is deleted")
			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify snapshot entry is deleted from CNS")
			err = verifySnapshotIsDeletedInCNS(pv1.Spec.CSI.VolumeHandle, snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Deleting volume snapshot Again to check Not found error")
			err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			time.Sleep(40 * time.Second)
		}()

		ginkgo.By("Create PVC-3 from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)
		pvclaim3, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes3, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim3}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle3 := persistentvolumes3[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle3).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify snapshot entry is deleted from CNS")
			err = verifySnapshotIsDeletedInCNS(volHandle3, snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pod-3")
		pod3, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podName := pod3.GetName
		framework.Logf("podName: %s", podName)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod3.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				volHandle3, pod3.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle3,
					pod3.Spec.NodeName))
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod3, namespace, preferredDatastorePaths,
			allDatastoresListMap)

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod3, namespace,
			allowedTopologies)

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePath1[0], allowedTopologyRacks[0])
		detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePath2[0], allowedTopologyRacks[1])
		detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePath3[0], allowedTopologyRacks[2])

		// new preference tags
		ginkgo.By("Assign new preferred datastore tags")
		preferredDatastorePathRack1, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[0],
			preferredDatastoreChosen, nonShareddatastoreListMapRack1, preferredDatastorePath1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePathRack1[0],
				allowedTopologyRacks[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		preferredDatastorePathRack2, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[1],
			preferredDatastoreChosen, nonShareddatastoreListMapRack2, preferredDatastorePath2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePathRack2[0],
				allowedTopologyRacks[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		preferredDatastorePathRack3, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, nonShareddatastoreListMapRack3, preferredDatastorePath3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePathRack3[0],
				allowedTopologyRacks[2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		var preferredDatastorePathsNew []string
		preferredDatastorePathsNew = append(preferredDatastorePathsNew, preferredDatastorePathRack1...)
		preferredDatastorePathsNew = append(preferredDatastorePathsNew, preferredDatastorePathRack2...)
		preferredDatastorePathsNew = append(preferredDatastorePathsNew, preferredDatastorePathRack3...)
		preferredDatastorePathsNew = append(preferredDatastorePathsNew, sharedPreferredDatastorePaths...)

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Creating PVC-4 after changing datastore preference")
		pvclaim4, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var pvclaims4 []*v1.PersistentVolumeClaim
		pvclaims4 = append(pvclaims4, pvclaim4)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs4, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims4, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs4).NotTo(gomega.BeEmpty())
		pv4 := pvs4[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim4.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv4.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", pv4.Spec.CSI.VolumeHandle))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(pv4.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(pv4.Spec.CSI.VolumeHandle))

		ginkgo.By("Creating Pod-4")
		pod4, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim4}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod4.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv4.Spec.CSI.VolumeHandle, pod4.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv4.Spec.CSI.VolumeHandle,
					pod4.Spec.NodeName))
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod4, namespace, preferredDatastorePathsNew,
			allDatastoresListMap)

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod4, namespace,
			allowedTopologies)

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass1, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass1.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot1, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass1.Name, pvclaim4.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot1.Name)

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot1, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(volumeSnapshot1.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent1, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(*snapshotContent1.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle1 := *snapshotContent1.Status.SnapshotHandle
		snapshotId1 := strings.Split(snapshothandle1, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(pv4.Spec.CSI.VolumeHandle, snapshotId1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotContent1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
			err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot1.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Wait till the volume snapshot is deleted")
			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot1.Status.BoundVolumeSnapshotContentName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify snapshot entry is deleted from CNS")
			err = verifySnapshotIsDeletedInCNS(pv4.Spec.CSI.VolumeHandle, snapshotId1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Deleting volume snapshot Again to check Not found error")
			err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot1.Name, metav1.DeleteOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			time.Sleep(40 * time.Second)

		}()

		ginkgo.By("Create PVC-5 from snapshot")
		pvcSpec = getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot1.Name, snapshotapigroup)
		pvclaim5, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes5, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim5}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle5 := persistentvolumes5[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle5).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim5.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle5)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pod")
		pod5, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim5}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podName5 := pod5.GetName
		framework.Logf("podName: %s", podName5)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod5.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod5)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				volHandle5, pod5.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle5,
					pod5.Spec.NodeName))
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod5, namespace, preferredDatastorePathsNew,
			allDatastoresListMap)

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod5, namespace,
			allowedTopologies)
	})

	/*
		Testcase-4
		Topology Snapshot workflow for statefulset

		1. Assign Tag rack-3, Category "cns.vmware.topology-preferred-datastores" to datastore specific to
		rack-3 (ex- NFS-3)
		2. Create SC with Immediate binding mode and allowed topologies set to rack-3 in the SC
		3. Create a statefulset with 3 replicas using above SC
		4. Wait for pvcs to be in Bound state
		5. Wait for pods to be in Running state
		6. Describe PV and verify node affinity details should contain specified allowed topology  details.
		7. Verify volume should be provisioned on the selected preferred datastore of rack-3.
		8. Create snapshot on 3rd replica's PVC.
		9. Scale down the statefulset to 2
		10. Delete the PVC on which snapshot was created
		11. PVC delete succeeds but PV delete will fail as there is snapshot - expected
		12. Create a new PVC with same name (using the snapshot from step-8) - verify a new PV is created
		13. Describe PV and verify node affinity details should contain specified allowed topologies.
		14. Verify volume should be provisioned on the selected preferred datastore of site-2.
		15. Scale up the statefulset to 3
		16. Verify if the new pod attaches to the PV created in step-13
		17. Describe PV and verify node affinity details should contain specified allowed details.
		18. Verify volume should be provisioned on the selected preferred datastore of site-2.
		19. Perform Cleanup. Delete StatefulSet, PVC,PV
		20. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag preferred datatsore and verify snapshot workflow for statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		ginkgo.By("Tag preferred datastore for volume provisioning in rack-3(cluster-3)")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, allowedTopologyRacks[2],
			preferredDatastoreChosen, nonShareddatastoreListMapRack3, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = detachTagCreatedOnPreferredDatastore(masterIp, preferredDatastorePaths[0],
				allowedTopologyRacks[2])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v time for preferred datatsore to get refreshed in "+
			"the environment", preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, nil, allowedTopologyForRack3,
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

		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack3, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)

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

		ginkgo.By("Create a volume snapshot - 1")
		volumeSnapshot1, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim3.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot1.Name)
		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot1.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot1, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = true
		gomega.Expect(volumeSnapshot1.Status.RestoreSize.Cmp(resource.MustParse("1Gi"))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent1, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		gomega.Expect(*snapshotContent1.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle1 := *snapshotContent1.Status.SnapshotHandle
		snapshotId1 := strings.Split(snapshothandle1, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle3, snapshotId1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas-1))
		_, scaledownErr := fss.Scale(client, statefulset, replicas-1)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas-1)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas-1)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		err = fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create a new PVC")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, "1Gi", storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot1.Name, snapshotapigroup)
		pvcSpec.Name = pvclaim3.Name
		pvclaim4, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expecting the volume to bound")
		newPV, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim4},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volHandleOfNewPV := newPV[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandleOfNewPV).NotTo(gomega.BeEmpty())

		replicas = 3
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		//time.Sleep(5 * time.Minute)
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)

		//verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, preferredDatastorePaths,
			nonShareddatastoreListMapRack3, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false)

		ginkgo.By("Delete volume snapshot 1 and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
			volumeSnapshot1.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot content is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot1.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot 1 entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle3, snapshotId1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

})
