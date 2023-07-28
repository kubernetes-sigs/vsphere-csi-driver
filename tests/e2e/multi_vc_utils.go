/*
Copyright 2023 The Kubernetes Authors.

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
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/cns"
	cnsmethods "github.com/vmware/govmomi/cns/methods"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"golang.org/x/crypto/ssh"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

/*
createCustomisedStatefulSets util methods creates statefulset as per the user's
specific requirement and returns the customised statefulset
*/
func createCustomisedStatefulSets(client clientset.Interface, namespace string,
	isParallelPodMgmtPolicy bool, replicas int32, nodeAffinityToSet bool,
	allowedTopologies []v1.TopologySelectorLabelRequirement, allowedTopologyLen int,
	podAntiAffinityToSet bool, modifyStsSpec bool, stsName string,
	accessMode v1.PersistentVolumeAccessMode, sc *storagev1.StorageClass) *appsv1.StatefulSet {
	framework.Logf("Preparing StatefulSet Spec")
	statefulset := GetStatefulSetFromManifest(namespace)

	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteOnce
	}

	if modifyStsSpec {
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = sc.Name
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.AccessModes[0] =
			accessMode
		statefulset.Name = stsName
		statefulset.Spec.Template.Labels["app"] = statefulset.Name
		statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
	}
	if nodeAffinityToSet {
		nodeSelectorTerms := getNodeSelectorTerms(allowedTopologies)
		statefulset.Spec.Template.Spec.Affinity = new(v1.Affinity)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity = new(v1.NodeAffinity)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity.
			RequiredDuringSchedulingIgnoredDuringExecution = new(v1.NodeSelector)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity.
			RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = nodeSelectorTerms
	}
	if podAntiAffinityToSet {
		statefulset.Spec.Template.Spec.Affinity = &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"key": "app",
							},
						},
						TopologyKey: "topology.kubernetes.io/zone",
					},
				},
			},
		}

	}
	if isParallelPodMgmtPolicy {
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
	}
	statefulset.Spec.Replicas = &replicas

	framework.Logf("Creating statefulset")
	CreateStatefulSet(namespace, statefulset, client)

	framework.Logf("Wait for StatefulSet pods to be in up and running state")
	fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
	gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
	ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
	gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
		fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
	gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
		"Number of Pods in the statefulset should match with number of replicas")

	return statefulset
}

/*
setSpecificAllowedTopology returns topology map with specific topology fields and values
in a multi-vc setup
*/
func setSpecificAllowedTopology(allowedTopologies []v1.TopologySelectorLabelRequirement,
	topkeyStartIndex int, startIndex int, endIndex int) []v1.TopologySelectorLabelRequirement {
	var allowedTopologiesMap []v1.TopologySelectorLabelRequirement
	specifiedAllowedTopology := v1.TopologySelectorLabelRequirement{
		Key:    allowedTopologies[topkeyStartIndex].Key,
		Values: allowedTopologies[topkeyStartIndex].Values[startIndex:endIndex],
	}
	allowedTopologiesMap = append(allowedTopologiesMap, specifiedAllowedTopology)

	return allowedTopologiesMap
}

/*
If we have multiple statefulsets, deployment Pods, PVCs/PVs created on a given namespace and for performing
cleanup of these multiple sts creation, deleteAllStsAndPodsPVCsInNamespace is used
*/
func deleteAllStsAndPodsPVCsInNamespace(ctx context.Context, c clientset.Interface, ns string) {
	StatefulSetPoll := 10 * time.Second
	StatefulSetTimeout := 10 * time.Minute
	ssList, err := c.AppsV1().StatefulSets(ns).List(context.TODO(),
		metav1.ListOptions{LabelSelector: labels.Everything().String()})
	framework.ExpectNoError(err)
	errList := []string{}
	for i := range ssList.Items {
		ss := &ssList.Items[i]
		var err error
		if ss, err = scaleStatefulSetPods(c, ss, 0); err != nil {
			errList = append(errList, fmt.Sprintf("%v", err))
		}
		fss.WaitForStatusReplicas(c, ss, 0)
		framework.Logf("Deleting statefulset %v", ss.Name)
		if err := c.AppsV1().StatefulSets(ss.Namespace).Delete(context.TODO(), ss.Name,
			metav1.DeleteOptions{OrphanDependents: new(bool)}); err != nil {
			errList = append(errList, fmt.Sprintf("%v", err))
		}
	}
	pvNames := sets.NewString()
	pvcPollErr := wait.PollImmediate(StatefulSetPoll, StatefulSetTimeout, func() (bool, error) {
		pvcList, err := c.CoreV1().PersistentVolumeClaims(ns).List(context.TODO(),
			metav1.ListOptions{LabelSelector: labels.Everything().String()})
		if err != nil {
			framework.Logf("WARNING: Failed to list pvcs, retrying %v", err)
			return false, nil
		}
		for _, pvc := range pvcList.Items {
			pvNames.Insert(pvc.Spec.VolumeName)
			framework.Logf("Deleting pvc: %v with volume %v", pvc.Name, pvc.Spec.VolumeName)
			if err := c.CoreV1().PersistentVolumeClaims(ns).Delete(context.TODO(), pvc.Name,
				metav1.DeleteOptions{}); err != nil {
				return false, nil
			}
		}
		return true, nil
	})
	if pvcPollErr != nil {
		errList = append(errList, "Timeout waiting for pvc deletion.")
	}

	pollErr := wait.PollImmediate(StatefulSetPoll, StatefulSetTimeout, func() (bool, error) {
		pvList, err := c.CoreV1().PersistentVolumes().List(context.TODO(),
			metav1.ListOptions{LabelSelector: labels.Everything().String()})
		if err != nil {
			framework.Logf("WARNING: Failed to list pvs, retrying %v", err)
			return false, nil
		}
		waitingFor := []string{}
		for _, pv := range pvList.Items {
			if pvNames.Has(pv.Name) {
				waitingFor = append(waitingFor, fmt.Sprintf("%v: %+v", pv.Name, pv.Status))
			}
		}
		if len(waitingFor) == 0 {
			return true, nil
		}
		framework.Logf("Still waiting for pvs of statefulset to disappear:\n%v", strings.Join(waitingFor, "\n"))
		return false, nil
	})
	if pollErr != nil {
		errList = append(errList, "Timeout waiting for pv provisioner to delete pvs, this might mean the test leaked pvs.")

	}
	if len(errList) != 0 {
		framework.ExpectNoError(fmt.Errorf("%v", strings.Join(errList, "\n")))
	}

	framework.Logf("Deleting Deployment Pods and its PVCs")
	depList, err := c.AppsV1().Deployments(ns).List(
		ctx, metav1.ListOptions{LabelSelector: labels.Everything().String()})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, deployment := range depList.Items {
		dep := &deployment
		err = updateDeploymentReplicawithWait(c, 0, dep.Name, ns)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		deletePolicy := metav1.DeletePropagationForeground
		err = c.AppsV1().Deployments(ns).Delete(ctx, dep.Name, metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return
			} else {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
}

/*
verifyVolumeMetadataInCNSForMultiVC verifies container volume metadata is matching the
one is CNS cache on a multivc environment.
*/
func verifyVolumeMetadataInCNSForMultiVC(vs *multiVCvSphere, volumeID string,
	PersistentVolumeClaimName string, PersistentVolumeName string,
	PodName string, Labels ...vim25types.KeyValue) error {
	queryResult, err := vs.queryCNSVolumeWithResultInMultiVC(volumeID)
	if err != nil {
		return err
	}
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
		return fmt.Errorf("failed to query cns volume %s", volumeID)
	}
	for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
		kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
		if kubernetesMetadata.EntityType == "POD" && kubernetesMetadata.EntityName != PodName {
			return fmt.Errorf("entity Pod with name %s not found for volume %s", PodName, volumeID)
		} else if kubernetesMetadata.EntityType == "PERSISTENT_VOLUME" &&
			kubernetesMetadata.EntityName != PersistentVolumeName {
			return fmt.Errorf("entity PV with name %s not found for volume %s", PersistentVolumeName, volumeID)
		} else if kubernetesMetadata.EntityType == "PERSISTENT_VOLUME_CLAIM" &&
			kubernetesMetadata.EntityName != PersistentVolumeClaimName {
			return fmt.Errorf("entity PVC with name %s not found for volume %s", PersistentVolumeClaimName, volumeID)
		}
	}
	labelMap := make(map[string]string)
	for _, e := range queryResult.Volumes[0].Metadata.EntityMetadata {
		if e == nil {
			continue
		}
		if e.GetCnsEntityMetadata().Labels == nil {
			continue
		}
		for _, al := range e.GetCnsEntityMetadata().Labels {
			labelMap[al.Key] = al.Value
		}
		for _, el := range Labels {
			if val, ok := labelMap[el.Key]; ok {
				gomega.Expect(el.Value == val).To(gomega.BeTrue(),
					fmt.Sprintf("Actual label Value of the statically provisioned PV is %s but expected is %s",
						val, el.Value))
			} else {
				return fmt.Errorf("label(%s:%s) is expected in the provisioned PV but its not found", el.Key, el.Value)
			}
		}
	}
	ginkgo.By(fmt.Sprintf("successfully verified metadata of the volume %q", volumeID))
	return nil
}

// govc login cmd for multivc setups
func govcLoginCmdForMultiVC(i int) string {
	configUser := strings.Split(multiVCe2eVSphere.multivcConfig.Global.User, ",")
	configPwd := strings.Split(multiVCe2eVSphere.multivcConfig.Global.Password, ",")
	configvCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
	configvCenterPort := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterPort, ",")

	loginCmd := "export GOVC_INSECURE=1;"
	loginCmd += fmt.Sprintf("export GOVC_URL='https://%s:%s@%s:%s';",
		configUser[i], configPwd[i], configvCenterHostname[i], configvCenterPort[i])
	return loginCmd
}

/*deletes storage profile deletes the storage profile*/
func deleteStorageProfile(masterIp string, sshClientConfig *ssh.ClientConfig,
	storagePolicyName string, clientIndex int) error {
	removeStoragePolicy := govcLoginCmdForMultiVC(clientIndex) +
		"govc storage.policy.rm " + storagePolicyName
	framework.Logf("Remove storage policy: %s ", removeStoragePolicy)
	removeStoragePolicytRes, err := sshExec(sshClientConfig, masterIp, removeStoragePolicy)
	if err != nil && removeStoragePolicytRes.Code != 0 {
		fssh.LogResult(removeStoragePolicytRes)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			removeStoragePolicy, masterIp, err)
	}
	return nil
}

/*
performScalingOnStatefulSetAndVerifyPvNodeAffinity accepts 3 bool values - one for scaleup,
second for scale down and third bool value to check node and pod topology affinites
*/
func performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx context.Context, client clientset.Interface,
	scaleUpReplicaCount int32, scaleDownReplicaCount int32, statefulset *appsv1.StatefulSet,
	parallelStatefulSetCreation bool, namespace string,
	allowedTopologies []v1.TopologySelectorLabelRequirement, stsScaleUp bool, stsScaleDown bool,
	verifyTopologyAffinity bool) {

	if stsScaleDown {
		framework.Logf("Scale down statefulset replica")
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, scaleDownReplicaCount,
			parallelStatefulSetCreation, true)
	}

	if stsScaleUp {
		framework.Logf("Scale up statefulset replica")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, scaleUpReplicaCount,
			parallelStatefulSetCreation, true)
	}

	if verifyTopologyAffinity {
		framework.Logf("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	}
}

/*
createStafeulSetAndVerifyPVAndPodNodeAffinty creates user specified statefulset and
further checks the node and volumes affinities
*/
func createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx context.Context, client clientset.Interface,
	namespace string, parallelPodPolicy bool, replicas int32, nodeAffinityToSet bool,
	allowedTopologies []v1.TopologySelectorLabelRequirement, allowedTopologyLen int,
	podAntiAffinityToSet bool, parallelStatefulSetCreation bool, modifyStsSpec bool,
	stsName string, accessMode v1.PersistentVolumeAccessMode,
	sc *storagev1.StorageClass, verifyTopologyAffinity bool) (*v1.Service, *appsv1.StatefulSet) {

	ginkgo.By("Create service")
	service := CreateService(namespace, client)

	framework.Logf("Create StatefulSet")
	statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
		replicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet, modifyStsSpec,
		"", "", nil)

	if verifyTopologyAffinity {
		framework.Logf("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	}

	return service, statefulset
}

/*
performOfflineVolumeExpansin performs offline volume expansion on the PVC passed as an input
*/
func performOfflineVolumeExpansin(client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, volHandle string, namespace string) {

	ginkgo.By("Expanding current pvc, performing offline volume expansion")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err := expandPVCSize(pvclaim, newSize, client)
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
	_, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := multiVCe2eVSphere.queryCNSVolumeWithResultInMultiVC(volHandle)
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
}

/*
performOnlineVolumeExpansin performs online volume expansion on the Pod passed as an input
*/
func performOnlineVolumeExpansin(f *framework.Framework, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, namespace string, pod *v1.Pod) {

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err := waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	var fsSize int64

	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err = getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("File system size after expansion : %s", fsSize)

	if fsSize < diskSizeInMb {
		framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
	}
	ginkgo.By("File system resize finished successfully")
}

/*
This util getClusterNameForMultiVC will return the cluster details of anyone VC passed to this util
*/
func getClusterNameForMultiVC(ctx context.Context, vs *multiVCvSphere,
	clientIndex int) ([]*object.ClusterComputeResource,
	*VsanClient, error) {

	var vsanHealthClient *VsanClient
	var err error
	c := newClientForMultiVC(ctx, vs)

	datacenter := strings.Split(multiVCe2eVSphere.multivcConfig.Global.Datacenters, ",")

	for i, client := range c {
		if clientIndex == i {
			vsanHealthClient, err = newVsanHealthSvcClient(ctx, client.Client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}

	finder := find.NewFinder(vsanHealthClient.vim25Client, false)
	dc, err := finder.Datacenter(ctx, datacenter[0])
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)

	clusterComputeResource, err := finder.ClusterComputeResourceList(ctx, "*")
	framework.Logf("clusterComputeResource %v", clusterComputeResource)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return clusterComputeResource, vsanHealthClient, err
}

/*
This util verifyPreferredDatastoreMatchInMultiVC will compare the prefrence of datatsore with the
actual datatsore and expected datastore and will return a bool value if both actual and expected datatsore
gets matched else will return false
This util will basically be used to check where exactly the volume provisioning has happened
*/
func (vs *multiVCvSphere) verifyPreferredDatastoreMatchInMultiVC(volumeID string, dsUrls []string) bool {
	framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID)
	queryResult, err := vs.queryCNSVolumeWithResultInMultiVC(volumeID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	actualDatastoreUrl := queryResult.Volumes[0].DatastoreUrl
	flag := false
	for _, dsUrl := range dsUrls {
		if actualDatastoreUrl == dsUrl {
			flag = true
			return flag
		}
	}
	return flag
}

/*
queryCNSVolumeSnapshotWithResultInMultiVC Call CnsQuerySnapshots and returns CnsSnapshotQueryResult
to client
*/
func (vs *multiVCvSphere) queryCNSVolumeSnapshotWithResultInMultiVC(fcdID string,
	snapshotId string) (*cnstypes.CnsSnapshotQueryResult, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var snapshotSpec []cnstypes.CnsSnapshotQuerySpec
	var taskResult *cnstypes.CnsSnapshotQueryResult
	snapshotSpec = append(snapshotSpec, cnstypes.CnsSnapshotQuerySpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: fcdID,
		},
		SnapshotId: &cnstypes.CnsSnapshotId{
			Id: snapshotId,
		},
	})

	queryFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: snapshotSpec,
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  100,
		},
	}

	req := cnstypes.CnsQuerySnapshots{
		This:                cnsVolumeManagerInstance,
		SnapshotQueryFilter: queryFilter,
	}

	for i := 0; i < len(vs.multiVcCnsClient); i++ {
		res, err := cnsmethods.CnsQuerySnapshots(ctx, vs.multiVcCnsClient[i].Client, &req)
		if err != nil {
			return nil, err
		}

		task, err := object.NewTask(vs.multiVcClient[i].Client, res.Returnval), nil
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		taskInfo, err := cns.GetTaskInfo(ctx, task)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		taskResult, err = cns.GetQuerySnapshotsTaskResult(ctx, taskInfo)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if taskResult.Entries[0].Snapshot.SnapshotId.Id == snapshotId {
			return taskResult, nil
		}
	}
	return taskResult, nil
}

/*
getDatastoresListFromMultiVCs util will fetch the list of datastores available in all
the multi-vc setup
This util will return key-value combination of datastore-name:datastore-url of all the 3 VCs available
in a multi-vc setup
*/
func getDatastoresListFromMultiVCs(masterIp string, sshClientConfig *ssh.ClientConfig,
	cluster *object.ClusterComputeResource, isMultiVCSetup bool) (map[string]string, map[string]string,
	map[string]string, error) {
	ClusterdatastoreListMapVC1 := make(map[string]string)
	ClusterdatastoreListMapVC2 := make(map[string]string)
	ClusterdatastoreListMapVC3 := make(map[string]string)

	configvCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
	for i := 0; i < len(configvCenterHostname); i++ {
		datastoreListByVC := govcLoginCmdForMultiVC(i) +
			"govc object.collect -s -d ' ' " + cluster.InventoryPath + " host | xargs govc datastore.info -H | " +
			"grep 'Path\\|URL' | tr -s [:space:]"

		framework.Logf("cmd : %s ", datastoreListByVC)
		result, err := sshExec(sshClientConfig, masterIp, datastoreListByVC)
		if err != nil && result.Code != 0 {
			fssh.LogResult(result)
			return nil, nil, nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				datastoreListByVC, masterIp, err)
		}

		datastoreList := strings.Split(result.Stdout, "\n")

		ClusterdatastoreListMap := make(map[string]string) // Empty the map

		for i := 0; i < len(datastoreList)-1; i = i + 2 {
			key := strings.ReplaceAll(datastoreList[i], " Path: ", "")
			value := strings.ReplaceAll(datastoreList[i+1], " URL: ", "")
			ClusterdatastoreListMap[key] = value
		}

		if i == 0 {
			ClusterdatastoreListMapVC1 = ClusterdatastoreListMap
		} else if i == 1 {
			ClusterdatastoreListMapVC2 = ClusterdatastoreListMap
		} else if i == 2 {
			ClusterdatastoreListMapVC3 = ClusterdatastoreListMap
		}
	}

	return ClusterdatastoreListMapVC1, ClusterdatastoreListMapVC2, ClusterdatastoreListMapVC3, nil
}
