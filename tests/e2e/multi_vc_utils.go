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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/cns"
	cnsmethods "github.com/vmware/govmomi/cns/methods"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/methods"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"golang.org/x/crypto/ssh"

	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

/*
createCustomisedStatefulSets util methods creates statefulset as per the user's
specific requirement and returns the customised statefulset
*/
func createCustomisedStatefulSets(ctx context.Context, client clientset.Interface, namespace string,
	isParallelPodMgmtPolicy bool, replicas int32, nodeAffinityToSet bool,
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	podAntiAffinityToSet bool, modifyStsSpec bool, stsName string,
	accessMode v1.PersistentVolumeAccessMode, sc *storagev1.StorageClass, storagePolicy string) *appsv1.StatefulSet {
	framework.Logf("Preparing StatefulSet Spec")
	statefulset := GetStatefulSetFromManifest(namespace)

	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		defaultAccessMode := v1.ReadWriteOnce
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.AccessModes[0] = defaultAccessMode
	} else {
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.AccessModes[0] =
			accessMode
	}

	if modifyStsSpec {
		if multipleSvc {
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Spec.StorageClassName = &storagePolicy
		} else {
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Spec.StorageClassName = &sc.Name
		}

		if stsName != "" {
			statefulset.Name = stsName
			statefulset.Spec.Template.Labels["app"] = statefulset.Name
			statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
		}

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
	fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
	gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
	ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	var err error
	adminClient, c := initializeClusterClientsByUserRoles(c)
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
		fss.WaitForStatusReplicas(ctx, c, ss, 0)
		framework.Logf("Deleting statefulset %v", ss.Name)
		if err := c.AppsV1().StatefulSets(ss.Namespace).Delete(context.TODO(), ss.Name,
			metav1.DeleteOptions{OrphanDependents: new(bool)}); err != nil {
			errList = append(errList, fmt.Sprintf("%v", err))
		}
	}
	pvNames := sets.NewString()
	pvcPollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true,
		func(ctx context.Context) (bool, error) {
			pvcList, err := c.CoreV1().PersistentVolumeClaims(ns).List(context.TODO(),
				metav1.ListOptions{LabelSelector: labels.Everything().String()})
			if err != nil {
				framework.Logf("WARNING: Failed to list pvcs, retrying %v", err)
				return false, nil
			}
			for _, pvc := range pvcList.Items {
				pvNames.Insert(pvc.Spec.VolumeName)
				framework.Logf("Deleting pvc: %v with volume %v", pvc.Name, pvc.Spec.VolumeName)
				if err := c.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, pvc.Name,
					metav1.DeleteOptions{}); err != nil {
					return false, nil
				}
			}
			return true, nil
		})
	if pvcPollErr != nil {
		errList = append(errList, "Timeout waiting for pvc deletion.")
	}

	pollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true,
		func(ctx context.Context) (bool, error) {
			pvList, err := adminClient.CoreV1().PersistentVolumes().List(context.TODO(),
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
	configUser := []string{multiVCe2eVSphere.multivcConfig.Global.User}
	configPwd := []string{multiVCe2eVSphere.multivcConfig.Global.Password}
	configvCenterHostname := []string{multiVCe2eVSphere.multivcConfig.Global.VCenterHostname}
	configvCenterPort := []string{multiVCe2eVSphere.multivcConfig.Global.VCenterPort}

	if strings.Contains(multiVCe2eVSphere.multivcConfig.Global.User, ",") {
		configUser = strings.Split(multiVCe2eVSphere.multivcConfig.Global.User, ",")
	}

	if strings.Contains(multiVCe2eVSphere.multivcConfig.Global.Password, ",") {
		configPwd = strings.Split(multiVCe2eVSphere.multivcConfig.Global.Password, ",")
	}

	if strings.Contains(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",") {
		configvCenterHostname = strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
	}

	if strings.Contains(multiVCe2eVSphere.multivcConfig.Global.VCenterPort, ",") {
		configvCenterPort = strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterPort, ",")
	}

	loginCmd := "export GOVC_INSECURE=1;"
	loginCmd += fmt.Sprintf("export GOVC_URL='https://%s:%s@%s:%s';",
		configUser[i], configPwd[i], configvCenterHostname[i], configvCenterPort[i])
	return loginCmd
}

// deleteStorageProfile util deletes the storage policy from vcenter
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

/*deletes storage profile deletes the storage profile*/
func createStorageProfile(masterIp string, sshClientConfig *ssh.ClientConfig,
	storagePolicyName string, clientIndex int) error {
	attachTagCat := govcLoginCmdForMultiVC(clientIndex) +
		"govc tags.attach -c " + "shared-cat-todelete1" + " " + "shared-tag-todelete1" +
		" " + "'" + "/VSAN-DC/datastore/vsanDatastore" + "'"
	framework.Logf("cmd to attach tag to preferred datastore: %s ", attachTagCat)
	attachTagCatRes, err := sshExec(sshClientConfig, masterIp, attachTagCat)
	if err != nil && attachTagCatRes.Code != 0 {
		fssh.LogResult(attachTagCatRes)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			attachTagCat, masterIp, err)
	}
	createStoragePolicy := govcLoginCmdForMultiVC(clientIndex) +
		"govc storage.policy.create -category=shared-cat-todelete1 -tag=shared-tag-todelete1 " + storagePolicyName
	framework.Logf("Create storage policy: %s ", createStoragePolicy)
	createStoragePolicytRes, err := sshExec(sshClientConfig, masterIp, createStoragePolicy)
	if err != nil && createStoragePolicytRes.Code != 0 {
		fssh.LogResult(createStoragePolicytRes)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			createStoragePolicy, masterIp, err)
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
	verifyTopologyAffinity bool) error {

	if stsScaleDown {
		framework.Logf("Scale down statefulset replica")
		err := scaleDownStatefulSetPod(ctx, client, statefulset, namespace, scaleDownReplicaCount,
			parallelStatefulSetCreation)
		if err != nil {
			return fmt.Errorf("error scaling down statefulset: %v", err)
		}
	}

	if stsScaleUp {
		framework.Logf("Scale up statefulset replica")
		err := scaleUpStatefulSetPod(ctx, client, statefulset, namespace, scaleUpReplicaCount,
			parallelStatefulSetCreation)
		if err != nil {
			return fmt.Errorf("error scaling up statefulset: %v", err)
		}
	}

	if verifyTopologyAffinity {
		framework.Logf("Verify PV node affinity and that the PODS are running on appropriate node")
		err := verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation)
		if err != nil {
			return fmt.Errorf("error verifying PV node affinity and POD node details: %v", err)
		}
	}

	return nil
}

/*
createStafeulSetAndVerifyPVAndPodNodeAffinty creates user specified statefulset and
further checks the node and volumes affinities
*/
func createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx context.Context, client clientset.Interface,
	namespace string, parallelPodPolicy bool, replicas int32, nodeAffinityToSet bool,
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	podAntiAffinityToSet bool, parallelStatefulSetCreation bool, modifyStsSpec bool,
	accessMode v1.PersistentVolumeAccessMode,
	sc *storagev1.StorageClass, verifyTopologyAffinity bool, storagePolicy string) (*v1.Service,
	*appsv1.StatefulSet, error) {

	ginkgo.By("Create service")
	service := CreateService(namespace, client)

	framework.Logf("Create StatefulSet")
	statefulset := createCustomisedStatefulSets(ctx, client, namespace, parallelPodPolicy,
		replicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet, modifyStsSpec,
		"", accessMode, sc, storagePolicy)

	if verifyTopologyAffinity {
		framework.Logf("Verify PV node affinity and that the PODS are running on appropriate node")
		err := verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation)
		if err != nil {
			return nil, nil, fmt.Errorf("error verifying PV node affinity and POD node details: %v", err)
		}
	}

	return service, statefulset, nil
}

/*
performOfflineVolumeExpansin performs offline volume expansion on the PVC passed as an input
*/
func performOfflineVolumeExpansin(client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, volHandle string, namespace string) error {

	ginkgo.By("Expanding current pvc, performing offline volume expansion")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	expandedPvc, err := expandPVCSize(pvclaim, newSize, client)
	if err != nil {
		return fmt.Errorf("error expanding PVC size: %v", err)
	}
	if expandedPvc == nil {
		return errors.New("expanded PVC is nil")
	}

	pvcSize := expandedPvc.Spec.Resources.Requests[v1.ResourceStorage]
	if pvcSize.Cmp(newSize) != 0 {
		return fmt.Errorf("error updating PVC size %q", expandedPvc.Name)
	}

	ginkgo.By("Waiting for controller volume resize to finish")
	err = waitForPvResizeForGivenPvc(expandedPvc, client, totalResizeWaitPeriod)
	if err != nil {
		return fmt.Errorf("error waiting for controller volume resize: %v", err)
	}

	ginkgo.By("Checking for conditions on PVC")
	_, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, expandedPvc.Name, pollTimeout)
	if err != nil {
		return fmt.Errorf("error waiting for PVC conditions: %v", err)
	}

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := multiVCe2eVSphere.queryCNSVolumeWithResultInMultiVC(volHandle)
	if err != nil {
		return fmt.Errorf("error querying CNS volume: %v", err)
	}

	if len(queryResult.Volumes) == 0 {
		return errors.New("queryCNSVolumeWithResult returned no volume")
	}

	ginkgo.By("Verifying disk size requested in volume expansion is honored")
	newSizeInMb := int64(3072)
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
		return errors.New("wrong disk size after volume expansion")
	}

	return nil
}

/*
performOnlineVolumeExpansin performs online volume expansion on the Pod passed as an input
*/
func performOnlineVolumeExpansion(f *framework.Framework, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, pod *v1.Pod) error {

	ginkgo.By("Waiting for file system resize to finish")
	expandedPVC, err := waitForFSResize(pvclaim, client)
	if err != nil {
		return fmt.Errorf("error waiting for file system resize: %v", err)
	}

	pvcConditions := expandedPVC.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	var fsSize int64

	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err = getFSSizeMb(f, pod)
	if err != nil {
		return fmt.Errorf("error getting file system size: %v", err)
	}
	framework.Logf("File system size after expansion : %d", fsSize)

	if fsSize < diskSizeInMb {
		return fmt.Errorf("error updating filesystem size for %q. Resulting filesystem size is %d", expandedPVC.Name, fsSize)
	}
	ginkgo.By("File system resize finished successfully")

	return nil
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
	cluster *object.ClusterComputeResource) (map[string]string, map[string]string,
	map[string]string, error) {
	ClusterdatastoreListMapVc1 := make(map[string]string)
	ClusterdatastoreListMapVc2 := make(map[string]string)
	ClusterdatastoreListMapVc3 := make(map[string]string)

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
			ClusterdatastoreListMapVc1 = ClusterdatastoreListMap
		} else if i == 1 {
			ClusterdatastoreListMapVc2 = ClusterdatastoreListMap
		} else if i == 2 {
			ClusterdatastoreListMapVc3 = ClusterdatastoreListMap
		}
	}

	return ClusterdatastoreListMapVc1, ClusterdatastoreListMapVc2, ClusterdatastoreListMapVc3, nil
}

/*
readVsphereConfCredentialsInMultiVCcSetup util accepts a vsphere conf parameter, reads all the values
of vsphere conf and returns a testConf file
*/
func readVsphereConfCredentialsInMultiVcSetup(cfg string) (e2eTestConfig, error) {
	var config e2eTestConfig

	virtualCenters := make([]string, 0)
	userList := make([]string, 0)
	passwordList := make([]string, 0)
	portList := make([]string, 0)
	dataCenterList := make([]string, 0)

	key, value := "", ""
	var netPerm NetPermissionConfig
	var permissions vsanfstypes.VsanFileShareAccessType
	var rootSquash bool

	lines := strings.Split(cfg, "\n")
	for index, line := range lines {
		if index == 0 {
			// Skip [Global].
			continue
		}
		words := strings.Split(line, " = ")
		if strings.Contains(words[0], "topology-categories=") {
			words = strings.Split(line, "=")
		}

		if len(words) == 1 {
			if strings.Contains(words[0], "Snapshot") {
				continue
			}
			if strings.Contains(words[0], "Labels") {
				continue
			}
			words = strings.Split(line, " ")
			if strings.Contains(words[0], "VirtualCenter") {
				value = words[1]
				value = strings.TrimSuffix(value, "]")
				value = trimQuotes(value)
				config.Global.VCenterHostname = value
				virtualCenters = append(virtualCenters, value)
			}
			continue
		}
		key = words[0]
		value = trimQuotes(words[1])
		var strconvErr error
		switch key {
		case "insecure-flag":
			if strings.Contains(value, "true") {
				config.Global.InsecureFlag = true
			} else {
				config.Global.InsecureFlag = false
			}
		case "cluster-id":
			config.Global.ClusterID = value
		case "cluster-distribution":
			config.Global.ClusterDistribution = value
		case "user":
			config.Global.User = value
			userList = append(userList, value)
		case "password":
			config.Global.Password = value
			passwordList = append(passwordList, value)
		case "datacenters":
			config.Global.Datacenters = value
			dataCenterList = append(dataCenterList, value)
		case "port":
			config.Global.VCenterPort = value
			portList = append(portList, value)
		case "cnsregistervolumes-cleanup-intervalinmin":
			config.Global.CnsRegisterVolumesCleanupIntervalInMin, strconvErr = strconv.Atoi(value)
			if strconvErr != nil {
				return config, fmt.Errorf("invalid value for cnsregistervolumes-cleanup-intervalinmin: %s", value)
			}
		case "topology-categories":
			config.Labels.TopologyCategories = value
		case "global-max-snapshots-per-block-volume":
			config.Snapshot.GlobalMaxSnapshotsPerBlockVolume, strconvErr = strconv.Atoi(value)
			if strconvErr != nil {
				return config, fmt.Errorf("invalid value for global-max-snapshots-per-block-volume: %s", value)
			}
		case "csi-fetch-preferred-datastores-intervalinmin":
			config.Global.CSIFetchPreferredDatastoresIntervalInMin, strconvErr = strconv.Atoi(value)
			if strconvErr != nil {
				return config, fmt.Errorf("invalid value for csi-fetch-preferred-datastores-intervalinmin: %s", value)
			}
		case "query-limit":
			config.Global.QueryLimit, strconvErr = strconv.Atoi(value)
			if strconvErr != nil {
				return config, fmt.Errorf("invalid value for query-limit: %s", value)
			}
		case "list-volume-threshold":
			config.Global.ListVolumeThreshold, strconvErr = strconv.Atoi(value)
			if strconvErr != nil {
				return config, fmt.Errorf("invalid value for list-volume-threshold: %s", value)
			}
		case "ips":
			netPerm.Ips = value
		case "permissions":
			netPerm.Permissions = permissions
		case "rootsquash":
			netPerm.RootSquash = rootSquash
		default:
			return config, fmt.Errorf("unknown key %s in the input string", key)
		}
	}

	config.Global.VCenterHostname = strings.Join(virtualCenters, ",")
	config.Global.User = strings.Join(userList, ",")
	config.Global.Password = strings.Join(passwordList, ",")
	config.Global.VCenterPort = strings.Join(portList, ",")
	config.Global.Datacenters = strings.Join(dataCenterList, ",")

	return config, nil
}

/*
writeNewDataAndUpdateVsphereConfSecret uitl edit the vsphere conf and returns the updated
vsphere config sceret
*/
func writeNewDataAndUpdateVsphereConfSecret(client clientset.Interface, ctx context.Context,
	csiNamespace string, cfg e2eTestConfig) error {
	var result string

	// fetch current secret
	currentSecret, err := client.CoreV1().Secrets(csiNamespace).Get(ctx, configSecret, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// modify vshere conf file
	vCenterHostnames := strings.Split(cfg.Global.VCenterHostname, ",")
	users := strings.Split(cfg.Global.User, ",")
	passwords := strings.Split(cfg.Global.Password, ",")
	dataCenters := strings.Split(cfg.Global.Datacenters, ",")
	ports := strings.Split(cfg.Global.VCenterPort, ",")

	result += fmt.Sprintf("[Global]\ncluster-distribution = \"%s\"\n"+
		"csi-fetch-preferred-datastores-intervalinmin = %d\n"+
		"query-limit = %d\nlist-volume-threshold = %d\n\n",
		cfg.Global.ClusterDistribution, cfg.Global.CSIFetchPreferredDatastoresIntervalInMin, cfg.Global.QueryLimit,
		cfg.Global.ListVolumeThreshold)
	for i := 0; i < len(vCenterHostnames); i++ {
		result += fmt.Sprintf("[VirtualCenter \"%s\"]\ninsecure-flag = \"%t\"\nuser = \"%s\"\npassword = \"%s\"\n"+
			"port = \"%s\"\ndatacenters = \"%s\"\n\n",
			vCenterHostnames[i], cfg.Global.InsecureFlag, users[i], passwords[i], ports[i], dataCenters[i])
	}

	result += fmt.Sprintf("[Snapshot]\nglobal-max-snapshots-per-block-volume = %d\n\n",
		cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume)
	result += fmt.Sprintf("[Labels]\ntopology-categories = \"%s\"\n", cfg.Labels.TopologyCategories)

	framework.Logf("%q", result)

	// update config secret with newly updated vshere conf file
	framework.Logf("Updating the secret to reflect new conf credentials")
	currentSecret.Data[vSphereCSIConf] = []byte(result)
	_, err = client.CoreV1().Secrets(csiNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	return nil
}

// readVsphereConfSecret method is used to read csi vsphere conf file
func readVsphereConfSecret(client clientset.Interface, ctx context.Context,
	csiNamespace string) (e2eTestConfig, error) {

	// fetch current secret
	currentSecret, err := client.CoreV1().Secrets(csiNamespace).Get(ctx, configSecret, metav1.GetOptions{})
	if err != nil {
		return e2eTestConfig{}, err
	}

	// read vsphere conf
	originalConf := string(currentSecret.Data[vSphereCSIConf])
	vsphereCfg, err := readVsphereConfCredentialsInMultiVcSetup(originalConf)
	if err != nil {
		return e2eTestConfig{}, err
	}

	return vsphereCfg, nil
}

/*
setNewNameSpaceInCsiYaml util installs the csi yaml in new namespace
*/
func setNewNameSpaceInCsiYaml(ctx context.Context, client clientset.Interface, sshClientConfig *ssh.ClientConfig,
	originalNS string, newNS string, allMasterIps []string) error {

	var controlIp string
	ignoreLabels := make(map[string]string)

	for _, masterIp := range allMasterIps {
		deleteCsiYaml := "kubectl delete -f vsphere-csi-driver.yaml"
		framework.Logf("Delete csi driver yaml: %s ", deleteCsiYaml)
		deleteCsi, err := sshExec(sshClientConfig, masterIp, deleteCsiYaml)
		if err != nil && deleteCsi.Code != 0 {
			if strings.Contains(err.Error(), "does not exist") {
				framework.Logf("Retry other master nodes")
				continue
			} else {
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "couldn't execute command on host: %v , error: %s",
					masterIp, err)
			}
		}
		if err == nil {
			controlIp = masterIp
			break
		}
	}

	findAndSetVal := "sed -i 's/" + originalNS + "/" + newNS + "/g' " + "vsphere-csi-driver.yaml"
	framework.Logf("Set test namespace to csi yaml: %s ", findAndSetVal)
	setVal, err := sshExec(sshClientConfig, controlIp, findAndSetVal)
	if err != nil && setVal.Code != 0 {
		fssh.LogResult(setVal)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			findAndSetVal, controlIp, err)
	}

	applyCsiYaml := "kubectl apply -f vsphere-csi-driver.yaml"
	framework.Logf("Apply updated csi yaml: %s ", applyCsiYaml)
	applyCsi, err := sshExec(sshClientConfig, controlIp, applyCsiYaml)
	if err != nil && applyCsi.Code != 0 {
		fssh.LogResult(applyCsi)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			applyCsiYaml, controlIp, err)
	}

	// Wait for the CSI Pods to be up and Running
	list_of_pods, err := fpod.GetPodsInNamespace(ctx, client, newNS, ignoreLabels)
	if err != nil {
		return err
	}
	num_csi_pods := len(list_of_pods)
	err = fpod.WaitForPodsRunningReady(ctx, client, newNS, int(num_csi_pods),
		time.Duration(pollTimeout))
	if err != nil {
		return err
	}
	return nil
}

/*
deleteVsphereConfigSecret deletes vsphere config secret
*/
func deleteVsphereConfigSecret(client clientset.Interface, ctx context.Context,
	originalNS string) error {

	// get current secret
	currentSecret, err := client.CoreV1().Secrets(originalNS).Get(ctx, configSecret, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// delete current secret
	err = client.CoreV1().Secrets(originalNS).Delete(ctx, currentSecret.Name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	return nil
}

/*
createNamespaceSpec util creates a spec required for creating a namespace
*/
func createNamespaceSpec(nsName string) *v1.Namespace {
	var namespace = &v1.Namespace{
		TypeMeta: metav1.TypeMeta{
			Kind: "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: nsName,
		},
	}
	return namespace
}

/*
createNamespace creates a namespace
*/
func createNamespace(client clientset.Interface, ctx context.Context, nsName string) (*v1.Namespace, error) {

	framework.Logf("Create namespace")
	namespaceSpec := createNamespaceSpec(nsName)
	namespace, err := client.CoreV1().Namespaces().Create(ctx, namespaceSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return namespace, nil
}

/*
createVsphereConfigSecret util creates csi vsphere conf and later creates a new config secret
*/
func createVsphereConfigSecret(namespace string, cfg e2eTestConfig, sshClientConfig *ssh.ClientConfig,
	allMasterIps []string) error {

	var conf string
	var controlIp string

	for _, masterIp := range allMasterIps {
		readCsiYaml := "ls -l vsphere-csi-driver.yaml"
		framework.Logf("list csi driver yaml: %s ", readCsiYaml)
		grepCsiNs, err := sshExec(sshClientConfig, masterIp, readCsiYaml)
		if err != nil && grepCsiNs.Code != 0 {
			if strings.Contains(err.Error(), "No such file or directory") {
				framework.Logf("Retry other master nodes")
				continue
			} else {
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "couldn't execute command on host: %v , error: %s",
					masterIp, err)
			}
		}
		if err == nil {
			controlIp = masterIp
			break
		}
	}

	vCenterHostnames := strings.Split(cfg.Global.VCenterHostname, ",")
	users := strings.Split(cfg.Global.User, ",")
	passwords := strings.Split(cfg.Global.Password, ",")
	dataCenters := strings.Split(cfg.Global.Datacenters, ",")
	ports := strings.Split(cfg.Global.VCenterPort, ",")

	conf = fmt.Sprintf("tee csi-vsphere.conf >/dev/null <<EOF\n[Global]\ncluster-distribution = \"%s\"\n"+
		"csi-fetch-preferred-datastores-intervalinmin = %d\n"+
		"query-limit = %d\nlist-volume-threshold = %d\n\n",
		cfg.Global.ClusterDistribution, cfg.Global.CSIFetchPreferredDatastoresIntervalInMin, cfg.Global.QueryLimit,
		cfg.Global.ListVolumeThreshold)
	for i := 0; i < len(vCenterHostnames); i++ {
		conf += fmt.Sprintf("[VirtualCenter \"%s\"]\ninsecure-flag = \"%t\"\nuser = \"%s\"\npassword = \"%s\"\n"+
			"port = \"%s\"\ndatacenters = \"%s\"\n\n",
			vCenterHostnames[i], cfg.Global.InsecureFlag, users[i], passwords[i], ports[i], dataCenters[i])
	}

	conf += fmt.Sprintf("[Snapshot]\nglobal-max-snapshots-per-block-volume = %d\n\n",
		cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume)
	conf += fmt.Sprintf("[Labels]\ntopology-categories = \"%s\"\n", cfg.Labels.TopologyCategories)
	conf += "\nEOF"

	framework.Logf("conf: %s", conf)

	result, err := sshExec(sshClientConfig, controlIp, conf)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s", conf, controlIp, err)
	}
	applyConf := "kubectl create secret generic vsphere-config-secret --from-file=csi-vsphere.conf " +
		"-n " + namespace
	framework.Logf("applyConf: %s", applyConf)
	result, err = sshExec(sshClientConfig, controlIp, applyConf)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			applyConf, controlIp, err)
	}
	return nil
}

/*
deleteNamespace util deletes the user created namespace
*/
func deleteNamespace(client clientset.Interface, ctx context.Context, nsName string) error {

	framework.Logf("Delete namespace")
	err := client.CoreV1().Namespaces().Delete(ctx, nsName, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

// This util method takes cluster name as input parameter and powers off esxi host of that cluster
func powerOffEsxiHostsInMultiVcCluster(ctx context.Context, vs *multiVCvSphere,
	esxCount int, hostsInCluster []*object.HostSystem) []string {
	var powerOffHostsList []string
	for i := 0; i < esxCount; i++ {
		for _, esxInfo := range tbinfo.esxHosts {
			host := hostsInCluster[i].Common.InventoryPath
			hostIp := strings.Split(host, "/")
			if hostIp[len(hostIp)-1] == esxInfo["ip"] {
				esxHostName := esxInfo["vmName"]
				powerOffHostsList = append(powerOffHostsList, esxHostName)
				err := vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, esxHostName, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitForHostToBeDown(ctx, esxInfo["ip"])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
	return powerOffHostsList
}

/*
suspendDatastore util suspends the datastore in one of the multivc setup passed in the
input parameter
Here, this util is expecting what datastore operation to perform, it can be suspend, off etc.
Next input parameter is the datastore name on which suspend operation needs to be performed
and last is on which of the multivc setup we need to perform this datastore operation
*/
func suspendDatastore(ctx context.Context, opName string, dsNameToPowerOff string, testbedInfoJsonIndex string) string {
	dsName := ""
	readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(testbedInfoJsonIndex))

	for _, dsInfo := range tbinfo.datastores {
		if strings.Contains(dsInfo["vmName"], dsNameToPowerOff) {
			dsName = dsInfo["vmName"]
			err := datatoreOperations(tbinfo.user, tbinfo.location, tbinfo.podname, dsName, opName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitForHostToBeDown(ctx, dsInfo["ip"])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			break
		}
	}
	return dsName
}

/*
resumeDatastore util resumes the datastore in one of the multivc setup passed in the
input parameter
Here, this util is expecting what datastore operation to perform, it can be resume, on etc.
Next input parameter is the datastore name on which resume operation needs to be performed
and last is on which of the multivc setup we need to perform this datastore operation
*/
func resumeDatastore(datastoreToPowerOn string, opName string, testbedInfoJsonIndex string) {

	readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(testbedInfoJsonIndex))
	err := datatoreOperations(tbinfo.user, tbinfo.location, tbinfo.podname, datastoreToPowerOn, opName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, dsInfo := range tbinfo.datastores {
		if strings.Contains(dsInfo["vmName"], datastoreToPowerOn) {
			err = waitForHostToBeUp(dsInfo["ip"])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			break
		}
	}
}

/*
createStaticPVCInMultiVC util creates static PV/PVC and returns the fcd-id, pv and pvc metadata
*/
func createStaticFCDPvAndPvc(ctx context.Context, f *framework.Framework,
	client clientset.Interface, namespace string, defaultDatastore *object.Datastore,
	pandoraSyncWaitTime int, allowedTopologies []v1.TopologySelectorLabelRequirement, clientIndex int,
	sc *storagev1.StorageClass) (string, *v1.PersistentVolumeClaim, *v1.PersistentVolume) {
	curtime := time.Now().Unix()

	ginkgo.By("Creating FCD Disk")
	curtimeinstring := strconv.FormatInt(curtime, 10)
	fcdID, err := multiVCe2eVSphere.createFCDInMultiVC(ctx, "BasicStaticFCD"+curtimeinstring, diskSizeInMb,
		defaultDatastore.Reference(), clientIndex)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("FCD ID : %s", fcdID)

	ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
		pandoraSyncWaitTime, fcdID))
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

	staticPVLabels := make(map[string]string)
	staticPVLabels["fcd-id"] = fcdID

	// Creating PV using above created SC and FCD
	ginkgo.By("Creating the PV")
	staticPv := getPersistentVolumeSpecWithStorageClassFCDNodeSelector(fcdID,
		v1.PersistentVolumeReclaimDelete, sc.Name, staticPVLabels,
		diskSize, allowedTopologies)
	staticPv, err = client.CoreV1().PersistentVolumes().Create(ctx, staticPv, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	err = multiVCe2eVSphere.waitForCNSVolumeToBeCreatedInMultiVC(staticPv.Spec.CSI.VolumeHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Creating PVC using above created PV
	ginkgo.By("Creating static PVC")
	staticPvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, staticPv.Name)
	staticPvc.Spec.StorageClassName = &sc.Name
	staticPvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, staticPvc,
		metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Wait for PV and PVC to Bind.
	ginkgo.By("Wait for PV and PVC to Bind")
	framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts,
		namespace, staticPv, staticPvc))

	ginkgo.By("Verifying CNS entry is present in cache")
	_, err = multiVCe2eVSphere.queryCNSVolumeWithResultInMultiVC(staticPv.Spec.CSI.VolumeHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return fcdID, staticPvc, staticPv
}

// createFCDForMultiVC creates an FCD disk on a multiVC setup
func (vs *multiVCvSphere) createFCDInMultiVC(ctx context.Context, fcdname string,
	diskCapacityInMB int64, dsRef vim25types.ManagedObjectReference, clientIndex int) (string, error) {
	KeepAfterDeleteVM := false
	spec := vim25types.VslmCreateSpec{
		Name:              fcdname,
		CapacityInMB:      diskCapacityInMB,
		KeepAfterDeleteVm: &KeepAfterDeleteVM,
		BackingSpec: &vim25types.VslmCreateSpecDiskFileBackingSpec{
			VslmCreateSpecBackingSpec: vim25types.VslmCreateSpecBackingSpec{
				Datastore: dsRef,
			},
			ProvisioningType: string(vim25types.BaseConfigInfoDiskFileBackingInfoProvisioningTypeThin),
		},
	}
	req := vim25types.CreateDisk_Task{
		This: *vs.multiVcClient[clientIndex].ServiceContent.VStorageObjectManager,
		Spec: spec,
	}
	res, err := methods.CreateDisk_Task(ctx, vs.multiVcClient[clientIndex].Client, &req)
	if err != nil {
		return "", err
	}
	task := object.NewTask(vs.multiVcClient[clientIndex].Client, res.Returnval)
	taskInfo, err := task.WaitForResultEx(ctx, nil)
	if err != nil {
		return "", err
	}
	fcdID := taskInfo.Result.(vim25types.VStorageObject).Config.Id.Id
	return fcdID, nil
}

// deleteFCDInMultiVc deletes an FCD disk from a multivc setup
func (vs *multiVCvSphere) deleteFCDInMultiVc(ctx context.Context, fcdID string,
	dsRef vim25types.ManagedObjectReference, clientIndex int) error {
	req := vim25types.DeleteVStorageObject_Task{
		This:      *vs.multiVcClient[clientIndex].ServiceContent.VStorageObjectManager,
		Datastore: dsRef,
		Id:        vim25types.ID{Id: fcdID},
	}
	res, err := methods.DeleteVStorageObject_Task(ctx, vs.multiVcClient[clientIndex].Client, &req)
	if err != nil {
		return err
	}
	task := object.NewTask(vs.multiVcClient[clientIndex].Client, res.Returnval)
	_, err = task.WaitForResultEx(ctx, nil)
	if err != nil {
		framework.Logf("%q", err.Error())
	}
	return nil
}
