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
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
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
		framework.Logf("Scale down statefulset")
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, scaleDownReplicaCount,
			parallelStatefulSetCreation, true)
	}

	if stsScaleUp {
		framework.Logf("Scale up statefulset")
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
				err = waitForHostToBeDown(esxInfo["ip"])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
	return powerOffHostsList
}

/*
putDatastoreInMaintenanceMode method is use to put preferred datastore in
maintenance mode
*/
func putDatastoreInMaintenanceMode(masterIp string, sshClientConfig *ssh.ClientConfig,
	dataCenter []*object.Datacenter, datastoreName string, isMultiVC bool, itr int) error {
	var enableDrsModeCmd string
	var putDatastoreInMMmodeCmd string
	for i := 0; i < len(dataCenter); i++ {
		if !isMultiVC {
			enableDrsModeCmd = govcLoginCmd() + "govc datastore.cluster.change -drs-mode automated"
		} else {
			enableDrsModeCmd = govcLoginCmdForMultiVC(itr) + "govc datastore.cluster.change -drs-mode automated"
		}
		framework.Logf("Enable drs mode: %s ", enableDrsModeCmd)
		enableDrsMode, err := sshExec(sshClientConfig, masterIp, enableDrsModeCmd)
		if err != nil && enableDrsMode.Code != 0 {
			fssh.LogResult(enableDrsMode)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				enableDrsModeCmd, masterIp, err)
		}
		if !isMultiVC {
			putDatastoreInMMmodeCmd = govcLoginCmd() +
				"govc datastore.maintenance.enter -ds " + datastoreName
		} else {
			putDatastoreInMMmodeCmd = govcLoginCmdForMultiVC(itr) +
				"govc datastore.maintenance.enter -ds " + datastoreName
		}
		framework.Logf("Enable drs mode: %s ", putDatastoreInMMmodeCmd)
		putDatastoreInMMmodeRes, err := sshExec(sshClientConfig, masterIp, putDatastoreInMMmodeCmd)
		if err != nil && putDatastoreInMMmodeRes.Code != 0 {
			fssh.LogResult(putDatastoreInMMmodeRes)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				putDatastoreInMMmodeCmd, masterIp, err)
		}
	}
	return nil
}

func verifyStsPodsRelicaStatusWhenSiteIsDown(client clientset.Interface, ss *appsv1.StatefulSet,
	expectedReplicas int32, multiVCSetupType string, namespace string) {
	framework.Logf("Waiting for statefulset status.replicas updated to %d", expectedReplicas)
	StatefulSetPoll := 10 * time.Second
	StatefulSetTimeout := 10 * time.Minute
	ns, name := ss.Namespace, ss.Name
	pollErr := wait.PollImmediate(StatefulSetPoll, StatefulSetTimeout,
		func() (bool, error) {
			ssGet, err := client.AppsV1().StatefulSets(ns).Get(context.TODO(), name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if ssGet.Status.ObservedGeneration < ss.Generation {
				return false, nil
			}
			if ssGet.Status.ReadyReplicas != expectedReplicas {
				framework.Logf("Waiting for stateful set status.readyReplicas to become %d, currently %d", expectedReplicas, ssGet.Status.ReadyReplicas)

				return false, nil
			}
			return true, nil
		})
	if pollErr != nil {
		if multiVCSetupType == "multi-2vc-setup" {
			expectedErrMsg := "Node is not ready"
			listOptions := metav1.ListOptions{}
			isFailureFound := checkEventsforError(client, namespace, listOptions, expectedErrMsg)
			fmt.Println(isFailureFound)

		}

	}
}

func readVsphereConfCredentialsInMultiVCcSetup(cfg string) (e2eTestConfig, error) {
	var config e2eTestConfig

	virtualCenters := make([]string, 0)
	userList := make([]string, 0)
	passwordList := make([]string, 0)
	portList := make([]string, 0)
	dataCenterList := make([]string, 0)

	key, value := "", ""
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
		case "targetvSANFileShareDatastoreURLs":
			config.Global.TargetvSANFileShareDatastoreURLs = value
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

	result += fmt.Sprintf("[Global]\ncluster-distribution = \"%s\"\n\n", cfg.Global.ClusterDistribution)
	for i := 0; i < len(vCenterHostnames); i++ {
		result += fmt.Sprintf("[VirtualCenter \"%s\"]\ninsecure-flag = \"%t\"\nuser = \"%s\"\npassword = \"%s\"\nport = \"%s\"\ndatacenters = \"%s\"\n\n",
			vCenterHostnames[i], cfg.Global.InsecureFlag, users[i], passwords[i], ports[i], dataCenters[i])
	}

	result += fmt.Sprintf("[Snapshot]\nglobal-max-snapshots-per-block-volume = %d\n\n", cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume)
	result += fmt.Sprintf("[Labels]\ntopology-categories = \"%s\"\n", cfg.Labels.TopologyCategories)

	framework.Logf(result)

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
	vsphereCfg, err := readVsphereConfCredentialsInMultiVCcSetup(originalConf)
	if err != nil {
		return e2eTestConfig{}, err
	}

	return vsphereCfg, nil
}

func setNewNameSpaceInCsiYaml(ctx context.Context, client clientset.Interface, sshClientConfig *ssh.ClientConfig,
	masterIp string, originalNS string, newNS string) error {

	createNS := "kubectl create ns test-ns"
	framework.Logf("Create test namespace: %s ", createNS)
	createNsCmd, err := sshExec(sshClientConfig, masterIp, createNS)
	if err != nil && createNsCmd.Code != 0 {
		fssh.LogResult(createNsCmd)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			createNS, masterIp, err)
	}

	deleteCsiYaml := "kubectl delete -f vsphere-csi-driver.yaml"
	framework.Logf("Delete csi driver yaml: %s ", deleteCsiYaml)
	deleteCsi, err := sshExec(sshClientConfig, masterIp, deleteCsiYaml)
	if err != nil && deleteCsi.Code != 0 {
		fssh.LogResult(deleteCsi)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			deleteCsiYaml, masterIp, err)
	}

	findAndSetVal := "sed -i 's/" + originalNS + "/" + newNS + "/g' " + "vsphere-csi-driver.yaml"
	framework.Logf("Set test namespace to csi yaml: %s ", findAndSetVal)
	setVal, err := sshExec(sshClientConfig, masterIp, findAndSetVal)
	if err != nil && setVal.Code != 0 {
		fssh.LogResult(setVal)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			findAndSetVal, masterIp, err)
	}

	applyCsiYaml := "kubectl apply -f vsphere-csi-driver.yaml"
	framework.Logf("Apply updated csi yaml: %s ", applyCsiYaml)
	applyCsi, err := sshExec(sshClientConfig, masterIp, applyCsiYaml)
	if err != nil && applyCsi.Code != 0 {
		fssh.LogResult(applyCsi)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			applyCsiYaml, masterIp, err)
	}
	return nil
}

// func recreateVsphereConfSecretForMultiVC1(client clientset.Interface, ctx context.Context,
// 	namespace string, vCenterIP string, vCenterUser string,
// 	vCenterPassword string, vCenterPort string, dataCenter string, itr int, originalVsphereConf bool) {

// 	csiNamespace := GetAndExpectStringEnvVar(envCSINamespace)
// 	csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
// 		ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
// 	csiReplicas := *csiDeployment.Spec.Replicas

// 	currentSecret, err := client.CoreV1().Secrets(csiNamespace).Get(ctx, configSecret, metav1.GetOptions{})
// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
// 	originalConf := string(currentSecret.Data[vSphereCSIConf])
// 	vsphereCfg, err := readVsphereConfCredentialsInMultiVCcSetup(originalConf)
// 	framework.Logf("original config: %v", vsphereCfg)
// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
// 	if originalVsphereConf && vCenterIP != "" && vCenterUser != "" &&
// 		vCenterPassword != "" && vCenterPort != "" && dataCenter != "" {
// 		vsphereCfg.Global.VCenterHostname = vCenterIP
// 		vsphereCfg.Global.User = vCenterUser
// 		vsphereCfg.Global.Password = vCenterPassword
// 		vsphereCfg.Global.VCenterPort = vCenterPort
// 		vsphereCfg.Global.Datacenters = dataCenter
// 	}
// 	if !originalVsphereConf && vCenterPassword != "" {
// 		passwordList := strings.Split(vsphereCfg.Global.Password, ",")
// 		passwordList[itr] = vCenterPassword
// 		vsphereCfg.Global.Password = strings.Join(passwordList, ",")
// 	}

// 	framework.Logf("updated config: %v", vsphereCfg)
// 	modifiedConf, err := writeDataInVsphereConfSecret(vsphereCfg)
// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
// 	framework.Logf("Updating the secret to reflect new conf credentials")
// 	currentSecret.Data[vSphereCSIConf] = []byte(modifiedConf)
// 	_, err = client.CoreV1().Secrets(csiNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())

// 	ginkgo.By("Restart CSI driver")
// 	restartSuccess, err := restartCSIDriver(ctx, client, namespace, csiReplicas)
// 	gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
// }
