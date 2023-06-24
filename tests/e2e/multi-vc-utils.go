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
	cnstypes "github.com/vmware/govmomi/cns/types"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"golang.org/x/crypto/ssh"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
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
	podAntiAffinityToSet bool) *appsv1.StatefulSet {
	framework.Logf("Preparing StatefulSet Spec")
	statefulset := GetStatefulSetFromManifest(namespace)

	if nodeAffinityToSet {
		nodeSelectorTerms := setNodeAffinitiesForStatefulSet(allowedTopologies, allowedTopologyLen)
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

/*setNodeAffinitiesForStatefulSet creates and returns nodeSelectorTerms for a statefulSet*/
func setNodeAffinitiesForStatefulSet(allowedTopologies []v1.TopologySelectorLabelRequirement,
	allowedTopologyLen int) []v1.NodeSelectorTerm {
	var nodeSelectorRequirements []v1.NodeSelectorRequirement
	var nodeSelectorTerms []v1.NodeSelectorTerm
	rackTopology := allowedTopologies[len(allowedTopologies)-1]
	if len(rackTopology.Key) == 32 {
		var nodeSelectorTerm v1.NodeSelectorTerm
		var nodeSelectorRequirement v1.NodeSelectorRequirement
		nodeSelectorRequirement.Key = rackTopology.Key
		nodeSelectorRequirement.Operator = "In"
		for i := 0; i < allowedTopologyLen; i++ {
			nodeSelectorRequirement.Values = append(nodeSelectorRequirement.Values, rackTopology.Values[i])
			nodeSelectorTerm.MatchExpressions = append(nodeSelectorRequirements, nodeSelectorRequirement)
		}
		nodeSelectorTerms = append(nodeSelectorTerms, nodeSelectorTerm)
	}
	return nodeSelectorTerms
}

/* setSpecificAllowedTopology returns allowedTopology map with specific topology fields and values */
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
If we have multiple statefulsets and PVCs/PVs created on a given namespace and for performing
cleanup of these multiple sts creation, deleteAllStatefulSetAndPVs is used
*/
func deleteAllStatefulSetAndPVs(c clientset.Interface, ns string) {
	StatefulSetPoll := 10 * time.Second
	StatefulSetTimeout := 10 * time.Minute
	ssList, err := c.AppsV1().StatefulSets(ns).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.Everything().String()})
	framework.ExpectNoError(err)

	// Scale down each statefulset, then delete it completely.
	// Deleting a pvc without doing this will leak volumes, #25101.
	errList := []string{}
	for i := range ssList.Items {
		ss := &ssList.Items[i]
		var err error
		if ss, err = scaleStatefulSetPods(c, ss, 0); err != nil {
			errList = append(errList, fmt.Sprintf("%v", err))
		}
		fss.WaitForStatusReplicas(c, ss, 0)
		framework.Logf("Deleting statefulset %v", ss.Name)
		// Use OrphanDependents=false so it's deleted synchronously.
		// We already made sure the Pods are gone inside Scale().
		if err := c.AppsV1().StatefulSets(ss.Namespace).Delete(context.TODO(), ss.Name, metav1.DeleteOptions{OrphanDependents: new(bool)}); err != nil {
			errList = append(errList, fmt.Sprintf("%v", err))
		}
	}

	// pvs are global, so we need to wait for the exact ones bound to the statefulset pvcs.
	pvNames := sets.NewString()
	// TODO: Don't assume all pvcs in the ns belong to a statefulset
	pvcPollErr := wait.PollImmediate(StatefulSetPoll, StatefulSetTimeout, func() (bool, error) {
		pvcList, err := c.CoreV1().PersistentVolumeClaims(ns).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.Everything().String()})
		if err != nil {
			framework.Logf("WARNING: Failed to list pvcs, retrying %v", err)
			return false, nil
		}
		for _, pvc := range pvcList.Items {
			pvNames.Insert(pvc.Spec.VolumeName)
			// TODO: Double check that there are no pods referencing the pvc
			framework.Logf("Deleting pvc: %v with volume %v", pvc.Name, pvc.Spec.VolumeName)
			if err := c.CoreV1().PersistentVolumeClaims(ns).Delete(context.TODO(), pvc.Name, metav1.DeleteOptions{}); err != nil {
				return false, nil
			}
		}
		return true, nil
	})
	if pvcPollErr != nil {
		errList = append(errList, "Timeout waiting for pvc deletion.")
	}

	pollErr := wait.PollImmediate(StatefulSetPoll, StatefulSetTimeout, func() (bool, error) {
		pvList, err := c.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{LabelSelector: labels.Everything().String()})
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
			// These are the actual labels in the provisioned PV. Populate them
			// in the label map.
			labelMap[al.Key] = al.Value
		}
		for _, el := range Labels {
			// Traverse through the slice of expected labels and see if all of them
			// are present in the label map.
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

/*
performOfflineAndOnlineVolumeExpansionOnPVC verifies and checks offline and online
volume expansion on a multivc setup
*/
func performOfflineAndOnlineVolumeExpansionOnPVC(f *framework.Framework, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, pv []*v1.PersistentVolume,
	volHandle string, namespace string) *v1.Pod {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error

	ginkgo.By("Invoking Test for Volume Expansion")

	// Modify PVC spec to trigger volume expansion
	// We expand the PVC while no pod is using it to ensure offline expansion
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err = expandPVCSize(pvclaim, newSize, client)
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
	pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
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

	// Create a Pod to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))
	vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
	framework.Logf("VMUUID : %s", vmUUID)
	isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	_, err = framework.LookForStringInPodExec(namespace, pod.Name,
		[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	var fsSize int64

	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err = getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("File system size after expansion : %s", fsSize)

	// Filesystem size may be smaller than the size of the block volume
	// so here we are checking if the new filesystem size is greater than
	// the original volume size as the filesystem is formatted for the
	// first time after pod creation
	if fsSize < diskSizeInMb {
		framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
	}
	ginkgo.By("File system resize finished successfully")

	return pod
}

// govc login cmd
func govcLoginCmdForMultiVC() string {
	// configUser := strings.Split(multiVCe2eVSphere.multivcConfig.Global.User[0], ",")
	// configPwd := strings.Split(multiVCe2eVSphere.multivcConfig.Global.Password[0], ",")
	// configvCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname[0], ",")
	// configvCenterPort := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterPort[0], ",")
	configUser := strings.Split(multiVCe2eVSphere.multivcConfig.Global.User, ",")
	configPwd := strings.Split(multiVCe2eVSphere.multivcConfig.Global.Password, ",")
	configvCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
	configvCenterPort := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterPort, ",")

	loginCmd := "export GOVC_INSECURE=1;"
	loginCmd += fmt.Sprintf("export GOVC_URL='https://%s:%s@%s:%s';",
		configUser[0], configPwd[0], configvCenterHostname[0], configvCenterPort[0])
	return loginCmd
}

func deleteStorageProfile(masterIp string, sshClientConfig *ssh.ClientConfig, storagePolicyName string) error {
	removeStoragePolicy := govcLoginCmdForMultiVC() +
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

func performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx context.Context, client clientset.Interface,
	scaleUpReplicaCount int32, scaleDownReplicaCount int32, statefulset *appsv1.StatefulSet,
	parallelStatefulSetCreation bool, namespace string,
	allowedTopologies []v1.TopologySelectorLabelRequirement) {

	framework.Logf("Scale down statefulset replica count to 1")
	scaleDownStatefulSetPod(ctx, client, statefulset, namespace, scaleDownReplicaCount,
		parallelStatefulSetCreation, true)

	framework.Logf("Scale up statefulset replica count to 4")
	scaleUpStatefulSetPod(ctx, client, statefulset, namespace, scaleUpReplicaCount,
		parallelStatefulSetCreation, true)

	framework.Logf("Verify PV node affinity and that the PODS are running on appropriate node")
	verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
		namespace, allowedTopologies, parallelStatefulSetCreation, true)
}

func createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx context.Context, client clientset.Interface,
	namespace string, parallelPodPolicy bool, replicas int32, nodeAffinityToSet bool,
	allowedTopologies []v1.TopologySelectorLabelRequirement, allowedTopologyLen int,
	podAntiAffinityToSet bool, parallelStatefulSetCreation bool) (*v1.Service, *appsv1.StatefulSet) {

	ginkgo.By("Create service")
	service := CreateService(namespace, client)

	framework.Logf("Create StatefulSet")
	statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
		replicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)

	framework.Logf("Verify PV node affinity and that the PODS are running on appropriate node")
	verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
		namespace, allowedTopologies, parallelStatefulSetCreation, true)

	return service, statefulset
}
