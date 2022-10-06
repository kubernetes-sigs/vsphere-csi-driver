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
	"math/rand"
	"os/exec"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

// checkAnnotationOnSvcPvc checks tkg HA specific annotations on SVC PVC
func checkAnnotationOnSvcPvc(svcPVC *v1.PersistentVolumeClaim,
	allowedTopologies map[string][]string, categories []string) error {
	annotationsMap := svcPVC.Annotations
	if accessibleTopoString, x := annotationsMap[tkgHAccessibleAnnotationKey]; x {
		accessibleTopology := strings.Split(accessibleTopoString, ":")
		topoKey := strings.Split(accessibleTopology[0], "{")[1]
		topoVal := strings.Split(accessibleTopology[1], "}")[0]
		category := strings.SplitAfter(topoKey, "/")[1]
		categoryKey := strings.Split(category, `"`)[0]
		if isValuePresentInTheList(categories, categoryKey) {
			if isValuePresentInTheList(allowedTopologies[topoKey], topoVal) {
				return fmt.Errorf("couldn't find allowed accessible topology: %v on svc pvc: %s"+
					"instead found: %v", allowedTopologies[topoKey], svcPVC.Name, topoVal)
			}
		} else {
			return fmt.Errorf("couldn't find key: %s on allowed categories %v",
				category, categories)
		}
	} else {
		return fmt.Errorf("couldn't find annotation key: %s on svc pvc: %s",
			tkgHAccessibleAnnotationKey, svcPVC.Name)
	}

	if requestedTopoString, y := annotationsMap[tkgHARequestedAnnotationKey]; y {
		availabilityTopo := strings.Split(requestedTopoString, ",")
		for _, avlTopo := range availabilityTopo {
			requestedTopology := strings.Split(avlTopo, ":")
			topoKey := strings.Split(requestedTopology[0], "{")[1]
			topoVal := strings.Split(requestedTopology[1], "}")[0]
			category := strings.SplitAfter(topoKey, "/")[1]
			categoryKey := strings.Split(category, `"`)[0]
			if isValuePresentInTheList(categories, categoryKey) {
				if isValuePresentInTheList(allowedTopologies[topoKey], topoVal) {
					return fmt.Errorf("couldn't find allowed accessible topology: %v on svc pvc: %s"+
						"instead found: %v", allowedTopologies[topoKey], svcPVC.Name, topoVal)
				}
			} else {
				return fmt.Errorf("couldn't find key: %s on allowed categories %v",
					category, categories)
			}
		}
	} else {
		return fmt.Errorf("couldn't find annotation key: %s on svc pvc: %s",
			tkgHARequestedAnnotationKey, svcPVC.Name)
	}
	return nil
}

// isValuePresentInTheList is a util method which checks whether a particular string
// is present in a given list or not
func isValuePresentInTheList(strArr []string, str string) bool {
	for _, s := range strArr {
		if strings.Contains(s, str) {
			return true
		}
	}
	return false
}

// verifyAnnotationsAndNodeAffinity verifies annotations on SVC PVC
// and node affinities and pod location of volumes on correct zones
func verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap map[string][]string,
	categories []string, pod *v1.Pod, nodeList *v1.NodeList,
	svcPVC *v1.PersistentVolumeClaim, pv *v1.PersistentVolume, svcPVCName string) {
	framework.Logf("Verify SV PVC has TKG HA annotations set")
	err := checkAnnotationOnSvcPvc(svcPVC, allowedTopologyHAMap, categories)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("SVC PVC: %s has TKG HA annotations set", svcPVC.Name)

	framework.Logf("Verify GV PV has has required PV node affinity details")
	_, err = verifyVolumeTopologyForLevel5(pv, allowedTopologyHAMap)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("GC PV: %s has required Pv node affinity details", pv.Name)

	framework.Logf("Verify SV PV has has required PV node affinity details")
	svcPV := getPvFromSupervisorCluster(svcPVCName)
	_, err = verifyVolumeTopologyForLevel5(svcPV, allowedTopologyHAMap)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("SVC PV: %s has required PV node affinity details", svcPV.Name)

	_, err = verifyPodLocationLevel5(pod, nodeList, allowedTopologyHAMap)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// verifyVolumeProvisioningWithServiceDown brings the service down and creates the statefulset and then brings up
// the service and validates the volumes are bound and required annotations and node affinity are present
func verifyVolumeProvisioningWithServiceDown(serviceName string, namespace string, client clientset.Interface,
	storagePolicyName string, allowedTopologyHAMap map[string][]string, categories []string, isServiceStopped bool,
	f *framework.Framework) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ginkgo.By("CNS_TEST: Running for GC setup")
	nodeList, err := fnodes.GetReadySchedulableNodes(client)
	framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
	if !(len(nodeList.Items) > 0) {
		framework.Failf("Unable to find ready and schedulable Node")
	}

	ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
	vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
	err = invokeVCenterServiceControl(stopOperation, serviceName, vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	isServiceStopped = true
	err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcStoppedMessage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		if isServiceStopped {
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
			err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			bootstrap()
			isServiceStopped = false
		}
	}()

	ginkgo.By("Create statefulset with default pod management policy with replica 3")
	createResourceQuota(client, namespace, rqLimit, storagePolicyName)
	storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Creating StatefulSet service
	ginkgo.By("Creating service")
	service := CreateService(namespace, client)
	defer func() {
		deleteService(namespace, client, service)
	}()

	statefulset := GetStatefulSetFromManifest(namespace)
	ginkgo.By("Creating statefulset")
	statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
		Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
	*statefulset.Spec.Replicas = 3
	_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	replicas := *(statefulset.Spec.Replicas)

	defer func() {
		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
		pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, claim := range pvcs.Items {
			pv := getPvFromClaim(client, namespace, claim.Name)
			err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeHandle))
		}
	}()

	ginkgo.By(fmt.Sprintf("PVC and POD creations should be in pending state since %s is down", serviceName))
	pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, pvc := range pvcs.Items {
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvc.Namespace, pvc.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
	}

	pods := fss.GetPodList(client, statefulset)
	for _, pod := range pods.Items {
		if pod.Status.Phase != v1.PodPending {
			framework.Failf("Expected pod to be in: %s state but is in: %s state", v1.PodPending,
				pod.Status.Phase)
		}
	}

	ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
	err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	isServiceStopped = false
	err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	bootstrap()

	verifyVolumeMetadataOnStatefulsets(client, ctx, namespace, statefulset, replicas,
		allowedTopologyHAMap, categories, storagePolicyName, nodeList, f)

}

// verifyOnlineVolumeExpansionOnGc is a util method which helps in verifying online volume expansion on gc
func verifyOnlineVolumeExpansionOnGc(client clientset.Interface, namespace string, svcPVCName string,
	volHandle string, pvclaim *v1.PersistentVolumeClaim, pod *v1.Pod, f *framework.Framework) {
	rand.Seed(time.Now().Unix())
	testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
	ginkgo.By(fmt.Sprintf("Creating a 512mb test data file %v", testdataFile))
	op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
		"bs=64k", "count=8000").Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		op, err = exec.Command("rm", "-f", testdataFile).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	_ = framework.RunKubectlOrDie(namespace, "cp", testdataFile,
		fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name))

	onlineVolumeResizeCheck(f, client, namespace, svcPVCName, volHandle, pvclaim, pod)

	ginkgo.By("Checking data consistency after PVC resize")
	_ = framework.RunKubectlOrDie(namespace, "cp",
		fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name), testdataFile+"_pod")
	defer func() {
		op, err = exec.Command("rm", "-f", testdataFile+"_pod").Output()
		fmt.Println("rm: ", op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	ginkgo.By("Running diff...")
	op, err = exec.Command("diff", testdataFile, testdataFile+"_pod").Output()
	fmt.Println("diff: ", op)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(len(op)).To(gomega.BeZero())

	ginkgo.By("File system resize finished successfully in GC")
	ginkgo.By("Checking for PVC resize completion on SVC PVC")
	_, err = waitForFSResizeInSvc(svcPVCName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// verifyOfflineVolumeExpansionOnGc is a util method which helps in verifying offline volume expansion on gc
func verifyOfflineVolumeExpansionOnGc(client clientset.Interface, pvclaim *v1.PersistentVolumeClaim, svcPVCName string,
	namespace string, volHandle string, pod *v1.Pod, pv *v1.PersistentVolume, f *framework.Framework) {
	cmd := []string{"exec", "", "--namespace=" + namespace, "--", "/bin/sh", "-c", "df -Tkm | grep /mnt/volume1"}
	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	cmd[1] = pod.Name
	lastOutput := framework.RunKubectlOrDie(namespace, cmd...)
	gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

	ginkgo.By("Check filesystem size for mount point /mnt/volume1 before expansion")
	originalFsSize, err := getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	rand.Seed(time.Now().Unix())
	testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
	ginkgo.By(fmt.Sprintf("Creating a 512mb test data file %v", testdataFile))
	op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
		"bs=64k", "count=8000").Output()
	fmt.Println(op)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		op, err = exec.Command("rm", "-f", testdataFile).Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	_ = framework.RunKubectlOrDie(namespace, "cp", testdataFile,
		fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name))

	// Delete POD.
	ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s before expansion", pod.Name, namespace))
	err = fpod.DeletePodWithWait(client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is detached from the node before expansion")
	isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
		pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
		fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))

	// Modify PVC spec to trigger volume expansion. We expand the PVC while
	// no pod is using it to ensure offline expansion.
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse(diskSize))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err = expandPVCSize(pvclaim, newSize, client)
	framework.ExpectNoError(err, "While updating pvc for more size")
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())

	pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	if pvcSize.Cmp(newSize) != 0 {
		framework.Failf("error updating pvc size %q", pvclaim.Name)
	}
	ginkgo.By("Checking for PVC request size change on SVC PVC")
	b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
	gomega.Expect(b).To(gomega.BeTrue())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Waiting for controller volume resize to finish")
	err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
	framework.ExpectNoError(err, "While waiting for pvc resize to finish")

	ginkgo.By("Checking for resize on SVC PV")
	verifyPVSizeinSupervisor(svcPVCName, newSize)

	ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
	err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPVCName, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Checking for conditions on pvc")
	pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if len(queryResult.Volumes) == 0 {
		err = fmt.Errorf("QueryCNSVolumeWithResult returned no volume")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verifying disk size requested in volume expansion is honored")
	newSizeInMb := convertGiStrToMibInt64(newSize)
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
		err = fmt.Errorf("got wrong disk size after volume expansion")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Create a new Pod to use this PVC, and verify volume has been attached.
	ginkgo.By("Creating a new pod to attach PV again to the node")
	pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		ginkgo.By("Delete pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By(fmt.Sprintf("Verify volume after expansion: %s is attached to the node: %s",
		pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	ginkgo.By("Verify after expansion the filesystem type is as expected")
	cmd[1] = pod.Name
	lastOutput = framework.RunKubectlOrDie(namespace, cmd...)
	gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	framework.ExpectNoError(err, "while waiting for fs resize to finish")

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	ginkgo.By("Verify filesystem size for mount point /mnt/volume1 after expansion")
	fsSize, err := getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Filesystem size may be smaller than the size of the block volume.
	// Here since filesystem was already formatted on the original volume,
	// we can compare the new filesystem size with the original filesystem size.
	if fsSize < originalFsSize {
		framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
	}

	ginkgo.By("Checking data consistency after PVC resize")
	_ = framework.RunKubectlOrDie(namespace, "cp",
		fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name), testdataFile+"_pod")
	defer func() {
		op, err = exec.Command("rm", "-f", testdataFile+"_pod").Output()
		fmt.Println("rm: ", op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	ginkgo.By("Running diff...")
	op, err = exec.Command("diff", testdataFile, testdataFile+"_pod").Output()
	fmt.Println("diff: ", op)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(len(op)).To(gomega.BeZero())

	ginkgo.By("File system resize finished successfully in GC")
	ginkgo.By("Checking for PVC resize completion on SVC PVC")
	_, err = waitForFSResizeInSvc(svcPVCName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

}

// verifyVolumeMetadataOnStatefulsets verifies sts pod replicas and tkg annotations and
// node affinities on svc pvc and verify cns volume meetadata
func verifyVolumeMetadataOnStatefulsets(client clientset.Interface, ctx context.Context, namespace string,
	statefulset *appsv1.StatefulSet, replicas int32, allowedTopologyHAMap map[string][]string,
	categories []string, storagePolicyName string, nodeList *v1.NodeList, f *framework.Framework) {
	// Waiting for pods status to be Ready
	fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
	gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
	ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
	gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
		fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
	gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
		"Number of Pods in the statefulset should match with number of replicas")

	ginkgo.By("Verify GV PV and SV PV has has required PV node affinity details")
	ginkgo.By("Verify SV PVC has TKG HA annotations set")
	// Get the list of Volumes attached to Pods before scale down
	for _, sspod := range ssPodsBeforeScaleDown.Items {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pvcName := volumespec.PersistentVolumeClaim.ClaimName
				pv := getPvFromClaim(client, statefulset.Namespace, pvcName)
				pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
					pvcName, metav1.GetOptions{})
				gomega.Expect(pvclaim).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				svcPVCName := pv.Spec.CSI.VolumeHandle

				svcPVC := getPVCFromSupervisorCluster(svcPVCName)
				gomega.Expect(*svcPVC.Spec.StorageClassName == storagePolicyName).To(
					gomega.BeTrue(), "SV Pvc storageclass does not match with SV storageclass")
				framework.Logf("GC PVC's storageclass matches SVC PVC's storageclass")

				verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod,
					nodeList, svcPVC, pv, svcPVCName)

				// Verify the attached volume match the one in CNS cache
				err = waitAndVerifyCnsVolumeMetadata4GCVol(volHandle, svcPVCName, pvclaim,
					pv, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}

	replicas = 5
	framework.Logf(fmt.Sprintf("Scaling up statefulset: %v to number of Replica: %v",
		statefulset.Name, replicas))
	_, scaleupErr := fss.Scale(client, statefulset, replicas)
	gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())

	fss.WaitForStatusReplicas(client, statefulset, replicas)
	fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
	ssPodsAfterScaleUp := fss.GetPodList(client, statefulset)
	gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
		fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
	gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
		"Number of Pods in the statefulset %s, %v, should match with number of replicas %v",
		statefulset.Name, ssPodsAfterScaleUp.Size(), replicas,
	)

	// Get the list of Volumes attached to Pods before scale down
	for _, sspod := range ssPodsBeforeScaleDown.Items {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pvcName := volumespec.PersistentVolumeClaim.ClaimName
				pv := getPvFromClaim(client, statefulset.Namespace, pvcName)
				pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
					pvcName, metav1.GetOptions{})
				gomega.Expect(pvclaim).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				svcPVCName := pv.Spec.CSI.VolumeHandle
				svcPVC := getPVCFromSupervisorCluster(svcPVCName)

				verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod,
					nodeList, svcPVC, pv, svcPVCName)

				framework.Logf(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
				var vmUUID string
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
					crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached to the node")
				framework.Logf("After scale up, verify the attached volumes match those in CNS Cache")
				err = waitAndVerifyCnsVolumeMetadata4GCVol(volHandle, svcPVCName, pvclaim,
					pv, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}

}

// verifyVolumeMetadataOnDeployments verifies tkg annotations and
// node affinities on svc pvc and and verify cns volume metadata
func verifyVolumeMetadataOnDeployments(ctx context.Context,
	client clientset.Interface, deployment *appsv1.Deployment, namespace string,
	allowedTopologyHAMap map[string][]string, categories []string,
	nodeList *v1.NodeList, storagePolicyName string) {

	pods, err := GetPodsForMultipleDeployment(client, deployment)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, depPod := range pods.Items {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, depPod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range depPod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pvcName := volumespec.PersistentVolumeClaim.ClaimName
				pv := getPvFromClaim(client, namespace, pvcName)
				pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
					pvcName, metav1.GetOptions{})
				gomega.Expect(pvclaim).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				svcPVCName := pv.Spec.CSI.VolumeHandle

				svcPVC := getPVCFromSupervisorCluster(svcPVCName)
				gomega.Expect(*svcPVC.Spec.StorageClassName == storagePolicyName).To(
					gomega.BeTrue(), "SV Pvc storageclass does not match with SV storageclass")
				framework.Logf("GC PVC's storageclass matches SVC PVC's storageclass")

				verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod,
					nodeList, svcPVC, pv, svcPVCName)

				// Verify the attached volume match the one in CNS cache
				err = waitAndVerifyCnsVolumeMetadata4GCVol(volHandle, svcPVCName, pvclaim,
					pv, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
}
