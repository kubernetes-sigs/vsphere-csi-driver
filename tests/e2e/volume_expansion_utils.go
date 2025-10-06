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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
)

// getFSSizeMb returns filesystem size in Mb
func getFSSizeMbWithoutF(namespace string, pod *v1.Pod) (int64, error) {
	var output string
	var err error

	cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c", "df -Tkm | grep /mnt/volume1"}
	output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
	gomega.Expect(strings.Contains(output, ext4FSType)).NotTo(gomega.BeFalse())

	arrMountOut := strings.Fields(string(output))
	if len(arrMountOut) <= 0 {
		return -1, fmt.Errorf("error when parsing output of `df -T`. output: %s", string(output))
	}
	var devicePath, strSize string
	devicePath = arrMountOut[0]
	if devicePath == "" {
		return -1, fmt.Errorf("error when parsing output of `df -T` to find out devicePath of /mnt/volume1. output: %s",
			string(output))
	}
	strSize = arrMountOut[2]
	if strSize == "" {
		return -1, fmt.Errorf("error when parsing output of `df -T` to find out size of /mnt/volume1: output: %s",
			string(output))
	}

	intSizeInMb, err := strconv.ParseInt(strSize, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("failed to parse size %s into int size", strSize)
	}

	return intSizeInMb, nil
}

// increaseSizeOfPvcAttachedToPod this method increases the PVC size, which is attached to POD
func increaseSizeOfPvcAttachedToPodWithoutF(client clientset.Interface,
	namespace string, pvclaim *v1.PersistentVolumeClaim, pod *v1.Pod) {
	var originalSizeInMb int64
	var err error
	//Fetch original FileSystemSize
	ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
	originalSizeInMb, err = getFSSizeMbWithoutF(namespace, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	//resize PVC
	// Modify PVC spec to trigger volume expansion
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err = expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	var fsSize int64
	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err = getFSSizeMbWithoutF(namespace, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("File system size after expansion : %v", fsSize)
	// Filesystem size may be smaller than the size of the block volume
	// so here we are checking if the new filesystem size is greater than
	// the original volume size as the filesystem is formatted for the
	// first time
	gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
		fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
	ginkgo.By("File system resize finished successfully")
}

// resize this method does PVC volume expansion to given size
func resize(client clientset.Interface, pvc *v1.PersistentVolumeClaim,
	currentPvcSize resource.Quantity, newSize resource.Quantity, wg *sync.WaitGroup) {
	defer wg.Done()
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	_, err := expandPVCSize(pvc, newSize, client)
	time.Sleep(pollTimeoutShort)
	framework.Logf("Error from expansion attempt on pvc %v, to %v: %v", pvc.Name, newSize, err)
}

// verifyPVCRequestedSizeInSupervisor compares
// Pvc.Spec.Resources.Requests[v1.ResourceStorage] with given size in SVC.
func verifyPVCRequestedSizeInSupervisor(pvcName string, size resource.Quantity) bool {
	SvcPvc := getPVCFromSupervisorCluster(pvcName)
	pvcSize := SvcPvc.Spec.Resources.Requests[v1.ResourceStorage]
	framework.Logf("SVC PVC requested size: %v", pvcSize)
	return pvcSize.Cmp(size) == 0
}

// verifyPvcRequestedSizeUpdateInSupervisorWithWait waits till
// Pvc.Spec.Resources.Requests[v1.ResourceStorage] matches with given size
// in SVC.
func verifyPvcRequestedSizeUpdateInSupervisorWithWait(pvcName string, size resource.Quantity) (bool, error) {
	var b bool
	waitErr := wait.PollUntilContextTimeout(context.Background(), resizePollInterval, totalResizeWaitPeriod, true,
		func(ctx context.Context) (bool, error) {
			b = verifyPVCRequestedSizeInSupervisor(pvcName, size)
			return b, nil
		})
	return b, waitErr
}

// waitForSvcPvcToReachFileSystemResizePendingCondition waits for SVC PVC to
// reach FileSystemResizePendingCondition status condition.
func waitForSvcPvcToReachFileSystemResizePendingCondition(ctx context.Context, svcPvcName string,
	timeout time.Duration) error {
	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, timeout, true,
		func(ctx context.Context) (bool, error) {
			_, err := checkSvcPvcHasGivenStatusCondition(svcPvcName, true, v1.PersistentVolumeClaimFileSystemResizePending)
			if err == nil {
				return true, nil
			}
			if strings.Contains(err.Error(), "not matching") {
				return false, nil
			}
			return false, err
		})
	return waitErr
}

// This method verifies PVC size after expansion with polling time
func verifyPVSizeinSupervisorWithWait(ctx context.Context, svcPVCName string, newSize resource.Quantity) error {
	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, totalResizeWaitPeriod, true,
		func(ctx context.Context) (bool, error) {
			svcPV := getPvFromSupervisorCluster(svcPVCName)
			svcPVSize := svcPV.Spec.Capacity[v1.ResourceStorage]
			return svcPVSize.Cmp(newSize) >= 0, nil
		})
	return waitErr
}

// waitForPVCToReachFileSystemResizePendingCondition waits for PVC to reach
// FileSystemResizePendingCondition status condition.
func waitForPVCToReachFileSystemResizePendingCondition(client clientset.Interface,
	namespace string, pvcName string, timeout time.Duration) (*v1.PersistentVolumeClaim, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, timeout, true,
		func(ctx context.Context) (bool, error) {
			pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				return false, fmt.Errorf("error while fetching pvc '%v' after controller resize: %v", pvcName, err)
			}
			gomega.Expect(pvclaim).NotTo(gomega.BeNil())

			inProgressConditions := pvclaim.Status.Conditions
			// If there are conditions on the PVC, it must be of
			// FileSystemResizePending type.
			resizeConditionFound := false
			for i := range inProgressConditions {
				if inProgressConditions[i].Type == v1.PersistentVolumeClaimFileSystemResizePending {
					resizeConditionFound = true
				}
			}
			if resizeConditionFound {
				framework.Logf("PVC '%v' is in 'FileSystemResizePending' status condition", pvcName)
				return true, nil
			} else {
				return false, fmt.Errorf("resize was not triggered on PVC '%v' or no status conditions related to "+
					"FileSystemResizePending found on it", pvcName)
			}
		})
	return pvclaim, waitErr
}

// checkPvcHasGivenStatusCondition checks if the status condition in PVC
// matches with the one we want.
func checkPvcHasGivenStatusCondition(client clientset.Interface, namespace string, pvcName string,
	conditionsPresent bool, condition v1.PersistentVolumeClaimConditionType) (*v1.PersistentVolumeClaim, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())
	inProgressConditions := pvclaim.Status.Conditions

	if len(inProgressConditions) == 0 {
		if conditionsPresent {
			return pvclaim, fmt.Errorf("no status conditions found on PVC: %v", pvcName)
		}
		return pvclaim, nil
	}
	conditionsMatches := false
	for i := range inProgressConditions {
		if inProgressConditions[i].Type == condition {
			conditionsMatches = true
			break
		}
	}
	if conditionsMatches == conditionsPresent {
		framework.Logf("PVC '%v' is in '%v' status condition", pvcName, condition)
	} else {
		return pvclaim, fmt.Errorf(
			"status conditions found on PVC '%v' are not matching with expected status condition '%v'",
			pvcName, condition)
	}
	return pvclaim, nil
}

// waitForFSResize waits for the filesystem in the pv to be resized.
func waitForFSResizeInSvc(svcPVCName string) (*v1.PersistentVolumeClaim, error) {
	svcClient, _ := getSvcClientAndNamespace()
	svcPvc := getPVCFromSupervisorCluster(svcPVCName)
	return waitForFSResize(svcPvc, svcClient)
}

// onlineVolumeResizeCheck this method increases the PVC size, which is attached
// to POD.
func onlineVolumeResizeCheck(f *framework.Framework, client clientset.Interface,
	namespace string, svcPVCName string, volHandle string, pvclaim *v1.PersistentVolumeClaim, pod *v1.Pod) {
	var originalSizeInMb int64
	var err error
	// Fetch original FileSystemSize.
	ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
	originalSizeInMb, err = getFileSystemSizeForOsType(f, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Resize PVC.
	// Modify PVC spec to trigger volume expansion.
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err = expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	_, err = waitForFSResizeInSvc(svcPVCName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	framework.ExpectNoError(err, "while waiting for fs resize to finish")

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if len(queryResult.Volumes) == 0 {
		err = fmt.Errorf("QueryCNSVolumeWithResult returned no volume")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var fsSize int64
	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err = getFileSystemSizeForOsType(f, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("File system size after expansion : %d", fsSize)
	// Filesystem size may be smaller than the size of the block volume
	// so here we are checking if the new filesystem size is greater than
	// the original volume size as the filesystem is formatted for the
	// first time.
	gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
		fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
	ginkgo.By("File system resize finished successfully")

	framework.Logf("Online volume expansion in GC PVC is successful")

}

// verifyPVSizeinSupervisor compares PV.Spec.Capacity[v1.ResourceStorage] in
// SVC with the size passed here.
func verifyPVSizeinSupervisor(svcPVCName string, newSize resource.Quantity) {
	svcPV := getPvFromSupervisorCluster(svcPVCName)
	svcPVSize := svcPV.Spec.Capacity[v1.ResourceStorage]
	// If pv size is greater or equal to requested size that means controller
	// resize is finished.
	gomega.Expect(svcPVSize.Cmp(newSize) >= 0).To(gomega.BeTrue())
}
