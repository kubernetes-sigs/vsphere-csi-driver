/*
Copyright 2019 The Kubernetes Authors.

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
	"fmt"
	"strings"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2elog "k8s.io/kubernetes/test/e2e/framework"
)

/*
   Tests to verify SPBM based dynamic volume provisioning using CSI Driver in Kubernetes.

   Steps
   1. Create StorageClass with.
   		a. policy which is compliant to shared datastores / policy which is compliant to non-shared datastores / invalid non-existent policy
   2. Create PVC which uses the StorageClass created in step 1.
   3. Wait for PV to be provisioned if policy is valid and compliant to shared datastores, else expect failure.
   4. Wait for PVC's status to become bound, if policy is valid and compliant
   5. Create pod using PVC.
   6. Wait for Disk to be attached to the node.
   7. Delete pod and Wait for Volume Disk to be detached from the Node.
   8. Delete PVC, PV and Storage Class
*/

var _ = ginkgo.Describe("[csi-block-e2e] Storage Policy Based Volume Provisioning", func() {
	f := framework.NewDefaultFramework("e2e-spbm-policy")
	var (
		client    clientset.Interface
		namespace string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	ginkgo.It("Verify dynamic volume provisioning works when storage policy specified in the storageclass is compliant for shared datastores", func() {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		ginkgo.By(fmt.Sprintf("Invoking test for storage policy: %s", storagePolicyNameForSharedDatastores))
		scParameters := make(map[string]string)
		scParameters[scParamStoragePolicyName] = storagePolicyNameForSharedDatastores
		verifyStoragePolicyBasedVolumeProvisioning(f, client, namespace, scParameters)
	})

	ginkgo.It("Verify dynamic volume provisioning fails when storage policy specified in the storageclass is compliant for non-shared datastores", func() {
		storagePolicyNameForNonSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForNonSharedDatastores)
		ginkgo.By(fmt.Sprintf("Invoking test for storage policy: %s", storagePolicyNameForNonSharedDatastores))
		scParameters := make(map[string]string)
		scParameters[scParamStoragePolicyName] = storagePolicyNameForNonSharedDatastores

		err := invokeInvalidPolicyTestNeg(client, namespace, scParameters)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrorMsg := "No compatible datastore found for storagePolicy"
		e2elog.Logf(fmt.Sprintf("expected error: %+q", expectedErrorMsg))
		if !strings.Contains(err.Error(), expectedErrorMsg) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), expectedErrorMsg)
		}
	})

	ginkgo.It("Verify non-existing SPBM policy is not honored for dynamic volume provisioning using storageclass", func() {
		ginkgo.By(fmt.Sprintf("Invoking test for SPBM policy: %s", f.Namespace.Name))
		scParameters := make(map[string]string)
		scParameters[scParamStoragePolicyName] = f.Namespace.Name

		err := invokeInvalidPolicyTestNeg(client, namespace, scParameters)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrorMsg := "no pbm profile found with name: \"" + f.Namespace.Name + "\""
		e2elog.Logf(fmt.Sprintf("expected error: %+q", expectedErrorMsg))
		if !strings.Contains(err.Error(), expectedErrorMsg) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), expectedErrorMsg)
		}
	})

})

// verifyStoragePolicyBasedVolumeProvisioning helps invokes storage policy related positive e2e tests
func verifyStoragePolicyBasedVolumeProvisioning(f *framework.Framework, client clientset.Interface, namespace string, scParameters map[string]string) {
	storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
	defer framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)

	ginkgo.By("Waiting for claim to be in bound phase")
	ginkgo.By("Expect claim to pass provisioning volume as shared datastore")
	err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verifying if volume is provisioned using specified storage policy")
	pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
	ok, err := e2eVSphere.VerifySpbmPolicyOfVolume(pv.Spec.CSI.VolumeHandle, scParameters[scParamStoragePolicyName])
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(ok).To(gomega.BeTrue(), fmt.Sprintf("storage policy verification failed"))

	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is attached to the node")
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node"))

	ginkgo.By("Deleting the pod")
	framework.DeletePodWithWait(f, client, pod)

	ginkgo.By("Verify volume is detached from the node")
	isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
}

// invokeInvalidPolicyTestNeg helps invokes storage policy related negative e2e tests
func invokeInvalidPolicyTestNeg(client clientset.Interface, namespace string, scParameters map[string]string) error {
	storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create a StorageClasse. Error: %v", err))
	defer client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
	defer framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)

	ginkgo.By("Waiting for claim to be in bound phase")
	err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, poll, pollTimeoutShort)
	gomega.Expect(err).To(gomega.HaveOccurred())

	// eventList contains the events related to pvc
	eventList, _ := client.CoreV1().Events(pvclaim.Namespace).List(metav1.ListOptions{})
	errMsg := eventList.Items[len(eventList.Items)-1].Message
	e2elog.Logf(fmt.Sprintf("Actual failure message: %+q", errMsg))
	return fmt.Errorf(errMsg)
}
