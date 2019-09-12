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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
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
		client                clientset.Interface
		namespace             string
		isK8SVanillaTestSetup bool
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		isK8SVanillaTestSetup = GetAndExpectBoolEnvVar(envK8SVanillaTestSetup)
		if isK8SVanillaTestSetup {
			namespace = f.Namespace.Name
		} else {
			namespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
		}
		bootstrap()
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	ginkgo.AfterEach(func() {
		if !isK8SVanillaTestSetup {
			deleteResourceQuota(client, namespace)
		}
	})

	ginkgo.It("[csi-common-e2e] Verify dynamic volume provisioning works when storage policy specified in the storageclass is compliant for shared datastores", func() {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		ginkgo.By(fmt.Sprintf("Invoking test for storage policy: %s", storagePolicyNameForSharedDatastores))
		scParameters := make(map[string]string)

		// decide which test setup is available to run
		if isK8SVanillaTestSetup {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = storagePolicyNameForSharedDatastores
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyNameForSharedDatastores)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyNameForSharedDatastores)
		}
		verifyStoragePolicyBasedVolumeProvisioning(f, client, namespace, scParameters, isK8SVanillaTestSetup, storagePolicyNameForSharedDatastores)
	})

	ginkgo.It("[csi-common-e2e] Verify dynamic volume provisioning fails when storage policy specified in the storageclass is compliant for non-shared datastores", func() {
		storagePolicyNameForNonSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForNonSharedDatastores)
		ginkgo.By(fmt.Sprintf("Invoking test for storage policy: %s", storagePolicyNameForNonSharedDatastores))
		scParameters := make(map[string]string)

		// decide which test setup is available to run
		if isK8SVanillaTestSetup {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = storagePolicyNameForNonSharedDatastores
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyNameForNonSharedDatastores)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyNameForNonSharedDatastores)
		}

		err := invokeInvalidPolicyTestNeg(client, namespace, scParameters, isK8SVanillaTestSetup, storagePolicyNameForNonSharedDatastores)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrorMsg := "No compatible datastore found for storagePolicy"
		framework.Logf(fmt.Sprintf("expected error: %+q", expectedErrorMsg))
		if !strings.Contains(err.Error(), expectedErrorMsg) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), expectedErrorMsg)
		}
	})

	ginkgo.It("Verify non-existing SPBM policy is not honored for dynamic volume provisioning using storageclass", func() {
		ginkgo.By(fmt.Sprintf("Invoking test for SPBM policy: %s", f.Namespace.Name))
		scParameters := make(map[string]string)
		scParameters[scParamStoragePolicyName] = f.Namespace.Name

		err := invokeInvalidPolicyTestNeg(client, namespace, scParameters, isK8SVanillaTestSetup, scParamStoragePolicyName)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrorMsg := "no pbm profile found with name: \"" + f.Namespace.Name + "\""
		framework.Logf(fmt.Sprintf("expected error: %+q", expectedErrorMsg))
		if !strings.Contains(err.Error(), expectedErrorMsg) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), expectedErrorMsg)
		}
	})

})

// verifyStoragePolicyBasedVolumeProvisioning helps invokes storage policy related positive e2e tests
func verifyStoragePolicyBasedVolumeProvisioning(f *framework.Framework, client clientset.Interface, namespace string, scParameters map[string]string, isK8SVanillaTestSetup bool, storagePolicyName string) {

	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error
	// decide which test setup is available to run
	if isK8SVanillaTestSetup {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "")
	} else {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", storagePolicyName)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
	defer framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)

	ginkgo.By("Waiting for claim to be in bound phase")
	ginkgo.By("Expect claim to pass provisioning volume as shared datastore")
	err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verifying if volume is provisioned using specified storage policy")
	pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
	volumeID := pv.Spec.CSI.VolumeHandle
	storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, storagePolicyName)
	gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), fmt.Sprintf("storage policy verification failed"))

	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var vmUUID string
	var exists bool
	nodeName := pod.Spec.NodeName
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if isK8SVanillaTestSetup {
		vmUUID = getNodeUUID(client, nodeName)
	} else {
		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node"))

	ginkgo.By("Deleting the pod")
	framework.DeletePodWithWait(f, client, pod)
	if isK8SVanillaTestSetup {
		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached to the node: %s", volumeID, nodeName))
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volumeID, nodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", volumeID, nodeName))
	} else {
		ginkgo.By("Wait for 3 minutes for the pod to get terminated successfully")
		time.Sleep(supervisorClusterOperationsTimeout)
		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", volumeID, nodeName))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", vmUUID, nodeName))
	}
}

// invokeInvalidPolicyTestNeg helps invokes storage policy related negative e2e tests
func invokeInvalidPolicyTestNeg(client clientset.Interface, namespace string, scParameters map[string]string, isK8SVanillaTestSetup bool, storagePolicyName string) error {

	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error
	// decide which test setup is available to run
	if isK8SVanillaTestSetup {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "")
	} else {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", storagePolicyName)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create a StorageClasse. Error: %v", err))
	defer client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
	defer framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)

	ginkgo.By("Waiting for claim to be in bound phase")
	err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, poll, pollTimeoutShort)
	gomega.Expect(err).To(gomega.HaveOccurred())

	// eventList contains the events related to pvc
	eventList, _ := client.CoreV1().Events(pvclaim.Namespace).List(metav1.ListOptions{})
	errMsg := eventList.Items[len(eventList.Items)-1].Message
	framework.Logf(fmt.Sprintf("Actual failure message: %+q", errMsg))
	return fmt.Errorf(errMsg)
}
