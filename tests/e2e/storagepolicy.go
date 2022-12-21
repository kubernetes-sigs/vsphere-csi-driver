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
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"

	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

// Tests to verify SPBM based dynamic volume provisioning using CSI Driver in
// Kubernetes.
//
// Steps
// 1. Create StorageClass with.
//    a. policy which is compliant to shared datastores / policy which is
//       compliant to non-shared datastores / invalid non-existent policy.
// 2. Create PVC which uses the StorageClass created in step 1.
// 3. Wait for PV to be provisioned if policy is valid and compliant to shared
//    datastores, else expect failure.
// 4. Wait for PVC's status to become bound, if policy is valid and compliant.
// 5. Create pod using PVC.
// 6. Wait for Disk to be attached to the node.
// 7. Delete pod and Wait for Volume Disk to be detached from the Node.
// 8. Delete PVC, PV and Storage Class.

var _ = ginkgo.Describe("[csi-block-vanilla] [csi-block-vanilla-parallelized] "+
	"Storage Policy Based Volume Provisioning", func() {

	f := framework.NewDefaultFramework("e2e-spbm-policy")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}
	})
	ginkgo.AfterEach(func() {
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		}
	})

	ginkgo.It("[csi-supervisor] [csi-guest] Verify dynamic volume provisioning works "+
		"when storage policy specified in the storageclass is compliant for shared datastores", func() {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		ginkgo.By(fmt.Sprintf("Invoking test for storage policy: %s", storagePolicyNameForSharedDatastores))
		scParameters := make(map[string]string)

		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = storagePolicyNameForSharedDatastores
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyNameForSharedDatastores)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyNameForSharedDatastores)
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyNameForSharedDatastores
		}
		verifyStoragePolicyBasedVolumeProvisioning(f, client,
			namespace, scParameters, storagePolicyNameForSharedDatastores)
	})

	ginkgo.It("[csi-supervisor] [csi-guest] Verify dynamic volume provisioning fails "+
		"when storage policy specified in the storageclass is compliant for non-shared datastores", func() {
		storagePolicyNameForNonSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForNonSharedDatastores)
		ginkgo.By(fmt.Sprintf("Invoking test for storage policy: %s", storagePolicyNameForNonSharedDatastores))
		scParameters := make(map[string]string)
		var expectedErrorMsg string = "failed to provision volume"
		var createVolumeWaitTime time.Duration = 1 * time.Minute / 2
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamStoragePolicyName] = storagePolicyNameForNonSharedDatastores
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyNameForNonSharedDatastores)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyNameForNonSharedDatastores)
		} else {
			scParameters[svStorageClassName] = storagePolicyNameForNonSharedDatastores
			createVolumeWaitTime = pollTimeout
		}

		pvc := invokeInvalidPolicyTestNeg(client, namespace, scParameters,
			storagePolicyNameForNonSharedDatastores, createVolumeWaitTime)
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvc.Name)}, expectedErrorMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), expectedErrorMsg)
	})

	ginkgo.It("Verify non-existing SPBM policy is not honored for dynamic volume provisioning "+
		"using storageclass", func() {
		ginkgo.By(fmt.Sprintf("Invoking test for SPBM policy: %s", f.Namespace.Name))
		scParameters := make(map[string]string)
		scParameters[scParamStoragePolicyName] = f.Namespace.Name
		var expectedErrorMsg string
		pvc := invokeInvalidPolicyTestNeg(client, namespace, scParameters, scParamStoragePolicyName, pollTimeoutShort)
		if guestCluster {
			expectedErrorMsg = "Volume parameter StoragePolicyName is not a valid GC CSI parameter"

		} else {
			expectedErrorMsg = "no pbm profile found with name: \"" + f.Namespace.Name + "\""
		}
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvc.Name)}, expectedErrorMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), expectedErrorMsg)
	})
})

// verifyStoragePolicyBasedVolumeProvisioning helps invokes storage policy related positive e2e tests
func verifyStoragePolicyBasedVolumeProvisioning(f *framework.Framework, client clientset.Interface,
	namespace string, scParameters map[string]string, storagePolicyName string) {
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var svcPVCName string // PVC Name in the Supervisor Cluster
	var err error
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// decide which test setup is available to run
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", false, "")
	} else if guestCluster {
		ginkgo.By("CNS_TEST: Running for GC setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", false, "")
	} else {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", false, "", storagePolicyName)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		if !supervisorCluster {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By("Waiting for claim to be in bound phase")
	ginkgo.By("Expect claim to pass provisioning volume as shared datastore")
	err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
		pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
	volumeID := pv.Spec.CSI.VolumeHandle
	ginkgo.By("Verifying if volume is provisioned using specified storage policy")
	if guestCluster {
		// svcPVCName refers to PVC Name in the supervisor cluster
		svcPVCName = volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
	}

	storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, storagePolicyName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")

	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var vmUUID string
	var exists bool
	nodeName := pod.Spec.NodeName
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
	if supervisorCluster {
		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else if vanillaCluster {
		vmUUID = getNodeUUID(ctx, client, nodeName)
	} else {
		vmUUID, _ = getVMUUIDFromNodeName(nodeName)
	}
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
	err = fpod.DeletePodWithWait(client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if supervisorCluster {
		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", volumeID, nodeName))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(),
			fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
				vmUUID, nodeName))
	} else {
		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached to the node: %s", volumeID, nodeName))
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volumeID, nodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volumeID, nodeName))
		if guestCluster {
			ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
			time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
			verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}
	}
}

// invokeInvalidPolicyTestNeg helps invokes storage policy related negative e2e tests
func invokeInvalidPolicyTestNeg(client clientset.Interface, namespace string, scParameters map[string]string,
	storagePolicyName string, createVolumeWaitTime time.Duration) *v1.PersistentVolumeClaim {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error
	// decide which test setup is available to run
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", false, "")
	} else if guestCluster {
		ginkgo.By("CNS_TEST: Running for GC setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", false, "")
	} else {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", false, "", storagePolicyName)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to create a StorageClass. Error: %v", err))

	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By("Waiting for claim to be in bound phase")
	err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
		pvclaim.Namespace, pvclaim.Name, poll, createVolumeWaitTime)
	gomega.Expect(err).To(gomega.HaveOccurred())
	return pvclaim
}
