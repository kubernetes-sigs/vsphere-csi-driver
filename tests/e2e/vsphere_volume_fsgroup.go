/*
Copyright 2021 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

/*

Test to verify fsgroup specified in pod is being honored after pod creation.

Steps
1. Create StorageClass without FStype mentioned.
2. Create PVC which uses the StorageClass created in step 1.
3. Wait for PV to be provisioned.
4. Wait for PVC's status to become Bound.
5. Create pod using PVC on specific node with securitycontext
6. Wait for Disk to be attached to the node.
7. Delete pod and Wait for Volume Disk to be detached from the Node.
8. Delete PVC, PV and Storage Class.

*/

var _ = ginkgo.Describe("[ef-vks] [csi-block-vanilla] [csi-file-vanilla] [csi-guest] [csi-supervisor] "+
	"[csi-block-vanilla-parallelized] Volume Filesystem Group Test", func() {
	f := framework.NewDefaultFramework("volume-fsgroup")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		scParameters      map[string]string
		storagePolicyName string
		datastoreURL      string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})
	ginkgo.AfterEach(func() {
		if supervisorCluster {
			dumpSvcNsEventsOnTestFailure(client, namespace)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}
	})

	// Test for Pod creation works when SecurityContext has FSGroup
	ginkgo.It("[cf-vanilla-file][cf-vanilla-block] Verify Pod Creation works when SecurityContext has "+
		"FSGroup", ginkgo.Label(p0, vanilla, block, file, wcp, tkg, core, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var fsGroup int64
		var runAsUser int64

		ginkgo.By("Creating a PVC")

		// Create a StorageClass
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamDatastoreURL] = datastoreURL
			storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, diskSize, nil, "", false, "")
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, diskSize, nil, "", true, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !supervisorCluster {
				err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")

		fsGroup = 1000
		runAsUser = 2000

		fsGroupInt64 := &fsGroup
		runAsUserInt64 := &runAsUser
		pod, err := createPodForFSGroup(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
			false, execCommand, fsGroupInt64, runAsUserInt64)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv := persistentvolumes[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		var vmUUID string
		var exists bool
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Get volume ID from Supervisor cluster
			volumeID = getVolumeIDFromSupervisorCluster(volumeID)
		} else {
			annotations := pod.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		}

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and filegroup type is as expected")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"ls -lh /mnt/volume1/fstype "}
		output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, strconv.Itoa(int(fsGroup)))).NotTo(gomega.BeFalse())
		gomega.Expect(strings.Contains(output, strconv.Itoa(int(runAsUser)))).NotTo(gomega.BeFalse())

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if supervisorCluster {
			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", volumeID, vmUUID))
			_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(),
				fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
					vmUUID, pv.Spec.CSI.VolumeHandle))
		} else {
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volumeID, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		}
	})
})
