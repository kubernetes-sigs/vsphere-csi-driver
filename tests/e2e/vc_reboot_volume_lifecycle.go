/*
Copyright 2020 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"
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
)

var _ bool = ginkgo.Describe("Verify volume life_cycle operations works fine after VC Reboots", func() {
	f := framework.NewDefaultFramework("e2e-volume-life-cycle")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		storagePolicyName string
		scParameters      map[string]string
		isVcRebooted      bool
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}
	})
	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if supervisorCluster || guestCluster {
			deleteResourceQuota(client, namespace)
			dumpSvcNsEventsOnTestFailure(client, namespace)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
			dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}
		// restarting pending and stopped services after vc reboot if any
		if isVcRebooted {
			err := checkVcServicesHealthPostReboot(ctx, vcAddress, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"Setup is not in healthy state, Got timed-out waiting for required VC services to be up and running")
		}
	})

	/*
		Steps
		1. Create StorageClass.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become bound
		5. Create pod using PVC.
		6. Wait for Disk to be attached to the node.
		7. Reboot VC
		8. Delete pod and Wait for Volume Disk to be detached from the Node.
		9. Delete PVC, PV and Storage Class
	*/

	ginkgo.It("[ef-f-stretched-svc][pq-f-wcp][csi-block-vanilla][csi-supervisor][csi-guest][csi-block-vanilla-serialized]"+
		"[stretched-svc][pq-vanilla-block][pq-vks][pq-vks-n1][pq-vks-n2] verify volume operations on VC works fine after vc "+
		"reboots", ginkgo.Label(p1, block, wcp, vanilla, tkg, core, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var svcPVCName string
		var err error
		ginkgo.By("Creating Storage Class and PVC")
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, nil, "", nil, "", false, "")
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			ginkgo.By(fmt.Sprintf("storagePolicyName: %s", storagePolicyName))
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, "", nil, "", true, "", storagePolicyName)
		} else if stretchedSVC {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			zonalPolicy := GetAndExpectStringEnvVar(envZonalStoragePolicyName)
			scParameters[svStorageClassName] = zonalPolicy
			storagePolicyName = zonalPolicy
			ginkgo.By(fmt.Sprintf("storagePolicyName: %s", storagePolicyName))
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, "", nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !supervisorCluster || !stretchedSVC {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Waiting for claim to be in bound phase")
		pvc, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc).NotTo(gomega.BeEmpty())
		pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		volumeID := pv.Spec.CSI.VolumeHandle

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster
			svcPVCName = volumeID
			volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}

		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var vmUUID string
		var exists bool
		nodeName := pod.Spec.NodeName
		if supervisorCluster {
			annotations := pod.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
			_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(nodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Rebooting VC")
		err = invokeVCenterReboot(ctx, vcAddress)
		isVcRebooted = true
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")
		var essentialServices []string
		if vanillaCluster {
			essentialServices = []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		} else {
			essentialServices = []string{spsServiceName, vsanhealthServiceName, vpxdServiceName, wcpServiceName}
		}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		//After reboot
		bootstrap()

		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if supervisorCluster || stretchedSVC {
			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", volumeID, nodeName))
			_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(),
				fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
					vmUUID, nodeName))
		}
		if guestCluster {
			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached to the node: %s", volumeID, nodeName))
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volumeID, nodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volumeID, nodeName))
			ginkgo.By(fmt.Sprintf("Waiting for %v seconds to allow CnsNodeVMAttachment controller to reconcile resource",
				waitTimeForCNSNodeVMAttachmentReconciler))
			time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
			verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}
		if vanillaCluster {
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		}
	})
})
