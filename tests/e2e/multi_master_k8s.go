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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	admissionapi "k8s.io/pod-security-admission/api"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
)

var _ = ginkgo.Describe("[csi-multi-master-block-e2e]", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-multi-master-k8s")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		namespace           string
		controllerNamespace string
		sc                  *storagev1.StorageClass
		pvc                 *v1.PersistentVolumeClaim
		pvs                 []*v1.PersistentVolume
		err                 error
		client              clientset.Interface
		storagePolicyName   string
		labelKey            string
		labelValue          string
		scParameters        map[string]string
		nodeNameIPMap       map[string]string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		if vanillaCluster {
			namespace = f.Namespace.Name
			controllerNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		} else {
			namespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
			controllerNamespace = csiSystemNamespace
		}
		bootstrap()
		var err error
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		nodeNameIPMap = make(map[string]string)
		ginkgo.By("Retrieving testbed configuration data")
		err = mapK8sMasterNodeWithIPs(client, nodeNameIPMap)
		framework.ExpectNoError(err)

		labelKey = "app"
		labelValue = "e2e-multi-master"

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Performing test cleanup")
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}

		if pvc != nil {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		for _, pv := range pvs {
			err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll, framework.PodDeleteTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from kubernetes",
					pv.Spec.CSI.VolumeHandle))
		}

		if sc != nil {
			if !supervisorCluster {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By("Waiting for old vsphere-csi-controller pod to be removed")
		err = waitForControllerDeletion(ctx, client, controllerNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if supervisorCluster {
			dumpSvcNsEventsOnTestFailure(client, namespace)
		} else {
			svcClient, svNamespace := getSvcClientAndNamespace()
			dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}
	})

	/*
		Test performs following operations

		Steps
		1. Create a storage class.
		2. Create a PVC using the storage class and wait for PVC to be bound.
		3. Update PVC label and verify the PVC label has been updated
		4. Power off the node where vsphere-csi-controller pod is running
		5. Wait for a new pod of vsphere-csi-controller pod to be scheduled and running
		6. Update the PV label and verify the PV label has been updated
		7. Delete all PVCs from the tests namespace.
		8. Delete the storage class.
	*/
	ginkgo.It("[pq-vanilla-block]Power off the node where vsphere-csi-controller pod is running", ginkgo.Label(p0,
		block, vanilla, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, podList := getControllerRuntimeDetails(client, controllerNamespace)
		ginkgo.By(fmt.Sprintf("vsphere-csi-controller pod(s) %+v is running on node(s) %+v", podList, nodeList))
		gomega.Expect(len(podList) == 1).To(gomega.BeTrue(), "Number of vsphere-csi-controller pod running is not 1")

		ginkgo.By("Create a pvc and wait for PVC to bound")
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil,
				scParameters, "", nil, "", true, "", storagePolicyName)
		}

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err = WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// power off the node where vsphere-csi-controller pod is currently running
		nodeNameOfvSphereCSIControllerPod := nodeList[0]
		ginkgo.By(fmt.Sprintf("Power off the node: %v", nodeNameOfvSphereCSIControllerPod))
		vmUUID := getNodeUUID(ctx, client, nodeNameOfvSphereCSIControllerPod)
		gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
		framework.Logf("VM uuid is: %s for node: %s", vmUUID, nodeNameOfvSphereCSIControllerPod)
		vmRef, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("vmRef: %+v for the VM uuid: %s", vmRef, vmUUID)
		gomega.Expect(vmRef).NotTo(gomega.BeNil(), "vmRef should not be nil")
		vm := object.NewVirtualMachine(e2eVSphere.Client.Client, vmRef.Reference())
		_, err = vm.PowerOff(ctx)
		framework.ExpectNoError(err)

		err = vm.WaitForPowerState(ctx, vimtypes.VirtualMachinePowerStatePoweredOff)
		framework.ExpectNoError(err, "Unable to power off the node")

		ginkgo.By("Wait for k8s to schedule the pod on other node")
		time.Sleep(k8sPodTerminationTimeOutLong)

		ginkgo.By("vsphere-csi-controller pod should be rescheduled to other node")
		newNodeList, newPodList := getControllerRuntimeDetails(client, controllerNamespace)
		ginkgo.By(fmt.Sprintf("vsphere-csi-controller pod(s) %+v is running on node(s) %+v", newPodList, newNodeList))
		gomega.Expect(len(newPodList) == 2).To(gomega.BeTrue(), "Number of vsphere-csi-controller pod running is not 2")

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels
		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// power on vm
		_, err = vm.PowerOn(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Test performs following operations

		Steps
		1. Create a storage class.
		2. Create a PVC using the storage class and wait for PVC to be bound.
		3. Update PVC label and verify the PVC label has been updated
		4. Stop the kubelet on the node where vsphere-csi-controller pod is running
		5. Wait for a new pod of vsphere-csi-controller pod to be scheduled and running
		6. Start the kubelet on  the node which was stopped in step 4
		7. Update the PV label and verify the PV label has been updated
		8. Delete all PVCs from the tests namespace.
		9. Delete the storage class.
	*/

	ginkgo.It("[csi-block-vanilla][csi-supervisor][pq-vanilla-block] Stop kubelet on the node where "+
		"vsphere-csi-controller pod is running", ginkgo.Label(p0, block, vanilla, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, podList := getControllerRuntimeDetails(client, controllerNamespace)
		ginkgo.By(fmt.Sprintf("vsphere-csi-controller pod(s) %+v is running on node(s) %+v", podList, nodeList))
		gomega.Expect(len(podList) == 1).To(gomega.BeTrue(), "Number of vsphere-csi-controller pod running is not 1")

		ginkgo.By("Create a pvc and wait for PVC to bound")
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil,
				scParameters, "", nil, "", false, "", storagePolicyName)
		}

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err = WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// stop the kubelet on the node where vsphere-csi-controller pod is currently running
		nodeNameOfvSphereCSIControllerPod := nodeList[0]
		ginkgo.By(fmt.Sprintf("Stop kubelet for node %s with IP %s",
			nodeNameOfvSphereCSIControllerPod, nodeNameIPMap[nodeNameOfvSphereCSIControllerPod]))

		sshCmd := "systemctl stop kubelet.service"
		ip, portNum, err := getPortNumAndIP(nodeNameOfvSphereCSIControllerPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		host := nodeNameIPMap[ip] + ":" + portNum
		ginkgo.By(fmt.Sprintf("Invoking command %+v on host %+v", sshCmd, host))
		result, err := fssh.SSH(ctx, sshCmd, host, framework.TestContext.Provider)
		ginkgo.By(fmt.Sprintf("%s returned result %s", sshCmd, result.Stdout))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result.Code == 0).To(gomega.BeTrue())

		ginkgo.By("Wait for k8s to schedule the pod on other node")
		time.Sleep(k8sPodTerminationTimeOutLong)

		ginkgo.By("vsphere-csi-controller pod should be rescheduled to other node")
		newNodeList, newPodList := getControllerRuntimeDetails(client, controllerNamespace)
		ginkgo.By(fmt.Sprintf("vsphere-csi-controller pod(s) %+v is running on node(s) %+v", newPodList, newNodeList))
		gomega.Expect(len(newPodList) == 2).To(gomega.BeTrue(), "Number of vsphere-csi-controller pod running is not 2")

		// start the kubelet
		sshCmd = "systemctl start kubelet"
		ginkgo.By(fmt.Sprintf("Invoking command %+v on host %+v", sshCmd, host))
		result, err = fssh.SSH(ctx, sshCmd, host, framework.TestContext.Provider)
		ginkgo.By(fmt.Sprintf("%s returned result %s", sshCmd, result.Stdout))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result.Code == 0).To(gomega.BeTrue())

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels
		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})
})
