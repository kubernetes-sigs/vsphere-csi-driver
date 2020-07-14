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
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/kubernetes/test/e2e/framework"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	vimtypes "github.com/vmware/govmomi/vim25/types"
)

var _ = ginkgo.Describe("[csi-multi-master-block-e2e]", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-multi-master-k8s")
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
			controllerNamespace = kubeSystemNamespace
		} else {
			namespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
			controllerNamespace = csiSystemNamespace
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		nodeNameIPMap = make(map[string]string)
		ginkgo.By("Retrieving testbed configuration data")
		err := getTestbedConfig(client, nodeNameIPMap)
		framework.ExpectNoError(err)

		labelKey = "app"
		labelValue = "e2e-multi-master"

	})

	ginkgo.AfterEach(func() {
		ginkgo.By("Performing test cleanup")
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}

		if pvc != nil {
			err = framework.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		for _, pv := range pvs {
			err = framework.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll, framework.PodDeleteTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from kubernetes", pv.Spec.CSI.VolumeHandle))
		}

		if sc != nil {
			err = client.StorageV1().StorageClasses().Delete(sc.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Waiting for old vsphere-csi-controller pod to be removed")
		err = waitForControllerDeletion(client, controllerNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	ginkgo.It("Power off the node where vsphere-csi-controller pod is running", func() {
		nodeList, podList := getControllerRuntimeDetails(client, controllerNamespace)
		ginkgo.By(fmt.Sprintf("vsphere-csi-controller pod(s) %+v is running on node(s) %+v", podList, nodeList))
		gomega.Expect(len(podList) == 1).To(gomega.BeTrue(), "Number of vsphere-csi-controller pod running is not 1")

		ginkgo.By("Create a pvc and wait for PVC to bound")
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "", storagePolicyName)
		}

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err = framework.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// power off the node where vsphere-csi-controller pod is currently running
		nodeNameOfvSphereCSIControllerPod := nodeList[0]
		ginkgo.By(fmt.Sprintf("Power off the node: %v", nodeNameOfvSphereCSIControllerPod))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		vmUUID := getNodeUUID(client, nodeNameOfvSphereCSIControllerPod)
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
		_, err = client.CoreV1().PersistentVolumes().Update(pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
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

	ginkgo.It("[csi-block-vanilla] [csi-supervisor] Stop kubelet on the node where vsphere-csi-controller pod is running", func() {
		nodeList, podList := getControllerRuntimeDetails(client, controllerNamespace)
		ginkgo.By(fmt.Sprintf("vsphere-csi-controller pod(s) %+v is running on node(s) %+v", podList, nodeList))
		gomega.Expect(len(podList) == 1).To(gomega.BeTrue(), "Number of vsphere-csi-controller pod running is not 1")

		ginkgo.By("Create a pvc and wait for PVC to bound")
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "", storagePolicyName)
		}

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err = framework.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// stop the kubelet on the node where vsphere-csi-controller pod is currently running
		nodeNameOfvSphereCSIControllerPod := nodeList[0]
		ginkgo.By(fmt.Sprintf("Stop kubelet for node %s with IP %s", nodeNameOfvSphereCSIControllerPod, nodeNameIPMap[nodeNameOfvSphereCSIControllerPod]))

		sshCmd := "systemctl stop kubelet.service"
		host := nodeNameIPMap[nodeNameOfvSphereCSIControllerPod] + ":22"
		ginkgo.By(fmt.Sprintf("Invoking command %+v on host %+v", sshCmd, host))
		result, err := framework.SSH(sshCmd, host, framework.TestContext.Provider)
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
		result, err = framework.SSH(sshCmd, host, framework.TestContext.Provider)
		ginkgo.By(fmt.Sprintf("%s returned result %s", sshCmd, result.Stdout))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result.Code == 0).To(gomega.BeTrue())

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels
		_, err = client.CoreV1().PersistentVolumes().Update(pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})
})

// getControllerRuntimeDetails return the NodeName and PodName for vSphereCSIControllerPod running
func getControllerRuntimeDetails(client clientset.Interface, nameSpace string) ([]string, []string) {
	pods, _ := client.CoreV1().Pods(nameSpace).List(metav1.ListOptions{
		FieldSelector: fields.SelectorFromSet(fields.Set{"status.phase": string(v1.PodRunning)}).String(),
	})
	var nodeNameList []string
	var podNameList []string
	for _, pod := range pods.Items {
		if strings.HasPrefix(pod.Name, vSphereCSIControllerPodNamePrefix) {
			framework.Logf(fmt.Sprintf("Found vSphereCSIController pod %s PodStatus %s", pod.Name, pod.Status.Phase))
			nodeNameList = append(nodeNameList, pod.Spec.NodeName)
			podNameList = append(podNameList, pod.Name)
		}
	}
	return nodeNameList, podNameList
}

// waitForControllerDeletion wait for the controller pod to be deleted
func waitForControllerDeletion(client clientset.Interface, namespace string) error {
	err := wait.Poll(poll, k8sPodTerminationTimeOutLong, func() (bool, error) {
		_, podNameList := getControllerRuntimeDetails(client, namespace)
		if len(podNameList) == 1 {
			framework.Logf("old vsphere-csi-controller pod  has been successfully deleted")
			return true, nil
		}
		framework.Logf("waiting for old vsphere-csi-controller pod to be deleted.")
		return false, nil
	})

	return err

}

// getTestbedConfig returns master node name and IP from K8S testbed
func getTestbedConfig(client clientset.Interface, nodeNameIPMap map[string]string) error {

	nodes, err := client.CoreV1().Nodes().List(metav1.ListOptions{LabelSelector: "node-role.kubernetes.io/master"})
	if err != nil {
		return err
	}
	if len(nodes.Items) <= 1 {
		return errors.New("K8S testbed does not have more than one master nodes")
	}
	for _, node := range nodes.Items {
		for _, addr := range node.Status.Addresses {
			if addr.Type == v1.NodeExternalIP && addr.Address != "" && net.ParseIP(addr.Address) != nil {
				framework.Logf("Found master node name %s with external IP %s", node.Name, addr.Address)
				nodeNameIPMap[node.Name] = addr.Address
			} else if addr.Type == v1.NodeInternalIP && addr.Address != "" && net.ParseIP(addr.Address) != nil {
				framework.Logf("Found master node name %s with internal IP %s", node.Name, addr.Address)
				nodeNameIPMap[node.Name] = addr.Address
			}
			if _, ok := nodeNameIPMap[node.Name]; !ok {
				framework.Logf("No IP address is found for master node %s", node.Name)
				return fmt.Errorf("IP address is not found for node %s", node.Name)
			}
		}
	}

	return nil
}
