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
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/apimachinery/pkg/util/wait"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/kubernetes/test/e2e/framework"

	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"gitlab.eng.vmware.com/hatchway/govmomi/object"
	vimtypes "gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
)

var _ = ginkgo.Describe("[csi-multi-master-block-e2e]", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-multi-master-k8s")
	var (
		namespace             string
		client                clientset.Interface
		isK8SVanillaTestSetup bool
		storagePolicyName     string
		labelKey              string
		labelValue            string
		scParameters          map[string]string
		nodeNameIPMap         map[string]string
	)
	ginkgo.BeforeEach(func() {
		namespace = f.Namespace.Name
		client = f.ClientSet
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		master1Name := GetAndExpectStringEnvVar(envK8SMaster1Name)
		master1IP := GetAndExpectStringEnvVar(envK8SMaster1IP)

		master2Name := GetAndExpectStringEnvVar(envK8SMaster2Name)
		master2IP := GetAndExpectStringEnvVar(envK8SMaster2IP)

		master3Name := GetAndExpectStringEnvVar(envK8SMaster3Name)
		master3IP := GetAndExpectStringEnvVar(envK8SMaster3IP)

		nodeNameIPMap = make(map[string]string)
		nodeNameIPMap[master1Name] = master1IP
		nodeNameIPMap[master2Name] = master2IP
		nodeNameIPMap[master3Name] = master3IP

		labelKey = "app"
		labelValue = "e2e-multi-master"

	})

	ginkgo.AfterEach(func() {
		if !isK8SVanillaTestSetup {
			deleteResourceQuota(client, namespace)
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
	ginkgo.It("Power off the node where vsphere-csi-controller pod is running", func() {
		nodeList, podList := getControllerRuntimeDetails(client, kubeSystemNamespace)
		ginkgo.By(fmt.Sprintf("vsphere-csi-controller pod(s) %+v is running on node(s) %+v", podList, nodeList))
		gomega.Expect(len(podList) == 1).To(gomega.BeTrue(), "Number of vsphere-csi-controller pod running is not 1")

		ginkgo.By("Create a pvc and wait for PVC to bound")
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		isK8SVanillaTestSetup = GetAndExpectBoolEnvVar(envK8SVanillaTestSetup)
		if isK8SVanillaTestSetup {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", storagePolicyName)
		}

		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer client.StorageV1().StorageClasses().Delete(sc.Name, nil)

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := framework.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
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
		newNodeList, newPodList := getControllerRuntimeDetails(client, kubeSystemNamespace)
		ginkgo.By(fmt.Sprintf("vsphere-csi-controller pod(s) %+v is running on node(s) %+v", newPodList, newNodeList))
		gomega.Expect(len(newPodList) == 2).To(gomega.BeTrue(), "Number of vsphere-csi-controller pod running is not 2")

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels
		_, err = client.CoreV1().PersistentVolumes().Update(pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

        // cleanup
		vm.PowerOn(ctx)

		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(pvc.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be deleted", pv.Spec.CSI.VolumeHandle))
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for old vsphere-csi-controller pod to be removed"))
		err = waitForControllerDeletion(client, kubeSystemNamespace)
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

	ginkgo.It("Stop kubelet on the node where vsphere-csi-controller pod is running", func() {
		nodeList, podList := getControllerRuntimeDetails(client, kubeSystemNamespace)
		ginkgo.By(fmt.Sprintf("vsphere-csi-controller pod(s) %+v is running on node(s) %+v", podList, nodeList))
		gomega.Expect(len(podList) == 1).To(gomega.BeTrue(), "Number of vsphere-csi-controller pod running is not 1")

		ginkgo.By("Create a pvc and wait for PVC to bound")
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		isK8SVanillaTestSetup = GetAndExpectBoolEnvVar(envK8SVanillaTestSetup)
		if isK8SVanillaTestSetup {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", storagePolicyName)
		}

		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer client.StorageV1().StorageClasses().Delete(sc.Name, nil)

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := framework.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
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
		newNodeList, newPodList := getControllerRuntimeDetails(client, kubeSystemNamespace)
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

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(pvc.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be deleted", pv.Spec.CSI.VolumeHandle))
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for old vsphere-csi-controller pod to be removed"))
		err = waitForControllerDeletion(client, kubeSystemNamespace)
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
