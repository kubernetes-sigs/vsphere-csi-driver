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

package linked_clone

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vmservice_vm"

	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
)

var _ bool = ginkgo.Describe("[linked-clone-vms] Linked-Clone-vms", func() {

	f := framework.NewDefaultFramework("linked-clone")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true
	var (
		client          clientset.Interface
		namespace       string
		storageclass    *v1.StorageClass
		err             error
		vcRestSessionId string
		statuscode      int
		doCleanup       bool
		vmClass         string
		vmopC           ctlrclient.Client
		cnsopC          ctlrclient.Client
		storagePolicy   string
		contentLibId    string
	)

	var e2eTestConfigvm *config.E2eTestConfig

	ginkgo.BeforeEach(func() {
		e2eTestConfigvm = bootstrap.Bootstrap()
		client = f.ClientSet
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// doCleanup set to false, to avoild individual resource cleaning up.
		doCleanup = false

		// Read Env variable needed for the test suite
		storagePolicy = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicy)

		// Get the storageclass from storagepolicy
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// reading vc session id
		if vcRestSessionId == "" {
			vcRestSessionId = k8testutil.CreateVcSession4RestApis(ctx, e2eTestConfigvm)
		}

		storagePolicyId := vcutil.GetSpbmPolicyID(storagePolicy, e2eTestConfigvm)

		// fetch shared vsphere datatsore url
		datastoreURL := env.GetAndExpectStringEnvVar(constants.EnvSharedDatastoreURL)
		dsRef := vcutil.GetDsMoRefFromURL(ctx, e2eTestConfigvm, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		// read or create content library if it is empty
		if contentLibId == "" {
			contentLibId, err = vmservice_vm.CreateAndOrGetContentlibId4Url(e2eTestConfigvm, vcRestSessionId, env.GetAndExpectStringEnvVar(constants.EnvContentLibraryUrl),
				dsRef.Value)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// get or set vm class required for VM creation
		vmClass = os.Getenv(constants.EnvVMClass)
		if vmClass == "" {
			vmClass = constants.VmClassBestEffortSmall
		}
		/* Sets up a Kubernetes client with a custom scheme, adds the vmopv1 API types to the scheme,
		and ensures that the client is properly initialized without errors */
		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// creating namespace with storagePolicy
		namespace, statuscode, err = k8testutil.CreatetWcpNsWithZonesAndPolicies(e2eTestConfigvm, vcRestSessionId,
			[]string{storagePolicyId}, k8testutil.GetSvcId(vcRestSessionId, e2eTestConfigvm),
			[]string{}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(204))

		//After NS creation need sometime to load usage CRs
		time.Sleep(constants.PollTimeoutShort)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		framework.Logf("Collecting supervisor PVC events before performing PV/PVC cleanup")
		k8testutil.DumpSvcNsEventsOnTestFailure(client, namespace)
		eventList, err := client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, item := range eventList.Items {
			framework.Logf("%q", item.Message)
		}

		// Cleanup the resources created in the test
		if doCleanup {
			k8testutil.Cleanup(ctx, client, e2eTestConfigvm, namespace)
		}

		// Delete namespace
		k8testutil.DelTestWcpNs(e2eTestConfigvm, vcRestSessionId, namespace)
		gomega.Expect(k8testutil.WaitForNamespaceToGetDeleted(ctx, client, namespace, constants.Poll, constants.PollTimeout)).To(gomega.Succeed())

	})

	// TC-38
	ginkgo.It("Verify VM service VM can be created on LC PVC attached to pod.", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Create a linked clone (PVC) from a snapshot ")

		// Create PVC, pod and volume snapshot
		_, _, volumeSnapshot := k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfigvm, client, namespace, storageclass, true, false)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfigvm)

		ginkgo.By("Create vm service vm")
		_, vm, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{linkdeClonePvc}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*corev1.PersistentVolumeClaim{linkdeClonePvc})).NotTo(gomega.HaveOccurred())

		framework.Logf("Ending test: Verify VM service VM can be created on LC PVC attached to pod. ")
	})

	// TC-39
	ginkgo.It("Create a snapshot from a linked clone attached to a VM service VM.", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Create a snapshot from a linked clone attached to a VM service VM")

		// Create PVC, and volume snapshot
		pvc, _, volumeSnapshot := k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfigvm, client, namespace, storageclass, false, false)

		ginkgo.By("Create vm service vm")
		_, vm, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{pvc}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*corev1.PersistentVolumeClaim{pvc})).NotTo(gomega.HaveOccurred())

		// create linked clone PVC and verify its bound
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfigvm)

		ginkgo.By("Create vm service vm")
		_, vmLc, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{linkdeClonePvc}, vmClass, storageclass.Name, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vmLc.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vmLc,
			[]*corev1.PersistentVolumeClaim{linkdeClonePvc})).NotTo(gomega.HaveOccurred())

		// Power-off the VM
		ginkgo.By("Power off vm1")
		vmLc = vmservice_vm.SetVmPowerState(ctx, vmopC, vmLc, vmopv1.VirtualMachinePoweredOff)
		_, err = vmservice_vm.Wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vmLc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a snapshot from the linked clone PVC
		_, _ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfigvm, namespace, linkdeClonePvc, lcPv, constants.DiskSize)

		//TODO Check the quota usage

		framework.Logf("Ending test: Create a snapshot from a linked clone attached to a VM service VM")
	})

	// TC-40
	ginkgo.It("Verify LC can be created on a PVC attached to VM which is power off", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Verify LC can be created on a PVC attached to VM which is power off")

		// Create PVC, and volume snapshot
		pvc, _, volumeSnapshot := k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfigvm, client, namespace, storageclass, false, false)

		ginkgo.By("Create vm service vm")
		_, vm, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{pvc}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*corev1.PersistentVolumeClaim{pvc})).NotTo(gomega.HaveOccurred())

		// Power-off the VM
		ginkgo.By("Power off vm")
		vm = vmservice_vm.SetVmPowerState(ctx, vmopC, vm, vmopv1.VirtualMachinePoweredOff)
		_, err = vmservice_vm.Wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create linked clone PVC and verify its bound
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfigvm)

		ginkgo.By("Create vm service vm")
		_, vmLc, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{linkdeClonePvc}, vmClass, storageclass.Name, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vmLc.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vmLc,
			[]*corev1.PersistentVolumeClaim{linkdeClonePvc})).NotTo(gomega.HaveOccurred())

		// Create a snapshot from the linked clone PVC
		_, _ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfigvm, namespace, linkdeClonePvc, lcPv, constants.DiskSize)

		//TODO Check the quota usage

		framework.Logf("Ending test: Verify LC can be created on a PVC attached to VM which is power off")
	})

	// TC-41
	ginkgo.It("Verify LC can be created on a PVC attached to VM which is power on", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Verify LC can be created on a PVC attached to VM which is power on")

		// Create PVC, and volume snapshot
		pvc, _, volumeSnapshot := k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfigvm, client, namespace, storageclass, false, false)

		ginkgo.By("Create vm service vm")
		_, vm, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{pvc}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*corev1.PersistentVolumeClaim{pvc})).NotTo(gomega.HaveOccurred())

		// create linked clone PVC and verify its bound
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfigvm)

		ginkgo.By("Create vm service vm")
		_, vmLc, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{linkdeClonePvc}, vmClass, storageclass.Name, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vmLc.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vmLc,
			[]*corev1.PersistentVolumeClaim{linkdeClonePvc})).NotTo(gomega.HaveOccurred())

		// Create a snapshot from the linked clone PVC
		_, _ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfigvm, namespace, linkdeClonePvc, lcPv, constants.DiskSize)

		ginkgo.By("Detach pvc3 from vm3")
		vmLc, err = vmservice_vm.GetVmsvcVM(ctx, vmopC, vmLc.Namespace, vmLc.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmLc.Spec.Volumes = []vmopv1.VirtualMachineVolume{}
		err = vmopC.Update(ctx, vmLc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmservice_vm.Wait4Pvc2Detach(ctx, vmopC, vmLc, linkdeClonePvc)

		framework.Logf("Ending test: Verify LC can be created on a PVC attached to VM which is power on")
	})

	// TC-42
	ginkgo.It("Verify LC PVC can be attached to VM created on parent PVC", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Verify LC PVC can be attached to VM created on parent PVC")

		// Create PVC, and volume snapshot
		pvc, _, volumeSnapshot := k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfigvm, client, namespace, storageclass, false, false)

		ginkgo.By("Create vm service vm")
		_, vm, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{pvc}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*corev1.PersistentVolumeClaim{pvc})).NotTo(gomega.HaveOccurred())

		// create linked clone PVC and verify its bound
		linkdeClonePvc, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfigvm)

		// attch parent vm to lc-vm
		ginkgo.By("Attach LC_PVC to vm")
		vm, err = vmservice_vm.GetVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm.Spec.Volumes = append(vm.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: linkdeClonePvc.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{ClaimName: linkdeClonePvc.Name},
			}})
		err = vmopC.Update(ctx, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Ending test: Verify LC PVC can be attached to VM created on parent PVC")
	})

	// TC-44
	ginkgo.It("Validate LC PVC-VM can be created with WFFC binding", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Validate LC PVC can be created with WFFC binding-vm ")

		// Fetch latebinding storage class
		spWffc := storagePolicy + "-latebinding"
		storageclassWffc, err := client.StorageV1().StorageClasses().Get(ctx, spWffc, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Create PVC
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		pvc, err := k8testutil.CreatePVC(ctx, client, namespace, labelsMap, "", storageclassWffc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vm service vm")
		_, vm, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{pvc}, vmClass, storageclassWffc.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*corev1.PersistentVolumeClaim{pvc})).NotTo(gomega.HaveOccurred())

		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*corev1.PersistentVolumeClaim{pvc}, constants.PollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]

		// Create snapshot
		volumeSnapshot, _ := k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfigvm, namespace, pvc, []*corev1.PersistentVolume{pv}, constants.DiskSize)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, err := k8testutil.CreateLinkedClonePvc(ctx, client, namespace, storageclassWffc, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create PVC: %v", err))

		// attch parent vm to lc-vm
		ginkgo.By("Attach LC_PVC to vm")
		vm, err = vmservice_vm.GetVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm.Spec.Volumes = append(vm.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: linkdeClonePvc.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{ClaimName: linkdeClonePvc.Name},
			}})
		err = vmopC.Update(ctx, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Validate PVC is bound
		_, err = fpv.WaitForPVClaimBoundPhase(ctx,
			client, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Ending test: Validate LC PVC can be created with WFFC binding-vm ")
	})

})
