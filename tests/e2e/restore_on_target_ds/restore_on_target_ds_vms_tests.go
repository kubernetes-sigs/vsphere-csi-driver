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

package restore_on_target_ds

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/csisnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vmservice_vm"

	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"

	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
)

var _ bool = ginkgo.Describe("[restore-on-target-ds-vms-p0] restore-on-target-ds-vms-p0", func() {

	f := framework.NewDefaultFramework("restore-on-target-ds-vms")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true
	var (
		client          clientset.Interface
		namespace       string
		storageclass    *v1.StorageClass
		restoreSc       *v1.StorageClass
		err             error
		vcRestSessionId string
		statuscode      int
		storagePolicy   string
		restoreSp       string
		contentLibId    string
		vmClass         string
		vmopC           ctlrclient.Client
		cnsopC          ctlrclient.Client
	)

	ginkgo.BeforeEach(func() {
		e2eTestConfig = bootstrap.Bootstrap()
		client = f.ClientSet
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Read Env variable needed for the test suite
		storagePolicy = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicy)
		restoreSp = env.GetAndExpectStringEnvVar(constants.EnvRestoreStoragePolicy)

		// Get the storageclass from storagepolicy
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		restoreSc, err = client.StorageV1().StorageClasses().Get(ctx, restoreSp, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// reading vc session id
		if vcRestSessionId == "" {
			vcRestSessionId = k8testutil.CreateVcSession4RestApis(ctx, e2eTestConfig)
		}

		storagePolicyId := vcutil.GetSpbmPolicyID(storagePolicy, e2eTestConfig)

		// fetch shared vsphere datatsore url
		datastoreURL := env.GetAndExpectStringEnvVar(constants.EnvSharedDatastoreURL)
		dsRef := vcutil.GetDsMoRefFromURL(ctx, e2eTestConfig, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		// read or create content library if it is empty
		if contentLibId == "" {
			contentLibId, err = vmservice_vm.CreateAndOrGetContentlibId4Url(e2eTestConfig, vcRestSessionId, env.GetAndExpectStringEnvVar(constants.EnvContentLibraryUrl),
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
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			namespace, statuscode, err = k8testutil.CreatetWcpNsWithZonesAndPolicies(e2eTestConfig, vcRestSessionId,
				[]string{storagePolicyId}, k8testutil.GetSvcId(vcRestSessionId, e2eTestConfig),
				[]string{}, "", "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(statuscode).To(gomega.Equal(204))
		} else {
			labels_ns := map[string]string{}
			labels_ns[admissionapi.EnforceLevelLabel] = string(admissionapi.LevelPrivileged)
			labels_ns["e2e-framework"] = f.BaseName
			gcNs, err := framework.CreateTestingNS(ctx, f.BaseName, client, labels_ns)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on GC")
			namespace = gcNs.Name
		}

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

		// Delete namespace
		k8testutil.DelTestWcpNs(e2eTestConfig, vcRestSessionId, namespace)
		gomega.Expect(k8testutil.WaitForNamespaceToGetDeleted(ctx, client, namespace, constants.Poll, constants.PollTimeout)).To(gomega.Succeed())

	})

	// TC-12
	ginkgo.It("Attach VM to restored PVC which already attached to original PVC", ginkgo.Label(
		constants.P0, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Attach VM to restored PVC which already attached to original PVC")

		// Create a PVC on sc-1 and attach them to VM
		pvclaim, pvList := k8testutil.CreateAndValidatePvc(ctx, client, namespace, storageclass)

		ginkgo.By("Create vm service vm")
		_, vm, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{pvclaim}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*corev1.PersistentVolumeClaim{pvclaim})).NotTo(gomega.HaveOccurred())

		// Create a snapshot of both the volume.
		volumeSnapshot, _ := csisnapshot.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, pvList, constants.DiskSize)

		//  Restore it on different SC
		ginkgo.By("Restore sanpshots to create new volumes")
		restoredPvc, _, _ := csisnapshot.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, restoreSc,
			volumeSnapshot, constants.DiskSize, false)

		// attach restored pvc to original VM
		ginkgo.By("Attach restored PVC to original vm")
		vm, err = vmservice_vm.GetVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm.Spec.Volumes = append(vm.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: restoredPvc.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{ClaimName: restoredPvc.Name},
			}})
		err = vmopC.Update(ctx, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// TODO : Run volume expansion on both the restored PVCs

		framework.Logf("Ending test: Attach VM to restored PVC which already attached to original PVC")

	})

	// TC -11
	ginkgo.It("Attach VM to restored PVC with detaching original PVC", ginkgo.Label(
		constants.P0, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Attach VM to restored PVC with detaching original PVC")

		// Create a PVC on sc-1 and attach them to VM
		pvclaim, pvList := k8testutil.CreateAndValidatePvc(ctx, client, namespace, storageclass)

		ginkgo.By("Create vm service vm")
		_, vm, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{pvclaim}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*corev1.PersistentVolumeClaim{pvclaim})).NotTo(gomega.HaveOccurred())

		// Create a snapshot of both the volume.
		volumeSnapshot, _ := csisnapshot.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, pvList, constants.DiskSize)

		//  Restore it on different SC
		ginkgo.By("Restore sanpshots to create new volumes")
		restoredPvc, _, _ := csisnapshot.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, restoreSc,
			volumeSnapshot, constants.DiskSize, false)

		// detach original volume from VM
		ginkgo.By("Edit vm spec and remove volume attached to it")
		vm, err = vmservice_vm.GetVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm.Spec.Volumes = []vmopv1.VirtualMachineVolume{}
		err = vmopC.Update(ctx, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Attach restored volume to vm
		vm.Spec.Volumes = append(vm.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: restoredPvc.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{ClaimName: restoredPvc.Name},
			}})
		err = vmopC.Update(ctx, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm, err = vmservice_vm.GetVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Wait and verify restored PVC is attached to vm")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*corev1.PersistentVolumeClaim{restoredPvc})).To(gomega.Succeed())

		// TODO : Run volume expansion on both the restored PVCs

		framework.Logf("Ending test: Attach VM to restored PVC with detaching original PVC")

	})

	// TC -10
	ginkgo.It("Create a PVC from a snapshot on a different datastore, verify the data integrity", ginkgo.Label(
		constants.P0, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Create a PVC from a snapshot on a different datastore, verify the data integrity")

		// Create a PVC on sc-1 and attach them to VM
		pvclaim, pvList := k8testutil.CreateAndValidatePvc(ctx, client, namespace, storageclass)

		ginkgo.By("Create vm service vm")
		_, vm, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{pvclaim}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*corev1.PersistentVolumeClaim{pvclaim})).NotTo(gomega.HaveOccurred())

		vmIp, err := vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volFolder := vmservice_vm.FormatNVerifyPvcIsAccessible(vm.Status.Volumes[0].DiskUuid, 1, vmIp)

		// Write data to VM
		ginkgo.By("write some data to a file in PVC from VM")
		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		framework.Logf("Creating a 100mb test data file %v", testdataFile)
		op, err := exec.Command(
			"bash", "-c", "dd if=/dev/urandom bs=1M count=1 | tr -dc 'a-zA-Z0-9' >"+testdataFile).Output()
		// using 'tr' to filter out non-alphanumeric characters
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		op, err = exec.Command("md5sum", testdataFile).Output()
		fmt.Println("md5sum", string(op[:]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		op, err = exec.Command("ls", "-l", testdataFile).Output()
		fmt.Println(string(op[:]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		framework.Logf("Copying test data file to VM")
		vmservice_vm.CopyFileToVm(vmIp, testdataFile, volFolder+"/f1")

		_ = vmservice_vm.ExecSshOnVmThroughGatewayVm(vmIp,
			[]string{"ls -l " + volFolder + "/f1", "md5sum " + volFolder + "/f1", "sync"})

		// Create a snapshot of both the volume.
		volumeSnapshot, _ := csisnapshot.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, pvList, constants.DiskSize)

		//  Restore it on different SC
		ginkgo.By("Restore sanpshots to create new volumes")
		restoredPvc, _, _ := csisnapshot.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, restoreSc,
			volumeSnapshot, constants.DiskSize, false)

		// Attach the restored volume to a VM
		_, vm2, _, err := vmservice_vm.CreateVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*corev1.PersistentVolumeClaim{restoredPvc}, vmClass, restoreSc.Name, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(vmservice_vm.WaitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*corev1.PersistentVolumeClaim{restoredPvc})).NotTo(gomega.HaveOccurred())

		// Verify data exists
		ginkgo.By("verify data in restored PVC from vm2")
		framework.Logf("Mounting the volume")
		vmIp2, err := vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volFolder = vmservice_vm.FormatNVerifyPvcIsAccessible(vm2.Status.Volumes[0].DiskUuid, 1, vmIp2)
		vmFileData := fmt.Sprintf("/tmp/vmdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		_ = vmservice_vm.ExecSshOnVmThroughGatewayVm(vmIp2, []string{"md5sum " + volFolder + "/f1"})
		framework.Logf("Fetching file from the VM")
		vmservice_vm.CopyFileFromVm(vmIp2, volFolder+"/f1", vmFileData)
		defer func() {
			c := []string{"rm", "-f", vmFileData}
			op, err = exec.Command(c[0], c[1:]...).Output()
			framework.Logf("Command: %v, output: %v", c, op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		framework.Logf("Comparing file fetched from the VM with test data file")
		c := []string{"md5sum", testdataFile, vmFileData}
		op, err = exec.Command(c[0], c[1:]...).Output()
		framework.Logf("Command: %v, output: %v", c, op)
		lines := strings.Split(string(op[:]), "\n")
		gomega.Expect(strings.Fields(lines[0])[0]).To(gomega.Equal(strings.Fields(lines[1])[0]))

		// Write new data on the restored volume
		ginkgo.By("write some data to a file in PVC from VM")
		rand.New(rand.NewSource(time.Now().Unix()))
		testdata2File := fmt.Sprintf("/tmp/testdata2_%v_%v", time.Now().Unix(), rand.Intn(1000))
		framework.Logf("Creating a 100mb test data file %v", testdata2File)
		op, err = exec.Command(
			"bash", "-c", "dd if=/dev/urandom bs=1M count=1 | tr -dc 'a-zA-Z0-9' >"+testdata2File).Output()
		// using 'tr' to filter out non-alphanumeric characters
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		op, err = exec.Command("md5sum", testdataFile).Output()
		fmt.Println("md5sum", string(op[:]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		op, err = exec.Command("ls", "-l", testdataFile).Output()
		fmt.Println(string(op[:]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		framework.Logf("Copying test data file to VM")
		vmservice_vm.CopyFileToVm(vmIp, testdataFile, volFolder+"/f1")

		_ = vmservice_vm.ExecSshOnVmThroughGatewayVm(vmIp,
			[]string{"ls -l " + volFolder + "/f1", "md5sum " + volFolder + "/f1", "sync"})

		framework.Logf("Ending test: Create a PVC from a snapshot on a different datastore, verify the data integrity")

	})

})
