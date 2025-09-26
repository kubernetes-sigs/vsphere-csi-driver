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

package rwx_vmservice_vm

import (
	"context"
	"strings"
	"time"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/csisnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vmservice_vm"
)

var _ bool = ginkgo.Describe("[rwx-vmsvc-vm] RWX support with VMService Vms", func() {

	f := framework.NewDefaultFramework("rwx-vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client clientset.Interface
		// namespace string
		// datastoreURL        string
		// storagePolicyName   string
		storageClassName string
		// storageProfileId    string
		// vcRestSessionId     string
		vmi                 string
		vmClass             string
		vmopC               ctlrclient.Client
		restConfig          *restclient.Config
		snapc               *snapclient.Clientset
		pandoraSyncWaitTime int
		// dsRef               types.ManagedObjectReference
		labelsMap   map[string]string
		accessMode  v1.PersistentVolumeAccessMode
		adminClient clientset.Interface
		// userName            string
		fullSyncWaitTime int
		clusterRef       types.ManagedObjectReference
		ctx              context.Context
		// cancel           context.CancelFunc
		testSetup *TestSetupResult
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// client connection
		client = f.ClientSet

		e2eTestConfig = bootstrap.Bootstrap()
		adminClient, client = k8testutil.InitializeClusterClientsByUserRoles(client, e2eTestConfig)
		testSetup = InitializeTestEnvironment(f, ctx, client, adminClient, e2eTestConfig)

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		k8testutil.DumpSvcNsEventsOnTestFailure(client, testSetup.Namespace)
		vmservice_vm.DelTestWcpNs(e2eTestConfig, testSetup.VcRestSessionId, testSetup.Namespace)
		gomega.Expect(k8testutil.WaitForNamespaceToGetDeleted(ctx, client, testSetup.Namespace, constants.Poll,
			constants.PollTimeout)).To(gomega.Succeed())
	})

	/*

	   Dynamic PVC creation with multiple VMServiceVMs
	   Steps:
	   1. Create a PVC using the storage class (storage policy) tagged to the supervisor namespace
	   2. Wait for PVC to reach the Bound state.
	   3. Create a VM service VM using the PVC created in step #1
	   4. Wait for the VM service to be up and in the powered-on state.
	   5. Once the VM is up, verify that the volume is accessible inside the VM
	   6. Write some data into the volume.
	   7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   8. Create a volume snapshot for the PVC created in step #1.
	   9. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   10. Verify CNS metadata for a PVC
	   11. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("Dynamic PVC creation with multiple"+
		" VMServiceVMs", ginkgo.Label(constants.P0, constants.File, constants.Wcp,
		constants.VmServiceVm, constants.Vc901), func() {

		var vmIPs, nfsAccessPointList []string
		vmCount := 3

		ginkgo.By("Create/Get a storageclass")
		storageclass, err := adminClient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvc, pvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig, testSetup.Namespace, labelsMap, accessMode,
			constants.DiskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, testSetup.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := vmservice_vm.CreateBootstrapSecretForVmsvcVms(ctx, client, testSetup.Namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(testSetup.Namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM")

		vms := vmservice_vm.CreateStandaloneVmServiceVm(
			ctx, vmopC, testSetup.Namespace, vmClass, vmi, storageClassName, secretName, vmopv1.VirtualMachinePoweredOn, vmCount)
		defer func() {
			ginkgo.By("Deleting VM")
			for _, vm := range vms {
				vmservice_vm.DeleteVmServiceVm(ctx, vmopC, testSetup.Namespace, vm.Name)
				crdInstanceName := pvc.Name + vm.Name
				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
					constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, false)
			}

		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := vmservice_vm.CreateService4Vm(ctx, vmopC, testSetup.Namespace, vms[0].Name)
		defer func() {

			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: testSetup.Namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for _, vm := range vms {
			ginkgo.By("Wait for VM to come up and get an IP")
			vmIp, err := vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, testSetup.Namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIPs = append(vmIPs, vmIp)

			ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
			crdInstanceName := pvc.Name + vm.Name
			err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vm.Name, testSetup.Namespace, crdInstanceName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
				constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)
			nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, testSetup.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nfsAccessPointList = append(nfsAccessPointList, nfsAccessPoint)

		}

		ginkgo.By("Write IO to file volume through multiple VMService VM")
		for _, nfsAccessPoint := range nfsAccessPointList {
			err = MountRWXVolumeAndVerifyIO(vmIPs, nfsAccessPoint, "foo")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("verify ACL permissions on file share volumes")
		vsanFileShares := vcutil.QueryVsanFileShares(ctx, e2eTestConfig, []string{volHandle}, clusterRef)
		err = VerifyACLPermissionsOnFileShare(vsanFileShares, volHandle, vmIPs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := csisnapshot.CreateVolumeSnapshotClass(ctx, e2eTestConfig, snapc, constants.DeletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snaphot from PVC")
		volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(testSetup.Namespace).Create(ctx,
			csisnapshot.GetVolumeSnapshotSpec(testSetup.Namespace, volumeSnapshotClass.Name, pvc.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			csisnapshot.DeleteVolumeSnapshotWithPandoraWait(ctx, snapc,
				testSetup.Namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
		}()
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)
		expectedErrMsg := "queried volume doesn't have the expected volume type"
		volumeSnapshot, err = snapc.SnapshotV1().VolumeSnapshots(testSetup.Namespace).Get(ctx,
			volumeSnapshot.Name, metav1.GetOptions{})
		actualErr := volumeSnapshot.Status.Error
		framework.Logf("Expected error: %s, actual error: %s", expectedErrMsg, *actualErr.Message)
		if !strings.Contains(*actualErr.Message, expectedErrMsg) {
			framework.Failf("Expected error: %s, actual error: %s", expectedErrMsg, *actualErr.Message)
		}

		ginkgo.By("Expanding the current pvc")
		currentPvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("4Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvc, err = k8testutil.ExpandPVCSize(pvc, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc).NotTo(gomega.BeNil())

		pvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvc.Name)
		}

		ginkgo.By("Verify if controller resize failed")
		err = k8testutil.WaitForPvResizeForGivenPvc(pvc, client, e2eTestConfig, constants.TotalResizeWaitPeriod)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	/*

		Delete PVC with CFC crd still existing
		Steps:
		1. Create a WCP namespace and assign a SPBM policy to it.
		2. Create a PVC with RWX access mode with the assigned storage policy.
		3. Create 3 VMService VMs.
		4. Verify all VMService VMs come to powered On state.
		5. Attach 3 VMServiceVms to this PVC by creating CNSFileAccessConfig CRD for each PVC-VM pair.
		6. Verify CNSFileAccessConfig CRD gets created and verify NFS Access point
			has been populated in the CR.
		7. Verify the number of CNSFileAccessConfig CRDs generated is equal to 3.
		8. Verify IO by reading and writing to the volume through multiple VMs.
		9. Delete all the CNSFileAccessConfig CRD in the namespace.
		10. Verify CNSFileAccessConfig CRD gets deleted for all the VMs.
		11. Try to write to volume through VMServiceVM which should fail
			with appropriate error.
		12. Verify IO by reading and writing to the volume through multiple VMs.
		13. Cleanup all the workloads created in the test.
	*/

	ginkgo.It("Delete PVC with CFC crd "+
		"still existing", ginkgo.Label(constants.P0, constants.File,
		constants.Wcp, constants.VmServiceVm, constants.Vc901), func() {

		// ctx, cancel := context.WithCancel(context.Background())
		// defer cancel()
		var vmIPs, nfsAccessPointList []string
		vmCount := 3

		ginkgo.By("Create/Get a storageclass")
		storageclass, err := adminClient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvc, pvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig,
			testSetup.Namespace, labelsMap, accessMode,
			constants.DiskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, testSetup.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := vmservice_vm.CreateBootstrapSecretForVmsvcVms(ctx, client, testSetup.Namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(testSetup.Namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VMs")
		vms := vmservice_vm.CreateStandaloneVmServiceVm(
			ctx, vmopC, testSetup.Namespace, vmClass, vmi, storageClassName, secretName, vmopv1.VirtualMachinePoweredOn, vmCount)
		defer func() {
			ginkgo.By("Deleting VM")
			for _, vm := range vms {
				vmservice_vm.DeleteVmServiceVm(ctx, vmopC, testSetup.Namespace, vm.Name)
				crdInstanceName := pvc.Name + vm.Name
				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, constants.CrdCNSFileAccessConfig,
					constants.CrdVersion, constants.CrdGroup, false)
			}

		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := vmservice_vm.CreateService4Vm(ctx, vmopC, testSetup.Namespace, vms[0].Name)
		defer func() {

			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: testSetup.Namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for _, vm := range vms {
			ginkgo.By("Wait for VM to come up and get an IP")
			vmIp, err := vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, testSetup.Namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIPs = append(vmIPs, vmIp)

			ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
			crdInstanceName := pvc.Name + vm.Name
			err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vm.Name, testSetup.Namespace, crdInstanceName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
				constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)
			nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, testSetup.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nfsAccessPointList = append(nfsAccessPointList, nfsAccessPoint)

		}

		ginkgo.By("Write IO to file volume through multiple VMService VM")
		for _, nfsAccessPoint := range nfsAccessPointList {
			err = MountRWXVolumeAndVerifyIO(vmIPs, nfsAccessPoint, "/foo2")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("verify ACL permissions on file share volumes")
		vsanFileShares := vcutil.QueryVsanFileShares(ctx, e2eTestConfig, []string{volHandle}, clusterRef)
		err = VerifyACLPermissionsOnFileShare(vsanFileShares, volHandle, vmIPs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, testSetup.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.CoreV1().PersistentVolumeClaims(testSetup.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, vm := range vms {
			crdInstanceName := pvc.Name + vm.Name
			err = DeleteCnsFileAccessConfig(ctx, restConfig, crdInstanceName, testSetup.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*

		RWX Volume attachment to powered off VMs
		Steps:
		1. Create a WCP namespace and assign a SPBM policy to it.
		2. Create a PVC with RWX access mode with the assigned storage policy.
		3. Create 5 VMService VMs.
		4. Verify all VMService VMs come to powered Off state.
		5. Attach 3 VMServiceVms to this PVC by creating CNSFileAccessConfig CRD
			for each PVC-VM pair.
		6. Verify CNSFileAccessConfig CRD gets created and verify NFS Access point
			has been populated in the CR.
		7. Verify the number of CNSFileAccessConfig CRDs generated is equal to 3.
		8. Power on the 3 VMService VMs.
		9. Wait for 2 full sync cycle
		10. Check ACL is updated on the File Share
		11. Verify IO by reading and writing to the volume
			through multiple VMs.
		12. Cleanup all the workloads created in the test.
	*/

	ginkgo.It("RWX Volume attachment "+
		"to powered off VMs", ginkgo.Label(constants.P0, constants.File, constants.Wcp,
		constants.VmServiceVm, constants.Vc901), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmIPs, nfsAccessPointList []string
		vmCount := 3

		ginkgo.By("Create/Get a storageclass")
		storageclass, err := adminClient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvc, pvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig, testSetup.Namespace, labelsMap, accessMode,
			constants.DiskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, testSetup.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := vmservice_vm.CreateBootstrapSecretForVmsvcVms(ctx, client, testSetup.Namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(testSetup.Namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VMs")
		vms := vmservice_vm.CreateStandaloneVmServiceVm(
			ctx, vmopC, testSetup.Namespace, vmClass, vmi, storageClassName, secretName,
			vmopv1.VirtualMachinePoweredOff, vmCount)
		defer func() {
			ginkgo.By("Deleting VM")
			for _, vm := range vms {
				vmservice_vm.DeleteVmServiceVm(ctx, vmopC, testSetup.Namespace, vm.Name)
				crdInstanceName := pvc.Name + vm.Name
				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
					constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, false)
			}

		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := vmservice_vm.CreateService4Vm(ctx, vmopC, testSetup.Namespace, vms[0].Name)
		defer func() {

			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: testSetup.Namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for _, vm := range vms {

			ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
			crdInstanceName := pvc.Name + vm.Name
			err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vm.Name, testSetup.Namespace, crdInstanceName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
			k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
				constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)
			cfc, err := GetCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, testSetup.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			errorMsg := cfc.Status.Error
			expectedMsg := "Failed to get external facing IP address for VM"
			framework.Logf("errorMsg: %s", errorMsg)
			if !strings.Contains(errorMsg, expectedMsg) {
				framework.Failf("Expected msg to be: %s, but got: %s", expectedMsg, errorMsg)
			}

		}

		ginkgo.By("Wait for VM to come up and get an IP")
		for _, vm := range vms {
			vm := vmservice_vm.SetVmPowerState(ctx, vmopC, vm, vmopv1.VirtualMachinePoweredOn)
			vm, err := vmservice_vm.Wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIp, err := vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, testSetup.Namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIPs = append(vmIPs, vmIp)

			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
			crdInstanceName := pvc.Name + vm.Name
			nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, testSetup.Namespace)
			gomega.Expect(nfsAccessPoint).NotTo(gomega.BeEmpty())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nfsAccessPointList = append(nfsAccessPointList, nfsAccessPoint)
		}

		ginkgo.By("Write IO to file volume through multiple VMService VM")
		for _, nfsAccessPoint := range nfsAccessPointList {
			err = MountRWXVolumeAndVerifyIO(vmIPs, nfsAccessPoint, "foo")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("verify ACL permissions on file share volumes")
		vsanFileShares := vcutil.QueryVsanFileShares(ctx, e2eTestConfig, []string{volHandle}, clusterRef)
		err = VerifyACLPermissionsOnFileShare(vsanFileShares, volHandle, vmIPs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Attach/Detach PVC with multiple VMServiceVMs
		Steps:
		1. Create a PVC with RWX access mode with the assigned storage policy.
		2. Create 5 VMService VMs.
		3. Verify all VMService VMs come to powered On state.
		4. Attach 3 VMServiceVms to this PVC by creating CNSFileAccessConfig CRD for each PVC-VM pair.
		5. Verify CNSFileAccessConfig CRD gets created and verify NFS Access point has been populated in the CR.
		6. Verify the number of CNSFileAccessConfig CRDs generated is equal to 3.
		7. Verify IO by reading and writing to the volume through multiple VMs.
		8. Detach 2 VMServiceVMs from PVC by deleting CFC CRDs.
		9. Attach the 2 remaining VMs to the volume by creating CNSFileAccessConfig CRD and mounting them to volume.
		10. Verify IO by writing to the volume through 1 VM and reading it through other VM
	*/

	ginkgo.It("Attach/Detach PVC with multiple VMServiceVMs", ginkgo.Label(constants.P0, constants.File,
		constants.Wcp, constants.VmServiceVm, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmIPs, nfsAccessPointList []string
		vmCount := 5
		pvcCount := 1

		pvcList, volHandles, err := CreateMultiplePVCs(ctx, adminClient, client, e2eTestConfig, testSetup.Namespace,
			testSetup.StorageClassName, nil, testSetup.AccessMode, pvcCount)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			k8testutil.CleanupPVCsAndCNSVolumes(ctx, client, testSetup.Namespace, pvcList, volHandles, e2eTestConfig)
		}()

		// Create VMs
		vms, vmIPs, vmlbsvc, secretName, err := CreateVmServiceVms(ctx, client, testSetup.VMOpClient, testSetup.Namespace,
			testSetup.VMClass, testSetup.VMI, testSetup.StorageClassName, vmCount)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Defer cleanup for VMs, VM service, and bootstrap secret
		defer func() {
			framework.Logf("Cleaning up VMs, VM service, and bootstrap secret")
			vmservice_vm.CleanupVmServiceResources(ctx, client, testSetup.VMOpClient, testSetup.Namespace, vms, vmlbsvc, secretName, pvcList)
		}()

		// Attaching initial 3 VMService VM to Pvc
		crdNames, nfsAccessPointList, err := CreateCnsFileAccessConfigCRDs(ctx, testSetup.RestConfig, testSetup.Namespace, pvcList, vms[:3], false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Write IO to file volume through initial 3 VMService VM")
		err = VerifyIOAcrossVMs(vmIPs[:3], nfsAccessPointList, "initial")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify ACL permissions on file share volumes")
		vsanFileShares := vcutil.QueryVsanFileShares(ctx, e2eTestConfig, volHandles, testSetup.ClusterRef)
		err = VerifyACLPermissionsOnFileShare(vsanFileShares, volHandles[0], vmIPs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Detaching PVC from 2 VMService Vms")
		err = DetachPVCFromVMs(ctx, testSetup.RestConfig, testSetup.Namespace, crdNames[:2])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Attach the 2 remaining VMService Vms")
		crdNames, nfsAccessPointList, err = CreateCnsFileAccessConfigCRDs(ctx, testSetup.RestConfig, testSetup.Namespace, pvcList, vms[3:5], false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Write IO to file volume through remaining VMService VM")
		err = VerifyIOAcrossVMs(vmIPs[3:5], nfsAccessPointList, "initial")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Attachment of VMs to a single volume in scale in parallel
		Steps:
		1. Create 5 PVC with RWX access mode with the assigned storage policy. (many-to-many)
		2. Create 5 VMService VMs.
		3. Verify all VMService VMs come to powered On state.
		4. Attach 5 VMServiceVms to this PVC by creating CNSFileAccessConfig CRD for each PVC-VM pair in parallel.
		5. Verify a CNSFileAccessConfig CRD gets created and Verify NFS Access point has been generated in the CR.
		6. Verify the number of CNSFileAccessConfig CRDs generated is equal to number of VMServiceVM created.
		7. Verify IO by reading and writing to the volume through multiple VMs.
	*/

	ginkgo.It("Attachment of VMs to a single volume in scale in parallel", ginkgo.Label(constants.P0, constants.File,
		constants.Wcp, constants.VmServiceVm, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		vmCount := 2
		pvcCount := 2

		// Create PVCs
		pvcList, volHandles, err := CreateMultiplePVCs(ctx, adminClient, client, e2eTestConfig, testSetup.Namespace,
			testSetup.StorageClassName, nil, testSetup.AccessMode, pvcCount)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			k8testutil.CleanupPVCsAndCNSVolumes(ctx, client, testSetup.Namespace, pvcList, volHandles, e2eTestConfig)
		}()

		// Create VMs
		vms, vmIPs, vmlbsvc, secretName, err := CreateVmServiceVms(ctx, client, testSetup.VMOpClient, testSetup.Namespace,
			testSetup.VMClass, testSetup.VMI, testSetup.StorageClassName, vmCount)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Cleaning up VMs, VM service, and bootstrap secret")
			vmservice_vm.CleanupVmServiceResources(ctx, client, testSetup.VMOpClient, testSetup.Namespace, vms, vmlbsvc, secretName, pvcList)
		}()

		// creating CNSFileAccessConfig CRD for each PVC-VM pair in parallel
		_, nfsAccessPointList, err := CreateCnsFileAccessConfigCRDs(ctx, testSetup.RestConfig, testSetup.Namespace, pvcList, vms, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Write IO to file volume through all VMService VM")
		err = VerifyIOAcrossVMs(vmIPs, nfsAccessPointList, "foo")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify ACL permissions on file share volumes")
		vsanFileShares := vcutil.QueryVsanFileShares(ctx, e2eTestConfig, volHandles, testSetup.ClusterRef)
		for _, volHandle := range volHandles {
			err = VerifyACLPermissionsOnFileShare(vsanFileShares, volHandle, vmIPs)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

	/*
		Deletion of VM without deleting CFC
		Steps:
		1. Create a PVC with RWX access mode with the assigned storage policy.
		2. Create a VMService VM.
		3. Verify all VMService VMs come to powered On state.
		4. Attach VMServiceVms to this PVC by creating CNSFileAccessConfig CRD.
		5. Verify a CNSFileAccessConfig CRD gets created and verify NFS Access point has been populated in the CR.
		6. Verify the number of CNSFileAccessConfig CRDs generated is equal to 1.
		7. Verify IO by reading and writing to the volume through multiple VMs.
		8. Delete VMService VMs which should also cleanup all the CNSFileAccessConfig CRDs associated with it.
	*/

	ginkgo.It("Deletion of VM without deleting CFC ", ginkgo.Label(constants.P0, constants.File,
		constants.Wcp, constants.VmServiceVm, constants.Vc901), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pvcCount := 1
		vmCount := 1
		isVmDeleted := false

		// Create PVCs
		pvcList, volHandles, err := CreateMultiplePVCs(ctx, adminClient, client, e2eTestConfig, testSetup.Namespace,
			testSetup.StorageClassName, nil, testSetup.AccessMode, pvcCount)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			k8testutil.CleanupPVCsAndCNSVolumes(ctx, client, testSetup.Namespace, pvcList, volHandles, e2eTestConfig)
		}()

		// Create VMs
		vms, vmIPs, vmlbsvc, secretName, err := CreateVmServiceVms(ctx, client, testSetup.VMOpClient, testSetup.Namespace,
			testSetup.VMClass, testSetup.VMI, testSetup.StorageClassName, vmCount)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Cleaning up VMs, VM service, and bootstrap secret")
			if !isVmDeleted {
				vmservice_vm.CleanupVmServiceResources(ctx, client, testSetup.VMOpClient, testSetup.Namespace, vms, vmlbsvc, secretName, pvcList)
			}
		}()

		// creating CNSFileAccessConfig CRD for each PVC-VM pair in parallel
		crdNames, nfsAccessPointList, err := CreateCnsFileAccessConfigCRDs(ctx, testSetup.RestConfig, testSetup.Namespace, pvcList, vms, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Write IO to file volume through multiple VMService VM")
		err = VerifyIOAcrossVMs(vmIPs, nfsAccessPointList, "foo")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify ACL permissions on file share volumes")
		vsanFileShares := vcutil.QueryVsanFileShares(ctx, e2eTestConfig, volHandles, testSetup.ClusterRef)
		err = VerifyACLPermissionsOnFileShare(vsanFileShares, volHandles[0], vmIPs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete VMService VMs which should also cleanup all the CNSFileAccessConfig CRDs associated with it")
		vmservice_vm.DeleteVmServiceVm(ctx, testSetup.VMOpClient, testSetup.Namespace, vms[0].Name)
		isVmDeleted = true
		k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdNames[0], constants.CrdCNSFileAccessConfig,
			constants.CrdVersion, constants.CrdGroup, false)
	})
})
