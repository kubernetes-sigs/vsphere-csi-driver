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

package vmservice_vm

import (
	"context"
	"os"
	"strconv"
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
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/csisnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

var e2eTestConfig *config.E2eTestConfig

var _ bool = ginkgo.Describe("[rwx-vmsvc-vm] RWX support with VMService Vms", func() {

	f := framework.NewDefaultFramework("rwx-vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client              clientset.Interface
		namespace           string
		datastoreURL        string
		storagePolicyName   string
		storageClassName    string
		storageProfileId    string
		vcRestSessionId     string
		vmi                 string
		vmClass             string
		vmopC               ctlrclient.Client
		restConfig          *restclient.Config
		snapc               *snapclient.Clientset
		pandoraSyncWaitTime int
		dsRef               types.ManagedObjectReference
		labelsMap           map[string]string
		accessMode          v1.PersistentVolumeAccessMode
		adminClient         clientset.Interface
		userName            string
		fullSyncWaitTime    int
		clusterRef          types.ManagedObjectReference
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// client connection
		client = f.ClientSet
		e2eTestConfig = bootstrap.Bootstrap()
		accessMode = v1.ReadWriteMany
		userName = env.GetorIgnoreStringEnvVar(constants.EnvDevopsUserName)
		adminClient, client = k8testutil.InitializeClusterClientsByUserRoles(client, e2eTestConfig)

		// fetching nodes and reading storage policy name
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		storagePolicyName = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicyNameForSharedDatastores)

		// creating vc session
		vcRestSessionId = k8testutil.CreateVcSession4RestApis(ctx, e2eTestConfig)

		// reading storage class name for wcp setup
		storageClassName = strings.ReplaceAll(storagePolicyName, " ", "-") // since this is a wcp setup
		storageClassName = strings.ToLower(storageClassName)

		// fetching shared datastore url
		datastoreURL = env.GetAndExpectStringEnvVar(constants.EnvSharedDatastoreURL)

		// reading datastore morf reference
		dsRef = vcutil.GetDsMoRefFromURL(ctx, e2eTestConfig, datastoreURL)

		storageProfileId = vcutil.GetSpbmPolicyID(storagePolicyName, e2eTestConfig)

		// creating/reading content library
		contentLibId, err := CreateAndOrGetContentlibId4Url(e2eTestConfig, vcRestSessionId,
			env.GetAndExpectStringEnvVar(constants.EnvContentLibraryUrl),
			dsRef.Value)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmClass = os.Getenv(constants.EnvVMClass)
		if vmClass == "" {
			vmClass = constants.VmClassBestEffortSmall
		}

		framework.Logf("Create a WCP namespace for the test")
		// creating wcp test namespace and setting vmclass, contlib, storage class fields in test ns
		namespace = CreateTestWcpNs(e2eTestConfig,
			vcRestSessionId, storageProfileId, vmClass, contentLibId, GetSvcId(e2eTestConfig, vcRestSessionId), userName)

		framework.Logf("Verifying storage policies usage for each storage class")
		restConfig = k8testutil.GetRestConfigClient(e2eTestConfig)

		// creating vm schema
		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// reading vm image name
		vmImageName := env.GetAndExpectStringEnvVar(constants.EnvVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi = WaitNGetVmiForImageName(ctx, vmopC, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())

		// Get snapshot client using the rest config
		restConfig = k8testutil.GetRestConfigClient(e2eTestConfig)
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// reading full sync wait time
		if os.Getenv(constants.EnvPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(constants.EnvPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = constants.DefaultPandoraSyncWaitTime
		}

		//setting map values
		labelsMap = make(map[string]string)
		labelsMap["app"] = "test"

		if os.Getenv(constants.EnvFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(constants.EnvFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if fullSyncWaitTime <= 0 || fullSyncWaitTime > constants.DefaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		}

		clusterName := env.GetAndExpectStringEnvVar(constants.EnvComputeClusterName)
		clusterRef, err = vcutil.GetClusterRefFromClusterName(ctx, e2eTestConfig, clusterName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		k8testutil.DumpSvcNsEventsOnTestFailure(client, namespace)
		DelTestWcpNs(e2eTestConfig, vcRestSessionId, namespace)
		gomega.Expect(k8testutil.WaitForNamespaceToGetDeleted(ctx, client, namespace, constants.Poll,
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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmIPs, nfsAccessPointList []string
		vmCount := 3

		ginkgo.By("Create/Get a storageclass")
		storageclass, err := adminClient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvc, pvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig, namespace, labelsMap, accessMode,
			constants.DiskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := CreateBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM")

		vms := CreateStandaloneVmServiceVm(
			ctx, vmopC, namespace, vmClass, vmi, storageClassName, secretName, vmopv1.VirtualMachinePoweredOn, vmCount)
		defer func() {
			ginkgo.By("Deleting VM")
			for _, vm := range vms {
				DeleteVmServiceVm(ctx, vmopC, namespace, vm.Name)
				crdInstanceName := pvc.Name + vm.Name
				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
					constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, false)
			}

		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := CreateService4Vm(ctx, vmopC, namespace, vms[0].Name)
		defer func() {

			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for _, vm := range vms {
			ginkgo.By("Wait for VM to come up and get an IP")
			vmIp, err := WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIPs = append(vmIPs, vmIp)

			ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
			crdInstanceName := pvc.Name + vm.Name
			err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vm.Name, namespace, crdInstanceName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
				constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)
			nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, namespace)
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
		volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			csisnapshot.GetVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvc.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			csisnapshot.DeleteVolumeSnapshotWithPandoraWait(ctx, snapc,
				namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
		}()
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)
		expectedErrMsg := "queried volume doesn't have the expected volume type"
		volumeSnapshot, err = snapc.SnapshotV1().VolumeSnapshots(namespace).Get(ctx,
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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmIPs, nfsAccessPointList []string
		vmCount := 3

		ginkgo.By("Create/Get a storageclass")
		storageclass, err := adminClient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvc, pvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig,
			namespace, labelsMap, accessMode,
			constants.DiskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := CreateBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VMs")
		vms := CreateStandaloneVmServiceVm(
			ctx, vmopC, namespace, vmClass, vmi, storageClassName, secretName, vmopv1.VirtualMachinePoweredOn, vmCount)
		defer func() {
			ginkgo.By("Deleting VM")
			for _, vm := range vms {
				DeleteVmServiceVm(ctx, vmopC, namespace, vm.Name)
				crdInstanceName := pvc.Name + vm.Name
				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, constants.CrdCNSFileAccessConfig,
					constants.CrdVersion, constants.CrdGroup, false)
			}

		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := CreateService4Vm(ctx, vmopC, namespace, vms[0].Name)
		defer func() {

			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for _, vm := range vms {
			ginkgo.By("Wait for VM to come up and get an IP")
			vmIp, err := WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIPs = append(vmIPs, vmIp)

			ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
			crdInstanceName := pvc.Name + vm.Name
			err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vm.Name, namespace, crdInstanceName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
				constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)
			nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, namespace)
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

		err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, vm := range vms {
			crdInstanceName := pvc.Name + vm.Name
			err = DeleteCnsFileAccessConfig(ctx, restConfig, crdInstanceName, namespace)
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
		pvc, pvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig, namespace, labelsMap, accessMode,
			constants.DiskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := CreateBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VMs")
		vms := CreateStandaloneVmServiceVm(
			ctx, vmopC, namespace, vmClass, vmi, storageClassName, secretName,
			vmopv1.VirtualMachinePoweredOff, vmCount)
		defer func() {
			ginkgo.By("Deleting VM")
			for _, vm := range vms {
				DeleteVmServiceVm(ctx, vmopC, namespace, vm.Name)
				crdInstanceName := pvc.Name + vm.Name
				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
					constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, false)
			}

		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := CreateService4Vm(ctx, vmopC, namespace, vms[0].Name)
		defer func() {

			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for _, vm := range vms {

			ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
			crdInstanceName := pvc.Name + vm.Name
			err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vm.Name, namespace, crdInstanceName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
			k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
				constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)
			cfc, err := GetCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, namespace)
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
			vm := SetVmPowerState(ctx, vmopC, vm, vmopv1.VirtualMachinePoweredOn)
			vm, err := Wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIp, err := WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIPs = append(vmIPs, vmIp)

			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
			crdInstanceName := pvc.Name + vm.Name
			nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, namespace)
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
})
