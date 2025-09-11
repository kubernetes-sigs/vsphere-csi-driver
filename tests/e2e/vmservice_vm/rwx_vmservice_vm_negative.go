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
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

var _ bool = ginkgo.Describe("[rwx-vmsvc-vm] RWX support with VMService Vms", func() {

	f := framework.NewDefaultFramework("rwx-vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client            clientset.Interface
		namespace         string
		datastoreURL      string
		storagePolicyName string
		storageClassName  string
		storageProfileId  string
		vcRestSessionId   string
		vmi               string
		vmClass           string
		vmopC             ctlrclient.Client
		restConfig        *restclient.Config
		dsRef             types.ManagedObjectReference
		labelsMap         map[string]string
		accessMode        v1.PersistentVolumeAccessMode
		adminClient       clientset.Interface
		userName          string
		fullSyncWaitTime  int
		clusterRef        types.ManagedObjectReference
		gcClient          clientset.Interface
		// gcNamespace         string
		scParameters        map[string]string
		gcStoragePolicyName string
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

		// gcNamespace = vcutil.GetNamespaceToRunTests(f, e2eTestConfig)

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
		Mount volume from another VM's NFS access point
		Steps:
		1. Create a PVC with RWX access mode with the assigned storage policy.
		2. Create 2 VMService VMs.
		3. Verify all VMService VMs come to powered On state.
		4. Attach a VMServiceVms to this PVC by creating CNSFileAccessConfig CRD for one PVC-VM pair.
		5. Verify CNSFileAccessConfig CRD gets created and verify NFS Access point has been populated in the CR.
		6. Verify IO metadata by reading and writing to the volume through multiple VMs.
		7. Try to mount this volume into another VM using NFS access point exposed above which should fail
		   with appropriate error.
	*/

	ginkgo.It("Mount volume from another VM's NFS access point", ginkgo.Label(constants.P0, constants.File,
		constants.Wcp, constants.VmServiceVm, constants.Vc901, constants.Negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmIPs, nfsAccessPointList []string
		vmCount := 2

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

		for _, vm := range vms[:1] {
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

		ginkgo.By("Write IO to file volume through first VMService VM")
		for _, nfsAccessPoint := range nfsAccessPointList {
			err = MountRWXVolumeAndVerifyIO(vmIPs[:1], nfsAccessPoint, "foo")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("verify ACL permissions on file share volumes")
		vsanFileShares := vcutil.QueryVsanFileShares(ctx, e2eTestConfig, []string{volHandle}, clusterRef)
		err = VerifyACLPermissionsOnFileShare(vsanFileShares, volHandle, vmIPs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to mount volume into 2nd VM using NFS access point exposed by 1st VM")
		for _, nfsAccessPoint := range nfsAccessPointList {
			err = MountRWXVolumeAndVerifyIO(vmIPs[1:2], nfsAccessPoint, "foo")
			gomega.Expect(err).To(gomega.HaveOccurred())
		}

	})

	/*
		Attachment of podVM and VMService VM to a volume
		Steps:
		1. Create 2 PVC with RWX access mode with the assigned storage policy.
		2. Create a podVM with PVC1, it should fail with approprirate error.
		3. Create a VMService VM.
		4. Verify all VMService VMs come to powered On state.
		5. Attach VMServiceVms to this PVC by creating CNSFileAccessConfig CRD.
		6. Verify a CNSFileAccessConfig CRD gets created and verify NFS Access point has been populated in the CR.
		7. Verify the number of CNSFileAccessConfig CRDs generated is equal to 1.
		8. Verify IO by reading and writing to the volume through multiple VMs.
		9. Attach a podVM to the PVC created in step 2 which should fail with appropriate error
	*/

	ginkgo.It("Attachment of podVM and VMService VM to a volume", ginkgo.Label(constants.P2, constants.File,
		constants.Wcp, constants.VmServiceVm, constants.Vc901, constants.Negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmIPs, nfsAccessPointList, volhandles []string
		vmCount := 1
		pvcCount := 2

		ginkgo.By("Create/Get a storageclass")
		storageclass, err := adminClient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create multiple PVCs")
		pvclaimsList := k8testutil.CreateMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount, nil)

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			// var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())

			pv := k8testutil.GetPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
			volhandle := pv.Spec.CSI.VolumeHandle
			volhandles = append(volhandles, volhandle)
		}

		ginkgo.By("Creating Pod with PVC-1")
		_, err = k8testutil.CreatePod(ctx, e2eTestConfig, client, namespace, nil,
			[]*v1.PersistentVolumeClaim{pvclaimsList[0]}, false, "")
		gomega.Expect(err).To(gomega.HaveOccurred())

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

		for _, vm := range vms[:1] {
			ginkgo.By("Wait for VM to come up and get an IP")
			vmIp, err := WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIPs = append(vmIPs, vmIp)

			ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
			crdInstanceName := pvclaimsList[1].Name + vm.Name
			err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvclaimsList[1].Name, vm.Name, namespace, crdInstanceName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
				constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)
			nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nfsAccessPointList = append(nfsAccessPointList, nfsAccessPoint)

		}

		ginkgo.By("Write IO to file volume through first VMService VM")
		for _, nfsAccessPoint := range nfsAccessPointList {
			err = MountRWXVolumeAndVerifyIO(vmIPs[:1], nfsAccessPoint, "foo")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("verify ACL permissions on file share volumes")
		vsanFileShares := vcutil.QueryVsanFileShares(ctx, e2eTestConfig, []string{volhandles[1]}, clusterRef)
		err = VerifyACLPermissionsOnFileShare(vsanFileShares, volhandles[1], vmIPs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Pod with PVC-2")
		_, err = k8testutil.CreatePod(ctx, e2eTestConfig, client, namespace, nil,
			[]*v1.PersistentVolumeClaim{pvclaimsList[1]}, false, "")
		gomega.Expect(err).To(gomega.HaveOccurred())

	})

	/*
		Wrong VM/PVC Name & creation of CFC CR
		Steps:
		1. Create a PVC with RWX access mode with the assigned storage policy.
		2. Create 3 VMService VMs.
		3. Verify all VMService VMs come to powered On state.
		4. Create a CNSFileAccessConfig CRD with following scenarios:
			a. Provide invalid pvc name.
			b. Provide invalid vm name.
			c. Provide same name for CNSFileAccessConfig CRD which already exists.
		5. Verify CNSFileAccessConfig CRD creation fails with an appropriate error in all the above scenarios.
	*/

	ginkgo.It("Wrong VM/PVC Name & creation of CFC CR", ginkgo.Label(constants.P2, constants.File,
		constants.Wcp, constants.VmServiceVm, constants.Vc901, constants.Negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		vmCount := 1

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

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vms[0].Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a CNSFileAccessConfig crd for wrong PVC & VM pair")
		crdInstanceName := "invalid-pvc" + vms[0].Name
		err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vms[0].Name, namespace, crdInstanceName)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Create a CNSFileAccessConfig crd for PVC & invalid-VM pair")
		crdInstanceName = pvc.Name + "invalid-vm"
		err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vms[0].Name, namespace, crdInstanceName)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Create a CNSFileAccessConfig crd for PVC & VM pair")
		crdInstanceName = pvc.Name + vms[0].Name
		err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vms[0].Name, namespace, crdInstanceName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a duplicate CNSFileAccessConfig crd with an existing name")
		crdInstanceName = pvc.Name + vms[0].Name
		err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vms[0].Name, namespace, crdInstanceName)
		gomega.Expect(err).To(gomega.HaveOccurred())

	})

	/*
		CNSFileAccessConfig CRD creation with Block Volume
		Steps:
		1. Create a PVC with RWO access mode with the assigned storage policy.
		2. Create a VMService VM in the wcp namespcae.
		3. Verify all VMService VMs come to powered On state.
		4. Create a CNSFileAccessConfig CRD with the pvc created in step 1 and step 2.
		5. Verify CNSFileAccessConfig CRD creation fails with an appropriate error.
	*/

	ginkgo.It("CNSFileAccessConfig CRD creation with Block Volume", ginkgo.Label(constants.P2, constants.File,
		constants.Wcp, constants.VmServiceVm, constants.Vc901, constants.Negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		vmCount := 1

		ginkgo.By("Create/Get a storageclass")
		storageclass, err := adminClient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC with RWO access mode")
		pvc, pvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig, namespace, labelsMap,
			v1.ReadWriteOnce, constants.DiskSize, storageclass, true)
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

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vms[0].Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a CNSFileAccessConfig crd for wrong PVC & VM pair")
		crdInstanceName := pvc.Name + vms[0].Name
		err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vms[0].Name, namespace, crdInstanceName)
		gomega.Expect(err).To(gomega.HaveOccurred())

	})

	//

	/*
		Mounting of volume using NFS accesspoint exposed from VMServiceVM CRs in TKG
		Steps:
		1. Create a PVC with RWX access mode with the assigned storage policy.
		2. Create 2 VMService VMs.
		3. Verify all VMService VMs come to powered On state.
		4. Attach a VMServiceVms to this PVC by creating CNSFileAccessConfig CRD for one PVC-VM pair.
		5. Verify a CNSFileAccessConfig CRD gets created and verify NFS Access point has been populated in the CR.
		6. Verify IO by reading and writing to the volume through multiple VMs.
		7. Create a TKG in the same supervisor namespace.
		8. Create some file volumes and attach some pods to the volume in TKG.
		9. Verify a CNSFileAccessConfig CRD gets created in supervisor and verify NFS Access point has been
		    populated in this CR.
		10. Try to mount the volume into another VM in supervisor using NFS access point exposed in step 10.
		11. Try to write to volume to VMServiceVM which should fail with appropriate error.
	*/

	ginkgo.It("Mounting of volume using NFS accesspoint exposed from VMServiceVM CRs in TKG", ginkgo.Label(
		constants.P2, constants.File, constants.Wcp, constants.VmServiceVm, constants.Vc901,
		constants.Negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		vmCount := 2

		ginkgo.By("Create/Get a storageclass")
		storageclass, err := adminClient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a PVC")
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

		ginkgo.By("Wait for VM to come up and get an IP")
		_, err = WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vms[0].Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a CNSFileAccessConfig crd for PVC & VM-1 pair")
		crdInstanceName := pvc.Name + vms[0].Name
		err = CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vms[0].Name, namespace, crdInstanceName)
		gomega.Expect(err).To(gomega.HaveOccurred())

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim

		ginkgo.By("Creating RWX volume in GC")
		scParameters[constants.SvStorageClassName] = gcStoragePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = k8testutil.CreatePVCAndStorageClass(ctx, e2eTestConfig, gcClient,
			namespace, nil, scParameters, constants.DiskSize, nil, "", false, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		volHandle = persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		volumeID := k8testutil.GetVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := vcutil.QueryCNSVolumeWithResult(e2eTestConfig, volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		nfsGcRwx := queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := k8testutil.CreatePod(ctx, e2eTestConfig, client, namespace, nil,
			[]*v1.PersistentVolumeClaim{pvclaim}, false, constants.ExecRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+volHandle))
			err = k8testutil.WaitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle,
				constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
				constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, false)
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
			constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)

		ginkgo.By("Mount the volume into another VM-2 in supervisor using NFS access point exposed due GC-RWX volume")
		ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", nfsGcRwx))
	})
})
