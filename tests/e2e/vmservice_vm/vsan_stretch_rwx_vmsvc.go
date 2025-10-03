/*
Copyright 2024 The Kubernetes Authors.

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
	"strings"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/nimbus"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vsan_stretch"
)

var _ bool = ginkgo.Describe("[vsan-stretch-vmsvc] vm service with rwx vol tests", func() {

	f := framework.NewDefaultFramework("vsan-stretch-vmsvc")
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
		csiNs             string
		vmopC             ctlrclient.Client
		restConfig        *rest.Config
		nodeList          *v1.NodeList
		accessMode        v1.PersistentVolumeAccessMode
		userName          string
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		var err error

		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		storagePolicyName = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicyNameForSharedDatastores)

		e2eTestConfig = bootstrap.Bootstrap()
		userName = "Administrator"
		nimbus.ReadVcEsxIpsViaTestbedInfoJson(env.GetAndExpectStringEnvVar(constants.EnvTestbedInfoJsonPath))
		vsan_stretch.InitialiseFdsVar(ctx, e2eTestConfig)
		accessMode = v1.ReadWriteMany

		vcRestSessionId = k8testutil.CreateVcSession4RestApis(ctx, e2eTestConfig)
		restConfig = k8testutil.GetRestConfigClient(e2eTestConfig)

		storageClassName = strings.ReplaceAll(storagePolicyName, " ", "-") // since this is a wcp setup
		storageClassName = strings.ToLower(storageClassName)
		framework.Logf("storageClassName: %s", storageClassName)

		datastoreURL = env.GetAndExpectStringEnvVar(constants.EnvSharedDatastoreURL)
		dsRef := vcutil.GetDsMoRefFromURL(ctx, e2eTestConfig, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		storageProfileId = vcutil.GetSpbmPolicyID(storagePolicyName, e2eTestConfig)
		contentLibId, err := CreateAndOrGetContentlibId4Url(e2eTestConfig, vcRestSessionId, env.GetAndExpectStringEnvVar(constants.EnvContentLibraryUrl),
			dsRef.Value)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Create a WCP namespace for the test")
		vmClass = os.Getenv(constants.EnvVMClass)
		if vmClass == "" {
			vmClass = constants.VmClassBestEffortSmall
		}
		namespace = CreateTestWcpNs(e2eTestConfig,
			vcRestSessionId, storageProfileId, vmClass, contentLibId, GetSvcId(e2eTestConfig, vcRestSessionId), userName)

		time.Sleep(5 * time.Minute)

		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmImageName := env.GetAndExpectStringEnvVar(constants.EnvVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi = WaitNGetVmiForImageName(ctx, vmopC, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		k8testutil.DumpSvcNsEventsOnTestFailure(client, namespace)
		DelTestWcpNs(e2eTestConfig, vcRestSessionId, namespace)
		gomega.Expect(k8testutil.WaitForNamespaceToGetDeleted(ctx, client, namespace, constants.Poll, constants.PollTimeout)).To(gomega.Succeed())
	})

	/*
		RWX volumes with VMService VMs and primary site goes down
		Steps:
		1. Create a few PVCs using the storageclass as mentioned in testbed structure.
		2. Create a VMservice VM from each PVC created in step2.
		3. Verify all PVC's metadata on CNS.
		4. Once the VMs are up verify that the volume is accessible inside the VM.
		5. Write data on volumes created.
		6. Bring down the primary site by powering off the hosts in primary site.
		7. Verify that the supervisor cluster should be in running and ready
			state after site failover.
		8. Verify that all the k8s constructs created in the test are running fine.
		9. Perform volume lifecycle actions which should work fine.
		10. Verify the data written in step 5.
		11. Bring primary site up and wait for testbed to be back to normal.
		12. Delete all objects created in this test.
	*/
	ginkgo.It("RWX volumes with VMService VMs and primary site goes down",
		ginkgo.Label(constants.P0, constants.Vmsvc, constants.VsanStretch, constants.File, constants.Wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var vmCount int = 5
			var err error
			var vmIPs, nfsAccessPointList []string

			ginkgo.By("Creating StorageClass")

			storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create PVC")
			pvc, pvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig, namespace, nil, accessMode,
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
					k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, false)
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

				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)
				nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				nfsAccessPointList = append(nfsAccessPointList, nfsAccessPoint)

			}
			csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Bring down the primary site")
			vsan_stretch.SiteFailover(ctx, e2eTestConfig, true)

			defer func() {
				ginkgo.By("Bring up the primary site before terminating the test")
				if len(vsan_stretch.Fds.HostsDown) > 0 && vsan_stretch.Fds.HostsDown != nil {
					vsan_stretch.SiteRestore(e2eTestConfig, true)
					vsan_stretch.Fds.HostsDown = nil
				}
			}()

			ginkgo.By("Wait for k8s cluster to be healthy")
			if e2eTestConfig.TestInput.ClusterFlavor.VanillaCluster {
				vsan_stretch.Wait4AllK8sNodesToBeUp(e2eTestConfig, nodeList)
			}

			time.Sleep(5 * time.Minute)
			// Check if csi pods are running fine after site failure
			ginkgo.By("Check if csi pods are running fine after site failure")
			err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, len(csipods.Items),
				time.Duration(constants.PollTimeout*2))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			for _, vm := range vms {
				_, err := Wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By("Perform volume and application lifecycle actions")
			newPvc, newPvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig, namespace, nil, accessMode,
				constants.DiskSize, storageclass, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := newPvs[0].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			defer func() {
				err := fpv.DeletePersistentVolumeClaim(ctx, client, newPvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			newVms := CreateStandaloneVmServiceVm(
				ctx, vmopC, namespace, vmClass, vmi, storageClassName, secretName, vmopv1.VirtualMachinePoweredOn, vmCount)
			defer func() {
				ginkgo.By("Deleting VM")
				for _, vm := range newVms {
					DeleteVmServiceVm(ctx, vmopC, namespace, vm.Name)
					crdInstanceName := pvc.Name + vm.Name
					k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, false)
				}
			}()

			for _, vm := range newVms {
				ginkgo.By("Wait for VM to come up and get an IP")
				vmIp, err := WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				vmIPs = append(vmIPs, vmIp)

				ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
				crdInstanceName := newPvc.Name + vm.Name
				err = CreateCnsFileAccessConfigCRD(ctx, restConfig, newPvc.Name, vm.Name, namespace, crdInstanceName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)
				nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				nfsAccessPointList = append(nfsAccessPointList, nfsAccessPoint)

			}
			ginkgo.By("Bring up the primary site")
			if len(vsan_stretch.Fds.HostsDown) > 0 && vsan_stretch.Fds.HostsDown != nil {
				vsan_stretch.SiteRestore(e2eTestConfig, true)
				vsan_stretch.Fds.HostsDown = nil
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			// wait for the VMs to move back
			err = vsan_stretch.WaitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	/*
		RWX volumes with VMService VMs deletion while secondary site goes down
		Steps:
		1. Create a few PVCs using the storageclass as mentioned in testbed structure.
		2. Create a VMservice VM from each PVC created in step2.
		3. Verify all PVC's metadata on CNS.
		4. Once the VMs are up verify that the volume is accessible inside the VM.
		5. Write data on volumes created.
		6. Bring down the primary site by powering off the hosts in primary site.
		7. Verify that the supervisor cluster should be in running and ready
			state after site failover.
		8. Verify that all the k8s constructs created in the test are running fine.
		9. Perform volume lifecycle actions which should work fine.
		10. Verify the data written in step 5.
		11. Bring primary site up and wait for testbed to be back to normal.
		12. Delete all objects created in this test.
	*/
	ginkgo.It("RWX volumes with VMService VMs deletion while secondary site goes down",
		ginkgo.Label(constants.P0, constants.Vmsvc, constants.VsanStretch, constants.File, constants.Wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var vmCount int = 5
			var err error
			var vmIPs, nfsAccessPointList []string
			vmsDeleted := false

			ginkgo.By("Creating StorageClass")
			storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create PVC")
			pvc, pvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig, namespace, nil, accessMode,
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
				if !vmsDeleted {
					for _, vm := range vms {
						DeleteVmServiceVm(ctx, vmopC, namespace, vm.Name)
						crdInstanceName := pvc.Name + vm.Name
						k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, constants.CrdCNSFileAccessConfig,
							constants.CrdVersion, constants.CrdGroup, false)
					}
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

				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)
				nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				nfsAccessPointList = append(nfsAccessPointList, nfsAccessPoint)

			}
			csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Bring down the primary site")
			var wg sync.WaitGroup
			wg.Add(2)
			go vsan_stretch.SiteFailureInParallel(ctx, e2eTestConfig, false, &wg)
			go DeleteVMServiceVmInParallel(ctx, vmopC, vms, namespace, &wg)
			wg.Wait()

			defer func() {
				ginkgo.By("Bring up the primary site before terminating the test")
				if len(vsan_stretch.Fds.HostsDown) > 0 && vsan_stretch.Fds.HostsDown != nil {
					vsan_stretch.SiteRestore(e2eTestConfig, false)
					vsan_stretch.Fds.HostsDown = nil
				}
			}()

			ginkgo.By("Wait for k8s cluster to be healthy")
			if e2eTestConfig.TestInput.ClusterFlavor.VanillaCluster {
				vsan_stretch.Wait4AllK8sNodesToBeUp(e2eTestConfig, nodeList)
			}

			time.Sleep(5 * time.Minute)
			// Check if csi pods are running fine after site failure
			ginkgo.By("Check if csi pods are running fine after site failure")
			err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, len(csipods.Items),
				time.Duration(constants.PollTimeout*2))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify all VMservice VMs and associated CFC CRDs got deleted")
			for _, vm := range vms {
				crdInstanceName := pvc.Name + vm.Name
				_, err = GetVmsvcVM(ctx, vmopC, namespace, vm.Name)
				gomega.Expect(err).To(gomega.HaveOccurred())
				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, constants.CrdCNSFileAccessConfig,
					constants.CrdVersion, constants.CrdGroup, false)
			}
			vmsDeleted = true
			ginkgo.By("Bring up the secondary site")
			if len(vsan_stretch.Fds.HostsDown) > 0 && vsan_stretch.Fds.HostsDown != nil {
				vsan_stretch.SiteRestore(e2eTestConfig, false)
				vsan_stretch.Fds.HostsDown = nil
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			// wait for the VMs to move back
			err = vsan_stretch.WaitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})
})
