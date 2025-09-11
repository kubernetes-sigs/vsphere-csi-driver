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
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
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
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vmservice_vm"
)

var _ bool = ginkgo.Describe("[rwx-vmsvc-vm] RWX support with VMService Vms", func() {

	f := framework.NewDefaultFramework("rwx-vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                  clientset.Interface
		namespace               string
		datastoreURL            string
		storagePolicyName       string
		storageClassName        string
		storageProfileId        string
		vcRestSessionId         string
		vmi                     string
		vmClass                 string
		vmopC                   ctlrclient.Client
		restConfig              *restclient.Config
		dsRef                   types.ManagedObjectReference
		labelsMap               map[string]string
		accessMode              v1.PersistentVolumeAccessMode
		adminClient             clientset.Interface
		userName                string
		fullSyncWaitTime        int
		clusterRef              types.ManagedObjectReference
		isVsanHealthServiceDown bool
		bindingModeWffc         storagev1.VolumeBindingMode
		scParameters            map[string]string
		clusterComputeResource  []*object.ClusterComputeResource
		clusterName             string
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
		bindingModeWffc = storagev1.VolumeBindingWaitForFirstConsumer
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
		contentLibId, err := vmservice_vm.CreateAndOrGetContentlibId4Url(e2eTestConfig, vcRestSessionId,
			env.GetAndExpectStringEnvVar(constants.EnvContentLibraryUrl),
			dsRef.Value)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmClass = os.Getenv(constants.EnvVMClass)
		if vmClass == "" {
			vmClass = constants.VmClassBestEffortSmall
		}

		framework.Logf("Create a WCP namespace for the test")
		// creating wcp test namespace and setting vmclass, contlib, storage class fields in test ns
		namespace = vmservice_vm.CreateTestWcpNs(e2eTestConfig,
			vcRestSessionId, storageProfileId, vmClass, contentLibId, vmservice_vm.GetSvcId(e2eTestConfig, vcRestSessionId), userName)

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
		vmi = vmservice_vm.WaitNGetVmiForImageName(ctx, vmopC, vmImageName)
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

		clusterName = env.GetAndExpectStringEnvVar(constants.EnvComputeClusterName)
		clusterRef, err = vcutil.GetClusterRefFromClusterName(ctx, e2eTestConfig, clusterName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isVsanHealthServiceDown = false

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if isVsanHealthServiceDown {
			err := vcutil.InvokeVCenterServiceControl(&e2eTestConfig.TestInput.TestBedInfo,
				ctx, constants.StartOperation, constants.VsanhealthServiceName, e2eTestConfig.TestInput.TestBedInfo.VcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		k8testutil.DumpSvcNsEventsOnTestFailure(client, namespace)
		vmservice_vm.DelTestWcpNs(e2eTestConfig, vcRestSessionId, namespace)
		gomega.Expect(k8testutil.WaitForNamespaceToGetDeleted(ctx, client, namespace, constants.Poll,
			constants.PollTimeout)).To(gomega.Succeed())

	})

	/*
	   vsan health goes down while attaching VMs
	   Steps:
	   1. Create a PVC with RWX access mode with the assigned storage policy.
	   2. Create 3 VMService VMs.
	   3. Verify all VMService VMs come to powered On state.
	   4. Attach 3 VMServiceVms to this PVC by creating CNSFileAccessConfig CRD for each PVC-VM pair.
	   5. Bring down vsan health down while attachment of VMs to volumes.
	   6. Check Events in CRD with some appropriate error and ACL creation fails
	   7. Verify the number of CNSFileAccessConfig CRDs generated is equal to number of PVCs created.
	   8. Apply labels to PVC and PV and verify CNS metadata & expect label not to present.
	   9. Bring up vsan health.
	   10. Wait for 2 full sync cycles.
	   11. Verify NFS Access point has been populated in the CR
	   12. Verify IO by reading and writing to the volume through multiple VMs.
	   13. Verify labels are updated for PVC and PV in CNS metadata.
	*/

	ginkgo.It("Mount volume from another VM's NFS access point", ginkgo.Label(constants.P0, constants.File,
		constants.Wcp, constants.VmServiceVm, constants.Vc901, constants.Disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmIPs, nfsAccessPointList, crdInstanceNameList []string
		vmCount := 3

		labelKey := "app"
		labelValue := "e2e-labels"

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
		secretName := vmservice_vm.CreateBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM")

		vms := vmservice_vm.CreateStandaloneVmServiceVm(
			ctx, vmopC, namespace, vmClass, vmi, storageClassName, secretName, vmopv1.VirtualMachinePoweredOn, vmCount)
		defer func() {
			ginkgo.By("Deleting VM")
			for _, vm := range vms {
				vmservice_vm.DeleteVmServiceVm(ctx, vmopC, namespace, vm.Name)
				crdInstanceName := pvc.Name + vm.Name
				k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
					constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, false)
			}

		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := vmservice_vm.CreateService4Vm(ctx, vmopC, namespace, vms[0].Name)
		defer func() {

			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		var wg sync.WaitGroup
		errChan := make(chan error)
		wg.Add(5)
		for _, vm := range vms[:1] {
			ginkgo.By("Wait for VM to come up and get an IP")
			vmIp, err := vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIPs = append(vmIPs, vmIp)

			ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
			crdInstanceName := pvc.Name + vm.Name
			crdInstanceNameList = append(crdInstanceNameList, crdInstanceName)
			go CreateCnsFileAccessConfigCRDWithWg(ctx, restConfig, pvc.Name, vm.Name, namespace, crdInstanceName, &wg)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		go vcutil.StopServiceWithWg(ctx, e2eTestConfig, e2eTestConfig.TestInput.TestBedInfo.VcAddress,
			constants.VsanhealthServiceName, &wg, errChan)
		isVsanHealthServiceDown = true

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pvs[0].Name))
		pvs[0].Labels = labels

		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pvs[0], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		wg.Wait()

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		vcutil.StartVCServiceWait4VPs(ctx, e2eTestConfig, e2eTestConfig.TestInput.TestBedInfo.VcAddress,
			constants.VsanhealthServiceName, &isVsanHealthServiceDown)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		for _, crdInstanceName := range crdInstanceNameList {
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

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvc.Name, pvc.Namespace))
		err = vcutil.WaitForLabelsToBeUpdated(e2eTestConfig, pvs[0].Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pvs[0].Name))
		err = vcutil.WaitForLabelsToBeUpdated(e2eTestConfig, pvs[0].Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pvs[0].Name, pvs[0].Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		PSOD few hosts in the cluster during CFC CRD creation
		Steps:
		1. Create a PVC with RWX access mode with the assigned storage policy(latebinding volume mode).
		2. Verify PVC is stuck in Pending state.
		3. Create 3 VMService VMs.
		4. Verify all VMService VMs come to powered On state.
		5. Attach 3 VMServiceVms to this PVC by creating CNSFileAccessConfig CRD for each PVC-VM pair.
		6. PSOD a few hosts hosting VMService VMs in supervisor cluster while attachment of VMServiceVMs to volume is ongoing.
		7. Verify CNSFileAccessConfig CRD gets created and verify NFS Access point has been populated in the CR.
		8. Verify the number of CNSFileAccessConfig CRDs generated is equal to number of VMServiceVM created.
		9. Verify PVC comes to bound state.
		10. Verify IO metadata by reading and writing to the volume through multiple VMs.
	*/

	ginkgo.It("PSOD few hosts in the cluster during CFC CRD creation", ginkgo.Label(constants.P1, constants.File,
		constants.Wcp, constants.VmServiceVm, constants.Vc901, constants.Disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmIPs, nfsAccessPointList, crdInstanceNameList []string
		vmCount := 3

		scParameters = make(map[string]string)
		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany,
			constants.Ext4FSType))
		storageclass, pvclaims, err := k8testutil.CreateStorageClassWithMultiplePVCs(client, e2eTestConfig, namespace,
			labelsMap, scParameters, constants.DiskSize, nil, bindingModeWffc, false, v1.ReadWriteMany,
			"", nil, 1, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pv := k8testutil.GetPvFromClaim(client, pvclaims[0].Namespace, pvclaims[0].Name)
		volHandle := pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[0].Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := vmservice_vm.CreateBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM")

		vms := vmservice_vm.CreateStandaloneVmServiceVm(
			ctx, vmopC, namespace, vmClass, vmi, storageClassName, secretName, vmopv1.VirtualMachinePoweredOn, vmCount)
		defer func() {
			ginkgo.By("Deleting VM")
			for _, vm := range vms {
				vmservice_vm.DeleteVmServiceVm(ctx, vmopC, namespace, vm.Name)
			}

		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := vmservice_vm.CreateService4Vm(ctx, vmopC, namespace, vms[0].Name)
		defer func() {

			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		var wg sync.WaitGroup
		errChan := make(chan error)
		wg.Add(4)
		for _, vm := range vms {
			ginkgo.By("Wait for VM to come up and get an IP")
			vmIp, err := vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIPs = append(vmIPs, vmIp)

			ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
			crdInstanceName := pvclaims[0].Name + vm.Name
			crdInstanceNameList = append(crdInstanceNameList, crdInstanceName)
			go CreateCnsFileAccessConfigCRDWithWg(ctx, restConfig, pvclaims[0].Name, vm.Name, namespace, crdInstanceName, &wg)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Perform Psod on few esxi hosts of the cluster")
		hostsInCluster := vcutil.GetHostsByClusterName(ctx, clusterComputeResource, clusterName)
		// for _, host := range hostsInCluster {
		// 	go vcutil.PsodHostwithWg(e2eTestConfig, host.Name(), constants.PsodTime, &wg, errChan)
		// }
		go vcutil.PsodHostwithWg(e2eTestConfig, hostsInCluster[0].Name(), constants.PsodTime, &wg, errChan)

		wg.Wait()
		defer func() {
			ginkgo.By("checking host status")
			// err := vcutil.WaitForHostToBeUp(hostsInCluster[0], e2eTestConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		for _, crdInstanceName := range crdInstanceNameList {
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

	})
})
