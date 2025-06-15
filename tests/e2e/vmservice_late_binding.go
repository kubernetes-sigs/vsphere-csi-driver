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

package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"

	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
)

var _ bool = ginkgo.Describe("[vmsvc] vm service with csi vol tests", func() {

	f := framework.NewDefaultFramework("vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                     clientset.Interface
		namespace                  string
		datastoreURL               string
		wffsStoragePolicyName      string
		storagePolicyName          string
		storageProfileId           string
		vcRestSessionId            string
		vmi                        string
		vmClass                    string
		vmopC                      ctlrclient.Client
		cnsopC                     ctlrclient.Client
		isVsanHealthServiceStopped bool
		isSPSserviceStopped        bool
		vcAddress                  string
		isQuotaValidationSupported bool
		defaultDatastore           *object.Datastore
		statuscode                 int
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client = f.ClientSet
		bootstrap()

		var err error
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		wffsStoragePolicyName = storagePolicyName + "-latebinding"

		isVsanHealthServiceStopped = false
		isSPSserviceStopped = false

		//storageClassName = strings.ReplaceAll(storagePolicyName, "_", "-") // since this is a wcp setup

		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		dsRef := getDsMoRefFromURL(ctx, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)
		framework.Logf("storageProfileId: %s", storageProfileId)
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort

		vcRestSessionId = createVcSession4RestApis(ctx)
		contentLibId, err := createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
			dsRef.Value)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Create a WCP namespace for the test")
		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}

		// creating namespace with zonal2 storage policy
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId),
			nil, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		framework.Logf("NameSpace : %s", namespace)

		//statusCode, _ := addCLToExistingNs(ctx, namespace, vmClass, contentLibId)
		//framework.Logf("Status code: %v", checkStatusCode(204, statusCode))

		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi = waitNGetVmiForImageName(ctx, vmopC, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())

		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)

		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		for _, dc := range datacenters {
			defaultDatacenter, err := finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("DefaultDatastore: %s", defaultDatastore)
		}

		if supervisorCluster || stretchedSVC {
			time.Sleep(60)
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			//if isQuotaValidationSupported is true then quotaValidation is considered in tests
			vcVersion = getVCversion(ctx, vcAddress)
			isQuotaValidationSupported = isVersionGreaterOrEqual(vcVersion, quotaSupportedVCVersion)
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if isVsanHealthServiceStopped {
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}

		if isSPSserviceStopped {
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", spsServiceName))
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isSPSserviceStopped)
		}
		dumpSvcNsEventsOnTestFailure(client, namespace)
	})

	/**
	vmserviceVM-WFFC-BasicCase
	*/
	ginkgo.It("vmserviceVM-WFFC-BasicCase", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvc_totalQuotaUsedBefore, pvc_storagePolicyQuotaBefore, pvc_storagePolicyUsageBefore *resource.Quantity
		var vm_totalQuotaUsedBefore, vm_storagePolicyQuotaBefore, vm_storagePolicyUsageBefore *resource.Quantity

		restConfig = getRestConfigClient()
		framework.Logf("NameSpace : %s", namespace)
		if isQuotaValidationSupported {
			ginkgo.By("create resource quota")
			setStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, rqLimit)
		}

		wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		Storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		if isQuotaValidationSupported {
			pvc_totalQuotaUsedBefore, _, pvc_storagePolicyQuotaBefore, _, pvc_storagePolicyUsageBefore, _ =
				getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					Storageclass.Name, namespace, pvcUsage, volExtensionName)

			framework.Logf("pvc_totalQuotaUsedBefore: %v, pvc_storagePolicyQuotaBefore: %v, pvc_storagePolicyUsageBefore: %v",
				pvc_totalQuotaUsedBefore, pvc_storagePolicyQuotaBefore, pvc_storagePolicyUsageBefore)

			vm_totalQuotaUsedBefore, _, vm_storagePolicyQuotaBefore, _, vm_storagePolicyUsageBefore, _ =
				getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					Storageclass.Name, namespace, vmUsage, vmServiceExtensionName)

			framework.Logf("vm_totalQuotaUsedBefore: %v, vm_storagePolicyQuotaBefore: %v, vm_storagePolicyUsageBefore: %v",
				vm_totalQuotaUsedBefore, vm_storagePolicyQuotaBefore, vm_storagePolicyUsageBefore)

		}
		ginkgo.By("Create a PVC")
		pvc, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VMs")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, wffsStoragePolicyName, secretName)
		defer func() {
			ginkgo.By("Deleting VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for all claims to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volHandle := pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc1 := createService4Vm(ctx, vmopC, namespace, vm1.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing services for ssh for the VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("write some data to a file in pvc1 from vm1")
		// rand.New(rand.NewSource(time.Now().Unix()))
		// testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		// framework.Logf("Creating a 100mb test data file %v", testdataFile)
		// op, err := exec.Command(
		// 	"bash", "-c", "dd if=/dev/urandom bs=1M count=1 | tr -dc 'a-zA-Z0-9' >"+testdataFile).Output()
		// // using 'tr' to filter out non-alphanumeric characters
		// fmt.Println(op)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// op, err = exec.Command("md5sum", testdataFile).Output()
		// fmt.Println("md5sum", string(op[:]))
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// op, err = exec.Command("ls", "-l", testdataFile).Output()
		// fmt.Println(string(op[:]))
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVC is attached to the VM1")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc})).To(gomega.Succeed())
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Mounting the volume")
		volFolder := mountFormattedVol2Vm(vm1.Status.Volumes[0].DiskUuid, 1, vmIp1)
		vmFileData := fmt.Sprintf("/tmp/vmdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		_ = execSshOnVmThroughGatewayVm(vmIp1, []string{"md5sum " + volFolder + "/f1"})
		framework.Logf("Fetching file from the VM")
		copyFileFromVm(vmIp1, volFolder+"/f1", vmFileData)

		// framework.Logf("Comparing file fetched from the VM with test data file")
		// c := []string{"md5sum", testdataFile, vmFileData}
		// op, err = exec.Command(c[0], c[1:]...).Output()
		// framework.Logf("Command: %v, output: %v", c, op)
		// lines := strings.Split(string(op[:]), "\n")
		// gomega.Expect(strings.Fields(lines[0])[0]).To(gomega.Equal(strings.Fields(lines[1])[0]))

		if isQuotaValidationSupported {
			pvc_totalQuotaUsedAfter, _, pvc_storagePolicyQuotaAfter, _, pvc_storagePolicyUsageAfter, _ :=
				getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					wffcStorageclass.Name, namespace, pvcUsage, volExtensionName)

			framework.Logf("pvc_totalQuotaUsedAfter: %v, pvc_storagePolicyQuotaAfter: %v, pvc_storagePolicyUsageAfter: %v",
				pvc_totalQuotaUsedAfter, pvc_storagePolicyQuotaAfter, pvc_storagePolicyUsageAfter)

			vm_totalQuotaUsedAfter, vm_totalQuota_reserved_after, vm_storagePolicyQuotaAfter, _,
				vm_storagePolicyUsageAfter, vm_storagePolicyUsageReserved :=
				getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					wffcStorageclass.Name, namespace, vmUsage, vmServiceExtensionName)

			framework.Logf("vm_totalQuotaUsedAfter: %v, vm_totalQuota_reserved_after: %v, vm_storagePolicyQuotaAfter: %v,"+
				"vm_storagePolicyUsageAfter: %v, vm_storagePolicyUsageReserved: %v", vm_totalQuotaUsedAfter,
				vm_totalQuota_reserved_after, vm_storagePolicyQuotaAfter, vm_storagePolicyUsageAfter, vm_storagePolicyUsageReserved)

		}

	})

	/**
	vmserviceVM-WFFC-BasicCase
	*/
	ginkgo.It("MultiplePVCs-Immediate-WFFC-Binding-Mapped-To-SingleVM", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvc_totalQuotaUsedBefore, pvc_storagePolicyQuotaBefore, pvc_storagePolicyUsageBefore *resource.Quantity
		var vm_totalQuotaUsedBefore, vm_storagePolicyQuotaBefore, vm_storagePolicyUsageBefore *resource.Quantity

		restConfig = getRestConfigClient()
		framework.Logf("NameSpace : %s", namespace)
		if isQuotaValidationSupported {
			ginkgo.By("create resource quota")
			setStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, rqLimit)
		}

		wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		if isQuotaValidationSupported {
			pvc_totalQuotaUsedBefore, _, pvc_storagePolicyQuotaBefore, _, pvc_storagePolicyUsageBefore, _ =
				getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, pvcUsage, volExtensionName)

			framework.Logf("pvc_totalQuotaUsedBefore: %v, pvc_storagePolicyQuotaBefore: %v, pvc_storagePolicyUsageBefore: %v",
				pvc_totalQuotaUsedBefore, pvc_storagePolicyQuotaBefore, pvc_storagePolicyUsageBefore)

			vm_totalQuotaUsedBefore, _, vm_storagePolicyQuotaBefore, _, vm_storagePolicyUsageBefore, _ =
				getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, vmUsage, vmServiceExtensionName)

			framework.Logf("vm_totalQuotaUsedBefore: %v, vm_storagePolicyQuotaBefore: %v, vm_storagePolicyUsageBefore: %v",
				vm_totalQuotaUsedBefore, vm_storagePolicyQuotaBefore, vm_storagePolicyUsageBefore)

		}

		ginkgo.By("Create a PVC")
		pvc1, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc1}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a PVC")
		pvc2, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc2}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a PVC")
		pvc3, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a PVC")
		pvc4, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VMs")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc1, pvc2, pvc3, pvc4}, vmi, wffsStoragePolicyName, secretName)
		defer func() {
			ginkgo.By("Deleting VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for all claims to be in bound state")
		pvs3, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc3}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv3 := pvs3[0]
		volHandle3 := pv3.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle3).NotTo(gomega.BeEmpty())

		ginkgo.By("Waiting for all claims to be in bound state")
		pvs4, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc4}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv4 := pvs4[0]
		volHandle4 := pv4.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle4).NotTo(gomega.BeEmpty())

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc1 := createService4Vm(ctx, vmopC, namespace, vm1.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing services for ssh for the VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("write some data to a file in pvc1 from vm1")
		// rand.New(rand.NewSource(time.Now().Unix()))
		// testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		// framework.Logf("Creating a 100mb test data file %v", testdataFile)
		// op, err := exec.Command(
		// 	"bash", "-c", "dd if=/dev/urandom bs=1M count=1 | tr -dc 'a-zA-Z0-9' >"+testdataFile).Output()
		// // using 'tr' to filter out non-alphanumeric characters
		// fmt.Println(op)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// op, err = exec.Command("md5sum", testdataFile).Output()
		// fmt.Println("md5sum", string(op[:]))
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// op, err = exec.Command("ls", "-l", testdataFile).Output()
		// fmt.Println(string(op[:]))
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVC is attached to the VM1")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc3})).To(gomega.Succeed())
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Mounting the volume")
		volFolder := mountFormattedVol2Vm(vm1.Status.Volumes[0].DiskUuid, 1, vmIp1)
		vmFileData := fmt.Sprintf("/tmp/vmdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		_ = execSshOnVmThroughGatewayVm(vmIp1, []string{"md5sum " + volFolder + "/f1"})
		framework.Logf("Fetching file from the VM")
		copyFileFromVm(vmIp1, volFolder+"/f1", vmFileData)

		// framework.Logf("Comparing file fetched from the VM with test data file")
		// c := []string{"md5sum", testdataFile, vmFileData}
		// op, err = exec.Command(c[0], c[1:]...).Output()
		// framework.Logf("Command: %v, output: %v", c, op)
		// lines := strings.Split(string(op[:]), "\n")
		// gomega.Expect(strings.Fields(lines[0])[0]).To(gomega.Equal(strings.Fields(lines[1])[0]))

		if isQuotaValidationSupported {
			pvc_totalQuotaUsedAfter, _, pvc_storagePolicyQuotaAfter, _, pvc_storagePolicyUsageAfter, _ :=
				getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					wffcStorageclass.Name, namespace, pvcUsage, volExtensionName)

			framework.Logf("pvc_totalQuotaUsedAfter: %v, pvc_storagePolicyQuotaAfter: %v, pvc_storagePolicyUsageAfter: %v",
				pvc_totalQuotaUsedAfter, pvc_storagePolicyQuotaAfter, pvc_storagePolicyUsageAfter)

			vm_totalQuotaUsedAfter, vm_totalQuota_reserved_after, vm_storagePolicyQuotaAfter, _,
				vm_storagePolicyUsageAfter, vm_storagePolicyUsageReserved :=
				getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					wffcStorageclass.Name, namespace, vmUsage, vmServiceExtensionName)

			framework.Logf("vm_totalQuotaUsedAfter: %v, vm_totalQuota_reserved_after: %v, vm_storagePolicyQuotaAfter: %v,"+
				"vm_storagePolicyUsageAfter: %v, vm_storagePolicyUsageReserved: %v", vm_totalQuotaUsedAfter,
				vm_totalQuota_reserved_after, vm_storagePolicyQuotaAfter, vm_storagePolicyUsageAfter, vm_storagePolicyUsageReserved)

		}

	})
})
