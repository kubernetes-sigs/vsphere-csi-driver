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
	"os"
	"strings"

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
		vcAddress                  string
		isQuotaValidationSupported bool
		defaultDatastore           *object.Datastore
		statuscode                 int
		vmImageName                string
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client = f.ClientSet
		bootstrap()

		var err error
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		wffsStoragePolicyName = storagePolicyName + "-latebinding"

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

		// Create SVC namespace and assign storage policy and vmContent Library
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId),
			nil, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		framework.Logf("NameSpace : %s", namespace)

		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())

		vmImageName = GetAndExpectStringEnvVar(envVmsvcVmImageName)
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
			vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			//if isQuotaValidationSupported is true then quotaValidation is considered in tests
			vcVersion = getVCversion(ctx, vcAddress)
			isQuotaValidationSupported = isVersionGreaterOrEqual(vcVersion, quotaSupportedVCVersion)
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		delTestWcpNs(vcRestSessionId, namespace)
		gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())

		dumpSvcNsEventsOnTestFailure(client, namespace)
	})

	/**
	1. Create a PVC using WFFC / Late binding storage class
	2. PVC's with WFFC will be in pending state
	3. Use the above  PVC and create a VmService VM
	4. Once the VM is on Verify that the PVC should go to bound state
	5. TODO : verify PVC with the below annotations cns.vmware.com/selected-node-is-zone is set to true
	   volume.kubernetes.io/selected-node is set to zone - This should have the zone name where the VM gets provisioned
	6. Verify CNS metadata for PVC
	7. Verify PVC's attached to VM
	8. Validate TotalQuota, StoragePolicyQuota, storageQuotaUsage of VmserviceVm's and PVc's

	*/
	ginkgo.It("vmserviceVM-WFFC-BasicCase", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var totalQuotaUsedBefore, pvc_storagePolicyQuotaBefore, pvc_storagePolicyUsageBefore *resource.Quantity
		var vm_totalQuotaUsedBefore, vm_storagePolicyQuotaBefore, vm_storagePolicyUsageBefore *resource.Quantity

		ginkgo.By("Set Storage Quota on namespace")
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

		ginkgo.By("Read QuotaDetails Before creating workload")
		if isQuotaValidationSupported {
			totalQuotaUsedBefore, _, pvc_storagePolicyQuotaBefore, _, pvc_storagePolicyUsageBefore, _ =
				getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, pvcUsage, volExtensionName, true)

			vm_totalQuotaUsedBefore, _, vm_storagePolicyQuotaBefore, _, vm_storagePolicyUsageBefore, _ =
				getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, vmUsage, vmServiceExtensionName, true)
		}

		ginkgo.By("Create a PVC")
		pvc, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete PVCs")
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VM images to get listeed undernamespace and create VM")
		err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
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
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm1.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting PVC to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volHandle := pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		//TODO : Annotations and Zonal details  on PVC to be added

		ginkgo.By("get VM storage")
		vmQuotaUsed := getVMStorageData(ctx, vmopC, namespace, vm1.Name)
		framework.Logf("vmQuotaUsed : %s", vmQuotaUsed)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm1.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp1)
			verifyDataIntegrityOnVmDisk(vmIp1, volFolder)
		}

		ginkgo.By("Validate StorageQuotaDetails after creating the workloads")
		if isQuotaValidationSupported {
			var expectedTotalStorage []string
			diskSizeInMbStr := convertInt64ToStrMbFormat(diskSizeInMb)
			expectedTotalStorage = append(expectedTotalStorage, diskSizeInMbStr, vmQuotaUsed)

			validateTotalQuota(ctx, restConfig, storageclass.Name, namespace, expectedTotalStorage,
				totalQuotaUsedBefore, true)

			validateQuotaUsageAfterResourceCreation(ctx, restConfig, storageclass.Name, namespace, pvcUsage,
				volExtensionName, []string{diskSizeInMbStr}, totalQuotaUsedBefore, pvc_storagePolicyQuotaBefore,
				pvc_storagePolicyUsageBefore, true)

			validateQuotaUsageAfterResourceCreation(ctx, restConfig, storageclass.Name, namespace, vmUsage,
				vmServiceExtensionName, []string{vmQuotaUsed}, vm_totalQuotaUsedBefore, vm_storagePolicyQuotaBefore,
				vm_storagePolicyUsageBefore, true)

		}
	})

})
