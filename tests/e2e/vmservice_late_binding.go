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
	storagev1 "k8s.io/api/storage/v1"
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

var _ bool = ginkgo.Describe("[vmsvc] VM-Service-VM-LateBinding", func() {
	f := framework.NewDefaultFramework("vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                   clientset.Interface
		namespace                string
		datastoreURL             string
		wffsStoragePolicyName    string
		storagePolicyName        string
		storageProfileId         string
		vcRestSessionId          string
		vmi                      string
		vmClass                  string
		vmopC                    ctlrclient.Client
		defaultDatastore         *object.Datastore
		statuscode               int
		vmImageName              string
		quota                    map[string]*resource.Quantity
		isLateBinding            bool
		expectedTotalStorage     []string
		expected_pvcQuotaInMbStr string
		storageclass             *storagev1.StorageClass
		expected_vmQuotaStr      string
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client = f.ClientSet
		bootstrap()

		var err error
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		wffsStoragePolicyName = storagePolicyName + "-latebinding"
		isLateBinding = true

		//datastoreURL is required to get dsRef ID which is used to get contentLibId
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		dsRef := getDsMoRefFromURL(ctx, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)
		framework.Logf("storageProfileId: %s", storageProfileId)
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort

		vcRestSessionId = createVcSession4RestApis(ctx)
		contentLibId, err := createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
			dsRef.Value, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Create a WCP namespace for the test")
		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}

		// Create SVC namespace and assign storage policy and vmContent Library
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			nil, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))

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

		ginkgo.By("Set Storage Quota on namespace")
		restConfig = getRestConfigClient()
		setStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, rqLimit)

		ginkgo.By("Get Immediate binding stotage class")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Read QuotaDetails Before creating workload")
		quota = make(map[string]*resource.Quantity)
		//PVCQuota Details Before creating workload
		quota["totalQuotaUsedBefore"], _, quota["pvc_storagePolicyQuotaBefore"], _,
			quota["pvc_storagePolicyUsageBefore"], _ = getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
			storageclass.Name, namespace, pvcUsage, volExtensionName, isLateBinding)

		framework.Logf("quota[totalQuotaUsedBefore] : %s", quota["totalQuotaUsedBefore"])

		//VMQuota Details Before creating workload
		quota["vm_totalQuotaUsedBefore"], _, quota["vm_storagePolicyQuotaBefore"], _,
			quota["vm_storagePolicyUsageBefore"], _ = getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
			storageclass.Name, namespace, vmUsage, vmServiceExtensionName, isLateBinding)
		framework.Logf("quota[vm_storagePolicyQuotaBefore] : %s", quota["vm_storagePolicyQuotaBefore"])

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("expected_pvcQuotaInMbStr: %s", expected_pvcQuotaInMbStr)
		framework.Logf("expected_vmQuotaStr: %s", expected_vmQuotaStr)

		//Validate TotalQuota
		_, quotavalidationStatus := validateTotalQuota(ctx, restConfig, storageclass.Name, namespace,
			expectedTotalStorage, quota["totalQuotaUsedBefore"], true)
		gomega.Expect(quotavalidationStatus).NotTo(gomega.BeFalse())

		//Validates PVC quota in both StoragePolicyQuota and StoragePolicyUsage CR
		sp_quota_status_pvc, sp_usage_status_pvc := validateQuotaUsageAfterResourceCreation(ctx, restConfig,
			storageclass.Name, namespace, pvcUsage, volExtensionName, []string{expected_pvcQuotaInMbStr},
			quota["totalQuotaUsedBefore"], quota["pvc_storagePolicyQuotaBefore"], quota["pvc_storagePolicyUsageBefore"], true)
		gomega.Expect(sp_quota_status_pvc && sp_usage_status_pvc).NotTo(gomega.BeFalse())

		//Validates VM quota in both StoragePolicyQuota and StoragePolicyUsage CR
		sp_quota_status_vm, sp_usage_status_vm := validateQuotaUsageAfterResourceCreation(ctx, restConfig,
			storageclass.Name, namespace, vmUsage, vmServiceExtensionName, []string{expected_vmQuotaStr},
			quota["vm_totalQuotaUsedBefore"], quota["vm_storagePolicyQuotaBefore"],
			quota["vm_storagePolicyUsageBefore"], true)
		gomega.Expect(sp_quota_status_vm && sp_usage_status_vm).NotTo(gomega.BeFalse())

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
	ginkgo.It("vmserviceVM-WFFC-BasicCase", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Get WFFC stotage class")
		wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		ginkgo.By("Create a PVC")
		pvc, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM images to get listed under namespace and create VM")
		err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		ginkgo.By("Create vm service vm")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, wffsStoragePolicyName, secretName)

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		_ = createService4Vm(ctx, vmopC, namespace, vm1.Name)

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
		expected_pvcQuotaInMbStr = convertInt64ToStrMbFormat(diskSizeInMb)
		expected_vmQuotaStr = vmQuotaUsed
		expectedTotalStorage = append(expectedTotalStorage, expected_pvcQuotaInMbStr, expected_vmQuotaStr)

	})

})
