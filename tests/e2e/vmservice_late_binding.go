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
	"os"
	"strings"
	"time"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmopv2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmopv3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	vmopv4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	vmopv5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
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
		snapc                    *snapclient.Clientset
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
		gomega.Expect(vmopv2.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		gomega.Expect(vmopv3.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		gomega.Expect(vmopv4.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		gomega.Expect(vmopv5.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())

		vmImageName = GetAndExpectStringEnvVar(envVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		ginkgo.By("Wait for VM images to get listed under namespace and create VM")
		err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmi = waitNGetVmiForImageName(ctx, vmopC, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())
		framework.Logf("vm image: %s", vmi)

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

		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("expected_pvcQuotaInMbStr: %s", expected_pvcQuotaInMbStr)
		framework.Logf("expected_vmQuotaStr: %s", expected_vmQuotaStr)

		//Validate TotalQuota
		// _, quotavalidationStatus := validateTotalQuota(ctx, restConfig, storageclass.Name, namespace,
		// 	expectedTotalStorage, quota["totalQuotaUsedBefore"], true)
		// gomega.Expect(quotavalidationStatus).NotTo(gomega.BeFalse())

		//Validates PVC quota in both StoragePolicyQuota and StoragePolicyUsage CR
		// sp_quota_status_pvc, sp_usage_status_pvc := validateQuotaUsageAfterResourceCreation(ctx, restConfig,
		// 	storageclass.Name, namespace, pvcUsage, volExtensionName, []string{expected_pvcQuotaInMbStr},
		// 	quota["totalQuotaUsedBefore"], quota["pvc_storagePolicyQuotaBefore"], quota["pvc_storagePolicyUsageBefore"], true)
		// gomega.Expect(sp_quota_status_pvc && sp_usage_status_pvc).NotTo(gomega.BeFalse())

		// //Validates VM quota in both StoragePolicyQuota and StoragePolicyUsage CR
		// sp_quota_status_vm, sp_usage_status_vm := validateQuotaUsageAfterResourceCreation(ctx, restConfig,
		// 	storageclass.Name, namespace, vmUsage, vmServiceExtensionName, []string{expected_vmQuotaStr},
		// 	quota["vm_totalQuotaUsedBefore"], quota["vm_storagePolicyQuotaBefore"],
		// 	quota["vm_storagePolicyUsageBefore"], true)
		// gomega.Expect(sp_quota_status_vm && sp_usage_status_vm).NotTo(gomega.BeFalse())

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
		vm1 := createVmServiceVmV4(ctx, vmopC, CreateVmOptionsV4{
			Namespace:          namespace,
			VmClass:            vmClass,
			VMI:                vmi,
			StorageClassName:   wffsStoragePolicyName,
			PVCs:               []*v1.PersistentVolumeClaim{pvc},
			SecretName:         secretName,
			WaitForReadyStatus: true,
		})

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("vmIp : %s", vmIp1)

		ginkgo.By("Waiting PVC to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volHandle := pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		_, err = e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		time.Sleep(pollTimeoutShort)
		vm1v4, err := getVmsvcVM4(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		//Get zone details of VM
		zone, err := getVMzone(ctx, vm1v4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Verify Annotations and Zonal details  on PVC
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = validateAnnotationOnPVC(pvc, selectedNodeIsZone, "true")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = validateAnnotationOnPVC(pvc, selectedNodeAnnotationOnPVC, zone)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("get VM storage")
		vmQuotaUsed := getVMStorageData(ctx, vmopC, namespace, vm1.Name)
		framework.Logf("vmQuotaUsed : %s", vmQuotaUsed)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
		if !isPrivateNetwork {
			ginkgo.By("Verify PVCs are accessible to the VM")
			ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
			vm1v4, err = getVmsvcVM4(ctx, vmopC, vm1v4.Namespace, vm1v4.Name) // refresh vm info
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for i, vol := range vm1v4.Status.Volumes {
				if vol.Name == pvc.Name {
					volFolder := formatNVerifyPvcIsAccessible(vol.DiskUUID, i+1, vmIp1)
					verifyDataIntegrityOnVmDisk(vmIp1, volFolder)
				}
			}
		}

		ginkgo.By("Validate StorageQuotaDetails after creating the workloads")
		expected_pvcQuotaInMbStr = convertInt64ToStrMbFormat(diskSizeInMb)
		expected_vmQuotaStr = vmQuotaUsed
		expectedTotalStorage = append(expectedTotalStorage, expected_pvcQuotaInMbStr, expected_vmQuotaStr)

	})

	/**
	1. Create multiple PVC's . Consider 2 PVC's with WFFC and 2 PVC's with immediate
	2. Use the above  PVC's and create a VmService VM's
	3. Wait for VM  get powered ON
	4. Once the VM is on Verify that the PVC's with WFFC should go to bound state
		Describe PVC and verify the below annotations
		cns.vmware.com/selected-node-is-zone is set to true
		volume.kubernetes.io/selected-node is set to  Zone - This should have the zone name where
		the VM gets provisioned
	5. PVC's with Immediate binding will not have any annotation
	6. Verify CNS metadata for PVC
	7. Verify the storagePolicyQuotaUsage and VMstorageQuotaUsage CR's on the late binding storage class
	   Quota should have the appropriate quota consumption details
	   Clean up all the above data
	*/
	ginkgo.It("MultiplePVC-AttachedTo-SingleVM-ImmediateAndWFFC", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pvc, pvc2 []*v1.PersistentVolumeClaim

		ginkgo.By("Get WFFC stotage class")
		wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		immediateBindingStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		for i := 0; i < 2; i++ {
			pvc[i], err = createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create a PVC")
			pvc2[i], err = createPVC(ctx, client, namespace, nil, "", immediateBindingStorageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		ginkgo.By("Wait for VM images to get listed under namespace and create VM")
		err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		ginkgo.By("Create vm service vm")
		vm1 := createVmServiceVmV4(ctx, vmopC, CreateVmOptionsV4{
			Namespace:          namespace,
			VmClass:            vmClass,
			VMI:                vmi,
			StorageClassName:   wffsStoragePolicyName,
			PVCs:               []*v1.PersistentVolumeClaim{pvc[0], pvc[1], pvc2[0], pvc2[1]},
			SecretName:         secretName,
			WaitForReadyStatus: true,
		})

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting PVC to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc[0], pvc[1], pvc2[0],
			pvc2[1]}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("get VM storage")
		vmQuotaUsed := getVMStorageData(ctx, vmopC, namespace, vm1.Name)
		framework.Logf("vmQuotaUsed : %s", vmQuotaUsed)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		instanceKey := ctlrclient.ObjectKey{Name: vm1.Name, Namespace: namespace}
		vm1 = &vmopv4.VirtualMachine{}
		err = vmopC.Get(ctx, instanceKey, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm1.Status.Volumes {
			if vol.Name == pvc[0].Name || vol.Name == pvc[1].Name || vol.Name == pvc2[0].Name || vol.Name == pvc2[1].Name {
				volFolder := formatNVerifyPvcIsAccessible(vol.DiskUUID, i+1, vmIp1)
				verifyDataIntegrityOnVmDisk(vmIp1, volFolder)
			}

		}

		vm1v4, err := getVmsvcVM4(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		//Get zone details of VM
		zone, err := getVMzone(ctx, vm1v4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Verify Annotations and Zonal details  on PVC
		for i := 0; i < 2; i++ {
			pvc[i], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc[i].Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = validateAnnotationOnPVC(pvc[i], selectedNodeIsZone, "true")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = validateAnnotationOnPVC(pvc[i], selectedNodeAnnotationOnPVC, zone)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = validateAnnotationOnPVC(pvc2[i], selectedNodeAnnotationOnPVC, zone)
			gomega.Expect(err).To(gomega.HaveOccurred())
		}

		// ginkgo.By("Validate StorageQuotaDetails after creating the workloads")
		//expected_pvcQuotaInMbStr = convertInt64ToStrMbFormat(diskSizeInMb)
		// expected_vmQuotaStr = vmQuotaUsed
		// expectedTotalStorage = append(expectedTotalStorage, expected_pvcQuotaInMbStr, expected_vmQuotaStr)

	})

	/**
		1. Create PVC using WFFC storage class - PVC will be in pending state
		2. Create a VM service VM using above PVC
	    3. Wait for VM to power on
	    4. Wait for PVC to reach bound state, PVC should have below annotation
	       cns.vmware.com/selected-node-is-zone is set to true
	       volume.kubernetes.io/selected-node is set to  Zone - This should have the zone
		   name where the VM gets provisioned
	    5. Once the VM is up, verify that the volume is accessible inside the VM
	    6. Create a volume snapshot for the PVC created in step 1
	    7. Wait for snapshots ready state reach to true
	    8. Verify CNS metadata from Volume and snapshot
	    9. Create a new PVC2 from the snapshot created in step 7 use WFFC policy on same datastore,
		   PVC should not reach bound state until the VM is created]
	    10. attach the above created pvc2 to VM1.
	    11. Verify CNS metadata for a PVC2
	    12. Verify PVC2 should also has the annotations mentioned in step 4
	    13. Once the VM is up, verify that the volume is accessible inside the VM
	    14. Verify reading/writing data in the volume.
	    15. Clean up all the  above data
	*/

	ginkgo.It("Attach-restored-WFFC-PVC-to-ExistingVMServiceVM", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Get WFFC stotage class")
		wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		ginkgo.By("Create a PVC")
		pvc1, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM images to get listed under namespace and create VM")
		err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		ginkgo.By("Create vm service vm")
		vm1 := createVmServiceVmV4(ctx, vmopC, CreateVmOptionsV4{
			Namespace:          namespace,
			VmClass:            vmClass,
			VMI:                vmi,
			StorageClassName:   wffsStoragePolicyName,
			PVCs:               []*v1.PersistentVolumeClaim{pvc1},
			SecretName:         secretName,
			WaitForReadyStatus: true,
		})

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("vmIp1 : %s", vmIp1)

		ginkgo.By("Waiting PVC to be in bound state")
		pv, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc1}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv[0].Spec.CSI.VolumeHandle

		ginkgo.By("get VM storage")
		vmQuotaUsed := getVMStorageData(ctx, vmopC, namespace, vm1.Name)
		framework.Logf("vmQuotaUsed : %s", vmQuotaUsed)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		vm1, err = getVmsvcVM4(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, _, _,
			_, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc1, volHandle, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		snapshotSize := getAggregatedSnapshotCapacityInMb(e2eVSphere, volHandle)
		snapshotSizeStr := convertInt64ToStrMbFormat(snapshotSize)
		framework.Logf("snapshotSizeStr: %s", snapshotSizeStr)

		ginkgo.By("Restore a pvc using a dynamic volume snapshot created above using WFFC storageclass")
		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, wffcStorageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to attach pvc2 to vm1")
		vm1v5, err := getVmsvcVM5(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Append new volume with a required Name field
		vm1v5.Spec.Volumes = append(vm1v5.Spec.Volumes,
			vmopv5.VirtualMachineVolume{
				Name: pvclaim2.Name,
				VirtualMachineVolumeSource: vmopv5.VirtualMachineVolumeSource{
					PersistentVolumeClaim: &vmopv5.PersistentVolumeClaimVolumeSource{
						PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvclaim2.Name,
						},
					},
				},
			},
		)

		err = vmopC.Update(ctx, vm1v5)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting PVC to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim2}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Get zone details of VM
		zone, err := getVMzone(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Verify Annotations and Zonal details  on PVC
		pvclaim2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = validateAnnotationOnPVC(pvclaim2, selectedNodeIsZone, "true")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = validateAnnotationOnPVC(pvclaim2, selectedNodeAnnotationOnPVC, zone)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Validate StorageQuotaDetails after creating the workloads")
		//expected_pvcQuotaInMbStr = convertInt64ToStrMbFormat(diskSizeInMb)
		// expected_vmQuotaStr = vmQuotaUsed
		// expectedTotalStorage = append(expectedTotalStorage, expected_pvcQuotaInMbStr, expected_vmQuotaStr)

	})

	/**
		1. Create PVC using WFFC storage class - PVC will be in pending state
	    2. Create a VM service VM using above PVC
	    3. Wait for VM to power on
	    4. Wait for PVC to reach bound state, PVC should have below annotation
			cns.vmware.com/selected-node-is-zone is set to true
			volume.kubernetes.io/selected-node is set to  Zone - This should have the zone name where the VM
			gets provisioned
			Once the VM is up, verify that the volume is accessible inside the VM
		5. Write some IO to the CSI volumes, read it back from them and verify the data integrity
		6. Create a volume snapshot for the PVC created in step 1
		7. Wait for snapshots ready state reach to true
		8. Verify CNS metadata from Volume and snapshot
		9. Create a new PVC2 from the snapshot created in step 7 use different policy on the same datastore with
		immediate binding
		10. Create VM2 from PVC2 created in step10
		11. Verify CNS metadata for a PVC2
		12. Verify PVC2 should also has the annotations mentioned in step 4
		13. Once the VM is up, verify that the volume is accessible inside the VM
		14. Verify reading/writing data in the volume.
		15. Clean up all the  above data
	*/
	ginkgo.It("Attach-restoredPVC-UsingImmediateBindingStorageclass-to-ExistingVMServiceVM", ginkgo.Label(p0,
		block, wcp, vmsvc, vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Get WFFC stotage class")
		wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		ginkgo.By("Create a PVC")
		pvc1, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM images to get listed under namespace and create VM")
		err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		ginkgo.By("Create vm service vm")
		vm1 := createVmServiceVmV4(ctx, vmopC, CreateVmOptionsV4{
			Namespace:          namespace,
			VmClass:            vmClass,
			VMI:                vmi,
			StorageClassName:   wffsStoragePolicyName,
			PVCs:               []*v1.PersistentVolumeClaim{pvc1},
			SecretName:         secretName,
			WaitForReadyStatus: true,
		})

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("vmIp1 : %s", vmIp1)

		ginkgo.By("Waiting PVC to be in bound state")
		pv, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc1}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv[0].Spec.CSI.VolumeHandle

		ginkgo.By("get VM storage")
		vmQuotaUsed := getVMStorageData(ctx, vmopC, namespace, vm1.Name)
		framework.Logf("vmQuotaUsed : %s", vmQuotaUsed)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		vm1, err = getVmsvcVM4(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, _, _,
			_, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc1, volHandle, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		snapshotSize := getAggregatedSnapshotCapacityInMb(e2eVSphere, volHandle)
		snapshotSizeStr := convertInt64ToStrMbFormat(snapshotSize)
		framework.Logf("snapshotSizeStr : %s", snapshotSizeStr)

		ginkgo.By("Restore a pvc using a dynamic volume snapshot created above using immediate storageclass")
		pvclaim2, _, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot, diskSize, false)

		ginkgo.By("Waiting PVC to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim2}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to attach pvc2 to vm1")
		vm1v5, err := getVmsvcVM5(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Append new volume with a required Name field
		vm1v5.Spec.Volumes = append(vm1v5.Spec.Volumes,
			vmopv5.VirtualMachineVolume{
				Name: pvclaim2.Name,
				VirtualMachineVolumeSource: vmopv5.VirtualMachineVolumeSource{
					PersistentVolumeClaim: &vmopv5.PersistentVolumeClaimVolumeSource{
						PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvclaim2.Name,
						},
					},
				},
			},
		)
		err = vmopC.Update(ctx, vm1v5)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var match bool
		vm1v5, err = getVmsvcVM5(ctx, vmopC, vm1v5.Namespace, vm1v5.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm1v5.Status.Volumes {
			if vol.Name == pvclaim2.Name {
				framework.Logf("PVC[%d] is attached : %s", i, vol.Name)
				match = true
			}
		}
		gomega.Expect(match).To(gomega.BeTrue())

		//Verify Annotations and Zonal details  on PVC
		pvclaim2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim2.Name, metav1.GetOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		err = validateAnnotationOnPVC(pvclaim2, selectedNodeIsZone, "true")
		gomega.Expect(err).To(gomega.HaveOccurred())

		// ginkgo.By("Validate StorageQuotaDetails after creating the workloads")
		//expected_pvcQuotaInMbStr = convertInt64ToStrMbFormat(diskSizeInMb)
		// expected_vmQuotaStr = vmQuotaUsed
		// expectedTotalStorage = append(expectedTotalStorage, expected_pvcQuotaInMbStr, expected_vmQuotaStr)

	})

	/**
	1. Create a PVC's using different datastore using WFFC / Late binding storage class
	2. PVC's with WFFC will be in pending state
	3. Use the above  PVC and create a VmService VM
	4. Once the VM is on Verify that the PVC should go to bound state
	5. Verify CNS metadata for PVC
	6. Verify PVC's attached to VM
	7. Validate TotalQuota, StoragePolicyQuota, storageQuotaUsage of VmserviceVm's and PVc's

	*/
	ginkgo.It("vmserviceVM-With-VolumesFrom-AllDifferentDatastore", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv4.AddToScheme(vmopScheme)).Should(gomega.Succeed())

		storagePolicyNameForNFSDatastore := GetAndExpectStringEnvVar(envStoragePolicyNameForNfsDatastores)
		storagePolicyNameForSharedVMFSDatastore := GetAndExpectStringEnvVar(envStoragePolicyNameForVmfsDatastores)

		if storagePolicyNameForNFSDatastore == "" || storagePolicyNameForSharedVMFSDatastore == "" {
			ginkgo.Skip("Skipping the test because NFS and SHARED_VMFS datastore ")
		} else {
			cnsOpScheme := runtime.NewScheme()
			gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())

			ginkgo.By("Get WFFC stotage class")
			wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName,
				metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

			ginkgo.By("Get WFFC stotage class of NFS datastore")
			nfsPolicyLateBinding := storagePolicyNameForNFSDatastore + "-latebinding"
			nfsPolicyLateBindingStorageClass, err := client.StorageV1().StorageClasses().Get(ctx,
				nfsPolicyLateBinding, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", nfsPolicyLateBinding,
				nfsPolicyLateBindingStorageClass)

			ginkgo.By("Get WFFC stotage class of sharedVMFS datastore")
			sharedVMFSPolicyLateBinding := storagePolicyNameForSharedVMFSDatastore + "-latebinding"
			sharedVMFPolicyLateBindingStorageClass, err := client.StorageV1().StorageClasses().Get(ctx,
				sharedVMFSPolicyLateBinding, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", sharedVMFSPolicyLateBinding,
				sharedVMFPolicyLateBindingStorageClass)

			ginkgo.By("Create a PVC")
			pvc, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create a PVC")
			pvc_NFS, err := createPVC(ctx, client, namespace, nil, "", nfsPolicyLateBindingStorageClass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create a PVC")
			pvc_sharedVMFS, err := createPVC(ctx, client, namespace, nil, "", sharedVMFPolicyLateBindingStorageClass,
				"")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Creating VM bootstrap data")
			secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

			ginkgo.By("Create vm service vm")
			vm1 := createVmServiceVmV4(ctx, vmopC, CreateVmOptionsV4{
				Namespace:          namespace,
				VmClass:            vmClass,
				VMI:                vmi,
				StorageClassName:   wffsStoragePolicyName,
				PVCs:               []*v1.PersistentVolumeClaim{pvc, pvc_NFS, pvc_sharedVMFS},
				SecretName:         secretName,
				WaitForReadyStatus: true,
			})

			ginkgo.By("Wait for VMs to come up and get an IP")
			vmIp1, err := waitNgetVmsvcVmIpV4(ctx, vmopC, namespace, vm1.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("vmIp1 : %s", vmIp1)

			ginkgo.By("Waiting PVC to be in bound state")
			pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc, pvc_NFS, pvc_sharedVMFS},
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pv := pvs[0]
			volHandle := pv.Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		}

		// ginkgo.By("Validate StorageQuotaDetails after creating the workloads")
		//expected_pvcQuotaInMbStr = convertInt64ToStrMbFormat(diskSizeInMb)
		// expected_vmQuotaStr = vmQuotaUsed
		// expectedTotalStorage = append(expectedTotalStorage, expected_pvcQuotaInMbStr, expected_vmQuotaStr)

	})

	/**
	1. Consider a namespace with limited Quota, Where VMshould come up But PVC should not reach bound state
	2. Using WFFC storage class , create PVC it will be in pending state
	3. Create VM - wait for Vm to reach power ON state
	4. Now, For PVC to reach bound state , The quota is not sufficient  - Verify the behaviour
	*/
	ginkgo.It("vmserviceVM-InsuffecientQuota", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv4.AddToScheme(vmopScheme)).Should(gomega.Succeed())

		setStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, "3Gi")

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())

		ginkgo.By("Get WFFC stotage class")
		wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		ginkgo.By("Create a PVC")
		pvc, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		vols := []vmopv1.VirtualMachineVolume{}

		vols = append(vols, vmopv1.VirtualMachineVolume{
			Name: pvc.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
			},
		})

		ginkgo.By("Create vm service vm")
		vm1 := vmopv1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: "vm1", Namespace: namespace},
			Spec: vmopv1.VirtualMachineSpec{
				PowerState:   vmopv1.VirtualMachinePoweredOn,
				ImageName:    vmi,
				ClassName:    vmClass,
				StorageClass: wffsStoragePolicyName,
				Volumes:      vols,
				VmMetadata:   &vmopv1.VirtualMachineMetadata{Transport: cloudInitLabel, SecretName: secretName},
			},
		}
		err = vmopC.Create(ctx, &vm1)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

})
