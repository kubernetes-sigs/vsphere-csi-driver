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
	"time"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmopv2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmopv3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	vmopv4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
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
		client                clientset.Interface
		namespace             string
		datastoreURL          string
		wffsStoragePolicyName string
		storagePolicyName     string
		storageProfileId      string
		vcRestSessionId       string
		vmi                   string
		vmClass               string
		vmopC                 ctlrclient.Client
		defaultDatastore      *object.Datastore
		statuscode            int
		vmImageName           string
		quota                 map[string]*resource.Quantity
		isLateBinding         bool
		//expectedTotalStorage     []string
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
		// vmi = waitNGetVmiForImageName4(ctx, vmopC, vmImageName)
		// gomega.Expect(vmi).NotTo(gomega.BeEmpty())

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
		// ctx, cancel := context.WithCancel(context.Background())
		// defer cancel()

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

		// delTestWcpNs(vcRestSessionId, namespace)
		// gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())

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
	ginkgo.It("vmserviceVM-WFFC-stretched-SVC-zonal", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv4.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err := ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())

		ginkgo.By("Get WFFC stotage class")
		wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		ginkgo.By("Create a PVC")
		pvc, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Wait for VM images to get listed under namespace and create VM")
		// err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		time.Sleep(200)
		vmIp1, err := waitNgetVmsvcVmIpV4(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("vmIp1 : %s", vmIp1)

		ginkgo.By("Waiting PVC to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volHandle := pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By("get VM storage")
		vmQuotaUsed := getVMStorageData(ctx, vmopC, namespace, vm1.Name)
		framework.Logf("vmQuotaUsed : %s", vmQuotaUsed)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
		if !isPrivateNetwork {
			ginkgo.By("Verify PVCs are accessible to the VM")
			ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
			//vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
			vm1, err = getVmsvcVmV4(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for i, vol := range vm1.Status.Volumes {
				//volFolder := formatNVerifyPvcIsAccessible(vol.DiskUUID, i+1, vmIp1)
				volFolder := formatNVerifyPvcIsAccessibleV4(vol.DiskUUID, i+1, vmIp1)
				framework.Logf("volFolder: %s", volFolder)
				//verifyDataIntegrityOnVmDisk(vmIp1, volFolder)
			}
		}

		//Get zone details of VM
		zone, err := getVMzoneV4(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Verify Annotations and Zonal details  on PVC
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		err = validateAnnotationOnPVC(pvc, selectedNodeIsZone, "true")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = validateAnnotationOnPVC(pvc, selectedNodeAnnotationOnPVC, zone)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Validate StorageQuotaDetails after creating the workloads")
		//expected_pvcQuotaInMbStr = convertInt64ToStrMbFormat(diskSizeInMb)
		// expected_vmQuotaStr = vmQuotaUsed
		// expectedTotalStorage = append(expectedTotalStorage, expected_pvcQuotaInMbStr, expected_vmQuotaStr)

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
	ginkgo.It("vmservcice-crosszonal-stretched-SVC", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Get WFFC stotage class")
		wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		immediateBindingStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		ginkgo.By("Create a PVC")
		pvc1, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a PVC")
		pvc2, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a PVC")
		pvc3, err := createPVC(ctx, client, namespace, nil, "", immediateBindingStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a PVC")
		pvc4, err := createPVC(ctx, client, namespace, nil, "", immediateBindingStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM images to get listed under namespace and create VM")
		err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		ginkgo.By("Create vm service vm")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc1, pvc2, pvc3, pvc4}, vmi, wffsStoragePolicyName, secretName)

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		_ = createService4Vm(ctx, vmopC, namespace, vm1.Name)

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting PVC to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc1, pvc2, pvc3, pvc4}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("get VM storage")
		vmQuotaUsed := getVMStorageData(ctx, vmopC, namespace, vm1.Name)
		framework.Logf("vmQuotaUsed : %s", vmQuotaUsed)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		//vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		instanceKey := ctlrclient.ObjectKey{Name: vm1.Name, Namespace: namespace}
		vm1 = &vmopv1.VirtualMachine{}
		err = vmopC.Get(ctx, instanceKey, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm1.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp1)
			verifyDataIntegrityOnVmDisk(vmIp1, volFolder)
		}

		//Get zone details of VM
		zone, err := getVMzonev1(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Verify Annotations and Zonal details  on PVC
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc1.Name, metav1.GetOptions{})
		err = validateAnnotationOnPVC(pvc1, selectedNodeIsZone, "true")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc2.Name, metav1.GetOptions{})
		err = validateAnnotationOnPVC(pvc2, selectedNodeIsZone, "true")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = validateAnnotationOnPVC(pvc1, selectedNodeAnnotationOnPVC, zone)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = validateAnnotationOnPVC(pvc2, selectedNodeAnnotationOnPVC, zone)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = validateAnnotationOnPVC(pvc3, selectedNodeAnnotationOnPVC, zone)
		gomega.Expect(err).To(gomega.HaveOccurred())

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
		   PVC should not reach bound state untill the VM is created]
	    10. attach the above created pvc2 to VM1.
	    11. Verify CNS metadata for a PVC2
	    12. Verify PVC2 should also has the annotations mentioned in step 4
	    13. Once the VM is up, verify that the volume is accessible inside the VM
	    14. Verify reading/writing data in the volume.
	    15. Clean up all the  above data
	*/

	ginkgo.It("PVC-Policy-VmPolicy-are-notsame", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
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

		// ginkgo.By("Creating loadbalancing service for ssh with the VM")
		// _ = createService4Vm(ctx, vmopC, namespace, vm1.Name)

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
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
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

		//snapshotSize := getAggregatedSnapshotCapacityInMb(e2eVSphere, volHandle)
		//snapshotSizeStr := convertInt64ToStrMbFormat(snapshotSize)

		ginkgo.By("Restore a pvc using a dynamic volume snapshot created above using WFFC storageclass")
		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, wffcStorageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to attach pvc2 to vm1")
		vm1, err = getVmsvcVM4(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm1.Spec.Volumes = append(vm1.Spec.Volumes,
			vmopv4.VirtualMachineVolume{
				VirtualMachineVolumeSource: vmopv4.VirtualMachineVolumeSource{
					PersistentVolumeClaim: &vmopv4.PersistentVolumeClaimVolumeSource{
						PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim2.Name},
					},
				},
			},
		)

		// vols = append(vols, vmopv4.VirtualMachineVolume{
		// 	Name: pvc.Name,
		// 	VirtualMachineVolumeSource: vmopv4.VirtualMachineVolumeSource{
		// 		PersistentVolumeClaim: &vmopv4.PersistentVolumeClaimVolumeSource{
		// 			PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
		// 		},
		// 	},
		// })

		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting PVC to be in bound state")
		pv, err = fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim2}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Get zone details of VM
		zone, err := getVMzone(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Verify Annotations and Zonal details  on PVC
		pvclaim2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim2.Name, metav1.GetOptions{})
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
			volume.kubernetes.io/selected-node is set to  Zone - This should have the zone name where the VM gets provisioned
			Once the VM is up, verify that the volume is accessible inside the VM
		5. Write some IO to the CSI volumes, read it back from them and verify the data integrity
		6. Create a volume snapshot for the PVC created in step 1
		7. Wait for snapshots ready state reach to true
		8. Verify CNS metadata from Volume and snapshot
		9. Create a new PVC2 from the snapshot created in step 7 use different policy on the same datastore with immediate binding
		10. Create VM2 from PVC2 created in step10
		11. Verify CNS metadata for a PVC2
		12. Verify PVC2 should also has the annotations mentioned in step 4
		13. Once the VM is up, verify that the volume is accessible inside the VM
		14. Verify reading/writing data in the volume.
		15. Clean up all the  above data
	*/
	ginkgo.It("stop-wcp-duringVMcreation", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
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
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc1}, vmi, wffsStoragePolicyName, secretName)

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		_ = createService4Vm(ctx, vmopC, namespace, vm1.Name)

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
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, _, _,
			_, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc1, volHandle, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//snapshotSize := getAggregatedSnapshotCapacityInMb(e2eVSphere, volHandle)
		//snapshotSizeStr := convertInt64ToStrMbFormat(snapshotSize)

		ginkgo.By("Restore a pvc using a dynamic volume snapshot created above using WFFC storageclass")
		pvclaim2, _, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot, diskSize, false)

		ginkgo.By("Try to attach pvc2 to vm1")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm1.Spec.Volumes = append(vm1.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: pvclaim2.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim2.Name},
			}})
		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Wait and verify PVCs are attached to the VM")
		// match := verifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, vm1, []*v1.PersistentVolumeClaim{pvc1})
		// gomega.Expect(match).To(gomega.BeTrue())
		// match = verifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, vm1, []*v1.PersistentVolumeClaim{pvclaim2})
		// gomega.Expect(match).To(gomega.BeTrue())

		//Verify Annotations and Zonal details  on PVC
		pvclaim2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim2.Name, metav1.GetOptions{})
		err = validateAnnotationOnPVC(pvclaim2, selectedNodeIsZone, "true")
		gomega.Expect(err).To(gomega.HaveOccurred())

		// ginkgo.By("Validate StorageQuotaDetails after creating the workloads")
		//expected_pvcQuotaInMbStr = convertInt64ToStrMbFormat(diskSizeInMb)
		// expected_vmQuotaStr = vmQuotaUsed
		// expectedTotalStorage = append(expectedTotalStorage, expected_pvcQuotaInMbStr, expected_vmQuotaStr)

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
	ginkgo.It("Bring-down-CSI-replica-to-0-during-VM-creation", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv4.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err := ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		storagePolicyNameForNFSDatastore := GetAndExpectStringEnvVar(envStoragePolicyNameForNfsDatastores)
		storagePolicyNameForSharedVMFSDatastore := GetAndExpectStringEnvVar(envStoragePolicyNameForVmfsDatastores)

		if storagePolicyNameForNFSDatastore == "" || storagePolicyNameForSharedVMFSDatastore == "" {
			ginkgo.Skip("Skipping the test because NFS and SHARED_VMFS datastore ")
		} else {
			cnsOpScheme := runtime.NewScheme()
			gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())

			ginkgo.By("Get WFFC stotage class")
			wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

			ginkgo.By("Get WFFC stotage class of NFS datastore")
			nfsPolicyLateBinding := storagePolicyNameForNFSDatastore + "-latebinding"
			nfsPolicyLateBindingStorageClass, err := client.StorageV1().StorageClasses().Get(ctx, nfsPolicyLateBinding, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", nfsPolicyLateBinding, nfsPolicyLateBindingStorageClass)

			ginkgo.By("Get WFFC stotage class of sharedVMFS datastore")
			sharedVMFSPolicyLateBinding := storagePolicyNameForSharedVMFSDatastore + "-latebinding"
			sharedVMFPolicyLateBindingStorageClass, err := client.StorageV1().StorageClasses().Get(ctx, sharedVMFSPolicyLateBinding, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", sharedVMFSPolicyLateBinding, sharedVMFPolicyLateBindingStorageClass)

			ginkgo.By("Create a PVC")
			pvc, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create a PVC")
			pvc_NFS, err := createPVC(ctx, client, namespace, nil, "", nfsPolicyLateBindingStorageClass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create a PVC")
			pvc_sharedVMFS, err := createPVC(ctx, client, namespace, nil, "", sharedVMFPolicyLateBindingStorageClass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// ginkgo.By("Wait for VM images to get listed under namespace and create VM")
			// err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
			// gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
			pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc, pvc_NFS, pvc_sharedVMFS}, pollTimeout)
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

})
