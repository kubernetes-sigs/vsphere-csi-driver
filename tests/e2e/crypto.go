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

package e2e

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/zapr"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmopv3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	cr_log "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

var _ = ginkgo.Describe("[csi-supervisor] [encryption] Block volume encryption", func() {
	f := framework.NewDefaultFramework("encryption")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	log := logger.GetLogger(context.Background())
	cr_log.SetLogger(zapr.NewLogger(log.Desugar()))

	var (
		client                clientset.Interface
		cryptoClient          crypto.Client
		vmopClient            ctlrclient.Client
		vmi                   string
		vmClass               string
		restConfig            *restclient.Config
		namespace             string
		standardStorageClass  *storagev1.StorageClass
		encryptedStorageClass *storagev1.StorageClass
		keyProviderID         string
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		restConfig = getRestConfigClient()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		// Init VC client
		err = connectCns(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Init crypto client
		cryptoClient, err = crypto.NewClientWithConfig(ctx, f.ClientConfig())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Load standard storage class
		standardStoragePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		standardStorageClass, err = createStorageClass(client,
			map[string]string{
				scParamStoragePolicyID: e2eVSphere.GetSpbmPolicyID(standardStoragePolicyName),
			},
			nil, "", "", false, standardStoragePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create storage class with err: %v", err))
		validateEncryptedStorageClass(ctx, cryptoClient, standardStoragePolicyName, false)
		setStoragePolicyQuota(ctx, restConfig, standardStoragePolicyName, namespace, rqLimit)

		// Load encrypted storage class
		encryptedStoragePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameWithEncryption)
		encryptedStorageClass, err = createStorageClass(client,
			map[string]string{
				scParamStoragePolicyID: e2eVSphere.GetSpbmPolicyID(encryptedStoragePolicyName),
			},
			nil, "", "", false, encryptedStoragePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create storage class with err: %v", err))
		validateEncryptedStorageClass(ctx, cryptoClient, encryptedStoragePolicyName, true)
		setStoragePolicyQuota(ctx, restConfig, encryptedStoragePolicyName, namespace, rqLimit)

		// Load key providers
		keyProviderID = GetAndExpectStringEnvVar(envKeyProvider)
		validateKeyProvider(ctx, keyProviderID)

		// Load VM-related properties
		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}
		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		gomega.Expect(vmopv3.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopClient, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi = waitNGetVmiForImageName(ctx, vmopClient, namespace, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			if standardStorageClass != nil {
				err := client.
					StorageV1().
					StorageClasses().
					Delete(ctx, standardStorageClass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if encryptedStorageClass != nil {
				err := client.
					StorageV1().
					StorageClasses().
					Delete(ctx, encryptedStorageClass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		svcClient, svNamespace := getSvcClientAndNamespace()
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
	})

	/*
		Verify PVC is encrypted with EC.

		1. Generate encryption key from KMS.
		2. Create EncryptionClass with newly generated encryption key.
		3. Create PVC associated with EncryptionClass, encrypted StorageClass and ReadWriteOnce access mode.
		4. Validate volume is encrypted with the encryption key specified in the
			EncryptionClass created in (2).
	*/
	ginkgo.It("Verify PVC is encrypted with EC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating new encryption key")
		keyID, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Creating an EncryptionClass")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Creating encrypted PVC")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, encClass.Name, true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Validate volume is encrypted with EC")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)
	})

	/*
		Verify PVC is encrypted with the default EC.

		1. Generate encryption key from KMS.
		2. Create EncryptionClass with newly generated encryption key and mark it as default for the namespace.
		3. Create PVC associated with encrypted StorageClass and ReadWriteOnce access mode.
		4. Validate volume is encrypted with the encryption key specified
			in the default EncryptionClass created in (2).
	*/
	ginkgo.It("Verify PVC is encrypted with the default EC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating new encryption key")
		keyID, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Creating an EncryptionClass")
		defaultEncClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, true)
		defer deleteEncryptionClass(ctx, cryptoClient, defaultEncClass)

		ginkgo.By("3. Creating encrypted PVC")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, "", true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Validate volume is encrypted")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)
	})

	/*
		Verify PVC is recrypted when setting new EC.

		1. Generate first encryption key from KMS.
		2. Generate second encryption key from KMS.
		3. Create EncryptionClass with generated encryption key (1).
		4. Create EncryptionClass with generated encryption key (2).
		5. Create PVC associated with EncryptionClass (3), encrypted StorageClass and ReadWriteOnce access mode.
		6. Validate volume is encrypted with encryption key (1)
		7. Associate PVC with EncryptionClass  (4)
		8. Validate volume is encrypted with encryption key (2)
	*/
	ginkgo.It("Verify PVC is recrypted when setting new EC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating first encryption key")
		keyID1, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Generating second encryption key")
		keyID2, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("3. Creating first EncryptionClass")
		encClass1 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID1, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass1)

		ginkgo.By("4. Creating second EncryptionClass")
		encClass2 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID2, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass2)

		ginkgo.By("5. Creating encrypted PVC")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, encClass1.Name, true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("6. Validate volume is encrypted with first key")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID1)

		ginkgo.By("7. Update encrypted PVC with second EncryptionClass")
		pvc = updatePersistentVolumeClaimWithCrypto(ctx, client, pvc, encryptedStorageClass.Name, encClass2.Name)

		ginkgo.By("8. Validate volume is encrypted with second key")
		validateVolumeToBeUpdatedWithEncryptedKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID2)
	})

	/*
		Verify PVC is recrypted when associated EC has its key rotated.

		1. Generate first encryption key from KMS.
		2. Generate second encryption key from KMS.
		3. Create EncryptionClass with generated encryption key (1).
		4. Create PVC associated with EncryptionClass, encrypted StorageClass and ReadWriteOnce access mode.
		5. Validate volume is encrypted with encryption key (1)
		6. Change EncryptionClass key (2)
		7. Validate volume is encrypted with encryption key (2)
	*/
	ginkgo.It("Verify PVC is recrypted when associated EC has its key rotated", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating first encryption key")
		keyID1, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Generating second encryption key")
		keyID2, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("3. Creating EncryptionClass")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID1, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("4. Creating encrypted PVC")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, encClass.Name, true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("5. Validate volume is encrypted with first encryption key")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID1)

		ginkgo.By("6. Update EncryptionClass with second key")
		updateEncryptionClass(ctx, cryptoClient, encClass, keyProviderID, keyID2, false)

		ginkgo.By("7. Validate volume is encrypted with second encryption key")
		validateVolumeToBeUpdatedWithEncryptedKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID2)
	})

	/*
		Verify PVC creation fails when associated with EC and SC not supporting encryption.

		1. Generate encryption key from KMS.
		2. Create EncryptionClass with newly generated encryption key.
		3. Create PVC associated with EncryptionClass, StorageClass without encryption capabilities.
		4. Validate operation fails.
	*/
	ginkgo.It("Verify PVC creation fails when associated with EC and SC not supporting encryption", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating new encryption key")
		keyID, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Creating an EncryptionClass")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Creating encrypted PVC")
		pvc := buildPersistentVolumeClaimWithCryptoSpec(namespace, standardStorageClass.Name, encClass.Name)
		_, err = client.
			CoreV1().
			PersistentVolumeClaims(namespace).
			Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	/*
		Verify VM encrypted with EC, PVC is not encrypted.
		Expectation: PVC attachment to VM should succeed.

		1. Generating encryption key
		2. Creating EncryptionClass
		3. Creating non-encrypted PVC
		4. Creating encrypted VM with non-encrypted PVC
		5. Validate VM is encrypted and attached volume is not encrypted
	*/
	ginkgo.It("Verify VM encrypted with EC, PVC is not encrypted.", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating encryption key")
		keyID, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Creating EncryptionClass")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Creating non-encrypted PVC")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, standardStorageClass.Name, "", true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Creating encrypted VM with PVC")
		vm := createVmServiceVmV3(ctx, vmopClient, CreateVmOptionsV3{
			Namespace:        namespace,
			VmClass:          vmClass,
			VMI:              vmi,
			StorageClassName: encryptedStorageClass.Name,
			PVCs:             []*v1.PersistentVolumeClaim{pvc},
			CryptoSpec: &vmopv3.VirtualMachineCryptoSpec{
				EncryptionClassName: encClass.Name,
			},
			WaitForReadyStatus: true,
		})
		defer deleteVmServiceVm(ctx, vmopClient, namespace, vm.Name)

		ginkgo.By("5. Validate VM is encrypted and attached volume is not encrypted")
		validateVmToBeEncryptedWithKey(vm, keyProviderID, keyID)
		validateVolumeNotToBeEncrypted(ctx, pvc.Spec.VolumeName)
	})

	/*
		Verify VM and PVC are encrypted with EC
		Expectation: PVC attachment to VM should succeed.

		1. Generating encryption key
		2. Creating EncryptionClass
		3. Creating encrypted PVC
		4. Creating encrypted VM with PVC
		5. Validate VM and attached volume are encrypted
	*/
	ginkgo.It("Verify VM encrypted with EC and PVC is encrypted with EC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating encryption key")
		keyID, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Creating EncryptionClass")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Creating encrypted PVC")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, encClass.Name, true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Creating encrypted VM with PVC")
		vm := createVmServiceVmV3(ctx, vmopClient, CreateVmOptionsV3{
			Namespace:        namespace,
			VmClass:          vmClass,
			VMI:              vmi,
			StorageClassName: encryptedStorageClass.Name,
			PVCs:             []*v1.PersistentVolumeClaim{pvc},
			CryptoSpec: &vmopv3.VirtualMachineCryptoSpec{
				EncryptionClassName: encClass.Name,
			},
			WaitForReadyStatus: true,
		})
		defer deleteVmServiceVm(ctx, vmopClient, namespace, vm.Name)

		ginkgo.By("5. Validate VM and attached volume are encrypted")
		validateVmToBeEncryptedWithKey(vm, keyProviderID, keyID)
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)
	})

	/*
		Verify VM and attached PVC are encrypted with default EncryptionClass
		Expectation: PVC attachment to VM should succeed.

		1. Generating encryption key
		2. Creating default EncryptionClass
		3. Creating encrypted PVC without EncryptionClass
		4. Creating encrypted VM without EncryptionClass and attached encrypted PVC
		5. Validate VM and attached volume are encrypted with default EncryptionClass
	*/
	ginkgo.It("Verify VM and attached PVC are encrypted with default EncryptionClass", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating encryption key")
		keyID, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Creating default EncryptionClass")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, true)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Creating encrypted PVC without EncryptionClass")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, "", true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Validate volume is encrypted")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)

		ginkgo.By("5. Creating encrypted VM without EncryptionClass and attached encrypted PVC")
		vm := createVmServiceVmV3(ctx, vmopClient, CreateVmOptionsV3{
			Namespace:          namespace,
			VmClass:            vmClass,
			VMI:                vmi,
			StorageClassName:   encryptedStorageClass.Name,
			PVCs:               []*v1.PersistentVolumeClaim{pvc},
			WaitForReadyStatus: true,
		})
		defer deleteVmServiceVm(ctx, vmopClient, namespace, vm.Name)

		ginkgo.By("6. Validate VM and attached volume are encrypted with default EncryptionClass")
		validateVmToBeEncryptedWithKey(vm, keyProviderID, keyID)
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)
	})

	/*
		Verify VM and attached PVC are encrypted with different keys
		Expectation: PVC attachment to VM should succeed.

		1. Generate first encryption key
		2. Generate second encryption key
		3. Create first EncryptionClass with generated encryption key (1)
		4. Create secord EncryptionClass with generated encryption key (2)
		5. Creating encrypted PVC with EncryptionClass (3)
		6. Creating encrypted VM with EncryptionClass (4) and attached encrypted PVC
		7. Validate VM is encrypted with key (2) and attached volume is encrypted with key (1)
	*/
	ginkgo.It("Verify VM and attached PVC are encrypted with different keys", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating first encryption key")
		keyID1, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Generating second encryption key")
		keyID2, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("3. Create secord EncryptionClass with generated encryption key (2)")
		encClass1 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID1, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass1)

		ginkgo.By("4. Creating second EncryptionClass")
		encClass2 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID2, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass2)

		ginkgo.By("5. Creating encrypted PVC with EncryptionClass (3)")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, encClass1.Name, true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("6. Creating encrypted VM with EncryptionClass (4) and attached encrypted PVC")
		vm := createVmServiceVmV3(ctx, vmopClient, CreateVmOptionsV3{
			Namespace:        namespace,
			VmClass:          vmClass,
			VMI:              vmi,
			StorageClassName: encryptedStorageClass.Name,
			CryptoSpec: &vmopv3.VirtualMachineCryptoSpec{
				EncryptionClassName: encClass2.Name,
			},
			PVCs:               []*v1.PersistentVolumeClaim{pvc},
			WaitForReadyStatus: true,
		})
		defer deleteVmServiceVm(ctx, vmopClient, namespace, vm.Name)

		ginkgo.By("7. Validate VM is encrypted with key (2) and attached volume is encrypted with key (1)")
		validateVmToBeEncryptedWithKey(vm, keyProviderID, keyID2)
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID1)
	})

	/*
		Verify VM and attached PVC are encrypted with EncryptionClass, key is rotated.
		Expectation: VM and PVC should be shallow re-crypted with the new key..

		1. Generate encryption key
		2. Create EncryptionClass with generated encryption key
		3. Creating encrypted PVC with EncryptionClass
		4. Creating encrypted VM with EncryptionClass and attached encrypted PVC
		5. Valida
		7. Validate VM is encrypted with key (2) and attached volume is encrypted with key (1)
	*/
	ginkgo.It("Verify VM and attached PVC are encrypted with different keys", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating first encryption key")
		keyID1, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Generating second encryption key")
		keyID2, err := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("3. Create secord EncryptionClass with generated encryption key (2)")
		encClass1 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID1, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass1)

		ginkgo.By("4. Creating second EncryptionClass")
		encClass2 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID2, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass2)

		ginkgo.By("5. Creating encrypted PVC with EncryptionClass (3)")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, encClass1.Name, true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("6. Creating encrypted VM with EncryptionClass (4) and attached encrypted PVC")
		vm := createVmServiceVmV3(ctx, vmopClient, CreateVmOptionsV3{
			Namespace:        namespace,
			VmClass:          vmClass,
			VMI:              vmi,
			StorageClassName: encryptedStorageClass.Name,
			CryptoSpec: &vmopv3.VirtualMachineCryptoSpec{
				EncryptionClassName: encClass2.Name,
			},
			PVCs:               []*v1.PersistentVolumeClaim{pvc},
			WaitForReadyStatus: true,
		})
		defer deleteVmServiceVm(ctx, vmopClient, namespace, vm.Name)

		ginkgo.By("7. Validate VM is encrypted with key (2) and attached volume is encrypted with key (1)")
		validateVmToBeEncryptedWithKey(vm, keyProviderID, keyID2)
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID1)
	})
})
