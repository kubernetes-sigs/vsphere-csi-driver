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
	"time"

	"github.com/go-logr/zapr"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmopv2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmopv3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	vmopv4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	cr_log "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

var _ = ginkgo.Describe("[ef-encryption][csi-supervisor] [encryption] Block volume encryption", func() {
	f := framework.NewDefaultFramework("encryption")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	log := logger.GetLogger(context.Background())
	cr_log.SetLogger(zapr.NewLogger(log.Desugar()))

	var (
		client                     clientset.Interface
		cryptoClient               crypto.Client
		vmopClient                 ctlrclient.Client
		vmi                        string
		vmClass                    string
		namespace                  string
		standardStorageClass       *storagev1.StorageClass
		encryptedStorageClass      *storagev1.StorageClass
		keyProviderID              string
		isVsanHealthServiceStopped bool
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		restConfig = getRestConfigClient()
		isVsanHealthServiceStopped = false

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
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to create storage class with err: %v", err))
		validateEncryptedStorageClass(ctx, cryptoClient, standardStoragePolicyName, false)

		// Load encrypted storage class
		encryptedStoragePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameWithEncryption)
		encryptedStorageClass, err = createStorageClass(client,
			map[string]string{
				scParamStoragePolicyID: e2eVSphere.GetSpbmPolicyID(encryptedStoragePolicyName),
			},
			nil, "", "", false, encryptedStoragePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to create storage class with err: %v", err))
		validateEncryptedStorageClass(ctx, cryptoClient, encryptedStoragePolicyName, true)

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
		gomega.Expect(vmopv2.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		gomega.Expect(vmopv3.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		gomega.Expect(vmopv4.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopClient, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi = waitNGetVmiForImageName(ctx, vmopClient, vmImageName)
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

		if isVsanHealthServiceStopped {
			ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
			err := invokeVCenterServiceControl(ctx, startOperation, vsanhealthServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}

		svcClient, svNamespace := getSvcClientAndNamespace()
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
	})

	/*
		Steps:
		1. Generate encryption key
		2. Create EncryptionClass with encryption key [1]
		3. Create PVC with EncryptionClass [2]
		4. Validate PVC volume [3] is encrypted with encryption key [1]
	*/
	ginkgo.It("Verify PVC is encrypted with EncryptionClass", ginkgo.Label(p1, block, wcp, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generate encryption key")
		keyID := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Create EncryptionClass with encryption key [1]")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Create PVC with EncryptionClass [2]")
		pvc := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:           namespace,
			StorageClassName:    encryptedStorageClass.Name,
			EncryptionClassName: encClass.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Validate PVC volume [3] is encrypted with encryption key [1]")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)
	})

	/*
		Steps:
		1. Generate encryption key
		2. Create default EncryptionClass with encryption key [1]
		3. Create a PVC with encrypted StorageClass but without specifying an EncryptionClass
		4. Validate PVC volume [3] is encrypted with encryption key [1]
	*/
	ginkgo.It("Verify PVC is encrypted with default EncryptionClass", ginkgo.Label(p1, block, wcp, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating encryption key")
		keyID := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Create default EncryptionClass with encryption key [1]")
		defaultEncClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, true)
		defer deleteEncryptionClass(ctx, cryptoClient, defaultEncClass)

		ginkgo.By("3. Create a PVC with encrypted StorageClass but without specifying an EncryptionClass")
		pvc := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:        namespace,
			StorageClassName: encryptedStorageClass.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Validate PVC volume [3] is encrypted with encryption key [1]")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)
	})

	/*
		Steps:
		1. Generate first encryption key
		2. Generate second encryption key
		3. Create first EncryptionClass with encryption key [1]
		4. Create second EncryptionClass with encryption key [2]
		5. Creating PVC with first EncryptionClass [3]
		6. Validate PVC volume [5] is encrypted with first encryption key [1]
		7. Update PVC with second EncryptionClass [4]
		8. Validate PVC volume [5] is encrypted with second encryption key [2]
	*/
	ginkgo.It("Verify PVC is recrypted when a new EncryptionClass is applied", ginkgo.Label(p1, block, wcp, vc90),
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ginkgo.By("1. Generate first encryption key")
			keyID1 := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

			ginkgo.By("2. Generate second encryption key")
			keyID2 := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

			ginkgo.By("3. Create first EncryptionClass with encryption key [1]")
			encClass1 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID1, false)
			defer deleteEncryptionClass(ctx, cryptoClient, encClass1)

			ginkgo.By("4. Create second EncryptionClass with encryption key [2]")
			encClass2 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID2, false)
			defer deleteEncryptionClass(ctx, cryptoClient, encClass2)

			ginkgo.By("5. Creating PVC with first EncryptionClass [3]")
			pvc := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
				Namespace:           namespace,
				StorageClassName:    encryptedStorageClass.Name,
				EncryptionClassName: encClass1.Name,
			})
			defer deletePersistentVolumeClaim(ctx, client, pvc)

			ginkgo.By("6. Validate PVC volume [5] is encrypted with first encryption key [1]")
			validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID1)

			ginkgo.By("7. Update PVC with second EncryptionClass [4]")
			pvc = updatePersistentVolumeClaimWithCrypto(ctx, client, pvc, encryptedStorageClass.Name, encClass2.Name)

			ginkgo.By("8. Validate PVC volume [5] is encrypted with second encryption key [2]")
			validateVolumeToBeUpdatedWithEncryptedKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID2)
		})

	/*
		Steps:
		1. Generate first encryption key
		2. Generate second encryption key
		3. Create EncryptionClass with encryption key [1]
		4. Create PVC with EncryptionClass [3]
		5. Validate PVC volume [4] is encrypted with first encryption key [1]
		6. Update EncryptionClass [3] with second encryption key [2]
		7. Validate PVC volume [4] is encrypted with second encryption key [2]
	*/
	ginkgo.It("Verify PVC is recrypted when a new encryption key is applied to its "+
		"associated EncryptionClass", ginkgo.Label(p1, block, wcp, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generate first encryption key")
		keyID1 := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Generate second encryption key")
		keyID2 := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("3. Create EncryptionClass with encryption key [1]")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID1, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("4. Create PVC with EncryptionClass [3]")
		pvc := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:           namespace,
			StorageClassName:    encryptedStorageClass.Name,
			EncryptionClassName: encClass.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("5. Validate PVC volume [4] is encrypted with first encryption key [1]")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID1)

		ginkgo.By("6. Update EncryptionClass [3] with second encryption key [2]")
		updateEncryptionClass(ctx, cryptoClient, encClass, keyProviderID, keyID2, false)

		ginkgo.By("7. Validate PVC volume [4] is encrypted with second encryption key [2]")
		validateVolumeToBeUpdatedWithEncryptedKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID2)
	})

	/*
		Steps:
		1. Generate encryption key
		2. Create EncryptionClass with encryption key [1]
		3. Create PVC with EncryptionClass [2]
	*/
	ginkgo.It("Verify PVC creation fails when associated with an EncryptionClass but the StorageClass "+
		"does not support encryption", ginkgo.Label(p1, block, wcp, vc90), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generate encryption key")
		keyID := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Create EncryptionClass with encryption key [1]")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Create PVC with EncryptionClass [2]")
		pvc := buildPersistentVolumeClaimSpec(PersistentVolumeClaimOptions{
			Namespace:           namespace,
			StorageClassName:    standardStorageClass.Name,
			EncryptionClassName: encClass.Name,
		})
		_, err := client.
			CoreV1().
			PersistentVolumeClaims(namespace).
			Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
	},
	)

	/*
		Steps:
		1. Generate encryption key
		2. Create EncryptionClass with encryption key [1]
		3. Create non-encrypted PVC
		4. Create VM with EncryptionClass [2] and PVC [3]
		5. Validate VM [4] is encrypted with encryption key [1]
		6. Validate PVC [3] is not encrypted
	*/
	ginkgo.It("Verify VM is encrypted with EncryptionClass while PVC is not encrypted", ginkgo.Label(p1, block,
		wcp, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generate encryption key")
		keyID := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Create EncryptionClass with encryption key [1]")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Create non-encrypted PVC")
		pvc := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:        namespace,
			StorageClassName: standardStorageClass.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Create VM with EncryptionClass [2] and PVC [3]")
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

		ginkgo.By("5. Validate VM [4] is encrypted with encryption key [1]")
		validateVmToBeEncryptedWithKey(vm, keyProviderID, keyID)

		ginkgo.By("6. Validate PVC [3] is not encrypted")
		validateVolumeNotToBeEncrypted(ctx, pvc.Spec.VolumeName)
	})

	/*
		Steps:
		1. Generate encryption key
		2. Create EncryptionClass with encryption key [1]
		3. Create PVC with EncryptionClass [2]
		4. Create VM with EncryptionClass [2] and PVC [3]
		5. Validate VM [4] is encrypted with encryption key [1]
		6. Validate PVC [3] is encrypted with encryption key [1]
	*/
	ginkgo.It("Verify VM and associated PVC are encrypted with EncryptionClass", ginkgo.Label(p1, block,
		wcp, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generate encryption key")
		keyID := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Create EncryptionClass with encryption key [1]")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Create PVC with EncryptionClass [2]")
		pvc := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:           namespace,
			StorageClassName:    encryptedStorageClass.Name,
			EncryptionClassName: encClass.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Create VM with EncryptionClass [2] and PVC [3]")
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

		ginkgo.By("5. Validate VM [4] is encrypted with encryption key [1]")
		validateVmToBeEncryptedWithKey(vm, keyProviderID, keyID)

		ginkgo.By("6. Validate PVC [3] is encrypted with encryption key [1]")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)
	})

	/*
		Steps:
		1. Generate encryption key
		2. Create default EncryptionClass with encryption key [1]
		3. Create PVC with encrypted StorageClass
		4. Create VM with encrypted StorageClass and PVC [3]
		5. Validate VM [4] is encrypted with encryption key [1]
		6. Validate PVC [3] is encrypted with encryption key [1]
	*/
	ginkgo.It("Verify VM and associated PVC are encrypted with default EncryptionClass", ginkgo.Label(p1, block,
		wcp, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generate encryption key")
		keyID := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Create default EncryptionClass with encryption key [1]")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, true)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Create PVC with encrypted StorageClass")
		pvc := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:        namespace,
			StorageClassName: encryptedStorageClass.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Create VM with encrypted StorageClass and PVC [3]")
		vm := createVmServiceVmV3(ctx, vmopClient, CreateVmOptionsV3{
			Namespace:          namespace,
			VmClass:            vmClass,
			VMI:                vmi,
			StorageClassName:   encryptedStorageClass.Name,
			PVCs:               []*v1.PersistentVolumeClaim{pvc},
			WaitForReadyStatus: true,
		})
		defer deleteVmServiceVm(ctx, vmopClient, namespace, vm.Name)

		ginkgo.By("5. Validate VM [4] is encrypted with encryption key [1]")
		validateVmToBeEncryptedWithKey(vm, keyProviderID, keyID)

		ginkgo.By("6. Validate PVC [3] is encrypted with encryption key [1]")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)
	})

	/*
		Steps:
		1. Generate first encryption key
		2. Generate second encryption key
		3. Create first EncryptionClass with encryption key [1]
		4. Create second EncryptionClass with encryption key [2]
		5. Create PVC with first EncryptionClass [3]
		6. Create VM with second EncryptionClass [4] and encrypted PVC [5]
		7. Validate PVC [5] is encrypted with first encryption key [1]
		8. Validate VM [6] is encrypted with second encryption key [2]
		9. Update first EncryptionClass [3] with second encryption key [2]
		10. Update second EncryptionClass [4] with first encryption key [1]
		11. Validate PVC [5] is encrypted with second encryption key [2]
		12. Validate VM [6] is encrypted with first encryption key [1]
	*/
	ginkgo.It("Verify VM and attached PVC are encrypted/recrypted with different keys", ginkgo.Label(p1, block,
		wcp, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generate first encryption key")
		keyID1 := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Generate second encryption key")
		keyID2 := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("3. Create first EncryptionClass with encryption key [1]")
		encClass1 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID1, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass1)

		ginkgo.By("4. Create second EncryptionClass with encryption key [2]")
		encClass2 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID2, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass2)

		ginkgo.By("5. Create PVC with first EncryptionClass [3]")
		pvc := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:           namespace,
			StorageClassName:    encryptedStorageClass.Name,
			EncryptionClassName: encClass1.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("6. Create VM with second EncryptionClass [4] and encrypted PVC [5]")
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

		ginkgo.By("7. Validate PVC [5] is encrypted with first encryption key [1]")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID1)

		ginkgo.By("8. Validate VM [6] is encrypted with second encryption key [2]")
		validateVmToBeEncryptedWithKey(vm, keyProviderID, keyID2)

		ginkgo.By("9. Update first EncryptionClass [3] with second encryption key [2]")
		updateEncryptionClass(ctx, cryptoClient, encClass1, keyProviderID, keyID2, false)

		ginkgo.By("10. Update second EncryptionClass [4] with first encryption key [1]")
		updateEncryptionClass(ctx, cryptoClient, encClass2, keyProviderID, keyID1, false)

		ginkgo.By("11. Validate PVC [5] is encrypted with second encryption key [2]")
		validateVolumeToBeUpdatedWithEncryptedKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID2)

		ginkgo.By("12. Validate VM [6] is encrypted with first encryption key [1]")
		validateVmToBeUpdatedWithEncryptedKey(ctx, vmopClient, vm.Namespace, vm.Name, keyProviderID, keyID1)
	})

	/*
		Steps:
			1. Generate first encryption key
			2. Generate second encryption key
			3. Create first EncryptionClass with encryption key [1]
			4. Create second EncryptionClass with encryption key [2]
			5. Creating PVC with first EncryptionClass [3]
			6. Validate PVC volume [5] is encrypted with first encryption key [1]
			7. Bring vsan-service down
			8. Update PVC with second EncryptionClass [4]
		    9. Bring vsan-service up
			10. Validate PVC volume [5] is encrypted with second encryption key [2]
	*/
	ginkgo.It("Verify PVC encryption when vsan-health is down", ginkgo.Label(p1, block,
		wcp, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generate first encryption key")
		keyID1 := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Generate second encryption key")
		keyID2 := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("3. Create first EncryptionClass with encryption key [1]")
		encClass1 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID1, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass1)

		ginkgo.By("4. Create second EncryptionClass with encryption key [2]")
		encClass2 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID2, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass2)

		ginkgo.By("5. Creating PVC with first EncryptionClass [3]")
		pvc := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:           namespace,
			StorageClassName:    encryptedStorageClass.Name,
			EncryptionClassName: encClass1.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("6. Validate PVC volume [5] is encrypted with first encryption key [1]")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID1)

		ginkgo.By("7. Stop Vsan-health service")
		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err := invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if isVsanHealthServiceStopped {
				ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
				err := invokeVCenterServiceControl(ctx, startOperation, vsanhealthServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
				time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
			}
		}()

		ginkgo.By("8. Update PVC with second EncryptionClass [4]")
		pvc = updatePersistentVolumeClaimWithCrypto(ctx, client, pvc, encryptedStorageClass.Name, encClass2.Name)

		ginkgo.By("9. Start Vsan-health service on the vCenter host")
		err = invokeVCenterServiceControl(ctx, startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("10. Validate PVC volume [5] is encrypted with second encryption key [2]")
		validateVolumeToBeUpdatedWithEncryptedKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID2)
	})

	/*
		Steps:
		1. Generate encryption key
		2. Create default EncryptionClass with encryption key [1]
		3. Get wncrypted latebinding storage class
		4. Create PVC with encrypted-latebinding StorageClass
		5. Create VM with encrypted-latebinding StorageClass and PVC [3]
		6. Wait and Verify PVC to reach bound state
		7. Validate VM [4] is encrypted with encryption key [1]
	*/
	ginkgo.It("Verify VM and associated PVC are encrypted with latebinding EncryptionClass", ginkgo.Label(p1, block,
		wcp, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generate encryption key")
		keyID := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Create default EncryptionClass with encryption key [1]")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, true)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Get encrypted late binding storage class")
		encryptedStoragePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameWithEncryption)
		encryptedStoragePolicyNameWffc := encryptedStoragePolicyName + "-latebinding"

		ginkgo.By("4. Create PVC with latebinding encrypted StorageClass")
		opts := PersistentVolumeClaimOptions{
			Namespace:        namespace,
			StorageClassName: encryptedStoragePolicyNameWffc,
		}
		pvc := buildPersistentVolumeClaimSpec(opts)
		pvc, err := client.
			CoreV1().
			PersistentVolumeClaims(namespace).
			Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("5. Create VM with encrypted StorageClass and PVC [3]")
		vm := createVmServiceVmV3(ctx, vmopClient, CreateVmOptionsV3{
			Namespace:          namespace,
			VmClass:            vmClass,
			VMI:                vmi,
			StorageClassName:   encryptedStoragePolicyNameWffc,
			PVCs:               []*v1.PersistentVolumeClaim{pvc},
			WaitForReadyStatus: true,
		})
		defer deleteVmServiceVm(ctx, vmopClient, namespace, vm.Name)

		ginkgo.By("6. Wait for PVC to reach bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(
			ctx,
			client,
			[]*v1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout*2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("7. Validate VM [4] is encrypted with encryption key [1]")
		validateVmToBeUpdatedWithEncryptedKey(ctx, vmopClient, vm.Namespace, vm.Name, keyProviderID, keyID)

	})

})
