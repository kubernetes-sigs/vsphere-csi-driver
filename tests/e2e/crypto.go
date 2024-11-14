/*
Copyright 2021 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
)

var _ = ginkgo.Describe("[csi-supervisor] [encryption] Block volume encryption", func() {
	f := framework.NewDefaultFramework("encryption")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                clientset.Interface
		cryptoClient          crypto.Client
		restConfig            *restclient.Config
		namespace             string
		standardStorageClass  *storagev1.StorageClass
		encryptedStorageClass *storagev1.StorageClass
		keyProvider           string
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

		err = connectCns(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cryptoClient, err = crypto.NewClientWithConfig(ctx, f.ClientConfig())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		standardStoragePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		standardStorageClass, err = createStorageClass(client,
			map[string]string{
				scParamStoragePolicyID: e2eVSphere.GetSpbmPolicyID(standardStoragePolicyName),
			},
			nil, "", "", false, standardStoragePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create storage class with err: %v", err))
		validateEncryptedStorageClass(ctx, cryptoClient, standardStoragePolicyName, false)
		setStoragePolicyQuota(ctx, restConfig, standardStoragePolicyName, namespace, rqLimit)

		encryptedStoragePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameWithEncryption)
		encryptedStorageClass, err = createStorageClass(client,
			map[string]string{
				scParamStoragePolicyID: e2eVSphere.GetSpbmPolicyID(encryptedStoragePolicyName),
			},
			nil, "", "", false, encryptedStoragePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create storage class with err: %v", err))
		validateEncryptedStorageClass(ctx, cryptoClient, encryptedStoragePolicyName, true)
		setStoragePolicyQuota(ctx, restConfig, encryptedStoragePolicyName, namespace, rqLimit)

		keyProvider = GetAndExpectStringEnvVar(envKeyProvider)
		validateKeyProvider(ctx, keyProvider)
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
		Verify block volume is encrypted with a specific KeyID/Provider when PVC is associated
		with EncryptionClass.

		1. Generate encryption key from KMS.
		2. Create EncryptionClass with newly generated encryption key.
		3. Create PVC associated with EncryptionClass, encrypted StorageClass and ReadWriteOnce access mode.
		4. Validate underlying storage object (FCD) is encrypted with the encryption key specified in the
			EncryptionClass created in (2).
	*/
	ginkgo.It("Verify volume encrypted specific EncryptionClass", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating new encryption key")
		keyID, err := e2eVSphere.generateEncryptionKey(ctx, keyProvider)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Creating an EncryptionClass")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProvider, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Creating encrypted PVC")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, encClass.Name, true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Validate storage object is encrypted")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProvider, keyID)
	})

	/*
		Verify block volume is encrypted with the default KeyID/Provider when PVC is not associated
		with EncryptionClass but has encrypted StorageClass.

		1. Generate encryption key from KMS.
		2. Create EncryptionClass with newly generated encryption key and mark it as default for the namespace.
		3. Create PVC associated with encrypted StorageClass and ReadWriteOnce access mode.
		4. Validate underlying storage object (FCD) is encrypted with the encryption key specified
			in the default EncryptionClass created in (2).
	*/
	ginkgo.It("Verify volume encrypted default EncryptionClass", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating new encryption key")
		keyID, err := e2eVSphere.generateEncryptionKey(ctx, keyProvider)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Creating an EncryptionClass")
		defaultEncClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProvider, keyID, true)
		defer deleteEncryptionClass(ctx, cryptoClient, defaultEncClass)

		ginkgo.By("3. Creating encrypted PVC")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, "", true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("4. Validate storage object is encrypted")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProvider, keyID)
	})

	/*
		Verify block volume is recrypted the changing EncryptionClass associated with PVCs

		1. Generate first encryption key from KMS.
		2. Generate second encryption key from KMS.
		3. Create EncryptionClass with generated encryption key (1).
		4. Create EncryptionClass with generated encryption key (2).
		5. Create PVC associated with EncryptionClass (3), encrypted StorageClass and ReadWriteOnce access mode.
		6. Validate underlying storage object (FCD) is encrypted with encryption key (1)
		7. Associate PVC with EncryptionClass  (4)
		8. Validate underlying storage object (FCD) is encrypted with encryption key (2)
	*/
	ginkgo.It("Verify volume recrypted on EncryptionClass change", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating first encryption key")
		keyID1, err := e2eVSphere.generateEncryptionKey(ctx, keyProvider)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Generating second encryption key")
		keyID2, err := e2eVSphere.generateEncryptionKey(ctx, keyProvider)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("3. Creating first EncryptionClass")
		encClass1 := createEncryptionClass(ctx, cryptoClient, namespace, keyProvider, keyID1, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass1)

		ginkgo.By("4. Creating second EncryptionClass")
		encClass2 := createEncryptionClass(ctx, cryptoClient, namespace, keyProvider, keyID2, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass2)

		ginkgo.By("5. Creating encrypted PVC")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, encClass1.Name, true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("6. Validate storage object is encrypted with first key")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProvider, keyID1)

		ginkgo.By("7. Update encrypted PVC with second EncryptionClass")
		pvc = updatePersistentVolumeClaimWithCrypto(ctx, client, pvc, encryptedStorageClass.Name, encClass2.Name)

		ginkgo.By("8. Validate storage object is encrypted with second key")
		validateVolumeToBeUpdatedWithEncryptedKey(ctx, pvc.Spec.VolumeName, keyProvider, keyID2)
	})

	/*
		Verify block volume is recrypted when the EncryptionClass associated with PVCs has its key rotated.

		1. Generate first encryption key from KMS.
		2. Generate second encryption key from KMS.
		3. Create EncryptionClass with generated encryption key (1).
		4. Create PVC associated with EncryptionClass, encrypted StorageClass and ReadWriteOnce access mode.
		5. Validate underlying storage object (FCD) is encrypted with encryption key (1)
		6. Change EncryptionClass key (2)
		7. Validate underlying storage object (FCD) is encrypted with encryption key (2)
	*/
	ginkgo.It("Verify volume recrypted on key rotation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating first encryption key")
		keyID1, err := e2eVSphere.generateEncryptionKey(ctx, keyProvider)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Generating second encryption key")
		keyID2, err := e2eVSphere.generateEncryptionKey(ctx, keyProvider)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("3. Creating EncryptionClass")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProvider, keyID1, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("4. Creating encrypted PVC")
		pvc := createPersistentVolumeClaimWithCrypto(ctx, client, namespace, encryptedStorageClass.Name, encClass.Name, true)
		defer deletePersistentVolumeClaim(ctx, client, pvc)

		ginkgo.By("5. Validate storage object is encrypted with first encryption key")
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProvider, keyID1)

		ginkgo.By("6. Update EncryptionClass with second key")
		updateEncryptionClass(ctx, cryptoClient, encClass, keyProvider, keyID2, false)

		ginkgo.By("7. Validate storage object is encrypted with second encryption key")
		validateVolumeToBeUpdatedWithEncryptedKey(ctx, pvc.Spec.VolumeName, keyProvider, keyID2)
	})

	/*
		Verify PVC creation fails when associated with EncryptionClass and StorageClass not supporting encryption.

		1. Generate encryption key from KMS.
		2. Create EncryptionClass with newly generated encryption key.
		3. Create PVC associated with EncryptionClass, StorageClass without encryption capabilities.
		4. Validate operation fails.
	*/
	ginkgo.It("Verify PVC creation fails with StorageClass not supporting encryption", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generating new encryption key")
		keyID, err := e2eVSphere.generateEncryptionKey(ctx, keyProvider)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("2. Creating an EncryptionClass")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProvider, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Creating encrypted PVC")
		pvc := buildPersistentVolumeClaimWithCryptoSpec(namespace, standardStorageClass.Name, encClass.Name)
		_, err = client.
			CoreV1().
			PersistentVolumeClaims(namespace).
			Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
	})
})
