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
	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapc "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmopv2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmopv3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	vmopv4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
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

var _ = ginkgo.Describe("[ef-encryption][csi-supervisor] [encryption] Block volume snapshot encryption", func() {
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
		snapClient            *snapc.Clientset
		volumeSnapshotClass   *snapV1.VolumeSnapshotClass
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

		// Load snapshot client
		restConfig = getRestConfigClient()
		snapClient, err = snapc.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volumeSnapshotClassName := GetAndExpectStringEnvVar(envVolSnapClassDel)
		volumeSnapshotClass, err = getVolumeSnapshotClass(ctx, snapClient, volumeSnapshotClassName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		svcClient, svNamespace := getSvcClientAndNamespace()
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
	})

	/*
		Steps:
		1. Generate encryption key
		2. Create EncryptionClass with encryption key [1]
		3. Create PVC with EncryptionClass [2]
		4. Create a dynamic volume snapshot from PVC [3]
		5. Create PVC with EncryptionClass [2] from snapshot [4]
	*/
	ginkgo.It("Verify PVC from snapshot is recrypted with the same EncryptionClass", ginkgo.Label(p1, block,
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
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)

		ginkgo.By("4. Create a dynamic volume snapshot from PVC [3]")
		volumeSnapshot := createDynamicVolumeSnapshotSimple(ctx, namespace, snapClient, volumeSnapshotClass, pvc)
		defer deleteVolumeSnapshotWithPollWait(ctx, snapClient, namespace, volumeSnapshot.Name)

		ginkgo.By("5. Create PVC with EncryptionClass [2] from snapshot [4]")
		pvc2 := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:           namespace,
			StorageClassName:    encryptedStorageClass.Name,
			EncryptionClassName: encClass.Name,
			SnapshotName:        volumeSnapshot.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc2)
		validateVolumeToBeEncryptedWithKey(ctx, pvc2.Spec.VolumeName, keyProviderID, keyID)
	})

	/*
		Steps:
		1. Generate encryption key
		2. Create EncryptionClass with encryption key [1]
		3. Create PVC with EncryptionClass [2]
		4. Create a dynamic volume snapshot from PVC [3]
		5. Generate second encryption key
		6. Create second EncryptionClass with encryption key [5]
		7. Create PVC with EncryptionClass [6] from snapshot [4]
	*/
	ginkgo.It("Verify PVC from snapshot is recrypted with different EncryptionClass", ginkgo.Label(p1, block,
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
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)

		ginkgo.By("4. Create a dynamic volume snapshot from PVC [3]")
		volumeSnapshot := createDynamicVolumeSnapshotSimple(ctx, namespace, snapClient, volumeSnapshotClass, pvc)
		defer deleteVolumeSnapshotWithPollWait(ctx, snapClient, namespace, volumeSnapshot.Name)

		ginkgo.By("5. Generate second encryption key")
		keyID2 := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("6. Create second EncryptionClass with encryption key [5]")
		encClass2 := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID2, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass2)

		ginkgo.By("7. Create PVC with EncryptionClass [6] from snapshot [4]")
		pvc2 := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:           namespace,
			StorageClassName:    encryptedStorageClass.Name,
			EncryptionClassName: encClass2.Name,
			SnapshotName:        volumeSnapshot.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc2)
		validateVolumeToBeEncryptedWithKey(ctx, pvc2.Spec.VolumeName, keyProviderID, keyID2)
	})

	/*
		Steps:
		1. Generate encryption key
		2. Create EncryptionClass with encryption key [1]
		3. Create PVC without encryption
		4. Create a dynamic volume snapshot from PVC [3]
		5. Create PVC with EncryptionClass [2] from snapshot [4]
	*/
	ginkgo.It("Verify PVC from snapshot is encrypted with EncryptionClass", ginkgo.Label(p1, block,
		wcp, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("1. Generate encryption key")
		keyID := e2eVSphere.generateEncryptionKey(ctx, keyProviderID)

		ginkgo.By("2. Create EncryptionClass with encryption key [1]")
		encClass := createEncryptionClass(ctx, cryptoClient, namespace, keyProviderID, keyID, false)
		defer deleteEncryptionClass(ctx, cryptoClient, encClass)

		ginkgo.By("3. Create PVC without encryption")
		pvc := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:        namespace,
			StorageClassName: standardStorageClass.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc)
		validateVolumeNotToBeEncrypted(ctx, pvc.Spec.VolumeName)

		ginkgo.By("4. Create a dynamic volume snapshot from PVC [3]")
		volumeSnapshot := createDynamicVolumeSnapshotSimple(ctx, namespace, snapClient, volumeSnapshotClass, pvc)
		defer deleteVolumeSnapshotWithPollWait(ctx, snapClient, namespace, volumeSnapshot.Name)

		ginkgo.By("5. Create PVC with EncryptionClass [2] from snapshot [4]")
		pvc2 := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:           namespace,
			StorageClassName:    encryptedStorageClass.Name,
			EncryptionClassName: encClass.Name,
			SnapshotName:        volumeSnapshot.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc2)
		validateVolumeToBeEncryptedWithKey(ctx, pvc2.Spec.VolumeName, keyProviderID, keyID)
	})

	/*
		Steps:
		1. Generate encryption key
		2. Create EncryptionClass with encryption key [1]
		3. Create PVC with EncryptionClass [2]
		4. Create a dynamic volume snapshot from PVC [3]
		5. Create PVC without encryption from snapshot [4]
	*/
	ginkgo.It("Verify PVC from snapshot is decrypted", ginkgo.Label(p1, block, wcp, vc90), func() {
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
		validateVolumeToBeEncryptedWithKey(ctx, pvc.Spec.VolumeName, keyProviderID, keyID)

		ginkgo.By("4. Create a dynamic volume snapshot from PVC [3]")
		volumeSnapshot := createDynamicVolumeSnapshotSimple(ctx, namespace, snapClient, volumeSnapshotClass, pvc)
		defer deleteVolumeSnapshotWithPollWait(ctx, snapClient, namespace, volumeSnapshot.Name)

		ginkgo.By("5. Create PVC without encryption from snapshot [4]")
		pvc2 := createPersistentVolumeClaim(ctx, client, PersistentVolumeClaimOptions{
			Namespace:        namespace,
			StorageClassName: standardStorageClass.Name,
			SnapshotName:     volumeSnapshot.Name,
		})
		defer deletePersistentVolumeClaim(ctx, client, pvc2)
		validateVolumeNotToBeEncrypted(ctx, pvc2.Spec.VolumeName)
	})
})
