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
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/gomega"
	vmopv3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	byokv1 "github.com/vmware-tanzu/vm-operator/external/byok/api/v1alpha1"
	"github.com/vmware/govmomi/cns"
	cnsmethods "github.com/vmware/govmomi/cns/methods"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
)

const (
	VolumeCryptoUpdateTimeout = 3 * time.Minute
)

func findVolumeCryptoKey(ctx context.Context, volumeName string) *types.CryptoKeyId {
	var volume *cnstypes.CnsVolume
	{
		req := cnstypes.CnsQueryVolume{
			This: cnsVolumeManagerInstance,
			Filter: cnstypes.CnsQueryFilter{
				Names: []string{volumeName},
			},
		}
		res, err := cnsmethods.CnsQueryVolume(ctx, e2eVSphere.CnsClient.Client, &req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(res).NotTo(gomega.BeNil())
		gomega.Expect(res.Returnval.Volumes).To(gomega.HaveLen(1))
		volume = &res.Returnval.Volumes[0]
	}

	var storageObj *types.VStorageObject
	{
		req := cnstypes.CnsQueryVolumeInfo{
			This:      cnsVolumeManagerInstance,
			VolumeIds: []cnstypes.CnsVolumeId{volume.VolumeId},
		}
		res, err := cnsmethods.CnsQueryVolumeInfo(ctx, e2eVSphere.CnsClient.Client, &req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(res).NotTo(gomega.BeNil())

		task := object.NewTask(e2eVSphere.Client.Client, res.Returnval)
		taskInfo, err := task.WaitForResult(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(res).NotTo(gomega.BeNil())

		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		gomega.Expect(volumeOperationRes.Fault).To(gomega.BeNil(), spew.Sdump(volumeOperationRes.Fault))

		volumeInfoResult := taskResult.(*cnstypes.CnsQueryVolumeInfoResult)
		blockVolumeInfo, ok := volumeInfoResult.VolumeInfo.(*cnstypes.CnsBlockVolumeInfo)
		gomega.Expect(ok).To(gomega.BeTrue(), "failed to convert VolumeInfo to BlockVolumeInfo")
		storageObj = &blockVolumeInfo.VStorageObject
	}

	diskFileBackingInfo, ok := storageObj.Config.Backing.(*types.BaseConfigInfoDiskFileBackingInfo)
	gomega.Expect(ok).To(gomega.BeTrue(), "failed to retrieve FCD backing info")

	return diskFileBackingInfo.KeyId
}

func validateEncryptedStorageClass(ctx context.Context, cryptoClient crypto.Client, scName string, expected bool) {
	var storageClass storagev1.StorageClass
	err := cryptoClient.Get(ctx, ctlrclient.ObjectKey{Name: scName}, &storageClass)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	actual, _, err := cryptoClient.IsEncryptedStorageClass(ctx, scName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if expected {
		gomega.Expect(actual).To(gomega.BeTrue(), "storage class must have encryption capabilities", scName)
	} else {
		gomega.Expect(actual).To(gomega.BeFalse(), "storage class must not have encryption capabilities", scName)
	}
}

func validateKeyProvider(ctx context.Context, keyProviderID string) {
	kms, err := e2eVSphere.findKeyProvier(ctx, keyProviderID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(kms).NotTo(gomega.BeNil())
	gomega.Expect(kms.UseAsDefault).NotTo(gomega.BeTrue(), "Key Provider must not be configured as default")
	gomega.Expect(kms.ManagementType).To(gomega.Equal("vCenter"), "Key Provider must have Standard type")
}

func validateVolumeToBeEncryptedWithKey(ctx context.Context, volumeName, keyProviderID, keyID string) {
	cryptoKey := findVolumeCryptoKey(ctx, volumeName)
	gomega.Expect(cryptoKey).NotTo(gomega.BeNil())
	gomega.Expect(cryptoKey.ProviderId).NotTo(gomega.BeNil())
	gomega.Expect(cryptoKey.ProviderId.Id).To(gomega.Equal(keyProviderID))
	gomega.Expect(cryptoKey.KeyId).To(gomega.Equal(keyID))
}

func validateVolumeNotToBeEncrypted(ctx context.Context, volumeName string) {
	cryptoKey := findVolumeCryptoKey(ctx, volumeName)
	gomega.Expect(cryptoKey).To(gomega.BeNil())
}

func validateVolumeToBeUpdatedWithEncryptedKey(ctx context.Context, volumeName, keyProviderID, keyID string) {
	waitErr := wait.PollUntilContextTimeout(ctx, poll, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			cryptoKey := findVolumeCryptoKey(ctx, volumeName)
			if cryptoKey != nil &&
				cryptoKey.ProviderId != nil &&
				cryptoKey.ProviderId.Id == keyProviderID &&
				cryptoKey.KeyId == keyID {
				return true, nil
			}
			return false, nil
		})

	var err error
	if waitErr != nil {
		err = fmt.Errorf("expected volume to be encrypted with %s/%s: %w",
			keyProviderID, keyID, waitErr)
	}

	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func validateVmToBeEncryptedWithKey(vm *vmopv3.VirtualMachine, keyProviderID, keyID string) {
	gomega.Expect(vm.Status.Crypto).NotTo(gomega.BeNil())
	gomega.Expect(vm.Status.Crypto.KeyID).To(gomega.Equal(keyID))
	gomega.Expect(vm.Status.Crypto.ProviderID).To(gomega.Equal(keyProviderID))
}

func validateVmToBeUpdatedWithEncryptedKey(ctx context.Context, vmopClient ctlrclient.Client,
	namespace, name, keyProviderID, keyID string) {

	vmKey := ctlrclient.ObjectKey{Name: name, Namespace: namespace}

	waitErr := wait.PollUntilContextTimeout(ctx, poll, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			vm := &vmopv3.VirtualMachine{}
			if err := vmopClient.Get(ctx, vmKey, vm); err != nil {
				return false, err
			}
			cryptoStatus := vm.Status.Crypto
			if cryptoStatus != nil &&
				cryptoStatus.ProviderID == keyProviderID &&
				cryptoStatus.KeyID == keyID {
				return true, nil
			}
			return false, nil
		})

	var err error
	if waitErr != nil {
		err = fmt.Errorf("expected VM to be encrypted with %s/%s: %w",
			keyProviderID, keyID, waitErr)
	}

	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func createEncryptionClass(ctx context.Context,
	cryptoClient crypto.Client,
	namespace, keyProviderID, keyID string,
	isDefault bool) *byokv1.EncryptionClass {

	encClass := &byokv1.EncryptionClass{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "encclass-",
			Namespace:    namespace,
		},
		Spec: byokv1.EncryptionClassSpec{
			KeyProvider: keyProviderID,
			KeyID:       keyID,
		},
	}

	if isDefault {
		encClass.Labels = map[string]string{
			crypto.DefaultEncryptionClassLabelName: crypto.DefaultEncryptionClassLabelValue,
		}
	}

	err := cryptoClient.Create(ctx, encClass)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return encClass
}

func updateEncryptionClass(ctx context.Context,
	cryptoClient crypto.Client,
	encClass *byokv1.EncryptionClass,
	keyProviderID, keyID string,
	isDefault bool) {

	encClass.Spec.KeyProvider = keyProviderID
	encClass.Spec.KeyID = keyID

	if isDefault {
		encClass.Labels = map[string]string{
			crypto.DefaultEncryptionClassLabelName: crypto.DefaultEncryptionClassLabelValue,
		}
	} else if encClass.Labels != nil {
		delete(encClass.Labels, crypto.DefaultEncryptionClassLabelName)
	}

	err := cryptoClient.Update(ctx, encClass)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func deleteEncryptionClass(
	ctx context.Context,
	cryptoClient crypto.Client,
	encClass *byokv1.EncryptionClass) {

	err := cryptoClient.Delete(ctx, encClass)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func updatePersistentVolumeClaimWithCrypto(
	ctx context.Context,
	client clientset.Interface,
	pvc *corev1.PersistentVolumeClaim,
	scName, encClassName string) *corev1.PersistentVolumeClaim {

	if scName != "" {
		pvc.Spec.StorageClassName = &scName
	}

	crypto.SetEncryptionClassNameForPVC(pvc, encClassName)

	var err error
	pvc, err = client.
		CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Update(ctx, pvc, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return pvc
}
