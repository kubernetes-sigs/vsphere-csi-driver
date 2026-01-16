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

package persistentvolumeclaim

import (
	"context"
	"reflect"

	byokv1 "github.com/vmware-tanzu/vm-operator/external/byok/api/v1alpha1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	cnsv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	cnsbatchv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	csicommon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	commonco "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	ctrlcommoon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/common"
)

func AddToManager(ctx context.Context, mgr manager.Manager, opts ctrlcommoon.Options) error {
	var (
		controlledType     = &corev1.PersistentVolumeClaim{}
		controlledTypeName = reflect.TypeOf(controlledType).Elem().Name()
	)

	r := &reconciler{
		Client:        mgr.GetClient(),
		logger:        logger.GetLoggerWithNoContext().Named("controllers").Named(controlledTypeName),
		cryptoClient:  opts.CryptoClient,
		volumeManager: opts.VolumeManager,
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}).
		Watches(&byokv1.EncryptionClass{},
			handler.EnqueueRequestsFromMapFunc(
				EncryptionClassToPersistentVolumeClaimMapper(ctx, r.Client),
			)).
		Complete(r)
}

type reconciler struct {
	client.Client
	logger        *zap.SugaredLogger
	cryptoClient  crypto.Client
	volumeManager volume.Manager
}

func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !obj.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.reconcileNormal(ctx, obj)
}

func (r *reconciler) reconcileNormal(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Spec.VolumeName == "" || pvc.Spec.StorageClassName == nil {
		return nil
	}

	encrypted, profileID, err := r.cryptoClient.IsEncryptedStorageClass(ctx, *pvc.Spec.StorageClassName)
	if err != nil {
		return err
	} else if !encrypted {
		return nil
	}

	encClass, err := r.findEncryptionClass(ctx, pvc)
	if err != nil {
		return err
	} else if encClass == nil {
		return nil
	}

	volume, err := r.findVolume(ctx, pvc)
	if err != nil {
		return err
	} else if volume == nil {
		r.logger.Infof("Volume %s not found for PVC %s ()", pvc.Spec.VolumeName, pvc.Name)
		return nil
	} else if volume.VolumeType != csicommon.BlockVolumeType {
		return nil
	}

	existingKeyID, err := csicommon.QueryVolumeCryptoKeyByID(ctx, r.volumeManager, volume.VolumeId.Id)
	if err != nil {
		return err
	}

	newKeyID := vimtypes.CryptoKeyId{
		KeyId: encClass.Spec.KeyID,
		ProviderId: &vimtypes.KeyProviderId{
			Id: encClass.Spec.KeyProvider,
		},
	}

	if existingKeyID != nil &&
		existingKeyID.KeyId == newKeyID.KeyId &&
		existingKeyID.ProviderId.Id == newKeyID.ProviderId.Id {
		return nil
	}

	// Check if PVC is attached to a VM
	isAttached, vmUUID, err := r.isPVCAttachedToVM(ctx, pvc)
	if err != nil {
		r.logger.Errorf("Failed to check PVC attachment status for PVC %s/%s: %v", pvc.Namespace, pvc.Name, err)
		return err
	}

	if isAttached {
		// PVC is attached - skip encryption and defer to VM Operator
		r.logger.Infof("Skipping encryption for PVC %s/%s as it is attached to VM with UUID %s. "+
			"Deferring to VM Operator for atomic encryption operation. "+
			"EncryptionClass: %s, KeyProvider: %s, KeyID: %s",
			pvc.Namespace, pvc.Name, vmUUID, encClass.Name, encClass.Spec.KeyProvider, encClass.Spec.KeyID)
		return nil
	}

	var cryptoSpec vimtypes.BaseCryptoSpec
	if existingKeyID != nil {
		cryptoSpec = &vimtypes.CryptoSpecShallowRecrypt{NewKeyId: newKeyID}
	} else {
		cryptoSpec = &vimtypes.CryptoSpecEncrypt{CryptoKeyId: newKeyID}
	}

	updateSpec := &cnstypes.CnsVolumeCryptoUpdateSpec{
		VolumeId: volume.VolumeId,
		Profile: []vimtypes.BaseVirtualMachineProfileSpec{
			&vimtypes.VirtualMachineDefinedProfileSpec{
				ProfileId: profileID,
			},
		},
		DisksCrypto: &vimtypes.DiskCryptoSpec{
			Crypto: cryptoSpec,
		},
	}

	return r.volumeManager.UpdateVolumeCrypto(ctx, updateSpec)
}

func (r *reconciler) findEncryptionClass(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) (*byokv1.EncryptionClass, error) {
	encClassName := crypto.GetEncryptionClassNameForPVC(pvc)
	if encClassName == "" {
		return nil, nil
	}

	encClass, err := r.cryptoClient.GetEncryptionClass(ctx, encClassName, pvc.Namespace)
	if err != nil {
		return nil, client.IgnoreNotFound(err)
	}

	return encClass, nil
}

func (r *reconciler) findVolume(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (*cnstypes.CnsVolume, error) {
	filter := cnstypes.CnsQueryFilter{
		Names: []string{pvc.Spec.VolumeName},
	}

	result, err := r.volumeManager.QueryVolume(ctx, filter)
	if err != nil {
		return nil, err
	}

	if len(result.Volumes) == 0 {
		return nil, nil
	}

	return &result.Volumes[0], nil
}

// isPVCAttachedToVM checks if the PVC is currently attached to a VM by looking for
// CnsNodeVmAttachment or CnsNodeVMBatchAttachment CRs.
// Returns (isAttached, vmUUID, error) where vmUUID is the NodeUUID or InstanceUUID of the VM.
func (r *reconciler) isPVCAttachedToVM(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (bool, string, error) {
	log := r.logger.With("pvc", pvc.Name, "namespace", pvc.Namespace)

	// Check for CnsNodeVmAttachment CRs
	var attachmentList cnsv1alpha1.CnsNodeVmAttachmentList
	if err := r.List(ctx, &attachmentList, client.InNamespace(pvc.Namespace)); err != nil {
		log.Errorf("Failed to list CnsNodeVmAttachment CRs: %v", err)
		return false, "", err
	}

	for _, attachment := range attachmentList.Items {
		if attachment.Spec.VolumeName == pvc.Spec.VolumeName && attachment.Status.Attached {
			log.Infof("Found CnsNodeVmAttachment CR %s with status.attached=true", attachment.Name)
			// Use NodeUUID from spec as VM identifier
			return true, attachment.Spec.NodeUUID, nil
		}
	}

	// Check for CnsNodeVMBatchAttachment CRs (only if SharedDiskFss is enabled)
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, csicommon.SharedDiskFss) {
		var batchAttachmentList cnsbatchv1alpha1.CnsNodeVMBatchAttachmentList
		if err := r.List(ctx, &batchAttachmentList, client.InNamespace(pvc.Namespace)); err != nil {
			log.Errorf("Failed to list CnsNodeVMBatchAttachment CRs: %v", err)
			return false, "", err
		}

		for _, batchAttachment := range batchAttachmentList.Items {
			// Check if PVC is in the spec
			for _, volumeSpec := range batchAttachment.Spec.Volumes {
				if volumeSpec.PersistentVolumeClaim.ClaimName == pvc.Name {
					// Check if it's attached in the status
					for _, volumeStatus := range batchAttachment.Status.VolumeStatus {
						if volumeStatus.PersistentVolumeClaim.ClaimName == pvc.Name &&
							volumeStatus.PersistentVolumeClaim.Attached {
							log.Infof("Found CnsNodeVMBatchAttachment CR %s with PVC attached", batchAttachment.Name)
							// Use InstanceUUID from spec as VM identifier
							return true, batchAttachment.Spec.InstanceUUID, nil
						}
					}
				}
			}
		}
	}

	log.Infof("PVC %s/%s is not attached to any VM", pvc.Namespace, pvc.Name)
	return false, "", nil
}
