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
	"fmt"
	"reflect"
	"strings"

	byokv1 "github.com/vmware-tanzu/vm-operator/external/byok/api/v1alpha1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	csicommon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
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

	// Check if PVC is referenced in a VM
	isAttached, vmName, err := r.isPVCAttachedToVM(ctx, pvc)
	if err != nil {
		r.logger.Errorf("Failed to check PVC attachment status for PVC %s/%s: %v", pvc.Namespace, pvc.Name, err)
		return err
	}

	if isAttached {
		// PVC is referenced in a VM - skip encryption and defer to VM Operator
		r.logger.Infof("Skipping encryption for PVC %s/%s as it is referenced in VirtualMachine %s. "+
			"Deferring to VM Operator which will aggregate all PVCs and VM encryption changes "+
			"and issue atomic reconfig API call to vCenter. EncryptionClass: %s, KeyProvider: %s, KeyID: %s",
			pvc.Namespace, pvc.Name, vmName, encClass.Name, encClass.Spec.KeyProvider, encClass.Spec.KeyID)
		return nil
	}

	volumeID, err := r.findVolume(ctx, pvc)
	if err != nil {
		return err
	} else if volumeID == "" {
		r.logger.Infof("Volume %s not found for PVC %s/%s", pvc.Spec.VolumeName, pvc.Namespace, pvc.Name)
		return nil
	}

	existingKeyID, err := csicommon.QueryVolumeCryptoKeyByID(ctx, r.volumeManager, volumeID)
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

	var cryptoSpec vimtypes.BaseCryptoSpec
	if existingKeyID != nil {
		cryptoSpec = &vimtypes.CryptoSpecShallowRecrypt{NewKeyId: newKeyID}
	} else {
		cryptoSpec = &vimtypes.CryptoSpecEncrypt{CryptoKeyId: newKeyID}
	}

	updateSpec := &cnstypes.CnsVolumeCryptoUpdateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeID,
		},
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

func (r *reconciler) findVolume(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (string, error) {
	pv := &corev1.PersistentVolume{}
	if err := r.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
		r.logger.Errorf("Failed to get PV %s for PVC %s/%s: %v", pvc.Spec.VolumeName,
			pvc.Namespace, pvc.Name, err)
		return "", err
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csicommon.VSphereCSIDriverName {
		r.logger.Errorf("PV %s for PVC %s is not a vSphere CSI volume", pv.Name, pvc.Name)
		return "", fmt.Errorf("PV %s for PVC %s is not a vSphere CSI volume", pv.Name, pvc.Name)
	}

	volumeID := pv.Spec.CSI.VolumeHandle
	if strings.HasPrefix(volumeID, "file:") {
		r.logger.Infof("Volume %s for PVC %s is a file volume. Skipping encryption.", volumeID, pvc.Name)
		return "", nil
	}

	return volumeID, nil
}

// isPVCAttachedToVM checks if the PVC is referenced in any VirtualMachine spec in the same namespace.
// Returns (isAttached, vmName, error) where vmName is the name of the VM using this PVC.
//
// This function uses the v1alpha2 VM Operator API client which can list VirtualMachines created
// with any API version (v1alpha1, v1alpha2, v1alpha3, v1alpha4, v1alpha5) due to Kubernetes
// API machinery's automatic version conversion.
func (r *reconciler) isPVCAttachedToVM(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (bool, string, error) {
	log := r.logger.With("pvc", pvc.Name, "namespace", pvc.Namespace)

	// List all VirtualMachines in the PVC's namespace
	vmList, err := utils.ListVirtualMachines(ctx, r.Client, pvc.Namespace)
	if err != nil {
		// If VM CRD is not installed or we can't list VMs, proceed with encryption
		// (don't block encryption if VM Operator is not present)
		log.Infof("Unable to list VirtualMachines in namespace %s: %v. Proceeding with encryption.",
			pvc.Namespace, err)
		return false, "", nil
	}

	// Check if this PVC is referenced in any VM's spec
	for _, vm := range vmList.Items {
		for _, vmVol := range vm.Spec.Volumes {
			if vmVol.PersistentVolumeClaim != nil &&
				vmVol.PersistentVolumeClaim.ClaimName == pvc.Name {
				log.Infof("Found VirtualMachine %s in namespace %s referencing PVC %s",
					vm.Name, pvc.Namespace, pvc.Name)
				return true, vm.Name, nil
			}
		}
	}

	log.Infof("PVC %s/%s is not referenced in any VirtualMachine", pvc.Namespace, pvc.Name)
	return false, "", nil
}
