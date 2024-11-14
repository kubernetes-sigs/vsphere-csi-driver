package persistentvolumeclaim

import (
	"context"
	"fmt"
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
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
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

	volume, err := r.findVolume(ctx, pvc)
	if err != nil {
		return err
	} else if volume == nil {
		r.logger.Infof("Volume %s not found for PVC %s ()", pvc.Spec.VolumeName, pvc.Name)
		return nil
	} else if volume.VolumeType != csicommon.BlockVolumeType {
		return nil
	}

	existingKeyID, err := r.findVolumeCryptoKeyID(ctx, volume)
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

func (r *reconciler) findEncryptionClass(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (*byokv1.EncryptionClass, error) {
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

func (r *reconciler) findVolumeCryptoKeyID(ctx context.Context, volume *cnstypes.CnsVolume) (*vimtypes.CryptoKeyId, error) {
	volumeIDList := []cnstypes.CnsVolumeId{volume.VolumeId}

	result, err := r.volumeManager.QueryVolumeInfo(ctx, volumeIDList)
	if err != nil {
		return nil, err
	}

	blockVolumeInfo, ok := result.VolumeInfo.(*cnstypes.CnsBlockVolumeInfo)
	if !ok {
		return nil, fmt.Errorf("failed to retrieve CNS volume info")
	}

	storageObj := blockVolumeInfo.VStorageObject

	diskFileBackingInfo, ok := storageObj.Config.Backing.(*vimtypes.BaseConfigInfoDiskFileBackingInfo)
	if !ok {
		return nil, fmt.Errorf("failed to retrieve FCD backing info")
	}

	return diskFileBackingInfo.KeyId, nil
}
