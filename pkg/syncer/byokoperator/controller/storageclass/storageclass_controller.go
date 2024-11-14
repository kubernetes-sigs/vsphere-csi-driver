package storageclass

import (
	"context"
	"reflect"

	"go.uber.org/zap"
	storagev1 "k8s.io/api/storage/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	ctrlcommoon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/vcenter"
)

func AddToManager(ctx context.Context, mgr manager.Manager, opts ctrlcommoon.Options) error {
	var (
		controlledType     = &storagev1.StorageClass{}
		controlledTypeName = reflect.TypeOf(controlledType).Elem().Name()
	)

	r := &reconciler{
		Client:       mgr.GetClient(),
		logger:       logger.GetLoggerWithNoContext().Named("controllers").Named(controlledTypeName),
		vcProvider:   opts.VCenterProvider,
		cryptoClient: opts.CryptoClient,
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&storagev1.StorageClass{}).
		Complete(r)
}

type reconciler struct {
	client.Client
	logger       *zap.SugaredLogger
	vcProvider   vcenter.Provider
	cryptoClient crypto.Client
}

func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj := &storagev1.StorageClass{}
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !obj.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.reconcileNormal(ctx, obj)
}

func (r *reconciler) reconcileNormal(ctx context.Context, obj *storagev1.StorageClass) error {
	policyID := crypto.GetStoragePolicyID(obj)
	if policyID == "" {
		return nil
	}

	ok, err := r.vcProvider.DoesProfileSupportEncryption(ctx, policyID)
	if err != nil {
		return err
	}

	r.logger.Debugf("Marking the storage class %s as encryption-enabled: %v", obj.Name, ok)

	return r.cryptoClient.MarkEncryptedStorageClass(ctx, obj, ok)
}
