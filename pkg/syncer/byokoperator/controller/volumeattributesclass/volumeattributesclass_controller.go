/*
Copyright 2026 The Kubernetes Authors.

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

package volumeattributesclass

import (
	"context"
	"fmt"
	"reflect"

	"go.uber.org/zap"
	storagev1 "k8s.io/api/storage/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	csicommon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	ctrlcommoon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/common"
)

func AddToManager(ctx context.Context, mgr manager.Manager, opts ctrlcommoon.Options) error {
	var (
		controlledType     = &storagev1.VolumeAttributesClass{}
		controlledTypeName = reflect.TypeOf(controlledType).Elem().Name()
	)

	r := &reconciler{
		Client:       mgr.GetClient(),
		logger:       logger.GetLoggerWithNoContext().Named("controllers").Named(controlledTypeName),
		vcClient:     opts.VCenterClient,
		cryptoClient: opts.CryptoClient,
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&storagev1.VolumeAttributesClass{}).
		Complete(r)
}

type reconciler struct {
	client.Client
	logger       *zap.SugaredLogger
	vcClient     *vsphere.VirtualCenter
	cryptoClient crypto.Client
}

func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj := &storagev1.VolumeAttributesClass{}
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !obj.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.reconcileNormal(ctx, obj)
}

func (r *reconciler) reconcileNormal(ctx context.Context, obj *storagev1.VolumeAttributesClass) error {
	policyID := crypto.GetStoragePolicyIDFromVAC(obj)
	if policyID == "" {
		return nil
	}

	// VolumeAttributesClass-driven storage-policy mutability is gated behind this capability;
	// when it's disabled, don't track VAC-derived policies as encryption-capable at all — no
	// PVC could ever actually reach one via ModifyVolume anyway.
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, csicommon.VMPVCStoragePolicyMutability) {
		return nil
	}

	if err := r.vcClient.ConnectPbm(ctx); err != nil {
		return fmt.Errorf("failed to connect VirtualCenter: %w", err)
	}

	ok, err := r.vcClient.PbmClient.SupportsEncryption(ctx, policyID)
	if err != nil {
		return err
	}

	r.logger.Debugf("Marking the VolumeAttributesClass %s as encryption-enabled: %v", obj.Name, ok)

	return r.cryptoClient.MarkEncryptedVAC(ctx, obj, ok)
}
