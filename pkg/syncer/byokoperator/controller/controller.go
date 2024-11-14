package controller

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/persistentvolumeclaim"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/storageclass"
)

var addToManagerFuncs = []func(ctx context.Context, mgr manager.Manager, opts common.Options) error{
	storageclass.AddToManager,
	persistentvolumeclaim.AddToManager,
}

func AddToManager(ctx context.Context, mgr manager.Manager, opts common.Options) error {
	for _, f := range addToManagerFuncs {
		if err := f(ctx, mgr, opts); err != nil {
			return err
		}
	}
	return nil
}
