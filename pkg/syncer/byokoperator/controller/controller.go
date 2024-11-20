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
