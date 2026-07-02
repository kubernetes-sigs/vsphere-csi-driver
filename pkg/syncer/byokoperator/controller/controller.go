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

	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/persistentvolumeclaim"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/storageclass"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/volumeattributesclass"
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

	// The VolumeAttributesClass API is only present on K8s 1.34+ clusters; registering a watch
	// for it on older clusters would fail since the API server does not serve the resource.
	if available, err := volumeAttributesClassAPIAvailable(mgr.GetConfig()); err != nil {
		return err
	} else if available {
		if err := volumeattributesclass.AddToManager(ctx, mgr, opts); err != nil {
			return err
		}
	}

	return nil
}

// volumeAttributesClassAPIAvailable checks whether the VolumeAttributesClass API is served by
// the API server the given REST config points to.
func volumeAttributesClassAPIAvailable(cfg *rest.Config) (bool, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return false, err
	}
	_, lists, err := dc.ServerGroupsAndResources()
	if lists == nil && err != nil {
		return false, err
	}
	gv := storagev1.SchemeGroupVersion.String()
	for _, list := range lists {
		if list.GroupVersion != gv {
			continue
		}
		for i := range list.APIResources {
			if list.APIResources[i].Name == "volumeattributesclasses" {
				return true, nil
			}
		}
	}
	return false, nil
}
