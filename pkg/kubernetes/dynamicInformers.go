/*
Copyright 2021 The Kubernetes Authors.

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

package kubernetes

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	restclient "k8s.io/client-go/rest"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

var (
	dynamicInformerFactoryMap           = make(map[string]dynamicinformer.DynamicSharedInformerFactory)
	dynamicInformerInitLock             = &sync.Mutex{}
	supervisorDynamicInformerFactoryMap = make(map[string]dynamicinformer.DynamicSharedInformerFactory)
	supervisorDynamicInformerInitLock   = &sync.Mutex{}
)

// newDynamicInformerFactory creates a dynamic informer factory for a given
// namespace if it doesn't exist already.
func newDynamicInformerFactory(ctx context.Context, cfg *restclient.Config, namespace string,
	isInCluster bool) (dynamicinformer.DynamicSharedInformerFactory, error) {
	log := logger.GetLogger(ctx)
	var (
		dynamicInformerFactory dynamicinformer.DynamicSharedInformerFactory
		exists                 bool
	)
	// Check if dynamic informer factory exists for the given namespace.
	if isInCluster {
		dynamicInformerInitLock.Lock()
		defer dynamicInformerInitLock.Unlock()

		dynamicInformerFactory, exists = dynamicInformerFactoryMap[namespace]
	} else {
		supervisorDynamicInformerInitLock.Lock()
		defer supervisorDynamicInformerInitLock.Unlock()

		dynamicInformerFactory, exists = supervisorDynamicInformerFactoryMap[namespace]
	}
	if !exists {
		// Grab a dynamic interface to create informer.
		dc, err := dynamic.NewForConfig(cfg)
		if err != nil {
			log.Errorf("could not generate dynamic client for config. InCluster: %t Error :%v", isInCluster, err)
			return nil, err
		}
		dynamicInformerFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(dc, 0, namespace, nil)
		if isInCluster {
			dynamicInformerFactoryMap[namespace] = dynamicInformerFactory
			log.Infof("Created new dynamic informer factory for %q namespace", namespace)
		} else {
			supervisorDynamicInformerFactoryMap[namespace] = dynamicInformerFactory
			log.Infof("Created new dynamic informer factory for %q namespace using the supervisor client", namespace)
		}
	}
	return dynamicInformerFactory, nil
}

// GetDynamicInformer returns informer for specified CRD group, version, name
// and namespace.
//
// isInCluster should be set to true if the resource is present in the same
// cluster, otherwise set false if the resource is present in the supervisor
// cluster in TKG flavor.
//
// Takes an input configuration to create a client for the dynamic informer.
// If isInCluster is set to true, the config contains credentials to the in
// cluster API server. If isInCluster is set to false, config contains
// credentials to the supervisor cluster.
func GetDynamicInformer(ctx context.Context, crdGroup, crdVersion, crdName, namespace string,
	cfg *restclient.Config, isInCluster bool) (informers.GenericInformer, error) {
	log := logger.GetLogger(ctx)
	var err error

	dynamicInformerFactory, err := newDynamicInformerFactory(ctx, cfg, namespace, isInCluster)
	if err != nil {
		log.Errorf("could not retrieve dynamic informer factory for %q namespace. Error: %+v", namespace, err)
		return nil, err
	}

	// Return informer from shared dynamic informer factory for input resource.
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	return dynamicInformerFactory.ForResource(gvr), nil
}
