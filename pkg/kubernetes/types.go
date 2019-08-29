/*
Copyright 2019 The Kubernetes Authors.

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
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// InformerManager is a service that notifies subscribers about changes
// to well-defined information in the Kubernetes API server.
type InformerManager struct {
	// k8s client
	client clientset.Interface
	// main shared informer factory
	informerFactory informers.SharedInformerFactory
	// main signal
	stopCh (<-chan struct{})

	// node informer
	nodeInformer cache.SharedInformer

	// PV informer
	pvInformer cache.SharedInformer

	// PVC informer
	pvcInformer cache.SharedInformer

	// Pod informer
	podInformer cache.SharedInformer
}
