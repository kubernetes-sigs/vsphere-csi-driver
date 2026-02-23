/*
Copyright 2025 The Kubernetes Authors.

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

// Package testing provides test helpers for the kubernetes package.
// It follows the same convention used by k8s.io/client-go/testing and
// k8s.io/apimachinery/pkg/api/testing: a real sub-package with a name that
// signals test-only use, keeping test scaffolding out of every other
// package's production binary.
//
// Import this package only from *_test.go files.
package testing

import (
	"context"

	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"

	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
)

// NewInformerForTest creates a fresh InformerManager bound to the given client
// without touching the process-level singleton. Each call returns an independent
// instance, so tests never share or corrupt each other's informer state.
// The informer factory runs until ctx is cancelled.
func NewInformerForTest(ctx context.Context, client clientset.Interface) *k8s.InformerManager {
	factory := informers.NewSharedInformerFactory(client, 0)
	return k8s.NewInformerFromFactory(ctx, client, factory)
}
