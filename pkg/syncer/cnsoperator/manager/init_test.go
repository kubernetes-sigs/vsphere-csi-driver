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

package manager

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// expectedRegisteredKinds is a representative set of GroupVersionKinds, one per
// AddToScheme call in getGlobalScheme. Verifying these GVKs are recognized by
// the scheme confirms each underlying AddToScheme ran successfully without
// pulling every API package into this test (which would bloat the test binary).
var expectedRegisteredKinds = []schema.GroupVersionKind{
	// corev1
	{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"},
	// storagev1
	{Group: "storage.k8s.io", Version: "v1", Kind: "StorageClass"},
	// cnsoperatorv1alpha1
	{Group: "cns.vmware.com", Version: "v1alpha1", Kind: "CnsVolumeMetadata"},
	// csinodetopologyv1alpha1
	{Group: "cns.vmware.com", Version: "v1alpha1", Kind: "CSINodeTopology"},
	// internalapis
	{Group: "cns.vmware.com", Version: "v1alpha1", Kind: "CnsFileVolumeClient"},
	// wcpcapapis
	{Group: "iaas.vmware.com", Version: "v1alpha1", Kind: "Capabilities"},
	// vmoperatortypes (vm-operator v1alpha5)
	{Group: "vmoperator.vmware.com", Version: "v1alpha5", Kind: "VirtualMachine"},
}

// resetGlobalScheme resets the package-level globalScheme / schemeOnce so each
// test exercises getGlobalScheme from a clean slate.
func resetGlobalScheme() {
	globalScheme = nil
	schemeOnce = sync.Once{}
}

func TestGetGlobalScheme(t *testing.T) {
	// Always leave the package-level globalScheme/schemeOnce in a reset state
	// after this test so subsequent tests in the package start from a clean slate.
	defer resetGlobalScheme()

	t.Run("WhenFirstCall_ShouldInitializeScheme", func(tt *testing.T) {
		resetGlobalScheme()
		ctx := logger.NewContextWithLogger(context.Background())

		scheme := getGlobalScheme(ctx)

		assert.NotNil(tt, scheme, "Scheme should not be nil")
		assert.Same(tt, globalScheme, scheme, "Should return the global scheme instance")
		verifySchemeContainsExpectedTypes(tt, scheme)
	})

	t.Run("WhenMultipleCalls_ShouldReturnSameInstance", func(tt *testing.T) {
		resetGlobalScheme()
		ctx := logger.NewContextWithLogger(context.Background())

		scheme1 := getGlobalScheme(ctx)
		scheme2 := getGlobalScheme(ctx)
		scheme3 := getGlobalScheme(ctx)

		assert.NotNil(tt, scheme1, "First scheme should not be nil")
		assert.NotNil(tt, scheme2, "Second scheme should not be nil")
		assert.NotNil(tt, scheme3, "Third scheme should not be nil")

		assert.Same(tt, scheme1, scheme2, "Second call should return same instance")
		assert.Same(tt, scheme1, scheme3, "Third call should return same instance")
		assert.Same(tt, globalScheme, scheme1, "Should return the global scheme instance")
	})

	t.Run("WhenConcurrentCalls_ShouldReturnSameInstance", func(tt *testing.T) {
		resetGlobalScheme()
		ctx := logger.NewContextWithLogger(context.Background())

		const numGoroutines = 10
		schemes := make([]*runtime.Scheme, numGoroutines)
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				schemes[index] = getGlobalScheme(ctx)
			}(i)
		}

		wg.Wait()

		for i, scheme := range schemes {
			assert.NotNil(tt, scheme, "Scheme %d should not be nil", i)
			assert.Same(tt, globalScheme, scheme, "Scheme %d should be the global instance", i)
		}

		for i := 1; i < numGoroutines; i++ {
			assert.Same(tt, schemes[0], schemes[i], "All concurrent calls should return same instance")
		}

		verifySchemeContainsExpectedTypes(tt, schemes[0])
	})

	t.Run("WhenSchemeAlreadyInitialized_ShouldReturnExistingInstance", func(tt *testing.T) {
		existingScheme := runtime.NewScheme()
		globalScheme = existingScheme
		schemeOnce = sync.Once{}
		// Mark sync.Once as already done so getGlobalScheme skips re-init.
		schemeOnce.Do(func() {})

		ctx := logger.NewContextWithLogger(context.Background())

		scheme := getGlobalScheme(ctx)

		assert.NotNil(tt, scheme, "Scheme should not be nil")
		assert.Same(tt, existingScheme, scheme, "Should return the existing global scheme instance")
	})

	t.Run("WhenBackgroundContext_ShouldInitializeScheme", func(tt *testing.T) {
		resetGlobalScheme()

		ctx := context.Background()
		scheme := getGlobalScheme(ctx)

		assert.NotNil(tt, scheme, "Scheme should not be nil with background context")
		assert.Same(tt, globalScheme, scheme, "Should return the global scheme instance")
		verifySchemeContainsExpectedTypes(tt, scheme)
	})
}

// verifySchemeContainsExpectedTypes verifies that the scheme has all GVKs
// registered by the AddToScheme calls in getGlobalScheme.
func verifySchemeContainsExpectedTypes(t *testing.T, scheme *runtime.Scheme) {
	t.Helper()

	assert.NotNil(t, scheme, "Scheme should not be nil")
	assert.NotEmpty(t, scheme.AllKnownTypes(), "Scheme should have registered types")

	for _, gvk := range expectedRegisteredKinds {
		assert.Truef(t, scheme.Recognizes(gvk),
			"scheme should recognize %s", gvk.String())
	}
}

// BenchmarkGetGlobalScheme measures the cost of getGlobalScheme on the hot
// path (after first-time initialization) where the sync.Once short-circuits.
func BenchmarkGetGlobalScheme(b *testing.B) {
	globalScheme = nil
	schemeOnce = sync.Once{}
	ctx := logger.NewContextWithLogger(context.Background())

	// Initialize once so subsequent iterations hit the fast path.
	getGlobalScheme(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getGlobalScheme(ctx)
	}
}

// BenchmarkGetGlobalSchemeConcurrent measures the cost of getGlobalScheme when
// called from multiple goroutines concurrently after initialization.
func BenchmarkGetGlobalSchemeConcurrent(b *testing.B) {
	globalScheme = nil
	schemeOnce = sync.Once{}
	ctx := logger.NewContextWithLogger(context.Background())

	getGlobalScheme(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			getGlobalScheme(ctx)
		}
	})
}
