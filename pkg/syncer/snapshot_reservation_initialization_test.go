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

package syncer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
)

// A reservation callback can read an uninitialized SPU and reach the API server after the Used
// reconciler. Its initialization patch must omit Used entirely; retaining a stale value or writing
// zero would overwrite valid usage. The stale patch is rejected on resourceVersion and re-applied
// to the latest copy. Exercise the same initializer and update path as CVOR Add.
func TestSnapshotReservationInitializationPreservesConcurrentUsed(t *testing.T) {
	initialUsed := resource.MustParse("1Gi")
	for _, tc := range []struct {
		name  string
		usage *storagepolicyv1alpha3.QuotaUsageDetails
	}{
		{name: "quotaUsage absent"},
		{name: "both fields absent", usage: &storagepolicyv1alpha3.QuotaUsageDetails{}},
		{name: "Used already initialized", usage: &storagepolicyv1alpha3.QuotaUsageDetails{Used: &initialUsed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			spu := &storagepolicyv1alpha3.StoragePolicyUsage{
				ObjectMeta: metav1.ObjectMeta{Name: "gold-snapshot-usage", Namespace: "ns1"},
				Status:     storagepolicyv1alpha3.StoragePolicyUsageStatus{ResourceTypeLevelQuotaUsage: tc.usage},
			}
			scheme := runtime.NewScheme()
			require.NoError(t, cnsoperatorv1alpha1.AddToScheme(scheme))
			patches := 0
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(spu).
				WithStatusSubresource(&storagepolicyv1alpha3.StoragePolicyUsage{}).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourcePatch: func(ctx context.Context, cl client.Client, sub string,
						obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
						patches++
						require.Equal(t, "status", sub)
						data, err := patch.Data(obj)
						require.NoError(t, err)
						var body map[string]interface{}
						require.NoError(t, json.Unmarshal(data, &body))
						delete(body, "metadata")
						require.Equal(t, map[string]interface{}{"status": map[string]interface{}{
							"quotaUsage": map[string]interface{}{"reserved": "10Gi"}}}, body)
						if patches == 1 {
							// Simulate the Used worker committing after CVOR Add's read but before its patch.
							err = cl.Status().Patch(ctx, obj.DeepCopyObject().(client.Object),
								client.RawPatch(apitypes.MergePatchType, []byte(`{"status":{"quotaUsage":{"used":"2Gi"}}}`)))
							require.NoError(t, err)
						}
						return cl.Status().Patch(ctx, obj, patch, opts...)
					},
				}).Build()
			oldSPU := &storagepolicyv1alpha3.StoragePolicyUsage{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(spu), oldSPU))
			require.NoError(t, UpdateStoragePolicyUsage(ctx, c, oldSPU,
				func(patched *storagepolicyv1alpha3.StoragePolicyUsage) bool {
					initializeStoragePolicyUsageReservation(patched, resource.MustParse("10Gi"), true)
					return true
				}))
			// The first patch is rejected on resourceVersion; the retry is based on the latest copy.
			require.Equal(t, 2, patches)
			got := &storagepolicyv1alpha3.StoragePolicyUsage{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(spu), got))
			require.Equal(t, "2Gi", got.Status.ResourceTypeLevelQuotaUsage.Used.String())
			require.Equal(t, "10Gi", got.Status.ResourceTypeLevelQuotaUsage.Reserved.String())
		})
	}
}

func TestPVCReservationInitializationRetainsExistingBehavior(t *testing.T) {
	used := resource.MustParse("1Gi")
	spu := &storagepolicyv1alpha3.StoragePolicyUsage{
		Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
			ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{Used: &used},
		},
	}
	oldSPU := spu.DeepCopy()
	initializeStoragePolicyUsageReservation(spu, resource.MustParse("10Gi"), false)
	patch, err := getPatchData(oldSPU, spu)
	require.NoError(t, err)
	require.JSONEq(t, `{"status":{"quotaUsage":{"reserved":"10Gi","used":"0"}}}`, string(patch))
}
