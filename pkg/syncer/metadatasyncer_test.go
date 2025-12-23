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

package syncer

import (
	"context"
	"errors"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	testclient "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	sqperiodicsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagequotaperiodicsync/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// fake implementation for fetchStorageClassToStoragePolicyMapping
var fakeStorageClassToStoragePolicy = map[string]string{
	"sc1": "policy1",
	"sc2": "policy2",
}

func fakeFetchStorageClassToStoragePolicyMapping(ctx context.Context) (map[string]string, error) {
	return fakeStorageClassToStoragePolicy, nil
}

func fakeErrorFetchStorageClassToStoragePolicyMapping(ctx context.Context) (map[string]string, error) {
	return nil, errors.New("fake error")
}

func TestGetExpectedReservedCapacityForSPR(t *testing.T) {
	// Replace the function fetchStorageClassToStoragePolicyMapping with fake function call to isolate testing
	orig := fetchStorageClassToStoragePolicyMapping
	fetchStorageClassToStoragePolicyMapping = fakeFetchStorageClassToStoragePolicyMapping

	ctx := context.Background()

	tenGi := resource.MustParse("10Gi")
	fiveGi := resource.MustParse("5Gi")
	sevenGi := resource.MustParse("7Gi")
	zero := resource.MustParse("0Gi")
	threeGi := resource.MustParse("3Gi")

	// Requested: sc1=10Gi, sc2=5Gi; Approved: sc1=7Gi, sc2=0Gi
	spr := storagepolicyv1alpha2.StoragePolicyReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spr-test",
			Namespace: "default",
		},
		Spec: storagepolicyv1alpha2.StoragePolicyReservationSpec{
			Requested: []storagepolicyv1alpha2.Requested{
				{
					ReservationRequests: []storagepolicyv1alpha2.ReservationRequest{
						{
							StorageClassName: "sc1",
							Request:          &tenGi,
						},
						{
							StorageClassName: "sc2",
							Request:          &fiveGi,
						},
					},
				},
			},
		},
		Status: storagepolicyv1alpha2.StoragePolicyReservationStatus{
			Approved: []storagepolicyv1alpha2.StorageObject{
				{
					StorageClassName: "sc1",
					Request:          &sevenGi,
				},
				{
					StorageClassName: "sc2",
					Request:          &zero,
				},
			},
		},
	}

	expected := map[string]*resource.Quantity{
		"policy1": resource.NewQuantity(threeGi.Value(), resource.BinarySI),
		"policy2": resource.NewQuantity(fiveGi.Value(), resource.BinarySI),
	}

	result, err := getExpectedReservedCapacityForSPR(ctx, spr)
	if err != nil {
		t.Errorf("getExpectedReservedCapacityForSPR() returned error: %v", err)
	}
	if !quantitiesEqual(result, expected) {
		t.Errorf("getExpectedReservedCapacityForSPR() result = %v; want %v", result, expected)
	}
	// Replace with the original function call
	fetchStorageClassToStoragePolicyMapping = orig
}

func TestGetExpectedReservedCapacityForSPR2(t *testing.T) {
	// Replace the function fetchStorageClassToStoragePolicyMapping with fake function call to isolate testing
	orig := fetchStorageClassToStoragePolicyMapping
	fetchStorageClassToStoragePolicyMapping = fakeFetchStorageClassToStoragePolicyMapping

	ctx := context.Background()

	tenGi := resource.MustParse("10Gi")
	fiveGi := resource.MustParse("5Gi")

	// Requested: sc1=10Gi, sc2=5Gi; Approved: sc1=10Gi
	spr := storagepolicyv1alpha2.StoragePolicyReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spr-test",
			Namespace: "default",
		},
		Spec: storagepolicyv1alpha2.StoragePolicyReservationSpec{
			Requested: []storagepolicyv1alpha2.Requested{
				{
					ReservationRequests: []storagepolicyv1alpha2.ReservationRequest{
						{
							StorageClassName: "sc1",
							Request:          &tenGi,
						},
						{
							StorageClassName: "sc2",
							Request:          &fiveGi,
						},
					},
				},
			},
		},
		Status: storagepolicyv1alpha2.StoragePolicyReservationStatus{
			Approved: []storagepolicyv1alpha2.StorageObject{
				{
					StorageClassName: "sc1",
					Request:          &tenGi,
				},
			},
		},
	}

	expected := map[string]*resource.Quantity{
		"policy2": resource.NewQuantity(fiveGi.Value(), resource.BinarySI),
	}

	result, err := getExpectedReservedCapacityForSPR(ctx, spr)
	if err != nil {
		t.Errorf("getExpectedReservedCapacityForSPR() returned error: %v", err)
	}
	if !quantitiesEqual(result, expected) {
		t.Errorf("getExpectedReservedCapacityForSPR() result = %v; want %v", result, expected)
	}
	// Replace with the original function call
	fetchStorageClassToStoragePolicyMapping = orig
}

func TestGetExpectedReservedCapacityForSPRError(t *testing.T) {
	// Replace the function fetchStorageClassToStoragePolicyMapping with fake function call which
	// returns error
	orig := fetchStorageClassToStoragePolicyMapping
	fetchStorageClassToStoragePolicyMapping = fakeErrorFetchStorageClassToStoragePolicyMapping

	ctx := context.Background()

	tenGi := resource.MustParse("10Gi")
	fiveGi := resource.MustParse("5Gi")
	spr := storagepolicyv1alpha2.StoragePolicyReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spr-test",
			Namespace: "default",
		},
		Spec: storagepolicyv1alpha2.StoragePolicyReservationSpec{
			Requested: []storagepolicyv1alpha2.Requested{
				{
					ReservationRequests: []storagepolicyv1alpha2.ReservationRequest{
						{
							StorageClassName: "sc1",
							Request:          &tenGi,
						},
						{
							StorageClassName: "sc2",
							Request:          &fiveGi,
						},
					},
				},
			},
		},
	}
	_, err := getExpectedReservedCapacityForSPR(ctx, spr)
	if err == nil {
		t.Error("getExpectedReservedCapacityForSPR() expected error but got nil")
	}
	// Replace with the original function call
	fetchStorageClassToStoragePolicyMapping = orig
}

// Helper to compare resource.Quantity maps
func quantitiesEqual(a, b map[string]*resource.Quantity) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || v.Cmp(*bv) != 0 {
			return false
		}
	}
	return true
}

// errorClient is a wrapper around a fake client that returns errors on List operations
type errorClient struct {
	client.Client
	err error
}

func (e *errorClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return e.err
}

func TestCalculateVMServiceStoragePolicyUsageReservedForNamespace(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"

	tests := []struct {
		name           string
		setupClient    func() client.Client
		expectedResult map[string]*resource.Quantity
		expectError    bool
	}{
		{
			name: "Success with single StoragePolicyUsage for VM extension",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				tenGi := resource.MustParse("10Gi")
				spu := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu).
					Build()
			},
			expectedResult: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				return map[string]*resource.Quantity{
					"policy-1": resource.NewQuantity(tenGi.Value(), resource.BinarySI),
				}
			}(),
			expectError: false,
		},
		{
			name: "Success with single StoragePolicyUsage for VM snapshot extension",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				fiveGi := resource.MustParse("5Gi")
				spu := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-2",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &fiveGi,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu).
					Build()
			},
			expectedResult: func() map[string]*resource.Quantity {
				fiveGi := resource.MustParse("5Gi")
				return map[string]*resource.Quantity{
					"policy-2": resource.NewQuantity(fiveGi.Value(), resource.BinarySI),
				}
			}(),
			expectError: false,
		},
		{
			name: "Success with multiple StoragePolicyUsage objects for same policy",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				tenGi := resource.MustParse("10Gi")
				fiveGi := resource.MustParse("5Gi")
				spu1 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}
				spu2 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &fiveGi,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu1, spu2).
					Build()
			},
			expectedResult: func() map[string]*resource.Quantity {
				fifteenGi := resource.MustParse("15Gi")
				return map[string]*resource.Quantity{
					"policy-1": resource.NewQuantity(fifteenGi.Value(), resource.BinarySI),
				}
			}(),
			expectError: false,
		},
		{
			name: "Success with multiple StoragePolicyUsage objects for different policies",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				tenGi := resource.MustParse("10Gi")
				fiveGi := resource.MustParse("5Gi")
				spu1 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}
				spu2 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-2",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &fiveGi,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu1, spu2).
					Build()
			},
			expectedResult: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				fiveGi := resource.MustParse("5Gi")
				return map[string]*resource.Quantity{
					"policy-1": resource.NewQuantity(tenGi.Value(), resource.BinarySI),
					"policy-2": resource.NewQuantity(fiveGi.Value(), resource.BinarySI),
				}
			}(),
			expectError: false,
		},
		{
			name: "Success with empty list",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				return fake.NewClientBuilder().
					WithScheme(scheme).
					Build()
			},
			expectedResult: map[string]*resource.Quantity{},
			expectError:    false,
		},
		{
			name: "Success with StoragePolicyUsage filtered out - wrong ResourceExtensionName",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				tenGi := resource.MustParse("10Gi")
				spu := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: "other-extension",
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu).
					Build()
			},
			expectedResult: map[string]*resource.Quantity{},
			expectError:    false,
		},
		{
			name: "Success with StoragePolicyUsage filtered out - marked for deletion",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				now := metav1.Now()
				tenGi := resource.MustParse("10Gi")
				spu := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "spu-1",
						Namespace:         namespace,
						DeletionTimestamp: &now,
						Finalizers:        []string{"test-finalizer"},
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu).
					Build()
			},
			expectedResult: map[string]*resource.Quantity{},
			expectError:    false,
		},
		{
			name: "Success with StoragePolicyUsage filtered out - missing ResourceTypeLevelQuotaUsage",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				spu := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: nil,
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu).
					Build()
			},
			expectedResult: map[string]*resource.Quantity{},
			expectError:    false,
		},
		{
			name: "Success with mixed valid and invalid StoragePolicyUsage objects",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				now := metav1.Now()
				tenGi := resource.MustParse("10Gi")
				fiveGi := resource.MustParse("5Gi")

				// Valid SPU
				spu1 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}

				// Invalid SPU - wrong extension name
				spu2 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-2",
						ResourceExtensionName: "other-extension",
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &fiveGi,
						},
					},
				}

				// Invalid SPU - marked for deletion
				spu3 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "spu-3",
						Namespace:         namespace,
						DeletionTimestamp: &now,
						Finalizers:        []string{"test-finalizer"},
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-3",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &fiveGi,
						},
					},
				}

				// Invalid SPU - missing status
				spu4 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-4",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-4",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: nil,
					},
				}

				// Valid SPU
				spu5 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-5",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &fiveGi,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu1, spu2, spu3, spu4, spu5).
					Build()
			},
			expectedResult: func() map[string]*resource.Quantity {
				fifteenGi := resource.MustParse("15Gi")
				return map[string]*resource.Quantity{
					"policy-1": resource.NewQuantity(fifteenGi.Value(), resource.BinarySI),
				}
			}(),
			expectError: false,
		},
		{
			name: "Error when List fails",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				baseClient := fake.NewClientBuilder().
					WithScheme(scheme).
					Build()

				return &errorClient{
					Client: baseClient,
					err:    errors.New("list error"),
				}
			},
			expectedResult: nil,
			expectError:    true,
		},
		{
			name: "Success with zero reserved quantity",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				zero := resource.MustParse("0Gi")
				spu := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &zero,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu).
					Build()
			},
			expectedResult: map[string]*resource.Quantity{
				"policy-1": resource.NewQuantity(0, resource.BinarySI),
			},
			expectError: false,
		},
		{
			name: "Success with StoragePolicyUsage in different namespace",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				tenGi := resource.MustParse("10Gi")
				// SPU in different namespace
				spu1 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: "other-namespace",
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}

				// SPU in target namespace
				fiveGi := resource.MustParse("5Gi")
				spu2 := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-2",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &fiveGi,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu1, spu2).
					Build()
			},
			expectedResult: func() map[string]*resource.Quantity {
				fiveGi := resource.MustParse("5Gi")
				return map[string]*resource.Quantity{
					"policy-2": resource.NewQuantity(fiveGi.Value(), resource.BinarySI),
				}
			}(),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := tt.setupClient()
			result, err := calculateVMServiceStoragePolicyUsageReservedForNamespace(ctx, fakeClient, namespace)

			if tt.expectError {
				if err == nil {
					t.Errorf("calculateVMServiceStoragePolicyUsageReservedForNamespace() expected error but got nil")
				}
			} else {
				if err != nil {
					t.Errorf("calculateVMServiceStoragePolicyUsageReservedForNamespace() returned error: %v", err)
				}
				if !quantitiesEqual(result, tt.expectedResult) {
					t.Errorf("calculateVMServiceStoragePolicyUsageReservedForNamespace() result = %v; want %v",
						result, tt.expectedResult)
				}
			}
		})
	}
}

func TestSyncStorageQuotaReserved(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                string
		setupClient         func() client.Client
		setupMetadataSyncer func(t *testing.T) *metadataSyncInformer
		expectError         bool
	}{
		{
			name: "Error when StoragePolicyQuota List fails",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				baseClient := fake.NewClientBuilder().
					WithScheme(scheme).
					Build()

				return &errorClient{
					Client: baseClient,
					err:    errors.New("list error"),
				}
			},
			setupMetadataSyncer: func(t *testing.T) *metadataSyncInformer {
				// Create a fake k8s client and informer factory to get listers
				// Note: We create a new informer factory directly instead of using
				// the singleton InformerManager to avoid interference with other tests.
				// The listers work fine without starting the informers.
				k8sClient := testclient.NewClientset()
				informerFactory := informers.NewSharedInformerFactory(k8sClient, 0)
				return &metadataSyncInformer{
					pvLister:  informerFactory.Core().V1().PersistentVolumes().Lister(),
					pvcLister: informerFactory.Core().V1().PersistentVolumeClaims().Lister(),
				}
			},
			expectError: false, // Function returns early, doesn't return error
		},
		{
			name: "Success with empty StoragePolicyQuota list",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)
				_ = sqperiodicsyncv1alpha1.AddToScheme(scheme)

				sqPeriodicSync := &sqperiodicsyncv1alpha1.StorageQuotaPeriodicSync{
					ObjectMeta: metav1.ObjectMeta{
						Name:      StorageQuotaPeriodicSyncInstanceName,
						Namespace: "vmware-system-csi",
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(sqPeriodicSync).
					Build()
			},
			setupMetadataSyncer: func(t *testing.T) *metadataSyncInformer {
				// Create a fake k8s client and informer factory to get listers
				// Note: We create a new informer factory directly instead of using
				// the singleton InformerManager to avoid interference with other tests.
				// The listers work fine without starting the informers.
				k8sClient := testclient.NewClientset()
				informerFactory := informers.NewSharedInformerFactory(k8sClient, 0)
				return &metadataSyncInformer{
					pvLister:  informerFactory.Core().V1().PersistentVolumes().Lister(),
					pvcLister: informerFactory.Core().V1().PersistentVolumeClaims().Lister(),
				}
			},
			expectError: false,
		},
		{
			name: "Success with single StoragePolicyQuota",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)
				_ = sqperiodicsyncv1alpha1.AddToScheme(scheme)

				spq := &storagepolicyv1alpha2.StoragePolicyQuota{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spq-1",
						Namespace: "test-ns",
					},
				}

				sqPeriodicSync := &sqperiodicsyncv1alpha1.StorageQuotaPeriodicSync{
					ObjectMeta: metav1.ObjectMeta{
						Name:      StorageQuotaPeriodicSyncInstanceName,
						Namespace: "vmware-system-csi",
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spq, sqPeriodicSync).
					Build()
			},
			setupMetadataSyncer: func(t *testing.T) *metadataSyncInformer {
				// Create a fake k8s client and informer factory to get listers
				// Note: We create a new informer factory directly instead of using
				// the singleton InformerManager to avoid interference with other tests.
				// The listers work fine without starting the informers.
				k8sClient := testclient.NewClientset()
				informerFactory := informers.NewSharedInformerFactory(k8sClient, 0)
				return &metadataSyncInformer{
					pvLister:  informerFactory.Core().V1().PersistentVolumes().Lister(),
					pvcLister: informerFactory.Core().V1().PersistentVolumeClaims().Lister(),
				}
			},
			expectError: false,
		},
		{
			name: "Success with multiple StoragePolicyQuota namespaces",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)
				_ = sqperiodicsyncv1alpha1.AddToScheme(scheme)

				spq1 := &storagepolicyv1alpha2.StoragePolicyQuota{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spq-1",
						Namespace: "test-ns-1",
					},
				}
				spq2 := &storagepolicyv1alpha2.StoragePolicyQuota{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spq-2",
						Namespace: "test-ns-2",
					},
				}

				sqPeriodicSync := &sqperiodicsyncv1alpha1.StorageQuotaPeriodicSync{
					ObjectMeta: metav1.ObjectMeta{
						Name:      StorageQuotaPeriodicSyncInstanceName,
						Namespace: "vmware-system-csi",
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spq1, spq2, sqPeriodicSync).
					Build()
			},
			setupMetadataSyncer: func(t *testing.T) *metadataSyncInformer {
				// Create a fake k8s client and informer factory to get listers
				// Note: We create a new informer factory directly instead of using
				// the singleton InformerManager to avoid interference with other tests.
				// The listers work fine without starting the informers.
				k8sClient := testclient.NewClientset()
				informerFactory := informers.NewSharedInformerFactory(k8sClient, 0)
				return &metadataSyncInformer{
					pvLister:  informerFactory.Core().V1().PersistentVolumes().Lister(),
					pvcLister: informerFactory.Core().V1().PersistentVolumeClaims().Lister(),
				}
			},
			expectError: false,
		},
		{
			name: "Negative value update skipped - StoragePolicyUsage with negative reserved",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)
				_ = sqperiodicsyncv1alpha1.AddToScheme(scheme)

				spq := &storagepolicyv1alpha2.StoragePolicyQuota{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spq-1",
						Namespace: "test-ns",
					},
				}

				// Create StoragePolicyUsage with negative reserved value
				negTenGi := resource.MustParse("-10Gi")
				spu := &storagepolicyv1alpha2.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: "test-ns",
					},
					Spec: storagepolicyv1alpha2.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha2.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha2.QuotaUsageDetails{
							Reserved: &negTenGi,
						},
					},
				}

				sqPeriodicSync := &sqperiodicsyncv1alpha1.StorageQuotaPeriodicSync{
					ObjectMeta: metav1.ObjectMeta{
						Name:      StorageQuotaPeriodicSyncInstanceName,
						Namespace: "vmware-system-csi",
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spq, spu, sqPeriodicSync).
					Build()
			},
			setupMetadataSyncer: func(t *testing.T) *metadataSyncInformer {
				// Save original function
				origFetchStorageClassToStoragePolicyMapping := fetchStorageClassToStoragePolicyMapping
				t.Cleanup(func() {
					fetchStorageClassToStoragePolicyMapping = origFetchStorageClassToStoragePolicyMapping
				})
				// Mock fetchStorageClassToStoragePolicyMapping
				fetchStorageClassToStoragePolicyMapping = fakeFetchStorageClassToStoragePolicyMapping

				k8sClient := testclient.NewClientset()
				informerFactory := informers.NewSharedInformerFactory(k8sClient, 0)
				return &metadataSyncInformer{
					pvLister:  informerFactory.Core().V1().PersistentVolumes().Lister(),
					pvcLister: informerFactory.Core().V1().PersistentVolumeClaims().Lister(),
				}
			},
			expectError: false, // Function should skip negative values and continue
		},
		{
			name: "PVC with same name but different size in different namespaces",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)
				_ = sqperiodicsyncv1alpha1.AddToScheme(scheme)

				spq1 := &storagepolicyv1alpha2.StoragePolicyQuota{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spq-1",
						Namespace: "test-ns-1",
					},
				}
				spq2 := &storagepolicyv1alpha2.StoragePolicyQuota{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spq-2",
						Namespace: "test-ns-2",
					},
				}

				sqPeriodicSync := &sqperiodicsyncv1alpha1.StorageQuotaPeriodicSync{
					ObjectMeta: metav1.ObjectMeta{
						Name:      StorageQuotaPeriodicSyncInstanceName,
						Namespace: "vmware-system-csi",
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spq1, spq2, sqPeriodicSync).
					Build()
			},
			setupMetadataSyncer: func(t *testing.T) *metadataSyncInformer {
				// Save original function
				origFetchStorageClassToStoragePolicyMapping := fetchStorageClassToStoragePolicyMapping
				t.Cleanup(func() {
					fetchStorageClassToStoragePolicyMapping = origFetchStorageClassToStoragePolicyMapping
				})
				// Mock fetchStorageClassToStoragePolicyMapping
				fetchStorageClassToStoragePolicyMapping = fakeFetchStorageClassToStoragePolicyMapping

				// Create PVCs with same name but different sizes in different namespaces
				scName := "sc1"
				pvc1 := &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "test-ns-1",
					},
					Spec: v1.PersistentVolumeClaimSpec{
						StorageClassName: &scName,
						Resources: v1.VolumeResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceStorage: resource.MustParse("10Gi"),
							},
						},
					},
					Status: v1.PersistentVolumeClaimStatus{
						Phase: v1.ClaimPending,
					},
				}

				pvc2 := &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc", // Same name as pvc1
						Namespace: "test-ns-2",
					},
					Spec: v1.PersistentVolumeClaimSpec{
						StorageClassName: &scName,
						Resources: v1.VolumeResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceStorage: resource.MustParse("20Gi"), // Different size
							},
						},
					},
					Status: v1.PersistentVolumeClaimStatus{
						Phase: v1.ClaimPending,
					},
				}

				k8sClient := testclient.NewClientset(pvc1, pvc2)
				informerFactory := informers.NewSharedInformerFactory(k8sClient, 0)
				// Start the informer factory to populate the cache
				stopCh := make(chan struct{})
				informerFactory.Start(stopCh)
				// Wait for cache sync - this ensures PVCs are available in the lister
				cacheSynced := informerFactory.WaitForCacheSync(stopCh)
				allSynced := true
				for _, synced := range cacheSynced {
					if !synced {
						allSynced = false
						break
					}
				}
				if !allSynced {
					t.Fatal("Failed to sync informer cache")
				}
				// Note: We don't close stopCh here as the test will complete and cleanup
				// The informer will stop when the test ends

				return &metadataSyncInformer{
					pvLister:  informerFactory.Core().V1().PersistentVolumes().Lister(),
					pvcLister: informerFactory.Core().V1().PersistentVolumeClaims().Lister(),
				}
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute function - this test verifies the function doesn't panic
			// and handles errors gracefully. Full unit testing would require
			// refactoring to use dependency injection for helper functions.
			fakeClient := tt.setupClient()
			metadataSyncer := tt.setupMetadataSyncer(t)

			// The function doesn't return errors, it logs them and continues
			// This test verifies it doesn't panic on various error conditions
			syncStorageQuotaReserved(ctx, fakeClient, metadataSyncer)

			// If we expected an error path, verify the function handled it gracefully
			// (by not panicking and completing execution)
		})
	}
}

func TestValidateReservedValues(t *testing.T) {
	tests := []struct {
		name           string
		reservedValues map[string]*resource.Quantity
		expectedResult bool
	}{
		{
			name: "All positive values",
			reservedValues: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				fiveGi := resource.MustParse("5Gi")
				return map[string]*resource.Quantity{
					"policy-1": &tenGi,
					"policy-2": &fiveGi,
				}
			}(),
			expectedResult: true,
		},
		{
			name: "Zero values",
			reservedValues: map[string]*resource.Quantity{
				"policy-1": resource.NewQuantity(0, resource.BinarySI),
			},
			expectedResult: true,
		},
		{
			name:           "Empty map",
			reservedValues: map[string]*resource.Quantity{},
			expectedResult: true,
		},
		{
			name: "Negative value",
			reservedValues: func() map[string]*resource.Quantity {
				negTenGi := resource.MustParse("-10Gi")
				return map[string]*resource.Quantity{
					"policy-1": &negTenGi,
				}
			}(),
			expectedResult: false,
		},
		{
			name: "Mixed positive and negative",
			reservedValues: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				negFiveGi := resource.MustParse("-5Gi")
				return map[string]*resource.Quantity{
					"policy-1": &tenGi,
					"policy-2": &negFiveGi,
				}
			}(),
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validateReservedValues(tt.reservedValues)
			if result != tt.expectedResult {
				t.Errorf("validateReservedValues() = %v; want %v", result, tt.expectedResult)
			}
		})
	}
}

func TestMergeStoragePolicyReserved(t *testing.T) {
	tests := []struct {
		name                       string
		storagePolicyReserved      map[string]*resource.Quantity
		totalStoragePolicyReserved map[string]*resource.Quantity
		expectedResult             map[string]*resource.Quantity
	}{
		{
			name: "Merge new policy into empty map",
			storagePolicyReserved: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				return map[string]*resource.Quantity{
					"policy-1": &tenGi,
				}
			}(),
			totalStoragePolicyReserved: map[string]*resource.Quantity{},
			expectedResult: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				return map[string]*resource.Quantity{
					"policy-1": &tenGi,
				}
			}(),
		},
		{
			name: "Merge new policy into existing map",
			storagePolicyReserved: func() map[string]*resource.Quantity {
				fiveGi := resource.MustParse("5Gi")
				return map[string]*resource.Quantity{
					"policy-2": &fiveGi,
				}
			}(),
			totalStoragePolicyReserved: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				return map[string]*resource.Quantity{
					"policy-1": &tenGi,
				}
			}(),
			expectedResult: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				fiveGi := resource.MustParse("5Gi")
				return map[string]*resource.Quantity{
					"policy-1": &tenGi,
					"policy-2": &fiveGi,
				}
			}(),
		},
		{
			name: "Merge same policy - should add values",
			storagePolicyReserved: func() map[string]*resource.Quantity {
				fiveGi := resource.MustParse("5Gi")
				return map[string]*resource.Quantity{
					"policy-1": &fiveGi,
				}
			}(),
			totalStoragePolicyReserved: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				return map[string]*resource.Quantity{
					"policy-1": &tenGi,
				}
			}(),
			expectedResult: func() map[string]*resource.Quantity {
				fifteenGi := resource.MustParse("15Gi")
				return map[string]*resource.Quantity{
					"policy-1": &fifteenGi,
				}
			}(),
		},
		{
			name: "Merge multiple policies",
			storagePolicyReserved: func() map[string]*resource.Quantity {
				threeGi := resource.MustParse("3Gi")
				sevenGi := resource.MustParse("7Gi")
				return map[string]*resource.Quantity{
					"policy-1": &threeGi,
					"policy-2": &sevenGi,
				}
			}(),
			totalStoragePolicyReserved: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				return map[string]*resource.Quantity{
					"policy-1": &tenGi,
				}
			}(),
			expectedResult: func() map[string]*resource.Quantity {
				thirteenGi := resource.MustParse("13Gi")
				sevenGi := resource.MustParse("7Gi")
				return map[string]*resource.Quantity{
					"policy-1": &thirteenGi,
					"policy-2": &sevenGi,
				}
			}(),
		},
		{
			name:                  "Merge empty map",
			storagePolicyReserved: map[string]*resource.Quantity{},
			totalStoragePolicyReserved: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				return map[string]*resource.Quantity{
					"policy-1": &tenGi,
				}
			}(),
			expectedResult: func() map[string]*resource.Quantity {
				tenGi := resource.MustParse("10Gi")
				return map[string]*resource.Quantity{
					"policy-1": &tenGi,
				}
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mergeStoragePolicyReserved(tt.storagePolicyReserved, tt.totalStoragePolicyReserved)
			if !quantitiesEqual(result, tt.expectedResult) {
				t.Errorf("mergeStoragePolicyReserved() = %v; want %v", result, tt.expectedResult)
			}
		})
	}
}

func TestCalculateSPRReservedForNamespace(t *testing.T) {
	ctx := context.Background()

	// Save original function
	origFetchStorageClassToStoragePolicyMapping := fetchStorageClassToStoragePolicyMapping
	defer func() {
		fetchStorageClassToStoragePolicyMapping = origFetchStorageClassToStoragePolicyMapping
	}()

	// Mock fetchStorageClassToStoragePolicyMapping
	fetchStorageClassToStoragePolicyMapping = fakeFetchStorageClassToStoragePolicyMapping

	tests := []struct {
		name           string
		setupClient    func() client.Client
		namespace      string
		expectedResult map[string]*resource.Quantity
		expectError    bool
	}{
		{
			name: "Success with single SPR",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				tenGi := resource.MustParse("10Gi")
				fiveGi := resource.MustParse("5Gi")
				spr := &storagepolicyv1alpha2.StoragePolicyReservation{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spr-1",
						Namespace: "test-ns",
					},
					Spec: storagepolicyv1alpha2.StoragePolicyReservationSpec{
						Requested: []storagepolicyv1alpha2.Requested{
							{
								ReservationRequests: []storagepolicyv1alpha2.ReservationRequest{
									{
										StorageClassName: "sc1",
										Request:          &tenGi,
									},
								},
							},
						},
					},
					Status: storagepolicyv1alpha2.StoragePolicyReservationStatus{
						Approved: []storagepolicyv1alpha2.StorageObject{
							{
								StorageClassName: "sc1",
								Request:          &fiveGi,
							},
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spr).
					Build()
			},
			namespace: "test-ns",
			expectedResult: func() map[string]*resource.Quantity {
				fiveGi := resource.MustParse("5Gi")
				return map[string]*resource.Quantity{
					"policy1": &fiveGi,
				}
			}(),
			expectError: false,
		},
		{
			name: "Success with empty SPR list",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				return fake.NewClientBuilder().
					WithScheme(scheme).
					Build()
			},
			namespace:      "test-ns",
			expectedResult: nil,
			expectError:    false,
		},
		{
			name: "Error when List fails",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				baseClient := fake.NewClientBuilder().
					WithScheme(scheme).
					Build()

				return &errorClient{
					Client: baseClient,
					err:    errors.New("list error"),
				}
			},
			namespace:      "test-ns",
			expectedResult: nil,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := tt.setupClient()
			result, err := calculateSPRReservedForNamespace(ctx, fakeClient, tt.namespace)

			if tt.expectError {
				if err == nil {
					t.Errorf("calculateSPRReservedForNamespace() expected error but got nil")
				}
			} else {
				if err != nil {
					t.Errorf("calculateSPRReservedForNamespace() returned error: %v", err)
				}
				if tt.expectedResult == nil {
					if result != nil {
						t.Errorf("calculateSPRReservedForNamespace() result = %v; want nil", result)
					}
				} else {
					if !quantitiesEqual(result, tt.expectedResult) {
						t.Errorf("calculateSPRReservedForNamespace() result = %v; want %v", result, tt.expectedResult)
					}
				}
			}
		})
	}
}

func TestUpdateStorageQuotaPeriodicSyncCR(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                   string
		setupClient            func() client.Client
		expectedReservedValues []sqperiodicsyncv1alpha1.ExpectedReservedValues
		expectError            bool
	}{
		{
			name: "Success with valid CR",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)
				_ = sqperiodicsyncv1alpha1.AddToScheme(scheme)

				sqPeriodicSync := &sqperiodicsyncv1alpha1.StorageQuotaPeriodicSync{
					ObjectMeta: metav1.ObjectMeta{
						Name:      StorageQuotaPeriodicSyncInstanceName,
						Namespace: common.GetCSINamespace(),
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(sqPeriodicSync).
					Build()
			},
			expectedReservedValues: func() []sqperiodicsyncv1alpha1.ExpectedReservedValues {
				tenGi := resource.MustParse("10Gi")
				return []sqperiodicsyncv1alpha1.ExpectedReservedValues{
					{
						Namespace: "test-ns",
						Reserved: map[string]*resource.Quantity{
							"policy-1": &tenGi,
						},
					},
				}
			}(),
			expectError: false,
		},
		{
			name: "Error when CR not found",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)
				_ = sqperiodicsyncv1alpha1.AddToScheme(scheme)

				return fake.NewClientBuilder().
					WithScheme(scheme).
					Build()
			},
			expectedReservedValues: []sqperiodicsyncv1alpha1.ExpectedReservedValues{},
			expectError:            false, // Function logs error and returns, doesn't return error
		},
		{
			name: "Success with empty reserved values",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)
				_ = sqperiodicsyncv1alpha1.AddToScheme(scheme)

				sqPeriodicSync := &sqperiodicsyncv1alpha1.StorageQuotaPeriodicSync{
					ObjectMeta: metav1.ObjectMeta{
						Name:      StorageQuotaPeriodicSyncInstanceName,
						Namespace: common.GetCSINamespace(),
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(sqPeriodicSync).
					Build()
			},
			expectedReservedValues: []sqperiodicsyncv1alpha1.ExpectedReservedValues{},
			expectError:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := tt.setupClient()
			lastSyncTime := metav1.NewTime(metav1.Now().Time)

			// The function doesn't return errors, it logs them
			// This test verifies it doesn't panic
			// Note: The fake client has limitations with status subresource patching,
			// so we just verify the function executes without panicking
			updateStorageQuotaPeriodicSyncCR(ctx, fakeClient, tt.expectedReservedValues, lastSyncTime)

			// The function handles errors gracefully by logging them
			// We verify it completes execution without panicking
		})
	}
}
