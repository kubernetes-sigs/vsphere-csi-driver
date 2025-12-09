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

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
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
