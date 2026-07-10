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
	"fmt"
	"testing"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	testclient "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
	sqperiodicsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagequotaperiodicsync/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
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

func TestCalculateExtensionResourceSPUReservedForNamespace(t *testing.T) {
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
				spu := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
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
				spu := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-2",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
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
				spu1 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}
				spu2 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
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
				spu1 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}
				spu2 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-2",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
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
				spu := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: "other-extension",
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
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
				spu := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "spu-1",
						Namespace:         namespace,
						DeletionTimestamp: &now,
						Finalizers:        []string{"test-finalizer"},
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
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

				spu := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
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
				spu1 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}

				// Invalid SPU - wrong extension name
				spu2 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-2",
						ResourceExtensionName: "other-extension",
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
							Reserved: &fiveGi,
						},
					},
				}

				// Invalid SPU - marked for deletion
				spu3 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "spu-3",
						Namespace:         namespace,
						DeletionTimestamp: &now,
						Finalizers:        []string{"test-finalizer"},
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-3",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
							Reserved: &fiveGi,
						},
					},
				}

				// Invalid SPU - missing status
				spu4 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-4",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-4",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: nil,
					},
				}

				// Valid SPU
				spu5 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-5",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
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
				spu := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
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
				spu1 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: "other-namespace",
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}

				// SPU in target namespace
				fiveGi := resource.MustParse("5Gi")
				spu2 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-2",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
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
		{
			name: "Success with single StoragePolicyUsage for PodVM extension",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				threeGi := resource.MustParse("3Gi")
				spu := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-podvm",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-podvm",
						ResourceExtensionName: PodVMQuotaExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
							Reserved: &threeGi,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu).
					Build()
			},
			expectedResult: func() map[string]*resource.Quantity {
				threeGi := resource.MustParse("3Gi")
				return map[string]*resource.Quantity{
					"policy-podvm": resource.NewQuantity(threeGi.Value(), resource.BinarySI),
				}
			}(),
			expectError: false,
		},
		{
			name: "Success with multiple extensions including PodVM",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorv1alpha1.AddToScheme(scheme)

				tenGi := resource.MustParse("10Gi")
				fiveGi := resource.MustParse("5Gi")
				threeGi := resource.MustParse("3Gi")
				spu1 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
							Reserved: &tenGi,
						},
					},
				}
				spu2 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-2",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMSnapshotExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
							Reserved: &fiveGi,
						},
					},
				}
				spu3 := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-3",
						Namespace: namespace,
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: PodVMQuotaExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
							Reserved: &threeGi,
						},
					},
				}

				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(spu1, spu2, spu3).
					Build()
			},
			expectedResult: func() map[string]*resource.Quantity {
				eighteenGi := resource.MustParse("18Gi")
				return map[string]*resource.Quantity{
					"policy-1": resource.NewQuantity(eighteenGi.Value(), resource.BinarySI),
				}
			}(),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := tt.setupClient()
			result, err := calculateExtensionResourceSPUReservedForNamespace(ctx, fakeClient,
				namespace)

			if tt.expectError {
				if err == nil {
					t.Errorf(
						"calculateExtensionResourceSPUReservedForNamespace() expected " +
							"error but got nil")
				}
			} else {
				if err != nil {
					t.Errorf("calculateExtensionResourceSPUReservedForNamespace() "+
						"returned error: %v", err)
				}
				if !quantitiesEqual(result, tt.expectedResult) {
					t.Errorf("calculateExtensionResourceSPUReservedForNamespace() "+
						"result = %v; want %v", result, tt.expectedResult)
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

				spq := &storagepolicyv1alpha3.StoragePolicyQuota{
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

				spq1 := &storagepolicyv1alpha3.StoragePolicyQuota{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spq-1",
						Namespace: "test-ns-1",
					},
				}
				spq2 := &storagepolicyv1alpha3.StoragePolicyQuota{
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

				spq := &storagepolicyv1alpha3.StoragePolicyQuota{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spq-1",
						Namespace: "test-ns",
					},
				}

				// Create StoragePolicyUsage with negative reserved value
				negTenGi := resource.MustParse("-10Gi")
				spu := &storagepolicyv1alpha3.StoragePolicyUsage{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spu-1",
						Namespace: "test-ns",
					},
					Spec: storagepolicyv1alpha3.StoragePolicyUsageSpec{
						StoragePolicyId:       "policy-1",
						ResourceExtensionName: VMExtensionServiceName,
					},
					Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
						ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
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

				spq1 := &storagepolicyv1alpha3.StoragePolicyQuota{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "spq-1",
						Namespace: "test-ns-1",
					},
				}
				spq2 := &storagepolicyv1alpha3.StoragePolicyQuota{
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

// TestHandleVACChangeForVolumeInfo verifies that a successful storage-policy
// migration rebalances StoragePolicyUsage quota from the OLD SPU (keyed by
// old VAC, or StorageClassName when there was no prior VAC) to the NEW SPU
// (always keyed by new VAC).
func TestHandleVACChangeForVolumeInfo(t *testing.T) {
	const (
		namespace = "test-ns"
		scName    = "sc-old"
		oldVAC    = "vac-silver"
		newVAC    = "vac-gold"
	)

	// mkSPU is a small builder for StoragePolicyUsage CRs with a non-nil
	// Status.ResourceTypeLevelQuotaUsage.Used so updateStoragePolicyUsageQuota
	// follows the in-place add/sub branch.
	mkSPU := func(name string, used resource.Quantity) *storagepolicyv1alpha3.StoragePolicyUsage {
		return &storagepolicyv1alpha3.StoragePolicyUsage{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Status: storagepolicyv1alpha3.StoragePolicyUsageStatus{
				ResourceTypeLevelQuotaUsage: &storagepolicyv1alpha3.QuotaUsageDetails{
					Used: &used,
				},
			},
		}
	}

	pvcCap := resource.MustParse("10Gi")
	snapCap := resource.MustParse("3Gi")

	cases := []struct {
		name             string
		oldVAC           string
		expectedOldSPU   string // SPU name we expect the helper to DECREMENT
		newVAC           string
		expectedNewSPU   string // SPU name we expect the helper to INCREMENT
		expectSnapshotIO bool   // whether we wire AggregatedSnapshotSize
	}{
		{
			name:             "first migration: no prior VAC -> SC-keyed SPU drained, VAC-keyed SPU filled",
			oldVAC:           "",
			expectedOldSPU:   scName + "-" + storagepolicyv1alpha3.NameSuffixForPVC,
			newVAC:           newVAC,
			expectedNewSPU:   "vac-" + newVAC + "-" + storagepolicyv1alpha3.NameSuffixForPVC,
			expectSnapshotIO: false,
		},
		{
			name:             "VAC -> VAC migration: old VAC SPU drained, new VAC SPU filled",
			oldVAC:           oldVAC,
			expectedOldSPU:   "vac-" + oldVAC + "-" + storagepolicyv1alpha3.NameSuffixForPVC,
			newVAC:           newVAC,
			expectedNewSPU:   "vac-" + newVAC + "-" + storagepolicyv1alpha3.NameSuffixForPVC,
			expectSnapshotIO: false,
		},
		{
			name:             "with AggregatedSnapshotSize: both PVC and snapshot SPUs are rebalanced",
			oldVAC:           oldVAC,
			expectedOldSPU:   "vac-" + oldVAC + "-" + storagepolicyv1alpha3.NameSuffixForPVC,
			newVAC:           newVAC,
			expectedNewSPU:   "vac-" + newVAC + "-" + storagepolicyv1alpha3.NameSuffixForPVC,
			expectSnapshotIO: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			_ = cnsoperatorv1alpha1.AddToScheme(scheme)

			// Seed the old SPU with the full quota and the new SPU with zero,
			// so we can read back: old.Used should decrement by pvcCap,
			// new.Used should increment by pvcCap.
			oldSPU := mkSPU(tc.expectedOldSPU, pvcCap.DeepCopy())
			newSPUStartUsed := resource.MustParse("0")
			newSPU := mkSPU(tc.expectedNewSPU, newSPUStartUsed)

			objs := []client.Object{oldSPU, newSPU}

			var oldSnapSPU, newSnapSPU *storagepolicyv1alpha3.StoragePolicyUsage
			if tc.expectSnapshotIO {
				oldSnapName := scName + "-" + storagepolicyv1alpha3.NameSuffixForSnapshot
				newSnapName := "vac-" + tc.newVAC + "-" + storagepolicyv1alpha3.NameSuffixForSnapshot
				if tc.oldVAC != "" {
					oldSnapName = "vac-" + tc.oldVAC + "-" + storagepolicyv1alpha3.NameSuffixForSnapshot
				}
				oldSnapSPU = mkSPU(oldSnapName, snapCap.DeepCopy())
				zero := resource.MustParse("0")
				newSnapSPU = mkSPU(newSnapName, zero)
				objs = append(objs, oldSnapSPU, newSnapSPU)
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				WithStatusSubresource(&storagepolicyv1alpha3.StoragePolicyUsage{}).
				Build()

			oldCVI := cnsvolumeinfov1alpha1.CNSVolumeInfo{
				Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
					Namespace:                namespace,
					StorageClassName:         scName,
					VolumeAttributeClassName: tc.oldVAC,
					K8sCompliantName:         tc.oldVAC,
					StoragePolicyID:          "policy-uuid-silver",
					Capacity:                 &pvcCap,
				},
			}
			if tc.expectSnapshotIO {
				oldCVI.Spec.AggregatedSnapshotSize = &snapCap
			}
			newCVI := oldCVI
			newCVI.Spec.VolumeAttributeClassName = tc.newVAC
			newCVI.Spec.K8sCompliantName = tc.newVAC
			newCVI.Spec.StoragePolicyID = "policy-uuid-gold"
			// StorageClassName is INTENTIONALLY unchanged - this mirrors what
			// the watcher writes (PVC StorageClassName is immutable).

			handleVACChangeForVolumeInfo(ctx, fakeClient, oldCVI, newCVI)

			// Verify quotas moved.
			gotOld := &storagepolicyv1alpha3.StoragePolicyUsage{}
			if err := fakeClient.Get(ctx, client.ObjectKey{
				Namespace: namespace, Name: tc.expectedOldSPU,
			}, gotOld); err != nil {
				t.Fatalf("failed to read OLD SPU %q: %v", tc.expectedOldSPU, err)
			}
			if gotOld.Status.ResourceTypeLevelQuotaUsage.Used.Value() != 0 {
				t.Errorf("OLD SPU %q used=%s want 0 (decremented by %s)",
					tc.expectedOldSPU,
					gotOld.Status.ResourceTypeLevelQuotaUsage.Used.String(),
					pvcCap.String())
			}

			gotNew := &storagepolicyv1alpha3.StoragePolicyUsage{}
			if err := fakeClient.Get(ctx, client.ObjectKey{
				Namespace: namespace, Name: tc.expectedNewSPU,
			}, gotNew); err != nil {
				t.Fatalf("failed to read NEW SPU %q: %v", tc.expectedNewSPU, err)
			}
			if gotNew.Status.ResourceTypeLevelQuotaUsage.Used.Value() != pvcCap.Value() {
				t.Errorf("NEW SPU %q used=%s want %s (incremented)",
					tc.expectedNewSPU,
					gotNew.Status.ResourceTypeLevelQuotaUsage.Used.String(),
					pvcCap.String())
			}

			if tc.expectSnapshotIO {
				gotOldSnap := &storagepolicyv1alpha3.StoragePolicyUsage{}
				if err := fakeClient.Get(ctx, client.ObjectKey{
					Namespace: namespace, Name: oldSnapSPU.Name,
				}, gotOldSnap); err != nil {
					t.Fatalf("failed to read OLD snapshot SPU: %v", err)
				}
				if gotOldSnap.Status.ResourceTypeLevelQuotaUsage.Used.Value() != 0 {
					t.Errorf("OLD snap SPU used=%s want 0",
						gotOldSnap.Status.ResourceTypeLevelQuotaUsage.Used.String())
				}
				gotNewSnap := &storagepolicyv1alpha3.StoragePolicyUsage{}
				if err := fakeClient.Get(ctx, client.ObjectKey{
					Namespace: namespace, Name: newSnapSPU.Name,
				}, gotNewSnap); err != nil {
					t.Fatalf("failed to read NEW snapshot SPU: %v", err)
				}
				if gotNewSnap.Status.ResourceTypeLevelQuotaUsage.Used.Value() != snapCap.Value() {
					t.Errorf("NEW snap SPU used=%s want %s",
						gotNewSnap.Status.ResourceTypeLevelQuotaUsage.Used.String(),
						snapCap.String())
				}
			}
		})
	}
}

// TestHandleVACChangeForVolumeInfo_NilCapacity verifies that nil PVC capacity
// is a no-op (no panics, no SPU read attempts).
func TestHandleVACChangeForVolumeInfo_NilCapacity(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = cnsoperatorv1alpha1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	oldCVI := cnsvolumeinfov1alpha1.CNSVolumeInfo{
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			Namespace:        "test-ns",
			StorageClassName: "sc-old",
			Capacity:         nil,
		},
	}
	newCVI := oldCVI
	newCVI.Spec.VolumeAttributeClassName = "vac-gold"
	newCVI.Spec.K8sCompliantName = "vac-gold"

	// Should not panic, no SPU lookups should happen.
	handleVACChangeForVolumeInfo(ctx, fakeClient, oldCVI, newCVI)
}

func TestInitMigrationWatchersOnStartup(t *testing.T) {
	// Helper to create a PVC with migration annotations
	makePVCWithAnnotations := func(namespace, name string, annotations map[string]string) *v1.PersistentVolumeClaim {
		return &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   namespace,
				Name:        name,
				Annotations: annotations,
			},
		}
	}

	// Helper to enable migration FSS
	enableMigrationFSS := func(t *testing.T) {
		t.Helper()
		fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
		if err != nil {
			t.Fatalf("GetFakeContainerOrchestratorInterface failed: %v", err)
		}
		if err := fakeCO.EnableFSS(context.Background(), common.VMPVCStoragePolicyMutability); err != nil {
			t.Fatalf("EnableFSS failed: %v", err)
		}
		commonco.ContainerOrchestratorUtility = fakeCO
	}

	// Helper to reset test state
	resetTestState := func() {
		commonco.ContainerOrchestratorUtility = nil
	}

	t.Run("FSS disabled - no-op", func(t *testing.T) {
		resetTestState()
		ctx := context.Background()

		// Create a simple mock syncer with minimal setup
		syncer := &metadataSyncInformer{}

		// Call initMigrationWatchersOnStartup - should return early due to FSS disabled
		initMigrationWatchersOnStartup(ctx, syncer)

		// Test passes if no panic occurred and function returned
	})

	t.Run("orchestrator nil - no-op", func(t *testing.T) {
		resetTestState()
		ctx := context.Background()

		// Explicitly set orchestrator to nil
		commonco.ContainerOrchestratorUtility = nil

		syncer := &metadataSyncInformer{}

		initMigrationWatchersOnStartup(ctx, syncer)

		// Test passes if no panic occurred
	})

	t.Run("PVC lister error handling", func(t *testing.T) {
		resetTestState()
		enableMigrationFSS(t)
		ctx := context.Background()

		// Create syncer with nil pvcLister to simulate error
		syncer := &metadataSyncInformer{
			pvcLister: nil, // This will cause List() to fail
		}

		// This should not panic and should handle the error gracefully
		// The function should log the error and return
		initMigrationWatchersOnStartup(ctx, syncer)
		// If we reach here without panic, the error was handled correctly
	})

	t.Run("integration test - basic functionality", func(t *testing.T) {
		resetTestState()
		enableMigrationFSS(t)
		ctx := context.Background()

		// Create test PVCs
		pvcWithAnnotations := makePVCWithAnnotations("test-ns", "test-pvc", map[string]string{
			common.AnnMigrationCRKind: common.MigrationCRKindVolume,
			common.AnnMigrationCRName: "test-cr",
		})
		pvcWithoutAnnotations := makePVCWithAnnotations("test-ns", "test-pvc2", nil)

		// Create basic informer setup using the pattern from other tests
		objs := []runtime.Object{pvcWithAnnotations, pvcWithoutAnnotations}
		client := testclient.NewClientset(objs...)

		// Use a simple informer factory setup
		factory := informers.NewSharedInformerFactory(client, 0)
		pvcInformer := factory.Core().V1().PersistentVolumeClaims()

		// Start the informer
		stopCh := make(chan struct{})
		defer close(stopCh)
		factory.Start(stopCh)
		factory.WaitForCacheSync(stopCh)

		syncer := &metadataSyncInformer{
			pvcLister: pvcInformer.Lister(),
		}

		// This should process the PVCs and call handlePvcMigrationAnnotations for the one with annotations
		// The actual behavior will be tested by handlePvcMigrationAnnotations which has its own FSS checks
		initMigrationWatchersOnStartup(ctx, syncer)

		// Test passes if no panic occurred and function processed PVCs correctly
	})
}

// ---------------------------------------------------------------------------
// TestCreateVACStoragePolicyUsageCRsFromList – unit tests for the inner
// VAC-SPU creation loop that is exercised by both
// createVACStoragePolicyUsageCRs and createVACStoragePolicyUsageCRsForFullSync.
// ---------------------------------------------------------------------------

// newVACFakeClient returns a fake controller-runtime client seeded with the
// CNS operator scheme so StoragePolicyUsage objects can be created/listed.
func newVACFakeClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	_ = cnsoperatorv1alpha1.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

// makeVAC builds a VolumeAttributesClass with the given policy ID in its
// parameters (the same key that getStoragePolicyIDFromVAC looks for).
func makeVAC(name, policyID string) storagev1.VolumeAttributesClass {
	return storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Parameters: map[string]string{"storagePolicyID": policyID},
	}
}

// listSPUs is a test helper that returns all StoragePolicyUsage CRs in a namespace.
func listSPUs(t *testing.T, ctx context.Context, cl client.Client, namespace string,
) []storagepolicyv1alpha3.StoragePolicyUsage {
	t.Helper()
	list := &storagepolicyv1alpha3.StoragePolicyUsageList{}
	if err := cl.List(ctx, list, &client.ListOptions{Namespace: namespace}); err != nil {
		t.Fatalf("listSPUs: %v", err)
	}
	return list.Items
}

// TestCreateVACStoragePolicyUsageCRsFromList_NamingPrefix verifies that
// VAC-based SPU names are prefixed with "vac-" to avoid collisions with
// StorageClass-based SPU names.
func TestCreateVACStoragePolicyUsageCRsFromList_NamingPrefix(t *testing.T) {
	const (
		ns       = "test-ns"
		policyID = "policy-abc"
		vacName  = "gold-vac"
	)

	ctx := context.Background()
	cl := newVACFakeClient()
	vacs := []storagev1.VolumeAttributesClass{makeVAC(vacName, policyID)}

	err := createVACStoragePolicyUsageCRsFromList(ctx, cl, vacs, ns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spus := listSPUs(t, ctx, cl, ns)
	// Both PVC and Snapshot SPUs are always created (no StorageQuotaM2 gating for VACs).
	if len(spus) != 2 {
		t.Fatalf("expected 2 SPUs (PVC + Snapshot), got %d: %v", len(spus), spuNames(spus))
	}

	wantPVC := fmt.Sprintf("vac-%s-%s", vacName, storagepolicyv1alpha3.NameSuffixForPVC)
	if !contains(spuNames(spus), wantPVC) {
		t.Errorf("PVC SPU %q not found among %v", wantPVC, spuNames(spus))
	}
}

// TestCreateVACStoragePolicyUsageCRsFromList_CreatesBothSPUs verifies that both a PVC SPU and a
// Snapshot SPU are always created for a matching VAC. VAC-based SPU creation is already gated by
// the VMPVCStoragePolicyMutability FSS at the call site, so no additional StorageQuotaM2 check is
// needed inside this function.
func TestCreateVACStoragePolicyUsageCRsFromList_CreatesBothSPUs(t *testing.T) {
	const (
		ns       = "test-ns"
		policyID = "policy-xyz"
		vacName  = "silver-vac"
	)
	vacs := []storagev1.VolumeAttributesClass{makeVAC(vacName, policyID)}

	wantPVC := fmt.Sprintf("vac-%s-%s", vacName, storagepolicyv1alpha3.NameSuffixForPVC)
	wantSnap := fmt.Sprintf("vac-%s-%s", vacName, storagepolicyv1alpha3.NameSuffixForSnapshot)

	ctx := context.Background()
	cl := newVACFakeClient()

	if err := createVACStoragePolicyUsageCRsFromList(ctx, cl, vacs, ns); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spus := listSPUs(t, ctx, cl, ns)
	if len(spus) != 2 {
		t.Fatalf("expected 2 SPUs (PVC + Snapshot), got %d: %v", len(spus), spuNames(spus))
	}

	names := spuNames(spus)
	if !contains(names, wantPVC) {
		t.Errorf("PVC SPU %q not found among %v", wantPVC, names)
	}
	if !contains(names, wantSnap) {
		t.Errorf("Snapshot SPU %q not found among %v", wantSnap, names)
	}
}

// TestCreateVACStoragePolicyUsageCRsFromList_IdempotentWhenSPUExists verifies
// that calling the function twice does not produce duplicate SPUs.
func TestCreateVACStoragePolicyUsageCRsFromList_IdempotentWhenSPUExists(t *testing.T) {
	const (
		ns       = "test-ns"
		policyID = "policy-idem"
		vacName  = "idem-vac"
	)
	vacs := []storagev1.VolumeAttributesClass{makeVAC(vacName, policyID)}
	ctx := context.Background()
	cl := newVACFakeClient()

	for i := range 2 {
		if err := createVACStoragePolicyUsageCRsFromList(ctx, cl, vacs, ns); err != nil {
			t.Fatalf("call %d: unexpected error: %v", i+1, err)
		}
	}

	spus := listSPUs(t, ctx, cl, ns)
	// Expect exactly 2 SPUs (PVC + Snapshot) — no duplicates from the second call.
	if len(spus) != 2 {
		t.Errorf("expected exactly 2 SPUs after two calls, got %d: %v", len(spus), spuNames(spus))
	}
}

// TestCreateVACStoragePolicyUsageCRsFromList_MultipleVACs verifies that SPUs are created
// independently for each VAC in the list, using each VAC's own storage policy ID.
// The VAC name is the unique key — policy ID is informational only.
func TestCreateVACStoragePolicyUsageCRsFromList_MultipleVACs(t *testing.T) {
	const ns = "test-ns"
	vacs := []storagev1.VolumeAttributesClass{
		makeVAC("vac-alpha", "policy-alpha"),
		makeVAC("vac-beta", "policy-beta"),
	}
	ctx := context.Background()
	cl := newVACFakeClient()

	if err := createVACStoragePolicyUsageCRsFromList(ctx, cl, vacs, ns); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spus := listSPUs(t, ctx, cl, ns)
	// Each VAC produces PVC + Snapshot SPUs independently.
	if len(spus) != 4 {
		t.Errorf("expected 4 SPUs (2 VACs × 2 kinds), got %d: %v", len(spus), spuNames(spus))
	}

	wantNames := []string{
		fmt.Sprintf("vac-vac-alpha-%s", storagepolicyv1alpha3.NameSuffixForPVC),
		fmt.Sprintf("vac-vac-alpha-%s", storagepolicyv1alpha3.NameSuffixForSnapshot),
		fmt.Sprintf("vac-vac-beta-%s", storagepolicyv1alpha3.NameSuffixForPVC),
		fmt.Sprintf("vac-vac-beta-%s", storagepolicyv1alpha3.NameSuffixForSnapshot),
	}
	names := spuNames(spus)
	for _, want := range wantNames {
		if !contains(names, want) {
			t.Errorf("SPU %q not found among %v", want, names)
		}
	}
}

// spuNames extracts the names from a StoragePolicyUsage slice for readable error messages.
func spuNames(spus []storagepolicyv1alpha3.StoragePolicyUsage) []string {
	names := make([]string, len(spus))
	for i, s := range spus {
		names[i] = s.Name
	}
	return names
}

// contains reports whether s is in the slice.
func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func TestSnapshotDeleted(t *testing.T) {
	syncer := &metadataSyncInformer{}

	t.Run("NonSnapshotObject", func(tt *testing.T) {
		assert.NotPanics(tt, func() {
			snapshotDeleted("bad-object", syncer)
		})
	})

	t.Run("NilObject", func(tt *testing.T) {
		assert.NotPanics(tt, func() {
			snapshotDeleted(nil, syncer)
		})
	})

	t.Run("NilTypedPointer", func(tt *testing.T) {
		var nilSnap *snapshotv1.VolumeSnapshot
		assert.NotPanics(tt, func() {
			snapshotDeleted(nilSnap, syncer)
		})
	})

	t.Run("SnapshotWithNilStatus", func(tt *testing.T) {
		snap := &snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "snap1", Namespace: "ns1"},
		}
		assert.NotPanics(tt, func() {
			snapshotDeleted(snap, syncer)
		})
	})

	t.Run("SnapshotWithEmptyBoundContentName", func(tt *testing.T) {
		empty := ""
		snap := &snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "snap1", Namespace: "ns1"},
			Status: &snapshotv1.VolumeSnapshotStatus{
				BoundVolumeSnapshotContentName: &empty,
			},
		}
		assert.NotPanics(tt, func() {
			snapshotDeleted(snap, syncer)
		})
	})

	t.Run("SnapshotWithValidContentName", func(tt *testing.T) {
		contentName := "snapcontent-xyz"
		snap := &snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "snap1", Namespace: "ns1"},
			Status: &snapshotv1.VolumeSnapshotStatus{
				BoundVolumeSnapshotContentName: &contentName,
			},
		}
		// A bound snapshot reaches pvcsiSnapshotDeleted, which is gated by the
		// ImprovedVolumeVisibility FSS. With the FSS disabled it must return early without
		// attempting to create real clients. The annotation-removal path itself is covered by
		// TestSnapshotDeletedRemoveAnnotation.
		co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
		assert.NoError(tt, err)
		fssDisabledSyncer := &metadataSyncInformer{coCommonInterface: co}
		assert.NotPanics(tt, func() {
			snapshotDeleted(snap, fssDisabledSyncer)
		})
	})

	t.Run("SnapshotWithValidContentName_FSSEnabled_NamespaceError", func(tt *testing.T) {
		contentName := "snapcontent-xyz"
		snap := &snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "snap1", Namespace: "ns1"},
			Status: &snapshotv1.VolumeSnapshotStatus{
				BoundVolumeSnapshotContentName: &contentName,
			},
		}
		// Build a syncer with the ImprovedVolumeVisibility FSS enabled so
		// pvcsiSnapshotDeleted proceeds past the FSS gate and reaches the
		// namespace-resolution step. The namespace lookup itself will fail in the
		// test environment (no namespace file), causing an early return — the assertion
		// here guards against any panic on that path.
		co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
		assert.NoError(tt, err)
		assert.NoError(tt, co.(interface {
			EnableFSS(context.Context, string) error
		}).EnableFSS(context.Background(), common.ImprovedVolumeVisibility))
		fssEnabledSyncer := &metadataSyncInformer{coCommonInterface: co}

		assert.NotPanics(tt, func() { snapshotDeleted(snap, fssEnabledSyncer) })
	})
}
