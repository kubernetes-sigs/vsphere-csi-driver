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
