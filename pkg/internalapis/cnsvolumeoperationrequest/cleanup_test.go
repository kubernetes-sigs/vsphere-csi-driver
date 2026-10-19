package cnsvolumeoperationrequest

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
)

func TestListPVCOperationNamesPagination(t *testing.T) {
	const firstUID = "11111111-1111-1111-1111-111111111111"
	const secondUID = "22222222-2222-2222-2222-222222222222"

	store, ctx := setupTestEnvironment(t, false)
	pvcClient := clientsetfake.NewClientset()
	listCalls := 0
	pvcClient.PrependReactor("list", "persistentvolumeclaims",
		func(action clientgotesting.Action) (bool, runtime.Object, error) {
			listCalls++
			listOptions := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
			if listOptions.Limit != 5000 {
				t.Errorf("expected PVC list limit 5000, got %d", listOptions.Limit)
			}
			switch listCalls {
			case 1:
				if listOptions.Continue != "" {
					t.Errorf("expected empty first-page token, got %q", listOptions.Continue)
				}
				return true, &corev1.PersistentVolumeClaimList{
					ListMeta: metav1.ListMeta{Continue: "next-page"},
					Items: []corev1.PersistentVolumeClaim{{
						ObjectMeta: metav1.ObjectMeta{UID: types.UID(firstUID)},
					}},
				}, nil
			case 2:
				if listOptions.Continue != "next-page" {
					t.Errorf("expected next-page token, got %q", listOptions.Continue)
				}
				return true, &corev1.PersistentVolumeClaimList{
					Items: []corev1.PersistentVolumeClaim{{
						ObjectMeta: metav1.ObjectMeta{UID: types.UID(secondUID)},
					}},
				}, nil
			default:
				t.Fatalf("unexpected PVC list call %d", listCalls)
				return true, nil, nil
			}
		})
	store.pvcClient = pvcClient

	names, err := store.listPVCOperationNames(ctx)
	if err != nil {
		t.Fatalf("listPVCOperationNames failed: %v", err)
	}
	if listCalls != 2 {
		t.Fatalf("expected two PVC list calls, got %d", listCalls)
	}
	if len(names) != 2 {
		t.Errorf("expected two PVC operation names, got %d", len(names))
	}
	for _, name := range []string{"pvc-" + firstUID, "pvc-" + secondUID} {
		if _, exists := names[name]; !exists {
			t.Errorf("expected %q in PVC operation names", name)
		}
	}
}

func TestListPVCOperationNamesFailureAfterFirstPage(t *testing.T) {
	store, ctx := setupTestEnvironment(t, false)
	pvcClient := clientsetfake.NewClientset()
	listCalls := 0
	pvcClient.PrependReactor("list", "persistentvolumeclaims",
		func(clientgotesting.Action) (bool, runtime.Object, error) {
			listCalls++
			if listCalls == 1 {
				return true, &corev1.PersistentVolumeClaimList{
					ListMeta: metav1.ListMeta{Continue: "next-page"},
					Items: []corev1.PersistentVolumeClaim{{
						ObjectMeta: metav1.ObjectMeta{UID: types.UID("11111111-1111-1111-1111-111111111111")},
					}},
				}, nil
			}
			return true, nil, errors.New("PVC list failed")
		})
	store.pvcClient = pvcClient

	names, err := store.listPVCOperationNames(ctx)
	if err == nil {
		t.Fatal("expected an error when a later PVC page fails")
	}
	if names != nil {
		t.Fatalf("expected no partial PVC operation names, got %v", names)
	}
	if listCalls != 2 {
		t.Fatalf("expected two PVC list calls, got %d", listCalls)
	}
}
