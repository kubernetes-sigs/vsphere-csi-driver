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

package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimefake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// createTestPVC creates a test PVC with optional finalizer
func createTestPVC(name, namespace string, withFinalizer bool) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	if withFinalizer {
		pvc.Finalizers = []string{cnsoperatortypes.CNSPvcFinalizer}
	}

	return pvc
}

// createTestBatchAttachCR creates a test CnsNodeVMBatchAttachment CR
func createTestBatchAttachCR(name, namespace string, pvcNames []string) *v1alpha1.CnsNodeVMBatchAttachment {
	var volumes []v1alpha1.VolumeSpec
	for _, pvcName := range pvcNames {
		volumes = append(volumes, v1alpha1.VolumeSpec{
			Name: pvcName,
			PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
				ClaimName: pvcName,
			},
		})
	}

	return &v1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
			Volumes: volumes,
		},
	}
}

func TestCleanupPVCs(t *testing.T) {
	// Save original function variables
	coUtilOrig := commonco.ContainerOrchestratorUtility
	newClientForGroupOrig := newClientForGroup
	newForConfigOrig := newForConfig
	defer func() {
		commonco.ContainerOrchestratorUtility = coUtilOrig
		newClientForGroup = newClientForGroupOrig
		newForConfig = newForConfigOrig
	}()

	setup := func(t *testing.T, pvcs []*corev1.PersistentVolumeClaim,
		batchAttachCRs []client.Object) *k8sfake.Clientset {
		t.Helper()

		// Setup fake k8s client for PVCs
		var k8sObjects []runtime.Object
		for _, pvc := range pvcs {
			k8sObjects = append(k8sObjects, pvc)
		}
		fakeK8sClient := k8sfake.NewSimpleClientset(k8sObjects...)

		// Setup fake controller-runtime client for batch attach CRs
		scheme := runtime.NewScheme()
		_ = cnsoperatorapis.AddToScheme(scheme)
		_ = corev1.AddToScheme(scheme)
		fakeCtrlClient := runtimefake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(batchAttachCRs...).
			Build()

		// Setup fake orchestrator
		fakeOrch := &unittestcommon.FakeK8SOrchestrator{}
		fakeOrch.SetPVCs(pvcs)
		commonco.ContainerOrchestratorUtility = fakeOrch

		// Override function variables to return our fake clients
		newClientForGroup = func(ctx context.Context, config *rest.Config, groupName string) (client.Client, error) {
			return fakeCtrlClient, nil
		}
		newForConfig = func(config *rest.Config) (kubernetes.Interface, error) {
			// Return the fake clientset as kubernetes.Interface
			return fakeK8sClient, nil
		}

		return fakeK8sClient
	}

	// Helper function to assert the complete state of all PVCs.
	// This ensures PVCs are not accidentally deleted or modified.
	assertPVCState := func(t *testing.T, k8sClient *k8sfake.Clientset,
		expectedPVCs map[string]map[string]bool) {
		t.Helper()

		// expectedPVCs format: map[namespace]map[pvcName]hasFinalizer
		// This allows us to verify the complete state of all PVCs

		// Get all PVCs from the fake k8s client
		pvcList, err := k8sClient.CoreV1().PersistentVolumeClaims("").List(
			context.Background(), metav1.ListOptions{})
		assert.NoError(t, err)

		// Build actual state
		actualPVCs := make(map[string]map[string]bool)
		for _, pvc := range pvcList.Items {
			if _, ok := actualPVCs[pvc.Namespace]; !ok {
				actualPVCs[pvc.Namespace] = make(map[string]bool)
			}
			actualPVCs[pvc.Namespace][pvc.Name] = controllerutil.ContainsFinalizer(&pvc, cnsoperatortypes.CNSPvcFinalizer)
		}

		// Compare expected vs actual
		for ns, expectedPVCsInNS := range expectedPVCs {
			actualPVCsInNS, ok := actualPVCs[ns]
			if !ok {
				t.Errorf("Namespace %s: expected %d PVCs but found none", ns, len(expectedPVCsInNS))
				continue
			}

			for pvcName, expectedHasFinalizer := range expectedPVCsInNS {
				actualHasFinalizer, exists := actualPVCsInNS[pvcName]
				if !exists {
					t.Errorf("Namespace %s: PVC %s expected but not found", ns, pvcName)
					continue
				}

				assert.Equal(t, expectedHasFinalizer, actualHasFinalizer,
					"Namespace %s: PVC %s finalizer state mismatch", ns, pvcName)
			}

			// Check for unexpected PVCs
			for pvcName := range actualPVCsInNS {
				if _, expected := expectedPVCsInNS[pvcName]; !expected {
					t.Errorf("Namespace %s: PVC %s found but not expected", ns, pvcName)
				}
			}
		}

		// Check for unexpected namespaces
		for ns := range actualPVCs {
			if _, expected := expectedPVCs[ns]; !expected {
				t.Errorf("Namespace %s: found but not expected", ns)
			}
		}
	}

	t.Run("WhenNoPVCsExist", func(tt *testing.T) {
		// Setup
		k8sClient := setup(tt, nil, nil)

		// Execute
		cleanupPVCs(context.Background(), rest.Config{})

		// Assert - no PVCs should exist
		pvcList, err := k8sClient.CoreV1().PersistentVolumeClaims("").List(
			context.Background(), metav1.ListOptions{})
		assert.NoError(tt, err)
		assert.Empty(tt, pvcList.Items, "No PVCs should exist")
	})

	t.Run("WhenPVCsWithoutFinalizerExist", func(tt *testing.T) {
		// Setup
		pvcs := []*corev1.PersistentVolumeClaim{
			createTestPVC("pvc-1", "ns-1", false),
			createTestPVC("pvc-2", "ns-1", false),
			createTestPVC("pvc-3", "ns-2", false),
		}
		k8sClient := setup(tt, pvcs, nil)

		// Execute
		cleanupPVCs(context.Background(), rest.Config{})

		// Assert - all PVCs should remain without finalizers
		assertPVCState(tt, k8sClient, map[string]map[string]bool{
			"ns-1": {
				"pvc-1": false,
				"pvc-2": false,
			},
			"ns-2": {
				"pvc-3": false,
			},
		})
	})

	t.Run("WhenPVCsWithFinalizerButNoBatchAttachCRs", func(tt *testing.T) {
		// Setup
		pvcs := []*corev1.PersistentVolumeClaim{
			createTestPVC("pvc-1", "ns-1", true),
			createTestPVC("pvc-2", "ns-1", true),
			createTestPVC("pvc-3", "ns-2", true),
		}
		k8sClient := setup(tt, pvcs, nil)

		// Execute
		cleanupPVCs(context.Background(), rest.Config{})

		// Assert - all PVCs should have finalizers removed
		assertPVCState(tt, k8sClient, map[string]map[string]bool{
			"ns-1": {
				"pvc-1": false,
				"pvc-2": false,
			},
			"ns-2": {
				"pvc-3": false,
			},
		})
	})

	t.Run("WhenPVCsWithFinalizerAndMatchingBatchAttachCRs", func(tt *testing.T) {
		// Setup - PVCs with finalizers that are referenced in batch attach CRs
		pvcs := []*corev1.PersistentVolumeClaim{
			createTestPVC("pvc-1", "ns-1", true),
			createTestPVC("pvc-2", "ns-1", true),
			createTestPVC("pvc-3", "ns-2", true),
		}
		batchAttachCRs := []client.Object{
			createTestBatchAttachCR("batch-1", "ns-1", []string{"pvc-1", "pvc-2"}),
			createTestBatchAttachCR("batch-2", "ns-2", []string{"pvc-3"}),
		}
		k8sClient := setup(tt, pvcs, batchAttachCRs)

		// Execute
		cleanupPVCs(context.Background(), rest.Config{})

		// Assert - all PVCs should keep their finalizers (referenced in CRs)
		assertPVCState(tt, k8sClient, map[string]map[string]bool{
			"ns-1": {
				"pvc-1": true,
				"pvc-2": true,
			},
			"ns-2": {
				"pvc-3": true,
			},
		})
	})

	t.Run("WhenMixedPVCsWithAndWithoutFinalizers", func(tt *testing.T) {
		// Setup
		pvcs := []*corev1.PersistentVolumeClaim{
			createTestPVC("pvc-with-finalizer-1", "ns-1", true),
			createTestPVC("pvc-without-finalizer-1", "ns-1", false),
			createTestPVC("pvc-with-finalizer-2", "ns-2", true),
			createTestPVC("pvc-without-finalizer-2", "ns-2", false),
		}
		k8sClient := setup(tt, pvcs, nil)

		// Execute
		cleanupPVCs(context.Background(), rest.Config{})

		// Assert - PVCs with finalizers should have them removed, others remain unchanged
		assertPVCState(tt, k8sClient, map[string]map[string]bool{
			"ns-1": {
				"pvc-with-finalizer-1":    false,
				"pvc-without-finalizer-1": false,
			},
			"ns-2": {
				"pvc-with-finalizer-2":    false,
				"pvc-without-finalizer-2": false,
			},
		})
	})

	t.Run("WhenMultipleNamespacesWithPVCs", func(tt *testing.T) {
		// Setup
		pvcs := []*corev1.PersistentVolumeClaim{
			createTestPVC("pvc-1", "ns-1", true),
			createTestPVC("pvc-2", "ns-1", true),
			createTestPVC("pvc-3", "ns-2", true),
			createTestPVC("pvc-4", "ns-3", true),
			createTestPVC("pvc-5", "ns-3", true),
		}
		k8sClient := setup(tt, pvcs, nil)

		// Execute
		cleanupPVCs(context.Background(), rest.Config{})

		// Assert - all PVCs should have finalizers removed
		assertPVCState(tt, k8sClient, map[string]map[string]bool{
			"ns-1": {
				"pvc-1": false,
				"pvc-2": false,
			},
			"ns-2": {
				"pvc-3": false,
			},
			"ns-3": {
				"pvc-4": false,
				"pvc-5": false,
			},
		})
	})

	t.Run("WhenPartialPVCsReferencedInBatchAttachCRs", func(tt *testing.T) {
		// Setup - Some PVCs are referenced in batch attach CRs, others are not
		pvcs := []*corev1.PersistentVolumeClaim{
			createTestPVC("pvc-1", "ns-1", true),
			createTestPVC("pvc-2", "ns-1", true),
			createTestPVC("pvc-3", "ns-1", true),
			createTestPVC("pvc-4", "ns-2", true),
			createTestPVC("pvc-5", "ns-2", true),
		}
		// Only reference pvc-1 and pvc-4 in batch attach CRs
		batchAttachCRs := []client.Object{
			createTestBatchAttachCR("batch-1", "ns-1", []string{"pvc-1"}),
			createTestBatchAttachCR("batch-2", "ns-2", []string{"pvc-4"}),
		}
		k8sClient := setup(tt, pvcs, batchAttachCRs)

		// Execute
		cleanupPVCs(context.Background(), rest.Config{})

		// Assert - only pvc-1 and pvc-4 should keep finalizers, others should have them removed
		assertPVCState(tt, k8sClient, map[string]map[string]bool{
			"ns-1": {
				"pvc-1": true,  // Referenced in CR, keeps finalizer
				"pvc-2": false, // Not referenced, finalizer removed
				"pvc-3": false, // Not referenced, finalizer removed
			},
			"ns-2": {
				"pvc-4": true,  // Referenced in CR, keeps finalizer
				"pvc-5": false, // Not referenced, finalizer removed
			},
		})
	})
}
