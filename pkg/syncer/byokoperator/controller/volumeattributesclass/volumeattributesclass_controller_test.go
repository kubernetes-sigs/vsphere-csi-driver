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

package volumeattributesclass

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	csicommon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// setVACSupportForTest points commonco.ContainerOrchestratorUtility at a fake orchestrator
// with VMPVCStoragePolicyMutability set to the given state, restoring the previous value on
// cleanup so other tests aren't affected.
func setVACSupportForTest(t *testing.T, enabled bool) {
	t.Helper()
	previous := commonco.ContainerOrchestratorUtility
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = previous })

	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(csicommon.Kubernetes)
	require.NoError(t, err)
	if enabled {
		require.NoError(t, fakeCO.EnableFSS(context.Background(), csicommon.VMPVCStoragePolicyMutability))
	}
	commonco.ContainerOrchestratorUtility = fakeCO
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, storagev1.AddToScheme(scheme))
	return scheme
}

func newTestReconciler(t *testing.T, objs ...client.Object) *reconciler {
	t.Helper()
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
	return &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
		// vcClient/cryptoClient intentionally left nil: the cases exercised here must not
		// dereference them (see TestReconcileNormal_NoPolicyID and
		// TestReconcile_BeingDeletedSkipsReconcileNormal).
	}
}

// TestReconcileNormal_NoPolicyID verifies that a VAC with no storagePolicyID parameter is a
// silent no-op and, critically, never touches the (possibly nil in this test) vcClient/
// cryptoClient — reconcileNormal must bail out before attempting any PBM/CNS I/O.
func TestReconcileNormal_NoPolicyID(t *testing.T) {
	r := newTestReconciler(t)
	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: "no-policy-vac", UID: types.UID("uid-1")},
	}

	err := r.reconcileNormal(context.Background(), vac)

	assert.NoError(t, err)
}

// TestReconcileNormal_VACSupportDisabled verifies that a VAC with a policyID set is still a
// no-op — without touching the (nil in this test) vcClient/cryptoClient — when the
// VMPVCStoragePolicyMutability capability is disabled, since no PVC could ever reach that
// policy via ModifyVolume in that case.
func TestReconcileNormal_VACSupportDisabled(t *testing.T) {
	setVACSupportForTest(t, false)
	r := newTestReconciler(t)
	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: "encrypted-vac", UID: types.UID("uid-3")},
		Parameters: map[string]string{"storagePolicyID": "policy-1"},
	}

	err := r.reconcileNormal(context.Background(), vac)

	assert.NoError(t, err)
}

// TestReconcile_ObjectNotFound verifies Reconcile tolerates a VAC that no longer exists
// (e.g. deleted between enqueue and processing) by ignoring the NotFound error.
func TestReconcile_ObjectNotFound(t *testing.T) {
	r := newTestReconciler(t)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "does-not-exist"},
	})

	assert.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

// TestReconcile_BeingDeletedSkipsReconcileNormal verifies that a VAC with a DeletionTimestamp
// set short-circuits before reconcileNormal runs, even though it has a policyID set (which
// would otherwise require a live vcClient/cryptoClient — both nil here).
func TestReconcile_BeingDeletedSkipsReconcileNormal(t *testing.T) {
	now := metav1.NewTime(time.Now())
	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "deleting-vac",
			UID:               types.UID("uid-2"),
			DeletionTimestamp: &now,
			Finalizers:        []string{"kubernetes.io/vac-protection"},
		},
		Parameters: map[string]string{"storagePolicyID": "policy-1"},
	}
	r := newTestReconciler(t, vac)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "deleting-vac"},
	})

	assert.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}
