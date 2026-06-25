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

package csivolumeinfo_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	csivolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	cnsvolumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
)

// recoveryVolumeManager embeds the shared mock volume manager and overrides only
// the two-phase unregister query/ack methods exercised by recovery.
type recoveryVolumeManager struct {
	*unittestcommon.MockVolumeManager
	pendingRecords []cnsvolumes.PendingUnregisterRecord
	queryErr       error
	ackedIDs       []string
}

func (r *recoveryVolumeManager) QueryPendingUnregisters(ctx context.Context) (
	[]cnsvolumes.PendingUnregisterRecord, error) {
	if r.queryErr != nil {
		return nil, r.queryErr
	}
	return r.pendingRecords, nil
}

func (r *recoveryVolumeManager) AckUnregister(ctx context.Context, volumeID string) error {
	r.ackedIDs = append(r.ackedIDs, volumeID)
	return nil
}

func (r *recoveryVolumeManager) GetDiskFolderURL(ctx context.Context, datastorePath string) (string, error) {
	return "", nil
}

func newRecoveryMgr() *recoveryVolumeManager {
	return &recoveryVolumeManager{MockVolumeManager: &unittestcommon.MockVolumeManager{}}
}

func newRecoveryCVISvc(t *testing.T, cvis ...*csivolumeinfov1alpha1.CsiVolumeInfo) csivolumeinfo.CsiVolumeInfoService {
	t.Helper()
	s := newScheme(t)
	clientObjs := make([]client.Object, len(cvis))
	for i, c := range cvis {
		clientObjs[i] = c
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(clientObjs...).
		WithStatusSubresource(&csivolumeinfov1alpha1.CsiVolumeInfo{}).
		Build()
	return csivolumeinfo.NewCsiVolumeInfoService(fakeClient)
}

// buildSimpleCVI creates a minimal CsiVolumeInfo with the given volumeID and ownership.
func buildSimpleCVI(volumeID string,
	ownership csivolumeinfov1alpha1.OwnershipState) *csivolumeinfov1alpha1.CsiVolumeInfo {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      csivolumeinfov1alpha1.CVINamePrefix + volumeID,
			Namespace: csivolumeinfov1alpha1.CVINamespace,
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID: volumeID,
		},
	}
	cvi.Status.Ownership = ownership
	return cvi
}

func TestRecoverPendingUnregisters_NoPendingRecords(t *testing.T) {
	mgr := newRecoveryMgr()
	svc := newRecoveryCVISvc(t)
	require.NoError(t, csivolumeinfo.RecoverPendingUnregisters(context.Background(), mgr, svc))
	require.Empty(t, mgr.ackedIDs)
}

func TestRecoverPendingUnregisters_QueryError(t *testing.T) {
	mgr := newRecoveryMgr()
	mgr.queryErr = errors.New("CNS unavailable")
	svc := newRecoveryCVISvc(t)
	require.ErrorContains(t, csivolumeinfo.RecoverPendingUnregisters(context.Background(), mgr, svc),
		"QueryPendingUnregisters")
}

func TestRecoverPendingUnregisters_OrphanRecord_Acked(t *testing.T) {
	mgr := newRecoveryMgr()
	mgr.pendingRecords = []cnsvolumes.PendingUnregisterRecord{
		{VolumeID: "vol-orphan", BackingDiskPath: "/ds/orphan.vmdk", DiskUUID: "uuid-1"},
	}
	svc := newRecoveryCVISvc(t) // no CVI stored

	require.NoError(t, csivolumeinfo.RecoverPendingUnregisters(context.Background(), mgr, svc))
	require.Equal(t, []string{"vol-orphan"}, mgr.ackedIDs)
}

func TestRecoverPendingUnregisters_AlreadyVMManaged_DoubleACK(t *testing.T) {
	cvi := buildSimpleCVI("vol-done", csivolumeinfov1alpha1.OwnershipStateVMManaged)
	mgr := newRecoveryMgr()
	mgr.pendingRecords = []cnsvolumes.PendingUnregisterRecord{
		{VolumeID: "vol-done", BackingDiskPath: "/ds/done.vmdk", DiskUUID: "uuid-done"},
	}
	svc := newRecoveryCVISvc(t, cvi)

	require.NoError(t, csivolumeinfo.RecoverPendingUnregisters(context.Background(), mgr, svc))
	// Double-ACK: no spec/status patches needed.
	require.Equal(t, []string{"vol-done"}, mgr.ackedIDs)
}

func TestRecoverPendingUnregisters_CSIManaged_PatchesAndACKs(t *testing.T) {
	cvi := buildSimpleCVI("vol-csi", csivolumeinfov1alpha1.OwnershipStateCSIManaged)
	mgr := newRecoveryMgr()
	mgr.pendingRecords = []cnsvolumes.PendingUnregisterRecord{
		{VolumeID: "vol-csi", BackingDiskPath: "/ds/csi.vmdk", DiskUUID: "uuid-csi"},
	}
	svc := newRecoveryCVISvc(t, cvi)

	require.NoError(t, csivolumeinfo.RecoverPendingUnregisters(context.Background(), mgr, svc))
	require.Equal(t, []string{"vol-csi"}, mgr.ackedIDs)
}

func TestRecoverPendingUnregisters_BlankOwnership_PatchesAndACKs(t *testing.T) {
	cvi := buildSimpleCVI("vol-blank", "") // empty = treat as CSIManaged
	mgr := newRecoveryMgr()
	mgr.pendingRecords = []cnsvolumes.PendingUnregisterRecord{
		{VolumeID: "vol-blank", BackingDiskPath: "/ds/blank.vmdk", DiskUUID: "uuid-blank"},
	}
	svc := newRecoveryCVISvc(t, cvi)

	require.NoError(t, csivolumeinfo.RecoverPendingUnregisters(context.Background(), mgr, svc))
	require.Equal(t, []string{"vol-blank"}, mgr.ackedIDs)
}

func TestRecoverPendingUnregisters_OneBadRecordDoesNotAbortBatch(t *testing.T) {
	goodCVI := buildSimpleCVI("vol-good", csivolumeinfov1alpha1.OwnershipStateCSIManaged)
	mgr := newRecoveryMgr()
	mgr.pendingRecords = []cnsvolumes.PendingUnregisterRecord{
		{VolumeID: "vol-missing"}, // no CVI → will ACK orphan
		{VolumeID: "vol-good", BackingDiskPath: "/ds/good.vmdk", DiskUUID: "uuid-good"},
	}
	svc := newRecoveryCVISvc(t, goodCVI)

	// Both records should be processed; vol-missing will be acked as orphan.
	require.NoError(t, csivolumeinfo.RecoverPendingUnregisters(context.Background(), mgr, svc))
	require.Len(t, mgr.ackedIDs, 2)
}
