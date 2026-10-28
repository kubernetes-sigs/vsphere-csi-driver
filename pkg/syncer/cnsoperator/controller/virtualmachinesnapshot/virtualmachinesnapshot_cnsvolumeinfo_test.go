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

package virtualmachinesnapshot

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

// eventLog records the order of CnsVolumeInfo reads and CNS syncs across goroutines.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *eventLog) index(event string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, e := range l.events {
		if e == event {
			return i
		}
	}
	return -1
}

// syncAndQueryManager syncs volumes and reports a fixed aggregated snapshot capacity per volume.
type syncAndQueryManager struct {
	volumes.Manager
	log        *eventLog
	capacities map[string]int64
}

func (m *syncAndQueryManager) SyncVolume(_ context.Context, specs []cnstypes.CnsSyncVolumeSpec) (string, error) {
	m.log.add("sync:" + specs[0].VolumeId.Id)
	return "", nil
}

func (m *syncAndQueryManager) QueryVolume(_ context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	result := &cnstypes.CnsQueryResult{}
	for _, id := range queryFilter.VolumeIds {
		result.Volumes = append(result.Volumes, cnstypes.CnsVolume{
			VolumeId:             id,
			BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{AggregatedSnapshotCapacityInMb: m.capacities[id.Id]},
		})
	}
	return result, nil
}

type vmSnapshotDetailsWrite struct {
	volumeID       string
	preReadVersion string
	capacityInMb   int64
}

// vmSnapshotVolumeInfoService serves cached CnsVolumeInfos and records snapshot detail writes.
type vmSnapshotVolumeInfoService struct {
	cnsvolumeinfo.VolumeInfoService
	log      *eventLog
	versions map[string]string
	writeErr error
	writes   []vmSnapshotDetailsWrite
	managers []volumes.Manager
}

func (s *vmSnapshotVolumeInfoService) GetVolumeInfoForVolumeID(_ context.Context,
	volumeID string) (*cnsvolumeinfov1alpha1.CNSVolumeInfo, error) {
	s.log.add("pre-read:" + volumeID)
	rv, ok := s.versions[volumeID]
	if !ok {
		return nil, errors.New("not in cache")
	}
	return &cnsvolumeinfov1alpha1.CNSVolumeInfo{ObjectMeta: metav1.ObjectMeta{ResourceVersion: rv}}, nil
}

func (s *vmSnapshotVolumeInfoService) UpdateVolumeInfoSnapshotDetails(_ context.Context, volumeID,
	preReadResourceVersion string, measured *volumes.CnsSnapshotInfo, volumeManager volumes.Manager) error {
	s.managers = append(s.managers, volumeManager)
	s.writes = append(s.writes, vmSnapshotDetailsWrite{volumeID, preReadResourceVersion,
		measured.AggregatedSnapshotCapacityInMb})
	return s.writeErr
}

func newVMWithVolumes(t *testing.T, volumeIDs ...string) *vmoperatortypes.VirtualMachine {
	t.Helper()
	orchestrator, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	require.NoError(t, err)
	fakeOrchestrator, ok := orchestrator.(*unittestcommon.FakeK8SOrchestrator)
	require.True(t, ok)
	saved := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = fakeOrchestrator
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = saved })

	vm := &vmoperatortypes.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: "vm-1", Namespace: testNamespace}}
	// A volume that is not a PVC is ignored.
	vm.Spec.Volumes = append(vm.Spec.Volumes, vmoperatortypes.VirtualMachineVolume{Name: "not-a-pvc"})
	var pvcs []*v1.PersistentVolumeClaim
	var pvs []*v1.PersistentVolume
	for _, volumeID := range volumeIDs {
		pvcs = append(pvcs, &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-" + volumeID, Namespace: testNamespace},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "pv-" + volumeID},
		})
		pvs = append(pvs, &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-" + volumeID},
			Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: volumeID},
			}},
		})
		vm.Spec.Volumes = append(vm.Spec.Volumes, vmoperatortypes.VirtualMachineVolume{
			Name: volumeID,
			VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
				PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc-" + volumeID},
				},
			},
		})
	}
	fakeOrchestrator.SetPVCs(pvcs)
	fakeOrchestrator.SetPVs(pvs)
	return vm
}

// The CnsVolumeInfo version must be read before the volume is synced, and each write must carry it.
func TestSyncVolumesAndUpdateCNSVolumeInfoPreReadsBeforeSync(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	vm := newVMWithVolumes(t, "vol-a", "vol-b")
	events := &eventLog{}
	service := &vmSnapshotVolumeInfoService{log: events, versions: map[string]string{"vol-a": "7"}}
	manager := &syncAndQueryManager{log: events, capacities: map[string]int64{"vol-a": 10, "vol-b": 20}}
	r := &ReconcileVirtualMachineSnapshot{volumeManager: manager, volumeInfoService: service}

	err := r.syncVolumesAndUpdateCNSVolumeInfo(ctx, logger.GetLogger(ctx), vm)

	require.NoError(t, err)
	for _, volumeID := range []string{"vol-a", "vol-b"} {
		preRead, synced := events.index("pre-read:"+volumeID), events.index("sync:"+volumeID)
		require.GreaterOrEqual(t, preRead, 0, volumeID)
		require.GreaterOrEqual(t, synced, 0, volumeID)
		assert.Less(t, preRead, synced, "CnsVolumeInfo of %s is read before it is synced", volumeID)
	}
	assert.ElementsMatch(t, []vmSnapshotDetailsWrite{
		{"vol-a", "7", 10},
		// Not in the cache: no pre-read version, so the write re-measures.
		{"vol-b", "", 20},
	}, service.writes)
	for _, m := range service.managers {
		assert.Same(t, manager, m, "CNS is re-measured through the reconciler's volume manager")
	}
}

func TestSyncVolumesAndUpdateCNSVolumeInfoWriteFailure(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	vm := newVMWithVolumes(t, "vol-a")
	events := &eventLog{}
	r := &ReconcileVirtualMachineSnapshot{
		volumeManager: &syncAndQueryManager{log: events, capacities: map[string]int64{"vol-a": 10}},
		volumeInfoService: &vmSnapshotVolumeInfoService{log: events, versions: map[string]string{"vol-a": "7"},
			writeErr: errors.New("conflicts exhausted")},
	}

	err := r.syncVolumesAndUpdateCNSVolumeInfo(ctx, logger.GetLogger(ctx), vm)

	require.Error(t, err)
}
