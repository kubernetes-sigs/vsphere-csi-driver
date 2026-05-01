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
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// withFVSEnabled toggles IsVsanFileVolumeServiceEnabled for the duration of the test
// (same variable is used on supervisor and guest; only one cluster flavor runs per process).
func withFVSEnabled(t *testing.T, enabled bool) {
	t.Helper()
	prev := IsVsanFileVolumeServiceEnabled
	IsVsanFileVolumeServiceEnabled = enabled
	t.Cleanup(func() { IsVsanFileVolumeServiceEnabled = prev })
}

func makeFVSPV(name, volumeID string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: volumeID,
				},
			},
		},
	}
}

func makePVWithSC(name, scName, volumeHandle string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PersistentVolumeSpec{
			StorageClassName: scName,
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: volumeHandle,
				},
			},
		},
	}
}

func makePVCWithSC(name, namespace, scName string) *v1.PersistentVolumeClaim {
	sc := scName
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1.PersistentVolumeClaimSpec{
			StorageClassName: &sc,
		},
	}
}

func TestIsFVSPersistentVolume(t *testing.T) {
	t.Run("nil pv", func(t *testing.T) {
		assert.False(t, isFVSPersistentVolume(nil))
	})
	t.Run("nil CSI source", func(t *testing.T) {
		pv := &v1.PersistentVolume{}
		assert.False(t, isFVSPersistentVolume(pv))
	})
	t.Run("non FVS handle", func(t *testing.T) {
		pv := makeFVSPV("pv-1", "block-1234")
		assert.False(t, isFVSPersistentVolume(pv))
	})
	t.Run("FVS handle", func(t *testing.T) {
		pv := makeFVSPV("pv-1", common.FVSVolumeIDPrefix+"tenant-ns:fv-foo")
		assert.True(t, isFVSPersistentVolume(pv))
	})
}

func TestIsFVSPersistentVolumeClaim(t *testing.T) {
	t.Run("nil pvc", func(t *testing.T) {
		assert.False(t, common.IsFVSPersistentVolumeClaim(nil))
	})
	t.Run("nil storage class", func(t *testing.T) {
		assert.False(t, common.IsFVSPersistentVolumeClaim(&v1.PersistentVolumeClaim{}))
	})
	t.Run("non FVS storage class", func(t *testing.T) {
		assert.False(t, common.IsFVSPersistentVolumeClaim(makePVCWithSC("a", "ns", "vsan-default")))
	})
	t.Run("FVS storage class - immediate binding", func(t *testing.T) {
		assert.True(t, common.IsFVSPersistentVolumeClaim(
			makePVCWithSC("a", "ns", common.StorageClassVsanFileServicePolicy)))
	})
	t.Run("FVS storage class - late binding", func(t *testing.T) {
		assert.True(t, common.IsFVSPersistentVolumeClaim(
			makePVCWithSC("a", "ns", common.StorageClassVsanFileServicePolicyLateBinding)))
	})
}

func TestShouldSkipFVSMetadataPushSupervisor(t *testing.T) {
	fvsPV := makeFVSPV("pv-fvs", common.FVSVolumeIDPrefix+"ns:fv-1")
	blockPV := makeFVSPV("pv-block", "block-1234")

	t.Run("FSS disabled - never skip", func(t *testing.T) {
		withFVSEnabled(t, false)
		assert.False(t, shouldSkipFVSMetadataPushSupervisor(fvsPV))
		assert.False(t, shouldSkipFVSMetadataPushSupervisor(blockPV))
	})
	t.Run("FSS enabled - skip only FVS PV", func(t *testing.T) {
		withFVSEnabled(t, true)
		assert.True(t, shouldSkipFVSMetadataPushSupervisor(fvsPV))
		assert.False(t, shouldSkipFVSMetadataPushSupervisor(blockPV))
	})
	t.Run("nil PV under enabled FSS", func(t *testing.T) {
		withFVSEnabled(t, true)
		assert.False(t, shouldSkipFVSMetadataPushSupervisor(nil))
	})
}

func TestShouldSkipFVSMetadataPushGuest(t *testing.T) {
	fvsPVC := makePVCWithSC("pvc-fvs", "ns", common.StorageClassVsanFileServicePolicy)
	fvsLBPVC := makePVCWithSC("pvc-fvs-lb", "ns", common.StorageClassVsanFileServicePolicyLateBinding)
	blockPVC := makePVCWithSC("pvc-block", "ns", "vsan-default")

	fvsPV := makePVWithSC("pv-fvs", common.StorageClassVsanFileServicePolicy, "supervisor-pvc-name")
	blockPV := makePVWithSC("pv-block", "vsan-default", "block-handle")

	t.Run("FSS disabled - never skip", func(t *testing.T) {
		withFVSEnabled(t, false)
		assert.False(t, shouldSkipFVSMetadataPushGuest(fvsPVC, fvsPV))
		assert.False(t, shouldSkipFVSMetadataPushGuest(blockPVC, blockPV))
	})
	t.Run("FSS enabled - skip when PVC has FVS storage class", func(t *testing.T) {
		withFVSEnabled(t, true)
		assert.True(t, shouldSkipFVSMetadataPushGuest(fvsPVC, nil))
		assert.True(t, shouldSkipFVSMetadataPushGuest(fvsLBPVC, nil))
	})
	t.Run("FSS enabled - skip when PV has FVS storage class and PVC is nil", func(t *testing.T) {
		withFVSEnabled(t, true)
		assert.True(t, shouldSkipFVSMetadataPushGuest(nil, fvsPV))
	})
	t.Run("FSS enabled - non FVS PVC/PV", func(t *testing.T) {
		withFVSEnabled(t, true)
		assert.False(t, shouldSkipFVSMetadataPushGuest(blockPVC, blockPV))
		assert.False(t, shouldSkipFVSMetadataPushGuest(nil, blockPV))
		assert.False(t, shouldSkipFVSMetadataPushGuest(blockPVC, nil))
	})
}

// TestSupervisorFullSyncFiltersFVSPVs validates the in-line filter applied in
// CsiFullSync that strips FVS-backed PVs before any CNS reconciliation runs.
// The test uses the same filtering primitive used by the implementation
// (isFVSPersistentVolume + IsVsanFileVolumeServiceEnabled) to assert behavior
// without standing up the full VC/CNS/informer stack.
func TestSupervisorFullSyncFiltersFVSPVs(t *testing.T) {
	fvsPV1 := makeFVSPV("pv-fvs-1", common.FVSVolumeIDPrefix+"ns-a:fv-1")
	fvsPV2 := makeFVSPV("pv-fvs-2", common.FVSVolumeIDPrefix+"ns-b:fv-2")
	blockPV := makeFVSPV("pv-block", "block-1234")

	pvs := []*v1.PersistentVolume{fvsPV1, blockPV, fvsPV2}

	t.Run("FSS disabled - keep all PVs", func(t *testing.T) {
		withFVSEnabled(t, false)
		filtered := filterPVsForFullSync(pvs)
		assert.Len(t, filtered, 3)
	})
	t.Run("FSS enabled - filter FVS PVs", func(t *testing.T) {
		withFVSEnabled(t, true)
		filtered := filterPVsForFullSync(pvs)
		assert.Len(t, filtered, 1)
		assert.Equal(t, "pv-block", filtered[0].Name)
	})
}

// filterPVsForFullSync mirrors the CsiFullSync filter so the behavior can be
// tested without invoking the full sync entrypoint (which requires a live
// VC/CNS connection). Any future change to the filter in CsiFullSync should
// keep this in lockstep.
func filterPVsForFullSync(pvList []*v1.PersistentVolume) []*v1.PersistentVolume {
	if !IsVsanFileVolumeServiceEnabled {
		return pvList
	}
	out := make([]*v1.PersistentVolume, 0, len(pvList))
	for _, pv := range pvList {
		if isFVSPersistentVolume(pv) {
			continue
		}
		out = append(out, pv)
	}
	return out
}
