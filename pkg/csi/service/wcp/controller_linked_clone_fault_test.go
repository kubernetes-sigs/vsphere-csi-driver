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

package wcp

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"github.com/vmware/govmomi/pbm"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cnstypes "github.com/vmware/govmomi/cns/types"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
)

const (
	notSupportedFaultType = "vim25:NotSupported"
	lcCryptoReason        = "Crypto ops are not supported when creating Linked Clone FCD."
	fullCloneVolumeID     = "full-clone-volume"
)

// createOutcome is what one CreateVolume call returns.
type createOutcome struct {
	info      *cnsvolume.CnsVolumeInfo
	faultType string
	err       error
}

// createCall records one CreateVolume call: whether the create spec carried a VolumeId, as it does with CSI
// transaction support, and whether it asked for a linked clone of the snapshot.
type createCall struct {
	carriedVolumeID bool
	linkedClone     bool
}

// scriptedCreateVolumeManager returns scripted outcomes from CreateVolume, one for each call; the last outcome repeats.
// It records the calls. Every other method is the wrapped manager's.
type scriptedCreateVolumeManager struct {
	cnsvolume.Manager
	outcomes []createOutcome
	calls    []createCall
}

func (m *scriptedCreateVolumeManager) CreateVolume(_ context.Context, spec *cnstypes.CnsVolumeCreateSpec,
	_ interface{}) (*cnsvolume.CnsVolumeInfo, string, error) {
	call := createCall{carriedVolumeID: spec.VolumeId != nil}
	if source, ok := spec.VolumeSource.(*cnstypes.CnsSnapshotVolumeSource); ok {
		call.linkedClone = source.LinkedClone
	}
	i := min(len(m.calls), len(m.outcomes)-1)
	m.calls = append(m.calls, call)
	return m.outcomes[i].info, m.outcomes[i].faultType, m.outcomes[i].err
}

// recordingVolumeInfoService records, for each CnsVolumeInfo that createBlockVolume creates, whether it is recorded as
// a linked clone. Every other method is unimplemented.
type recordingVolumeInfoService struct {
	cnsvolumeinfo.VolumeInfoService
	linkedClone []bool
}

func (s *recordingVolumeInfoService) CreateVolumeInfoWithPolicyInfo(_ context.Context, _, _, _, _, _ string,
	_ *resource.Quantity, isLinkedClone bool) error {
	s.linkedClone = append(s.linkedClone, isLinkedClone)
	return nil
}

// newLinkedCloneRequestBuilder returns a function that builds linked clone CreateVolumeRequests, from one snapshot of
// a new volume, which createBlockVolume accepts when the linked clone feature is on. Both the source volume and the
// snapshot exist in the simulator, as the linked clone path looks up the datastore of the snapshot's volume, and the
// request is placed in the simulator's cluster, whose hosts the full clone path looks up. They are created once,
// because every simulator task adds to the chance of tripping over the simulator's own races. fssOverrides adds to the
// features that are enabled.
func newLinkedCloneRequestBuilder(t *testing.T, ct *controllerTest,
	fssOverrides map[string]bool) func(volumeName string) *csi.CreateVolumeRequest {
	t.Helper()
	capabilities := []*csi.VolumeCapability{{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}}
	respCreate, err := ct.controller.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               testVolumeName + "-" + uuid.New().String(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 * common.GbInBytes},
		Parameters:         map[string]string{},
		VolumeCapabilities: capabilities,
	})
	if err != nil {
		t.Fatal(err)
	}
	srcVolID := respCreate.Volume.VolumeId
	t.Cleanup(func() {
		if _, err := ct.controller.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: srcVolID}); err != nil {
			t.Error(err)
		}
	})

	respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: srcVolID,
		Name:           "snapshot-" + uuid.New().String(),
		Parameters:     map[string]string{common.VolumeSnapshotNamespaceKey: "default"},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapID := respCreateSnapshot.Snapshot.SnapshotId
	t.Cleanup(func() {
		if _, err := ct.controller.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snapID}); err != nil {
			t.Error(err)
		}
	})

	pc, err := pbm.NewClient(ctx, ct.vcenter.Client.Client)
	if err != nil {
		t.Fatal(err)
	}
	profileID, err := pc.ProfileIDByName(ctx, "vSAN Default Storage Policy")
	if err != nil {
		t.Fatal(err)
	}

	originalCO := commonco.ContainerOrchestratorUtility
	originalMultiCluster := IsMultipleClustersPerVsphereZoneFSSEnabled
	t.Cleanup(func() {
		commonco.ContainerOrchestratorUtility = originalCO
		IsMultipleClustersPerVsphereZoneFSSEnabled = originalMultiCluster
	})
	IsMultipleClustersPerVsphereZoneFSSEnabled = true
	fss := map[string]bool{common.LinkedCloneSupport: true}
	for name, enabled := range fssOverrides {
		fss[name] = enabled
	}
	commonco.ContainerOrchestratorUtility = &fakeCOOverrides{
		COCommonInterface: originalCO,
		clusters:          clusterComputeResourceMoIds,
		fssOverrides:      fss,
	}

	return func(volumeName string) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:          volumeName,
			CapacityRange: &csi.CapacityRange{RequiredBytes: 1 * common.GbInBytes},
			Parameters: map[string]string{
				common.AttributePvcName:             "linked-clone-pvc",
				common.AttributePvcNamespace:        "default",
				common.AttributeStorageClassName:    "linked-clone-sc",
				common.AttributeIsLinkedCloneKey:    "true",
				common.AttributeStoragePolicyID:     profileID,
				common.AttributeStorageTopologyType: "zonal",
			},
			VolumeCapabilities: capabilities,
			AccessibilityRequirements: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{{Segments: map[string]string{v1.LabelTopologyZone: "zone-1"}}},
				Requisite: []*csi.Topology{{Segments: map[string]string{v1.LabelTopologyZone: "zone-1"}}},
			},
			VolumeContentSource: &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapID},
				},
			},
		}
	}
}

// TestCreateBlockVolumeLinkedCloneNotSupportedFault verifies that createBlockVolume creates a full clone when CNS
// rejects a linked clone as not supported, and only then; and that the retry of CreateVolume without a VolumeID, for
// a CNS that has no CSI transaction support, is kept.
func TestCreateBlockVolumeLinkedCloneNotSupportedFault(t *testing.T) {
	ct := getControllerTest(t)
	newRequest := newLinkedCloneRequestBuilder(t, ct, map[string]bool{common.FCDTransactionSupport: true})
	typedErr := errors.New("linked clone is not supported (" + csifault.VimFaultCnsLinkedCloneNotSupported + "): " +
		lcCryptoReason)
	genericErr := errors.New("failed to create volume with fault: generic")
	typed := createOutcome{faultType: csifault.VimFaultCnsLinkedCloneNotSupported, err: typedErr}
	notSupported := createOutcome{faultType: notSupportedFaultType, err: genericErr}
	otherFault := createOutcome{faultType: "vim.fault.CnsFault", err: genericErr}
	fullClone := createOutcome{info: &cnsvolume.CnsVolumeInfo{
		VolumeID: cnstypes.CnsVolumeId{Id: fullCloneVolumeID},
		Clusters: []vim25types.ManagedObjectReference{
			{Type: "ClusterComputeResource", Value: clusterComputeResourceMoIds[0]},
		},
	}}
	internalMsg := "failed to create volume. Error: " + genericErr.Error()

	tests := []struct {
		name string
		// stretchSupervisor sets the PodVMOnStretchSupervisor feature. A full clone of the snapshot needs it to be placed
		// in the clusters of the zone, as these requests are. It also enables the retry without a VolumeID, and records a
		// CnsVolumeInfo for the volume.
		stretchSupervisor bool
		// notLinkedClone removes the linked clone parameter from the request.
		notLinkedClone bool
		outcomes       []createOutcome
		wantCode       codes.Code
		wantFaultType  string
		wantMsg        string
		wantCalls      []createCall
		// wantLinkedCloneInfos records, for each CnsVolumeInfo created, whether it is recorded as a linked clone.
		wantLinkedCloneInfos []bool
	}{
		{
			name:                 "linked clone not supported: a full clone is created, and recorded as one",
			stretchSupervisor:    true,
			outcomes:             []createOutcome{typed, fullClone},
			wantCode:             codes.OK,
			wantCalls:            []createCall{{carriedVolumeID: true, linkedClone: true}, {carriedVolumeID: true}},
			wantLinkedCloneInfos: []bool{false},
		},
		{
			name:              "CNS without CSI transaction support: retried without a VolumeID, then a full clone",
			stretchSupervisor: true,
			outcomes:          []createOutcome{notSupported, typed, fullClone},
			wantCode:          codes.OK,
			wantCalls: []createCall{
				{carriedVolumeID: true, linkedClone: true},
				{linkedClone: true},
				{carriedVolumeID: true},
			},
			wantLinkedCloneInfos: []bool{false},
		},
		{
			name:              "the full clone fails too: its error is returned, without another attempt",
			stretchSupervisor: true,
			outcomes:          []createOutcome{typed, otherFault},
			wantCode:          codes.Internal,
			wantFaultType:     "vim.fault.CnsFault",
			wantMsg:           internalMsg,
			wantCalls:         []createCall{{carriedVolumeID: true, linkedClone: true}, {carriedVolumeID: true}},
		},
		{
			name:          "generic NotSupported: no full clone is created",
			outcomes:      []createOutcome{notSupported},
			wantCode:      codes.Internal,
			wantFaultType: notSupportedFaultType,
			wantMsg:       internalMsg,
			wantCalls:     []createCall{{carriedVolumeID: true, linkedClone: true}},
		},
		{
			name:              "generic NotSupported, retried without a VolumeID: no full clone is created",
			stretchSupervisor: true,
			outcomes:          []createOutcome{notSupported},
			wantCode:          codes.Internal,
			wantFaultType:     notSupportedFaultType,
			wantMsg:           internalMsg,
			wantCalls:         []createCall{{carriedVolumeID: true, linkedClone: true}, {linkedClone: true}},
		},
		{
			name:          "another fault: no full clone is created",
			outcomes:      []createOutcome{otherFault},
			wantCode:      codes.Internal,
			wantFaultType: "vim.fault.CnsFault",
			wantMsg:       internalMsg,
			wantCalls:     []createCall{{carriedVolumeID: true, linkedClone: true}},
		},
		{
			name:              "not a linked clone request: nothing to fall back from",
			stretchSupervisor: true,
			notLinkedClone:    true,
			outcomes:          []createOutcome{typed},
			wantCode:          codes.Internal,
			wantFaultType:     csifault.VimFaultCnsLinkedCloneNotSupported,
			wantMsg:           "failed to create volume. Error: " + typedErr.Error(),
			wantCalls:         []createCall{{carriedVolumeID: true}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A name that carries a UID makes the create spec carry a VolumeId, as with CSI transaction support.
			req := newRequest("pvc-" + uuid.New().String())
			if tt.notLinkedClone {
				delete(req.Parameters, common.AttributeIsLinkedCloneKey)
			}
			original := proto.Clone(req)

			originalStretch := isPodVMOnStretchSupervisorFSSEnabled
			originalManager := ct.controller.manager.VolumeManager
			originalVolumeInfoService := volumeInfoService
			t.Cleanup(func() {
				isPodVMOnStretchSupervisorFSSEnabled = originalStretch
				ct.controller.manager.VolumeManager = originalManager
				volumeInfoService = originalVolumeInfoService
			})
			isPodVMOnStretchSupervisorFSSEnabled = tt.stretchSupervisor
			scripted := &scriptedCreateVolumeManager{Manager: originalManager, outcomes: tt.outcomes}
			ct.controller.manager.VolumeManager = scripted
			infos := &recordingVolumeInfoService{}
			volumeInfoService = infos

			resp, faultType, err := ct.controller.createBlockVolume(ctx, req, true, clusterComputeResourceMoIds)

			if status.Code(err) != tt.wantCode {
				t.Fatalf("expected code %v, got %v: %v", tt.wantCode, status.Code(err), err)
			}
			if tt.wantCode == codes.OK {
				if resp.Volume.VolumeId != fullCloneVolumeID {
					t.Errorf("expected volume %q, got %q", fullCloneVolumeID, resp.Volume.VolumeId)
				}
				if _, ok := resp.Volume.VolumeContext[common.VolumeContextAttributeLinkedCloneVolumeSnapshotSourceUID]; ok {
					t.Errorf("a full clone must not carry the linked clone attribute: %v", resp.Volume.VolumeContext)
				}
			} else if st, _ := status.FromError(err); st.Message() != tt.wantMsg {
				t.Errorf("expected message %q, got %q", tt.wantMsg, st.Message())
			}
			if faultType != tt.wantFaultType {
				t.Errorf("expected fault type %q, got %q", tt.wantFaultType, faultType)
			}
			if !reflect.DeepEqual(scripted.calls, tt.wantCalls) {
				t.Errorf("CreateVolume calls = %+v, want %+v", scripted.calls, tt.wantCalls)
			}
			if !reflect.DeepEqual(infos.linkedClone, tt.wantLinkedCloneInfos) {
				t.Errorf("CnsVolumeInfo recorded as linked clone = %v, want %v", infos.linkedClone, tt.wantLinkedCloneInfos)
			}
			if !proto.Equal(req, original) {
				t.Errorf("the fallback changed the caller's request: %+v, want %+v", req, original)
			}
		})
	}
}

func TestFullCloneRequest(t *testing.T) {
	tests := []struct {
		name       string
		parameters map[string]string
		want       map[string]string
	}{
		{
			name: "removes the linked clone parameter",
			parameters: map[string]string{
				common.AttributeIsLinkedCloneKey: "true", common.AttributeStoragePolicyID: "policy",
			},
			want: map[string]string{common.AttributeStoragePolicyID: "policy"},
		},
		{
			name:       "matches the name case-insensitively",
			parameters: map[string]string{"CSI.vSphere.K8S.io/Linked-Clone": "true", "other": "value"},
			want:       map[string]string{"other": "value"},
		},
		{
			name:       "no linked clone parameter",
			parameters: map[string]string{common.AttributeStoragePolicyID: "policy"},
			want:       map[string]string{common.AttributeStoragePolicyID: "policy"},
		},
		{name: "no parameters"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &csi.CreateVolumeRequest{
				Name:       "pvc-1",
				Parameters: tt.parameters,
				VolumeContentSource: &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Snapshot{
						Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "volume+snapshot"},
					},
				},
			}
			original := proto.Clone(req)
			want := proto.Clone(req).(*csi.CreateVolumeRequest)
			want.Parameters = tt.want

			got := fullCloneRequest(req)

			if !proto.Equal(got, want) {
				t.Errorf("fullCloneRequest() = %+v, want %+v", got, want)
			}
			if !proto.Equal(req, original) {
				t.Errorf("fullCloneRequest changed its argument: %+v, want %+v", req, original)
			}
		})
	}
}
