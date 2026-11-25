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
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// attachedVolumeAttachmentCO returns a VolumeAttachment for every lookup, so
// ControllerPublishVolume stops at its already-attached check.
type attachedVolumeAttachmentCO struct {
	commonco.COCommonInterface
	va *storagev1.VolumeAttachment
}

func (f *attachedVolumeAttachmentCO) GetVolumeAttachment(context.Context, string, string) (
	*storagev1.VolumeAttachment, error) {
	return f.va, nil
}

func (f *attachedVolumeAttachmentCO) IsFSSEnabled(context.Context, string) bool { return false }

// The external-attacher saves a ControllerPublishVolume error in the VolumeAttachment's
// status.attachError. The already-attached error must name the VolumeAttachment, not embed
// the object, or the API server rejects the status update and the reason is never shown.
func TestControllerPublishVolumeAlreadyAttachedErrorNamesVolumeAttachment(t *testing.T) {
	original := commonco.ContainerOrchestratorUtility
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = original })
	pvName := "pvc-298fa34a-fb13-4259-9bb9-894249ac1048"
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "csi-73ba66cb66ced5b13fa056a5ba362bd4039fddbf3d2c733433d1193b24b50817",
			Annotations: map[string]string{"csi.alpha.kubernetes.io/node-id": "virtual-node-domain-c10"},
			Finalizers:  []string{"external-attacher/csi-vsphere-vmware-com"},
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.vsphere.vmware.com",
			NodeName: "virtual-node-domain-c10",
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
			AttachmentMetadata: map[string]string{
				"diskUUID": "6000c29b437d6ff95d60bbf683c4dd73",
				"vmUUID":   "500c60bd-623d-6989-8ac3-e81c9c9d4078",
			},
		},
	}
	commonco.ContainerOrchestratorUtility = &attachedVolumeAttachmentCO{va: va}

	c := &controller{}
	_, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "298fa34a-fb13-4259-9bb9-894249ac1048",
		NodeId:   "virtual-node-domain-c10",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	})
	assert.Error(t, err)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
	assert.Contains(t, st.Message(), `"`+va.Name+`" already has the attached status as true`)
	for _, leaked := range []string{"ObjectMeta", "AttachmentMetadata", va.Status.AttachmentMetadata["diskUUID"]} {
		assert.False(t, strings.Contains(st.Message(), leaked), "message must not embed the object: %q", leaked)
	}
}
