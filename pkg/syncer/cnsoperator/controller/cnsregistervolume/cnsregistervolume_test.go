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

package cnsregistervolume

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnsvolumeattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

func TestValidateCnsRegisterVolumeSpecWithDiskUrlPath(t *testing.T) {
	instance := &cnsvolumeattachmentv1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "register-vol",
			Namespace: "tets-ns",
		},
		Spec: cnsvolumeattachmentv1alpha1.CnsRegisterVolumeSpec{
			PvcName:     "pvc-1",
			DiskURLPath: "som-url",
			AccessMode:  v1.ReadWriteMany,
			VolumeMode:  v1.PersistentVolumeFilesystem,
		},
	}

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	err := validateCnsRegisterVolumeSpec(context.TODO(), instance)
	assert.Error(t, err)
	assert.Equal(t, "DiskURLPath cannot be used with accessMode: ReadWriteMany and volumeMode: Filesystem", err.Error())
}

func TestValidateCnsRegisterVolumeSpecWithVolumeIdAndNoAccessMode(t *testing.T) {
	instance := &cnsvolumeattachmentv1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "register-vol",
			Namespace: "tets-ns",
		},
		Spec: cnsvolumeattachmentv1alpha1.CnsRegisterVolumeSpec{
			PvcName:    "pvc-1",
			VolumeID:   "123456",
			VolumeMode: v1.PersistentVolumeFilesystem,
		},
	}

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	err := validateCnsRegisterVolumeSpec(context.TODO(), instance)
	assert.Error(t, err)
	assert.Equal(t, "AccessMode cannot be empty when volumeID is specified", err.Error())
}

func TestValidateCnsRegisterVolumeSpecWithVolumeIdAndAccessMode(t *testing.T) {
	instance := &cnsvolumeattachmentv1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "register-vol",
			Namespace: "tets-ns",
		},
		Spec: cnsvolumeattachmentv1alpha1.CnsRegisterVolumeSpec{
			PvcName:    "pvc-1",
			VolumeID:   "123456",
			AccessMode: v1.ReadWriteMany,
			VolumeMode: v1.PersistentVolumeFilesystem,
		},
	}

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	err := validateCnsRegisterVolumeSpec(context.TODO(), instance)
	assert.NoError(t, err)
}

func TestIsBlockVolumeRegisterRequestWithSharedBlockVolume(t *testing.T) {
	instance := &cnsvolumeattachmentv1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "register-vol",
			Namespace: "tets-ns",
		},
		Spec: cnsvolumeattachmentv1alpha1.CnsRegisterVolumeSpec{
			PvcName:    "pvc-1",
			VolumeID:   "123456",
			VolumeMode: v1.PersistentVolumeBlock,
			AccessMode: v1.ReadWriteMany,
		},
	}

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	isBlockVolume := isBlockVolumeRegisterRequest(t.Context(), instance)
	assert.Equal(t, true, isBlockVolume)
}

func TestIsBlockVolumeRegisterRequestWithFileVolume(t *testing.T) {
	instance := &cnsvolumeattachmentv1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "register-vol",
			Namespace: "tets-ns",
		},
		Spec: cnsvolumeattachmentv1alpha1.CnsRegisterVolumeSpec{
			PvcName:    "pvc-1",
			VolumeID:   "123456",
			AccessMode: v1.ReadWriteMany,
			VolumeMode: v1.PersistentVolumeFilesystem,
		},
	}

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	isBlockVolume := isBlockVolumeRegisterRequest(t.Context(), instance)
	assert.Equal(t, false, isBlockVolume)
}

func TestGetPersistentVolumeSpecWhenVolumeModeIsEmpty(t *testing.T) {
	var (
		volumeName = "vol-1"
		volumeID   = "123456"
		capacity   = 256
		accessMode = v1.ReadWriteMany
		scName     = "testsc"
	)

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	pv := getPersistentVolumeSpec(context.TODO(), volumeName, volumeID, int64(capacity), accessMode, "", scName, nil)
	assert.Equal(t, v1.PersistentVolumeBlock, *pv.Spec.VolumeMode)
}

func TestGetPersistentVolumeSpecWithVolumeMode(t *testing.T) {
	var (
		volumeName = "vol-1"
		volumeID   = "123456"
		capacity   = 256
		accessMode = v1.ReadWriteMany
		scName     = "testsc"
		volumeMode = v1.PersistentVolumeBlock
	)

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	pv := getPersistentVolumeSpec(context.TODO(), volumeName, volumeID, int64(capacity), accessMode, volumeMode, scName, nil)
	assert.Equal(t, volumeMode, *pv.Spec.VolumeMode)
}
