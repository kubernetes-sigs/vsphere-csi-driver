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

package admissionhandler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	byokv1 "github.com/vmware-tanzu/vm-operator/external/byok/api/v1alpha1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/k8sorchestrator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
)

// MockCOCommonInterface is a mock implementation of COCommonInterface
type MockCOCommonInterface struct {
	mock.Mock
}

func (m *MockCOCommonInterface) GetPVCNamespacedNameByUID(uid string) (apitypes.NamespacedName, bool) {
	//TODO implement me
	panic("implement me")
}

func (m *MockCOCommonInterface) GetVolumeSnapshotPVCSource(ctx context.Context, volumeSnapshotNamespace,
	volumeSnapshotName string) (*corev1.PersistentVolumeClaim, error) {
	args := m.Called(ctx, volumeSnapshotNamespace, volumeSnapshotName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*corev1.PersistentVolumeClaim), args.Error(1)
}

// Implement other required methods of COCommonInterface with no-op implementations for testing
func (m *MockCOCommonInterface) IsFSSEnabled(ctx context.Context, featureName string) bool {
	args := m.Called(ctx, featureName)
	return args.Bool(0)
}

func (m *MockCOCommonInterface) IsCNSCSIFSSEnabled(ctx context.Context, featureName string) bool {
	args := m.Called(ctx, featureName)
	return args.Bool(0)
}

func (m *MockCOCommonInterface) IsPVCSIFSSEnabled(ctx context.Context, featureName string) bool {
	args := m.Called(ctx, featureName)
	return args.Bool(0)
}

func (m *MockCOCommonInterface) EnableFSS(ctx context.Context, featureName string) error {
	args := m.Called(ctx, featureName)
	return args.Error(0)
}

func (m *MockCOCommonInterface) DisableFSS(ctx context.Context, featureName string) error {
	args := m.Called(ctx, featureName)
	return args.Error(0)
}

func (m *MockCOCommonInterface) IsFakeAttachAllowed(ctx context.Context, volumeID string,
	volumeManager volume.Manager) (bool, error) {
	args := m.Called(ctx, volumeID, volumeManager)
	return args.Bool(0), args.Error(1)
}

func (m *MockCOCommonInterface) MarkFakeAttached(ctx context.Context, volumeID string) error {
	args := m.Called(ctx, volumeID)
	return args.Error(0)
}

func (m *MockCOCommonInterface) ClearFakeAttached(ctx context.Context, volumeID string) error {
	args := m.Called(ctx, volumeID)
	return args.Error(0)
}

func (m *MockCOCommonInterface) InitTopologyServiceInController(
	ctx context.Context) (types.ControllerTopologyService, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(types.ControllerTopologyService), args.Error(1)
}

func (m *MockCOCommonInterface) InitTopologyServiceInNode(ctx context.Context) (types.NodeTopologyService, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(types.NodeTopologyService), args.Error(1)
}

func (m *MockCOCommonInterface) GetNodesForVolumes(ctx context.Context, volumeIds []string) map[string][]string {
	args := m.Called(ctx, volumeIds)
	return args.Get(0).(map[string][]string)
}

func (m *MockCOCommonInterface) GetNodeIDtoNameMap(ctx context.Context) map[string]string {
	args := m.Called(ctx)
	return args.Get(0).(map[string]string)
}

func (m *MockCOCommonInterface) GetFakeAttachedVolumes(ctx context.Context, volumeIDs []string) map[string]bool {
	args := m.Called(ctx, volumeIDs)
	return args.Get(0).(map[string]bool)
}

func (m *MockCOCommonInterface) GetVolumeAttachment(ctx context.Context, volumeId,
	nodeName string) (*storagev1.VolumeAttachment, error) {
	args := m.Called(ctx, volumeId, nodeName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storagev1.VolumeAttachment), args.Error(1)
}

func (m *MockCOCommonInterface) GetAllVolumes() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockCOCommonInterface) GetAllK8sVolumes() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockCOCommonInterface) AnnotateVolumeSnapshot(ctx context.Context, volumeSnapshotName,
	volumeSnapshotNamespace string, annotations map[string]string) (bool, error) {
	args := m.Called(ctx, volumeSnapshotName, volumeSnapshotNamespace, annotations)
	return args.Bool(0), args.Error(1)
}

func (m *MockCOCommonInterface) GetConfigMap(ctx context.Context, name, namespace string) (map[string]string, error) {
	args := m.Called(ctx, name, namespace)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}

func (m *MockCOCommonInterface) CreateConfigMap(ctx context.Context, name, namespace string,
	data map[string]string, isImmutable bool) error {
	args := m.Called(ctx, name, namespace, data, isImmutable)
	return args.Error(0)
}

func (m *MockCOCommonInterface) GetCSINodeTopologyInstancesList() []interface{} {
	args := m.Called()
	return args.Get(0).([]interface{})
}

func (m *MockCOCommonInterface) GetCSINodeTopologyInstanceByName(nodeName string) (interface{}, bool, error) {
	args := m.Called(nodeName)
	return args.Get(0), args.Bool(1), args.Error(2)
}

func (m *MockCOCommonInterface) GetPVNameFromCSIVolumeID(volumeID string) (string, bool) {
	args := m.Called(volumeID)
	return args.String(0), args.Bool(1)
}

func (m *MockCOCommonInterface) GetPVCNameFromCSIVolumeID(volumeID string) (string, string, bool) {
	args := m.Called(volumeID)
	return args.String(0), args.String(1), args.Bool(2)
}

func (m *MockCOCommonInterface) GetVolumeIDFromPVCName(pvcName string) (string, bool) {
	args := m.Called(pvcName)
	return args.String(0), args.Bool(1)
}

func (m *MockCOCommonInterface) InitializeCSINodes(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockCOCommonInterface) StartZonesInformer(ctx context.Context,
	restClientConfig *rest.Config, namespace string) error {
	args := m.Called(ctx, restClientConfig, namespace)
	return args.Error(0)
}

func (m *MockCOCommonInterface) GetZonesForNamespace(ns string) map[string]struct{} {
	args := m.Called(ns)
	return args.Get(0).(map[string]struct{})
}

func (m *MockCOCommonInterface) PreLinkedCloneCreateAction(ctx context.Context, pvcNamespace, pvcName string) error {
	args := m.Called(ctx, pvcNamespace, pvcName)
	return args.Error(0)
}

func (m *MockCOCommonInterface) GetLinkedCloneVolumeSnapshotSourceUUID(ctx context.Context,
	pvcName, pvcNamespace string) (string, error) {
	args := m.Called(ctx, pvcName, pvcNamespace)
	return args.String(0), args.Error(1)
}

func (m *MockCOCommonInterface) IsLinkedCloneRequest(ctx context.Context, pvcName, pvcNamespace string) (bool, error) {
	args := m.Called(ctx, pvcName, pvcNamespace)
	return args.Bool(0), args.Error(1)
}

func (m *MockCOCommonInterface) UpdatePersistentVolumeLabel(ctx context.Context, pvName, key, value string) error {
	args := m.Called(ctx, pvName, key, value)
	return args.Error(0)
}

func (m *MockCOCommonInterface) GetActiveClustersForNamespaceInRequestedZones(ctx context.Context,
	namespace string, zones []string) ([]string, error) {
	args := m.Called(ctx, namespace, zones)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockCOCommonInterface) GetPvcObjectByName(ctx context.Context,
	pvcName, namespace string) (*corev1.PersistentVolumeClaim, error) {
	args := m.Called(ctx, pvcName, namespace)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*corev1.PersistentVolumeClaim), args.Error(1)
}

func (m *MockCOCommonInterface) HandleLateEnablementOfCapability(ctx context.Context,
	clusterFlavor cnstypes.CnsClusterFlavor, capability, gcPort, gcEndpoint string) {
	m.Called(ctx, clusterFlavor, capability, gcPort, gcEndpoint)
}

// MockCryptoClient is a mock implementation of crypto.Client
type MockCryptoClient struct {
	mock.Mock
	client.Client
}

func (m *MockCryptoClient) IsEncryptedStorageClass(ctx context.Context, name string) (bool, string, error) {
	args := m.Called(ctx, name)
	return args.Bool(0), args.String(1), args.Error(2)
}

func (m *MockCryptoClient) IsEncryptedStorageProfile(ctx context.Context, profileID string) (bool, error) {
	args := m.Called(ctx, profileID)
	return args.Bool(0), args.Error(1)
}

func (m *MockCryptoClient) MarkEncryptedStorageClass(ctx context.Context,
	storageClass *storagev1.StorageClass, encrypted bool) error {
	args := m.Called(ctx, storageClass, encrypted)
	return args.Error(0)
}

func (m *MockCryptoClient) GetEncryptionClass(ctx context.Context,
	name, namespace string) (*byokv1.EncryptionClass, error) {
	args := m.Called(ctx, name, namespace)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*byokv1.EncryptionClass), args.Error(1)
}

func (m *MockCryptoClient) GetDefaultEncryptionClass(ctx context.Context,
	namespace string) (*byokv1.EncryptionClass, error) {
	args := m.Called(ctx, namespace)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*byokv1.EncryptionClass), args.Error(1)
}

func (m *MockCryptoClient) GetEncryptionClassForPVC(ctx context.Context,
	name, namespace string) (*byokv1.EncryptionClass, error) {
	args := m.Called(ctx, name, namespace)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*byokv1.EncryptionClass), args.Error(1)
}

func TestMutateNewPVC_LinkedClone_Success_SetTopologyAnnotation(t *testing.T) {
	ctx := context.Background()

	// Enable linked clone support
	originalFeatureGate := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = true
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureGate
	}()

	// Create test PVC with linked clone annotation but no topology annotation
	testPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vs1-lc-1",
			Namespace: "default",
			Annotations: map[string]string{
				common.AnnKeyLinkedClone: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				Name:     "vs-1",
				Kind:     "VolumeSnapshot",
				APIGroup: func() *string { s := common.VolumeSnapshotApiGroup; return &s }(),
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Create source PVC with topology annotation
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "src-pvc",
			Namespace: "default",
			Annotations: map[string]string{
				common.AnnVolumeAccessibleTopology: "zone-1",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Mock the data source reference
	dataSourceRef := &corev1.ObjectReference{
		Name:       "vs-1",
		Namespace:  "default",
		Kind:       "VolumeSnapshot",
		APIVersion: common.VolumeSnapshotApiGroup,
	}

	// Create admission request
	pvcBytes, _ := json.Marshal(testPVC)
	req := admission.Request{
		AdmissionRequest: v1.AdmissionRequest{
			Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
			Operation: v1.Create,
			Object:    runtime.RawExtension{Raw: pvcBytes},
		},
	}

	// Setup mocks
	mockCOInterface := &MockCOCommonInterface{}
	mockCryptoClient := &MockCryptoClient{}

	// Mock GetVolumeSnapshotPVCSource to return source PVC
	mockCOInterface.On("GetVolumeSnapshotPVCSource", ctx, "default", "vs-1").Return(sourcePVC, nil)

	// Patch GetPVCDataSource function using gomonkey
	patches := gomonkey.ApplyFunc(
		k8sorchestrator.GetPVCDataSource, func(ctx context.Context,
			pvc *corev1.PersistentVolumeClaim) (*corev1.ObjectReference, error) {
			return dataSourceRef, nil
		})
	defer patches.Reset()

	// Create webhook handler
	webhook := &CSISupervisorMutationWebhook{
		coCommonInterface: mockCOInterface,
		CryptoClient:      mockCryptoClient,
	}

	// Call mutateNewPVC
	response := webhook.mutateNewPVC(ctx, req)

	// Verify response
	assert.True(t, response.Allowed)
	// For successful mutations, the response should contain patches
	assert.NotEmpty(t, response.Patches)
	// Verify that both label and topology annotation were added
	assert.Equal(t, 2, len(response.Patches))
	// Check that one patch adds the linked-clone label
	labelPatchFound := false
	topologyPatchFound := false
	for _, patch := range response.Patches {
		if patch.Path == "/metadata/labels" {
			labelPatchFound = true
		}
		if patch.Path == "/metadata/annotations/csi.vsphere.volume-requested-topology" {
			topologyPatchFound = true
		}
	}
	assert.True(t, labelPatchFound, "Expected to find label patch")
	assert.True(t, topologyPatchFound, "Expected to find topology annotation patch")

	mockCOInterface.AssertExpectations(t)
}

func TestMutateNewPVC_LinkedClone_Success_ValidateTopologyAnnotation(t *testing.T) {
	ctx := context.Background()

	// Enable linked clone support
	originalFeatureGate := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = true
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureGate
	}()

	// Create test PVC with linked clone annotation and matching topology annotation
	testPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vs1-lc-1",
			Namespace: "default",
			Annotations: map[string]string{
				common.AnnKeyLinkedClone:                "true",
				common.AnnGuestClusterRequestedTopology: "zone-1",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				Name:     "vs-1",
				Kind:     "VolumeSnapshot",
				APIGroup: func() *string { s := common.VolumeSnapshotApiGroup; return &s }(),
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Create source PVC with matching topology annotation
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "src-pvc",
			Namespace: "default",
			Annotations: map[string]string{
				common.AnnVolumeAccessibleTopology: "zone-1",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Mock the data source reference
	dataSourceRef := &corev1.ObjectReference{
		Name:       "vs-1",
		Namespace:  "default",
		Kind:       "VolumeSnapshot",
		APIVersion: common.VolumeSnapshotApiGroup,
	}

	// Create admission request
	pvcBytes, _ := json.Marshal(testPVC)
	req := admission.Request{
		AdmissionRequest: v1.AdmissionRequest{
			Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
			Operation: v1.Create,
			Object:    runtime.RawExtension{Raw: pvcBytes},
		},
	}

	// Setup mocks
	mockCOInterface := &MockCOCommonInterface{}
	mockCryptoClient := &MockCryptoClient{}

	// Mock GetVolumeSnapshotPVCSource to return source PVC
	mockCOInterface.On("GetVolumeSnapshotPVCSource", ctx, "default", "vs-1").Return(sourcePVC, nil)

	// Patch GetPVCDataSource function using gomonkey
	patches := gomonkey.ApplyFunc(
		k8sorchestrator.GetPVCDataSource, func(ctx context.Context,
			pvc *corev1.PersistentVolumeClaim) (*corev1.ObjectReference, error) {
			return dataSourceRef, nil
		})
	defer patches.Reset()

	// Create webhook handler
	webhook := &CSISupervisorMutationWebhook{
		coCommonInterface: mockCOInterface,
		CryptoClient:      mockCryptoClient,
	}

	// Call mutateNewPVC
	response := webhook.mutateNewPVC(ctx, req)

	// Verify response - should succeed with only label mutation
	assert.True(t, response.Allowed)
	// For successful mutations, the response should contain patches
	assert.NotEmpty(t, response.Patches)
	// Should only have one patch for the label (topology already matches)
	assert.Equal(t, 1, len(response.Patches))
	// Check that the patch adds the linked-clone label
	assert.Equal(t, "/metadata/labels", response.Patches[0].Path)

	mockCOInterface.AssertExpectations(t)
}

func TestMutateNewPVC_LinkedClone_Error_TopologyMismatch(t *testing.T) {
	ctx := context.Background()

	// Enable linked clone support
	originalFeatureGate := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = true
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureGate
	}()

	// Create test PVC with linked clone annotation and mismatched topology annotation
	testPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vs1-lc-1",
			Namespace: "default",
			Annotations: map[string]string{
				common.AnnKeyLinkedClone:                "true",
				common.AnnGuestClusterRequestedTopology: "zone-2", // Different from source
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				Name:     "vs-1",
				Kind:     "VolumeSnapshot",
				APIGroup: func() *string { s := common.VolumeSnapshotApiGroup; return &s }(),
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Create source PVC with different topology annotation
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "src-pvc",
			Namespace: "default",
			Annotations: map[string]string{
				common.AnnVolumeAccessibleTopology: "zone-1", // Different from linked clone request
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Mock the data source reference
	dataSourceRef := &corev1.ObjectReference{
		Name:       "vs-1",
		Namespace:  "default",
		Kind:       "VolumeSnapshot",
		APIVersion: common.VolumeSnapshotApiGroup,
	}

	// Create admission request
	pvcBytes, _ := json.Marshal(testPVC)
	req := admission.Request{
		AdmissionRequest: v1.AdmissionRequest{
			Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
			Operation: v1.Create,
			Object:    runtime.RawExtension{Raw: pvcBytes},
		},
	}

	// Setup mocks
	mockCOInterface := &MockCOCommonInterface{}
	mockCryptoClient := &MockCryptoClient{}

	// Mock GetVolumeSnapshotPVCSource to return source PVC
	mockCOInterface.On("GetVolumeSnapshotPVCSource", ctx, "default", "vs-1").Return(sourcePVC, nil)

	// Patch GetPVCDataSource function using gomonkey
	patches := gomonkey.ApplyFunc(
		k8sorchestrator.GetPVCDataSource, func(ctx context.Context,
			pvc *corev1.PersistentVolumeClaim) (*corev1.ObjectReference, error) {
			return dataSourceRef, nil
		})
	defer patches.Reset()

	// Create webhook handler
	webhook := &CSISupervisorMutationWebhook{
		coCommonInterface: mockCOInterface,
		CryptoClient:      mockCryptoClient,
	}

	// Call mutateNewPVC
	response := webhook.mutateNewPVC(ctx, req)

	// Verify response - should be denied due to topology mismatch
	assert.False(t, response.Allowed)
	assert.Contains(t, response.Result.Message, "expected accessibility requirement: zone-1 but got zone-2")

	mockCOInterface.AssertExpectations(t)
}

func TestMutateNewPVC_LinkedClone_Error_GetDataSourceFails(t *testing.T) {
	ctx := context.Background()

	// Enable linked clone support
	originalFeatureGate := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = true
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureGate
	}()

	// Create test PVC with linked clone annotation
	testPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vs1-lc-1",
			Namespace: "default",
			Annotations: map[string]string{
				common.AnnKeyLinkedClone: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				Name:     "vs-1",
				Kind:     "VolumeSnapshot",
				APIGroup: func() *string { s := common.VolumeSnapshotApiGroup; return &s }(),
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Create admission request
	pvcBytes, _ := json.Marshal(testPVC)
	req := admission.Request{
		AdmissionRequest: v1.AdmissionRequest{
			Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
			Operation: v1.Create,
			Object:    runtime.RawExtension{Raw: pvcBytes},
		},
	}

	// Setup mocks
	mockCOInterface := &MockCOCommonInterface{}
	mockCryptoClient := &MockCryptoClient{}

	// Patch GetPVCDataSource function to return error using gomonkey
	patches := gomonkey.ApplyFunc(
		k8sorchestrator.GetPVCDataSource, func(ctx context.Context,
			pvc *corev1.PersistentVolumeClaim) (*corev1.ObjectReference, error) {
			return nil, assert.AnError
		})
	defer patches.Reset()

	// Create webhook handler
	webhook := &CSISupervisorMutationWebhook{
		coCommonInterface: mockCOInterface,
		CryptoClient:      mockCryptoClient,
	}

	// Call mutateNewPVC
	response := webhook.mutateNewPVC(ctx, req)

	// Verify response - should be denied due to error getting data source
	assert.False(t, response.Allowed)
	assert.Contains(t, response.Result.Message, "failed to retrieve the linked clone source volumesnapshot")

	mockCOInterface.AssertExpectations(t)
}

func TestMutateNewPVC_LinkedClone_Error_GetSourcePVCFails(t *testing.T) {
	ctx := context.Background()

	// Enable linked clone support
	originalFeatureGate := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = true
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureGate
	}()

	// Create test PVC with linked clone annotation
	testPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vs1-lc-1",
			Namespace: "default",
			Annotations: map[string]string{
				common.AnnKeyLinkedClone: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				Name:     "vs-1",
				Kind:     "VolumeSnapshot",
				APIGroup: func() *string { s := common.VolumeSnapshotApiGroup; return &s }(),
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Mock the data source reference
	dataSourceRef := &corev1.ObjectReference{
		Name:       "vs-1",
		Namespace:  "default",
		Kind:       "VolumeSnapshot",
		APIVersion: common.VolumeSnapshotApiGroup,
	}

	// Create admission request
	pvcBytes, _ := json.Marshal(testPVC)
	req := admission.Request{
		AdmissionRequest: v1.AdmissionRequest{
			Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
			Operation: v1.Create,
			Object:    runtime.RawExtension{Raw: pvcBytes},
		},
	}

	// Setup mocks
	mockCOInterface := &MockCOCommonInterface{}
	mockCryptoClient := &MockCryptoClient{}

	// Mock GetVolumeSnapshotPVCSource to return error
	mockCOInterface.On("GetVolumeSnapshotPVCSource", ctx, "default", "vs-1").Return(nil, assert.AnError)

	// Patch GetPVCDataSource function using gomonkey
	patches := gomonkey.ApplyFunc(
		k8sorchestrator.GetPVCDataSource, func(ctx context.Context,
			pvc *corev1.PersistentVolumeClaim) (*corev1.ObjectReference, error) {
			return dataSourceRef, nil
		})
	defer patches.Reset()

	// Create webhook handler
	webhook := &CSISupervisorMutationWebhook{
		coCommonInterface: mockCOInterface,
		CryptoClient:      mockCryptoClient,
	}

	// Call mutateNewPVC
	response := webhook.mutateNewPVC(ctx, req)

	// Verify response - should be denied due to error getting source PVC
	assert.False(t, response.Allowed)
	assert.Contains(t, response.Result.Message, "failed to retrieve the linked clone source PVC")

	mockCOInterface.AssertExpectations(t)
}

func TestMutateNewPVC_LinkedClone_Error_SourcePVCMissingTopologyAnnotation(t *testing.T) {
	ctx := context.Background()

	// Enable linked clone support
	originalFeatureGate := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = true
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureGate
	}()

	// Create test PVC with linked clone annotation
	testPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vs1-lc-1",
			Namespace: "default",
			Annotations: map[string]string{
				common.AnnKeyLinkedClone: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				Name:     "vs-1",
				Kind:     "VolumeSnapshot",
				APIGroup: func() *string { s := common.VolumeSnapshotApiGroup; return &s }(),
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Create source PVC without topology annotation
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "src-pvc",
			Namespace:   "default",
			Annotations: map[string]string{
				// Missing common.AnnVolumeAccessibleTopology annotation
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Mock the data source reference
	dataSourceRef := &corev1.ObjectReference{
		Name:       "vs-1",
		Namespace:  "default",
		Kind:       "VolumeSnapshot",
		APIVersion: common.VolumeSnapshotApiGroup,
	}

	// Create admission request
	pvcBytes, _ := json.Marshal(testPVC)
	req := admission.Request{
		AdmissionRequest: v1.AdmissionRequest{
			Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
			Operation: v1.Create,
			Object:    runtime.RawExtension{Raw: pvcBytes},
		},
	}

	// Setup mocks
	mockCOInterface := &MockCOCommonInterface{}
	mockCryptoClient := &MockCryptoClient{}

	// Mock GetVolumeSnapshotPVCSource to return source PVC without topology annotation
	mockCOInterface.On("GetVolumeSnapshotPVCSource", ctx, "default", "vs-1").Return(sourcePVC, nil)

	// Patch GetPVCDataSource function using gomonkey
	patches := gomonkey.ApplyFunc(
		k8sorchestrator.GetPVCDataSource, func(ctx context.Context,
			pvc *corev1.PersistentVolumeClaim) (*corev1.ObjectReference, error) {
			return dataSourceRef, nil
		})
	defer patches.Reset()

	// Create webhook handler
	webhook := &CSISupervisorMutationWebhook{
		coCommonInterface: mockCOInterface,
		CryptoClient:      mockCryptoClient,
	}

	// Call mutateNewPVC
	response := webhook.mutateNewPVC(ctx, req)

	// Verify response - should be denied due to missing topology annotation on source PVC
	assert.False(t, response.Allowed)
	assert.Contains(t, response.Result.Message, "source PVC default/src-pvc does not have volume accessiblity annotation")

	mockCOInterface.AssertExpectations(t)
}

func TestMutateNewPVC_NonLinkedClone_Success(t *testing.T) {
	ctx := context.Background()

	// Enable linked clone support
	originalFeatureGate := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = true
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureGate
	}()

	// Create test PVC without linked clone annotation
	testPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "regular-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Create admission request
	pvcBytes, _ := json.Marshal(testPVC)
	req := admission.Request{
		AdmissionRequest: v1.AdmissionRequest{
			Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
			Operation: v1.Create,
			Object:    runtime.RawExtension{Raw: pvcBytes},
		},
	}

	// Setup mocks
	mockCOInterface := &MockCOCommonInterface{}
	mockCryptoClient := &MockCryptoClient{}

	// Create webhook handler
	webhook := &CSISupervisorMutationWebhook{
		coCommonInterface: mockCOInterface,
		CryptoClient:      mockCryptoClient,
	}

	// Call mutateNewPVC
	response := webhook.mutateNewPVC(ctx, req)

	// Verify response - should be allowed without mutations
	assert.True(t, response.Allowed)

	mockCOInterface.AssertExpectations(t)
}

func TestMutateNewPVC_LinkedCloneDisabled_Success(t *testing.T) {
	ctx := context.Background()

	// Disable linked clone support
	originalFeatureGate := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = false
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureGate
	}()

	// Create test PVC with linked clone annotation (should be ignored)
	testPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vs1-lc-1",
			Namespace: "default",
			Annotations: map[string]string{
				common.AnnKeyLinkedClone: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				Name:     "vs-1",
				Kind:     "VolumeSnapshot",
				APIGroup: func() *string { s := common.VolumeSnapshotApiGroup; return &s }(),
			},
			StorageClassName: &[]string{"wcpglobal-storage-profile"}[0],
		},
	}

	// Create admission request
	pvcBytes, _ := json.Marshal(testPVC)
	req := admission.Request{
		AdmissionRequest: v1.AdmissionRequest{
			Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
			Operation: v1.Create,
			Object:    runtime.RawExtension{Raw: pvcBytes},
		},
	}

	// Setup mocks
	mockCOInterface := &MockCOCommonInterface{}
	mockCryptoClient := &MockCryptoClient{}

	// Create webhook handler
	webhook := &CSISupervisorMutationWebhook{
		coCommonInterface: mockCOInterface,
		CryptoClient:      mockCryptoClient,
	}

	// Call mutateNewPVC
	response := webhook.mutateNewPVC(ctx, req)

	// Verify response - should be allowed without mutations since feature is disabled
	assert.True(t, response.Allowed)

	mockCOInterface.AssertExpectations(t)
}
