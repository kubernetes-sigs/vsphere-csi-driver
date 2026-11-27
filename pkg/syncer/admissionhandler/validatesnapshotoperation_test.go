/*
Copyright 2023 The Kubernetes Authors.

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
	"time"

	snap "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	v1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	snapshotclientfake "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fakesnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

var admissionReview_snapshotclass = v1.AdmissionReview{
	Request: &v1.AdmissionRequest{
		Kind: metav1.GroupVersionKind{
			Kind: "VolumeSnapshotClass",
		},
	},
}

var admissionReview_snapshot = v1.AdmissionReview{
	Request: &v1.AdmissionRequest{
		Kind: metav1.GroupVersionKind{
			Kind: "VolumeSnapshot",
		},
	},
}

var admissionReview_snapshotcontent = v1.AdmissionReview{
	Request: &v1.AdmissionRequest{
		Kind: metav1.GroupVersionKind{
			Kind: "VolumeSnapshotContent",
		},
	},
}

// TestValidateVolumeSnapshotClassInGuestWithFSSDisabled is the unit test for
// validating admissionReview request containing VolumeSnapshotClass with
// CSI snapshot FSS set to false.
func TestValidateVolumeSnapshotClassInGuestWithFSSDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	featureGateBlockVolumeSnapshotEnabled = false

	// Validate vSphere VolumeSnapshotClass creation with CSI snapshot FSS disabled
	admissionReview_snapshotclass.Request.Object = runtime.RawExtension{
		Raw: []byte("{\n  \"kind\": \"VolumeSnapshotClass\",\n  \"apiVersion\": \"snapshot.storage.k8s.io/v1\",\n" +
			"\"metadata\": {\n    \"name\": \"test-vsclass\",\n    \"creationTimestamp\": \"2023-08-29T20:19:00Z\"\n  }" +
			",\n  \"driver\": \"csi.vsphere.vmware.com\",\n  \"deletionPolicy\": \"Delete\"\n}"),
	}
	admissionResponse := validateSnapshotOperationGuestRequest(ctx, admissionReview_snapshotclass.Request)
	if admissionResponse.Allowed {
		t.Fatalf("TestValidateVolumeSnapshotClassInGuestWithFSSDisabled failed for vSphere VolumeSnapshotClass. "+
			"admissionReview_snapshot.Request: %v, admissionResponse: %v", admissionReview_snapshotclass.Request,
			admissionResponse)
	}

	// Validate non-vSphere VolumeSnapshotClass creation with CSI snapshot FSS disabled
	admissionReview_snapshotclass.Request.Object = runtime.RawExtension{
		Raw: []byte("{\n  \"kind\": \"VolumeSnapshotClass\",\n  \"apiVersion\": \"snapshot.storage.k8s.io/v1\",\n" +
			"\"metadata\": {\n    \"name\": \"test-hostpath-snapclass\",\n    " +
			"\"creationTimestamp\": \"2023-08-29T20:19:00Z\"\n  },\n  " +
			"\"driver\": \"hostpath.csi.k8s.io\",\n  \"deletionPolicy\": \"Delete\"\n}"),
	}
	admissionResponse = validateSnapshotOperationGuestRequest(ctx, admissionReview_snapshotclass.Request)
	if !admissionResponse.Allowed {
		t.Fatalf("TestValidateVolumeSnapshotClassInGuestWithFSSDisabled failed for non-vSphere VolumeSnapshotClass. "+
			"admissionReview_snapshot.Request: %v, admissionResponse: %v", admissionReview_snapshotclass.Request,
			admissionResponse)
	}
}

// TestValidateVolumeSnapshotInGuestWithFSSDisabled is the unit test for
// validating admissionReview request containing VolumeSnapshot with
// CSI snapshot FSS set to false.
func TestValidateVolumeSnapshotInGuestWithFSSDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	featureGateBlockVolumeSnapshotEnabled = false

	// Create test vSphere snapclass for verification
	snapshotClassObj := []runtime.Object{
		&snap.VolumeSnapshotClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-vsclass",
			},
			Driver:         "csi.vsphere.vmware.com",
			DeletionPolicy: "Delete",
		},
	}
	snapshotClient := snapshotclientfake.NewClientset(snapshotClassObj...)
	origSnapshotterClient := newSnapshotterClient
	defer func() { newSnapshotterClient = origSnapshotterClient }()
	newSnapshotterClient = func(ctx context.Context) (snapshotterClientSet.Interface, error) {
		return snapshotClient, nil
	}
	// The source PVC is looked up to tell a guest NFS snapshot apart; it does not exist here.
	origK8sClient := newK8sClient
	defer func() { newK8sClient = origK8sClient }()
	newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
		return fake.NewClientset(), nil
	}

	// Validate vSphere VolumeSnapshot creation with CSI snapshot FSS disabled
	admissionReview_snapshot.Request.Object = runtime.RawExtension{
		Raw: []byte("{\n  \"kind\": \"VolumeSnapshot\",\n  \"apiVersion\": \"snapshot.storage.k8s.io/v1\",\n " +
			"\"metadata\": {\n    \"name\": \"test-vs\",\n    \"creationTimestamp\": \"2023-08-29T20:20:00Z\"\n  },\n  " +
			"\"spec\": {\n    \"volumeSnapshotClassName\": \"test-vsclass\",\n  " +
			"\"source\": {\n    \"persistentVolumeClaimName\": \"test-pvc\"\n } \n} \n}"),
	}

	admissionResponse := validateSnapshotOperationGuestRequest(ctx, admissionReview_snapshot.Request)
	if admissionResponse.Allowed {
		t.Fatalf("TestValidateVolumeSnapshotClassInGuestWithFSSDisabled failed for vSphere VolumeSnapshot. "+
			"admissionReview_snapshot.Request: %v, admissionResponse: %v", admissionReview_snapshot.Request,
			admissionResponse)
	}

	// Create test non-vSphere snapclass for verification
	snapshotClassObj = []runtime.Object{
		&snap.VolumeSnapshotClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-hostpath-vsclass",
			},
			Driver:         "hostpath.csi.k8s.io",
			DeletionPolicy: "Delete",
		},
	}
	snapshotClient = snapshotclientfake.NewClientset(snapshotClassObj...)
	newSnapshotterClient = func(ctx context.Context) (snapshotterClientSet.Interface, error) {
		return snapshotClient, nil
	}

	// Validate non-vSphere VolumeSnapshot creation with CSI snapshot FSS disabled
	admissionReview_snapshot.Request.Object = runtime.RawExtension{
		Raw: []byte("{\n  \"kind\": \"VolumeSnapshot\",\n  \"apiVersion\": \"snapshot.storage.k8s.io/v1\",\n  " +
			"\"metadata\": {\n    \"name\": \"test-hostpath-vs\",\n   \"creationTimestamp\": \"2023-08-29T20:20:00Z\"\n" +
			"},\n  \"spec\": {\n    \"volumeSnapshotClassName\": \"test-hostpath-vsclass\",\n  " +
			"\"source\": {\n    \"persistentVolumeClaimName\": \"test-pvc\"\n } \n} \n}"),
	}

	admissionResponse = validateSnapshotOperationGuestRequest(ctx, admissionReview_snapshot.Request)
	if !admissionResponse.Allowed {
		t.Fatalf("TestValidateVolumeSnapshotClassInGuestWithFSSDisabled failed for non-vSphere VolumeSnapshot. "+
			"admissionReview_snapshot.Request: %v, admissionResponse: %v", admissionReview_snapshot.Request,
			admissionResponse)
	}
}

// TestValidateVolumeSnapshotWithoutClassNameInGuest is the unit test for
// validating admissionReview request containing a VolumeSnapshot that does not
// specify a VolumeSnapshotClassName (i.e. default snapshot class is used).
// The request must be allowed and must not panic.
func TestValidateVolumeSnapshotWithoutClassNameInGuest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	featureGateBlockVolumeSnapshotEnabled = false

	req := &v1.AdmissionRequest{
		Kind: metav1.GroupVersionKind{
			Kind: "VolumeSnapshot",
		},
		Object: runtime.RawExtension{
			Raw: []byte("{\n  \"kind\": \"VolumeSnapshot\",\n  \"apiVersion\": \"snapshot.storage.k8s.io/v1\",\n " +
				"\"metadata\": {\n    \"name\": \"test-vs\",\n    \"creationTimestamp\": \"2023-08-29T20:20:00Z\"\n  },\n  " +
				"\"spec\": {\n  \"source\": {\n    \"persistentVolumeClaimName\": \"test-pvc\"\n } \n} \n}"),
		},
	}

	admissionResponse := validateSnapshotOperationGuestRequest(ctx, req)
	if !admissionResponse.Allowed {
		t.Fatalf("TestValidateVolumeSnapshotWithoutClassNameInGuest failed. "+
			"request: %v, admissionResponse: %v", req, admissionResponse)
	}
}

// TestValidateVolumeSnapshotContentInGuestWithFSSDisabled is the unit test for
// validating admissionReview request containing VolumeSnapshotContent with
// CSI snapshot FSS set to false. (Static provisioning case)
func TestValidateVolumeSnapshotContentInGuestWithFSSDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	featureGateBlockVolumeSnapshotEnabled = false

	// Validate vSphere VolumeSnapshot creation with CSI snapshot FSS disabled
	admissionReview_snapshotcontent.Request.Object = runtime.RawExtension{
		Raw: []byte("{\n  \"kind\": \"VolumeSnapshotContent\",\n  \"apiVersion\": \"snapshot.storage.k8s.io/v1\",\n  " +
			"\"metadata\": {\n    \"name\": \"test-static-vsc\",\n    \"creationTimestamp\": \"2023-08-29T20:30:00Z\"\n" +
			"},\n  \"spec\": {\n    \"driver\": \"csi.vsphere.vmware.com\",\n  \"deletionPolicy\": \"Delete\",\n  " +
			"\"source\": {\n    \"snapshotHandle\": " +
			"\"4ef058e4-d941-447d-a427-438440b7d306+766f7158-b394-4cc1-891b-4667df0822fa\"\n }\n }\n }"),
	}
	admissionResponse := validateSnapshotOperationGuestRequest(ctx, admissionReview_snapshotcontent.Request)
	if admissionResponse.Allowed {
		t.Fatalf("TestValidateVolumeSnapshotObjectsInGuestWithFSSDisabled failed for vSphere VolumeSnapshotContent. "+
			"admissionReview_snapshot.Request: %v, admissionResponse: %v", admissionReview_snapshotcontent.Request,
			admissionResponse)
	}

	// Validate non-vSphere VolumeSnapshot creation with CSI snapshot FSS disabled
	admissionReview_snapshotcontent.Request.Object = runtime.RawExtension{
		Raw: []byte("{\n  \"kind\": \"VolumeSnapshotContent\",\n  \"apiVersion\": \"snapshot.storage.k8s.io/v1\",\n  " +
			"\"metadata\": {\n    \"name\": \"test-static-hostpath-vsc\",\n    " +
			"\"creationTimestamp\": \"2023-08-29T20:30:00Z\"\n  },\n  " +
			"\"spec\": {\n    \"driver\": \"hostpath.csi.k8s.io\",\n  \"deletionPolicy\": \"Delete\",\n  " +
			"\"source\": {\n    \"snapshotHandle\": " +
			"\"567fg93h-d941-447d-a427-438440b7d306+766f7158-b394-4cc1-891b-4667df0822fa\"\n }\n }\n }"),
	}
	admissionResponse = validateSnapshotOperationGuestRequest(ctx, admissionReview_snapshotcontent.Request)
	if !admissionResponse.Allowed {
		t.Fatalf("TestValidateVolumeSnapshotObjectsInGuestWithFSSDisabled failed for non-vSphere VolumeSnapshotContent. "+
			"admissionReview_snapshot.Request: %v, admissionResponse: %v", admissionReview_snapshotcontent.Request,
			admissionResponse)
	}
}

// TestValidateSnapshotOperationSupervisorRequestWithNamespaceDeletion tests the namespace deletion logic
func TestValidateSnapshotOperationSupervisorRequestWithNamespaceDeletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Enable linked clone support for these tests
	featureIsLinkedCloneSupportEnabled = true

	tests := []struct {
		name                    string
		namespace               *corev1.Namespace
		expectAllowed           bool
		expectMessage           string
		volumeSnapshotName      string
		volumeSnapshotNamespace string
	}{
		{
			name: "Allow deletion when namespace is being deleted",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-namespace",
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			expectAllowed:           true,
			expectMessage:           "Namespace is being deleted",
			volumeSnapshotName:      "test-snapshot",
			volumeSnapshotNamespace: "test-namespace",
		},
		{
			name: "Proceed with normal validation when namespace is not being deleted",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-namespace",
					// No DeletionTimestamp set
				},
			},
			expectAllowed:           true, // Will pass linked clone check (no linked clones exist)
			expectMessage:           "",
			volumeSnapshotName:      "test-snapshot",
			volumeSnapshotNamespace: "test-namespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake k8s client with the test namespace
			k8sClient := fake.NewClientset(tt.namespace)

			origK8sClient := newK8sClient
			defer func() { newK8sClient = origK8sClient }()
			newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
				return k8sClient, nil
			}

			// Create admission request for VolumeSnapshot deletion
			volumeSnapshotJSON := `{
				"apiVersion": "snapshot.storage.k8s.io/v1",
				"kind": "VolumeSnapshot",
				"metadata": {
					"name": "` + tt.volumeSnapshotName + `",
					"namespace": "` + tt.volumeSnapshotNamespace + `",
					"uid": "test-uid-123"
				},
				"spec": {
					"source": {
						"persistentVolumeClaimName": "test-pvc"
					}
				}
			}`

			admissionRequest := &v1.AdmissionRequest{
				Kind: metav1.GroupVersionKind{
					Kind: "VolumeSnapshot",
				},
				Operation: v1.Delete,
				OldObject: runtime.RawExtension{
					Raw: []byte(volumeSnapshotJSON),
				},
			}

			// Call the function under test
			response := validateSnapshotOperationSupervisorRequest(ctx, admissionRequest)

			// Verify the response
			if response.Allowed != tt.expectAllowed {
				t.Errorf("Expected Allowed=%v, got Allowed=%v", tt.expectAllowed, response.Allowed)
			}

			if tt.expectMessage != "" && response.Result != nil && response.Result.Message != tt.expectMessage {
				t.Errorf("Expected message='%s', got message='%s'", tt.expectMessage, response.Result.Message)
			}
		})
	}
}

// TestIsNamespaceBeingDeleted tests the isNamespaceBeingDeleted helper function
func TestIsNamespaceBeingDeleted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tests := []struct {
		name          string
		namespace     *corev1.Namespace
		namespaceName string
		expected      bool
	}{
		{
			name: "Namespace with DeletionTimestamp should return true",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "deleting-namespace",
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			namespaceName: "deleting-namespace",
			expected:      true,
		},
		{
			name: "Namespace without DeletionTimestamp should return false",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "normal-namespace",
				},
			},
			namespaceName: "normal-namespace",
			expected:      false,
		},
		{
			name:          "Non-existent namespace should return true (already deleted)",
			namespace:     nil,
			namespaceName: "non-existent-namespace",
			expected:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake k8s client
			var k8sClient kubernetes.Interface
			if tt.namespace != nil {
				k8sClient = fake.NewClientset(tt.namespace)
			} else {
				k8sClient = fake.NewClientset()
			}

			origK8sClient := newK8sClient
			defer func() { newK8sClient = origK8sClient }()
			newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
				return k8sClient, nil
			}

			// Call the function under test
			result := isNamespaceBeingDeleted(ctx, tt.namespaceName)

			// Verify the result
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestValidateSnapshotOperationSupervisorRequestWithLinkedCloneFeatureDisabled tests behavior
// when linked clone feature is disabled
func TestValidateSnapshotOperationSupervisorRequestWithLinkedCloneFeatureDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Disable linked clone support for this test
	originalFeatureState := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = false
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureState
	}()

	volumeSnapshotJSON := `{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind": "VolumeSnapshot",
		"metadata": {
			"name": "test-snapshot",
			"namespace": "test-namespace",
			"uid": "test-uid-123"
		},
		"spec": {
			"source": {
				"persistentVolumeClaimName": "test-pvc"
			}
		}
	}`

	admissionRequest := &v1.AdmissionRequest{
		Kind: metav1.GroupVersionKind{
			Kind: "VolumeSnapshot",
		},
		Operation: v1.Delete,
		OldObject: runtime.RawExtension{
			Raw: []byte(volumeSnapshotJSON),
		},
	}

	// Call the function under test
	response := validateSnapshotOperationSupervisorRequest(ctx, admissionRequest)

	// Should be allowed when feature is disabled
	if !response.Allowed {
		t.Errorf("Expected request to be allowed when linked clone feature is disabled, got: %v", response)
	}
}

// TestValidateSnapshotOperationGuestRequestWithNamespaceDeletion tests the namespace deletion logic for guest requests
func TestValidateSnapshotOperationGuestRequestWithNamespaceDeletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Enable linked clone support for these tests
	featureIsLinkedCloneSupportEnabled = true

	tests := []struct {
		name                    string
		namespace               *corev1.Namespace
		expectAllowed           bool
		expectMessage           string
		volumeSnapshotName      string
		volumeSnapshotNamespace string
	}{
		{
			name: "Allow deletion when namespace is being deleted",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-namespace",
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			expectAllowed:           true,
			expectMessage:           "Namespace is being deleted",
			volumeSnapshotName:      "test-snapshot",
			volumeSnapshotNamespace: "test-namespace",
		},
		{
			name: "Proceed with normal validation when namespace is not being deleted",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-namespace",
					// No DeletionTimestamp set
				},
			},
			expectAllowed:           true, // Will pass linked clone check (no linked clones exist)
			expectMessage:           "",
			volumeSnapshotName:      "test-snapshot",
			volumeSnapshotNamespace: "test-namespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake k8s client with the test namespace
			k8sClient := fake.NewClientset(tt.namespace)

			origK8sClient := newK8sClient
			defer func() { newK8sClient = origK8sClient }()
			newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
				return k8sClient, nil
			}

			// Create admission request for VolumeSnapshot deletion
			volumeSnapshotJSON := `{
				"apiVersion": "snapshot.storage.k8s.io/v1",
				"kind": "VolumeSnapshot",
				"metadata": {
					"name": "` + tt.volumeSnapshotName + `",
					"namespace": "` + tt.volumeSnapshotNamespace + `",
					"uid": "test-uid-123"
				},
				"spec": {
					"source": {
						"persistentVolumeClaimName": "test-pvc"
					}
				}
			}`

			admissionRequest := &v1.AdmissionRequest{
				Kind: metav1.GroupVersionKind{
					Kind: "VolumeSnapshot",
				},
				Operation: v1.Delete,
				OldObject: runtime.RawExtension{
					Raw: []byte(volumeSnapshotJSON),
				},
			}

			// Call the function under test
			response := validateSnapshotOperationGuestRequest(ctx, admissionRequest)

			// Verify the response
			if response.Allowed != tt.expectAllowed {
				t.Errorf("Expected Allowed=%v, got Allowed=%v", tt.expectAllowed, response.Allowed)
			}

			if tt.expectMessage != "" && response.Result != nil && response.Result.Message != tt.expectMessage {
				t.Errorf("Expected message='%s', got message='%s'", tt.expectMessage, response.Result.Message)
			}
		})
	}
}

// TestValidateSnapshotOperationGuestRequestWithLinkedCloneFeatureDisabled tests behavior when
// linked clone feature is disabled
func TestValidateSnapshotOperationGuestRequestWithLinkedCloneFeatureDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Disable linked clone support for this test
	originalFeatureState := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = false
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureState
	}()

	volumeSnapshotJSON := `{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind": "VolumeSnapshot",
		"metadata": {
			"name": "test-snapshot",
			"namespace": "test-namespace",
			"uid": "test-uid-123"
		},
		"spec": {
			"source": {
				"persistentVolumeClaimName": "test-pvc"
			}
		}
	}`

	admissionRequest := &v1.AdmissionRequest{
		Kind: metav1.GroupVersionKind{
			Kind: "VolumeSnapshot",
		},
		Operation: v1.Delete,
		OldObject: runtime.RawExtension{
			Raw: []byte(volumeSnapshotJSON),
		},
	}

	// Call the function under test
	response := validateSnapshotOperationGuestRequest(ctx, admissionRequest)

	// Should be allowed when feature is disabled
	if !response.Allowed {
		t.Errorf("Expected request to be allowed when linked clone feature is disabled, got: %v", response)
	}
}

// withSnapshotFeatureStates sets the block-volume-snapshot and guest-nfs-volume feature states the
// snapshot webhook was started with, for the duration of the test.
func withSnapshotFeatureStates(t *testing.T, blockVolumeSnapshot, guestNFS bool) {
	t.Helper()
	origBlock, origNFS := featureGateBlockVolumeSnapshotEnabled, featureIsGuestNFSVolumeEnabled
	featureGateBlockVolumeSnapshotEnabled, featureIsGuestNFSVolumeEnabled = blockVolumeSnapshot, guestNFS
	t.Cleanup(func() {
		featureGateBlockVolumeSnapshotEnabled, featureIsGuestNFSVolumeEnabled = origBlock, origNFS
	})
}

func snapshotAdmissionRequest(t *testing.T, kind string, obj interface{}) *v1.AdmissionRequest {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal %s: %v", kind, err)
	}
	return &v1.AdmissionRequest{
		Kind:      metav1.GroupVersionKind{Kind: kind},
		Namespace: "ns",
		Operation: v1.Create,
		Object:    runtime.RawExtension{Raw: raw},
	}
}

const (
	testGuestNFSVolumeHandle   = common.GuestNFSVolumeIDPrefix + "10.0.0.1#export#pvc-nfs##"
	testGuestNFSSnapshotHandle = common.GuestNFSVolumeIDPrefix + "10.0.0.1#export#snap-1#snap-1#pvc-nfs"
)

// A VolumeSnapshotClass names no volume, so with block volume snapshots off it is allowed exactly
// when guest NFS volumes, whose snapshots do not need the Supervisor, are enabled.
func TestValidateVolumeSnapshotClassInGuestWithGuestNFS(t *testing.T) {
	class := &snap.VolumeSnapshotClass{ObjectMeta: metav1.ObjectMeta{Name: "nfs-snapshot-class"},
		Driver: common.VSphereCSIDriverName, DeletionPolicy: snap.VolumeSnapshotContentDelete}
	for _, guestNFS := range []bool{true, false} {
		withSnapshotFeatureStates(t, false, guestNFS)
		resp := validateSnapshotOperationGuestRequest(context.Background(),
			snapshotAdmissionRequest(t, "VolumeSnapshotClass", class))
		if resp.Allowed != guestNFS {
			t.Errorf("guest-nfs-volume=%t: Allowed=%t, want %t (%v)", guestNFS, resp.Allowed, guestNFS, resp.Result)
		}
	}
}

// With block volume snapshots off, a VolumeSnapshotContent of a guest NFS volume or snapshot is
// allowed, whatever the guest-nfs-volume feature state: refusing updates to an existing one would
// keep the snapshot controllers from ever removing its finalizers.
func TestValidateVolumeSnapshotContentInGuestAllowsGuestNFS(t *testing.T) {
	withSnapshotFeatureStates(t, false, false)
	contents := map[string]snap.VolumeSnapshotContentSource{
		"dynamic":         {VolumeHandle: ptr.To(testGuestNFSVolumeHandle)},
		"pre-provisioned": {SnapshotHandle: ptr.To(testGuestNFSSnapshotHandle)},
	}
	for name, src := range contents {
		vsc := &snap.VolumeSnapshotContent{ObjectMeta: metav1.ObjectMeta{Name: "snapcontent-" + name},
			Spec: snap.VolumeSnapshotContentSpec{Driver: common.VSphereCSIDriverName,
				DeletionPolicy: snap.VolumeSnapshotContentDelete, Source: src}}
		resp := validateSnapshotOperationGuestRequest(context.Background(),
			snapshotAdmissionRequest(t, "VolumeSnapshotContent", vsc))
		if !resp.Allowed {
			t.Errorf("%s guest NFS VolumeSnapshotContent was denied: %v", name, resp.Result)
		}
	}

	blockVSC := &snap.VolumeSnapshotContent{ObjectMeta: metav1.ObjectMeta{Name: "snapcontent-block"},
		Spec: snap.VolumeSnapshotContentSpec{Driver: common.VSphereCSIDriverName,
			DeletionPolicy: snap.VolumeSnapshotContentDelete,
			Source:         snap.VolumeSnapshotContentSource{VolumeHandle: ptr.To("cluster-uid-pvc-1")}}}
	if resp := validateSnapshotOperationGuestRequest(context.Background(),
		snapshotAdmissionRequest(t, "VolumeSnapshotContent", blockVSC)); resp.Allowed {
		t.Errorf("block VolumeSnapshotContent must still be denied")
	}
}

// With block volume snapshots off, a VolumeSnapshot is allowed only if it is of a guest NFS volume:
// one whose PVC is bound to a guest NFS PV, or whose content is a guest NFS snapshot.
func TestValidateVolumeSnapshotInGuestAllowsGuestNFS(t *testing.T) {
	withSnapshotFeatureStates(t, false, true)
	class := &snap.VolumeSnapshotClass{ObjectMeta: metav1.ObjectMeta{Name: "nfs-snapshot-class"},
		Driver: common.VSphereCSIDriverName, DeletionPolicy: snap.VolumeSnapshotContentDelete}
	nfsContent := &snap.VolumeSnapshotContent{ObjectMeta: metav1.ObjectMeta{Name: "snapcontent-nfs"},
		Spec: snap.VolumeSnapshotContentSpec{Driver: common.VSphereCSIDriverName,
			Source: snap.VolumeSnapshotContentSource{SnapshotHandle: ptr.To(testGuestNFSSnapshotHandle)}}}
	snapshotClient := snapshotclientfake.NewClientset(class, nfsContent)
	origSnapshotterClient := newSnapshotterClient
	defer func() { newSnapshotterClient = origSnapshotterClient }()
	newSnapshotterClient = func(ctx context.Context) (snapshotterClientSet.Interface, error) {
		return snapshotClient, nil
	}

	pv := func(name, handle string) *corev1.PersistentVolume {
		return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: common.VSphereCSIDriverName, VolumeHandle: handle}}}}
	}
	pvc := func(name, volumeName string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Spec: corev1.PersistentVolumeClaimSpec{VolumeName: volumeName}}
	}
	k8sClient := fake.NewClientset(pv("pv-nfs", testGuestNFSVolumeHandle), pvc("pvc-nfs", "pv-nfs"),
		pv("pv-block", "cluster-uid-pvc-1"), pvc("pvc-block", "pv-block"), pvc("pvc-pending", ""))
	origK8sClient := newK8sClient
	defer func() { newK8sClient = origK8sClient }()
	newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
		return k8sClient, nil
	}

	ofPVC := func(claim string) snap.VolumeSnapshotSource {
		return snap.VolumeSnapshotSource{PersistentVolumeClaimName: ptr.To(claim)}
	}
	tests := []struct {
		name    string
		source  snap.VolumeSnapshotSource
		status  *snap.VolumeSnapshotStatus
		allowed bool
	}{
		{name: "guest NFS PVC", source: ofPVC("pvc-nfs"), allowed: true},
		{name: "pre-provisioned guest NFS content",
			source: snap.VolumeSnapshotSource{VolumeSnapshotContentName: ptr.To("snapcontent-nfs")}, allowed: true},
		{name: "bound to guest NFS content", source: ofPVC("pvc-deleted"),
			status: &snap.VolumeSnapshotStatus{BoundVolumeSnapshotContentName: ptr.To("snapcontent-nfs")}, allowed: true},
		{name: "bound to deleted content, guest NFS PVC", source: ofPVC("pvc-nfs"),
			status: &snap.VolumeSnapshotStatus{BoundVolumeSnapshotContentName: ptr.To("gone")}, allowed: true},
		{name: "block PVC", source: ofPVC("pvc-block")},
		{name: "unbound PVC", source: ofPVC("pvc-pending")},
		{name: "missing PVC", source: ofPVC("pvc-deleted")},
		{name: "missing content", source: snap.VolumeSnapshotSource{VolumeSnapshotContentName: ptr.To("gone")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vs := &snap.VolumeSnapshot{ObjectMeta: metav1.ObjectMeta{Name: "vs"},
				Spec:   snap.VolumeSnapshotSpec{VolumeSnapshotClassName: ptr.To(class.Name), Source: tc.source},
				Status: tc.status}
			resp := validateSnapshotOperationGuestRequest(context.Background(),
				snapshotAdmissionRequest(t, "VolumeSnapshot", vs))
			if resp.Allowed != tc.allowed {
				t.Errorf("Allowed=%t, want %t (%v)", resp.Allowed, tc.allowed, resp.Result)
			}
		})
	}

	// The snapshot controller removes the last finalizer of a VolumeSnapshot being deleted only once
	// its content and, often, its PVC are gone; that update must not be refused.
	deleting := &snap.VolumeSnapshot{ObjectMeta: metav1.ObjectMeta{Name: "vs",
		DeletionTimestamp: &metav1.Time{Time: time.Now()}, Finalizers: []string{"bound-protection"}},
		Spec:   snap.VolumeSnapshotSpec{VolumeSnapshotClassName: ptr.To(class.Name), Source: ofPVC("pvc-deleted")},
		Status: &snap.VolumeSnapshotStatus{BoundVolumeSnapshotContentName: ptr.To("gone")}}
	req := snapshotAdmissionRequest(t, "VolumeSnapshot", deleting)
	req.Operation = v1.Update
	if resp := validateSnapshotOperationGuestRequest(context.Background(), req); !resp.Allowed {
		t.Errorf("update of a VolumeSnapshot being deleted was denied: %v", resp.Result)
	}
}
