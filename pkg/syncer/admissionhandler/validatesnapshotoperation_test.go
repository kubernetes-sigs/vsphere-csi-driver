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
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	snap "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
	snapshotclientfake "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned/fake"
	v1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
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
	snapshotClient := snapshotclientfake.NewSimpleClientset(snapshotClassObj...)
	patches := gomonkey.ApplyFunc(
		k8s.NewSnapshotterClient, func(ctx context.Context) (snapshotterClientSet.Interface, error) {
			return snapshotClient, nil
		})
	defer patches.Reset()

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
	snapshotClient = snapshotclientfake.NewSimpleClientset(snapshotClassObj...)
	patches_nonvSphere := gomonkey.ApplyFunc(
		k8s.NewSnapshotterClient, func(ctx context.Context) (snapshotterClientSet.Interface, error) {
			return snapshotClient, nil
		})
	defer patches_nonvSphere.Reset()

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
