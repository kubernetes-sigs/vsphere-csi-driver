/*
Copyright 2020 The Kubernetes Authors.

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
	"strings"
	"testing"

	v1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var admissionReview = v1.AdmissionReview{
	Request: &v1.AdmissionRequest{
		Kind: metav1.GroupVersionKind{
			Kind: "StorageClass",
		},
	},
}

// TestValidateStorageClassForAllowVolumeExpansion is the unit test for
// validating admissionReview request containing StorageClass with
// allowVolumeExpansion set to true.
func TestValidateStorageClassForAllowVolumeExpansion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admissionReview.Request.Object = runtime.RawExtension{
		Raw: []byte("{\n  \"kind\": \"StorageClass\",\n  \"apiVersion\": \"storage.k8s.io/v1\",\n  \"metadata\": " +
			"{\n    \"name\": \"sc\",\n    \"uid\": \"e5d6b37e-db23-4c1d-9aed-e38cdd0f9ec6\",\n    " +
			"\"creationTimestamp\": \"2020-08-27T20:19:00Z\"\n  },\n  " +
			"\"provisioner\": \"kubernetes.io/vsphere-volume\",\n  " +
			"\"parameters\": {\n    \"hostFailuresToTolerate\": \"2\"\n  },\n  " +
			"\"reclaimPolicy\": \"Delete\",\n  \"allowVolumeExpansion\": true,\n  " +
			"\"volumeBindingMode\": \"Immediate\"\n}"),
	}
	admissionResponse := validateStorageClass(ctx, &admissionReview)
	if admissionResponse.Allowed {
		t.Log("TestValidateStorageClassForAllowVolumeExpansion Passed")
	} else {
		t.Fatalf("TestValidateStorageClassForAllowVolumeExpansion failed. "+
			"admissionReview.Request: %v, admissionResponse: %v", admissionReview.Request, admissionResponse)
	}
}

// TestValidateStorageClassForMigrationParameter is the unit test for validating
// admissionReview request containing StorageClass with migration related
// parameters.
func TestValidateStorageClassForMigrationParameter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admissionReview.Request.Object = runtime.RawExtension{
		Raw: []byte("{\n  \"kind\": \"StorageClass\",\n  \"apiVersion\": \"storage.k8s.io/v1\",\n  \"metadata\": " +
			"{\n    \"name\": \"sc\",\n    \"uid\": \"a9ed134e-aab1-4624-8de4-b9d961cad861\",\n    " +
			"\"creationTimestamp\": \"2020-08-27T20:57:00Z\"\n  },\n  " +
			"\"provisioner\": \"csi.vsphere.vmware.com\",\n  " +
			"\"parameters\": {\n    \"hostfailurestotolerate-migrationparam\": \"2\"\n  },\n  " +
			"\"reclaimPolicy\": \"Delete\",\n  \"volumeBindingMode\": \"Immediate\"\n}"),
	}
	admissionResponse := validateStorageClass(ctx, &admissionReview)
	if !strings.Contains(string(admissionResponse.Result.Reason), migrationParamErrorMessage) ||
		admissionResponse.Allowed {
		t.Fatalf("TestValidateStorageClassForMigrationParameter failed. "+
			"admissionReview.Request: %v, admissionResponse: %v", admissionReview.Request, admissionResponse)
	}
	admissionReview.Request.Object = runtime.RawExtension{
		Raw: []byte("{\n  \"kind\": \"StorageClass\",\n  \"apiVersion\": \"storage.k8s.io/v1\",\n  \"metadata\": " +
			"{\n    \"name\": \"sc\",\n    \"uid\": \"a9ed134e-aab1-4624-8de4-b9d961cad861\",\n    " +
			"\"creationTimestamp\": \"2020-08-27T20:57:00Z\"\n  },\n  " +
			"\"provisioner\": \"csi.vsphere.vmware.com\",\n  " +
			"\"parameters\": {\n    \"datastore-migrationparam\": \"vsanDatastore\"\n  },\n  " +
			"\"reclaimPolicy\": \"Delete\",\n  \"volumeBindingMode\": \"Immediate\"\n}"),
	}
	admissionResponse = validateStorageClass(ctx, &admissionReview)
	if !strings.Contains(string(admissionResponse.Result.Reason), migrationParamErrorMessage) ||
		admissionResponse.Allowed {
		t.Fatalf(
			"TestValidateStorageClassForMigrationParameter failed. admissionReview.Request: %v, admissionResponse: %v",
			admissionReview.Request, admissionResponse)
	}
	t.Log("TestValidateStorageClassForMigrationParameter Passed")
}

// TestValidateStorageClassForValidStorageClass is the unit test for validating
// admissionReview request containing a valid StorageClass.
func TestValidateStorageClassForValidStorageClass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admissionReview.Request.Object = runtime.RawExtension{
		Raw: []byte("{\n  \"kind\": \"StorageClass\",\n  \"apiVersion\": \"storage.k8s.io/v1\",\n  \"metadata\": " +
			"{\n    \"name\": \"sc\",\n    \"uid\": \"a896f427-929a-4fc5-be95-078d99d57774\",\n    " +
			"\"creationTimestamp\": \"2020-08-27T20:20:28Z\"\n  },\n  " +
			"\"provisioner\": \"kubernetes.io/vsphere-volume\",\n  " +
			"\"parameters\": {\n    \"hostFailuresToTolerate\": \"2\"\n  },\n  " +
			"\"reclaimPolicy\": \"Delete\",\n  \"volumeBindingMode\": \"Immediate\"\n}"),
	}
	admissionResponse := validateStorageClass(ctx, &admissionReview)
	if admissionResponse.Result != nil || !admissionResponse.Allowed {
		t.Fatalf("TestValidateStorageClassForValidStorageClass failed. "+
			"admissionReview.Request: %v, admissionResponse: %v", admissionReview.Request, admissionResponse)
	}
	t.Log("TestValidateStorageClassForValidStorageClass Passed")
}
