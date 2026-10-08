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

package wcpguest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	testclient "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	fakesnapshotclient "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fakesnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

const (
	insufficientQuotaMsg = "Operation denied, failed to validate against storage quota for resource sv-pvc, " +
		"namespace sv-ns with err Operation denied due to insufficient storage quota for storage policy " +
		"resource sv-pvc, namespace sv-ns"
	scNotAssociatedMsg = "Operation denied, storageclass gold is not associated with sv-ns namespace"
)

// forbiddenErr returns a Forbidden StatusError with the given message, as returned by the API server.
func forbiddenErr(resource, message string) *apierrors.StatusError {
	err := apierrors.NewForbidden(schema.GroupResource{Resource: resource}, "obj", errors.New("x"))
	err.ErrStatus.Message = message
	return err
}

// quotaWebhookDenial returns the error the API server returns when the given supervisor storage
// quota webhook denies a request with the given message.
func quotaWebhookDenial(resource, webhook, message string) *apierrors.StatusError {
	return forbiddenErr(resource, fmt.Sprintf("admission webhook %q denied the request: %s", webhook, message))
}

func TestIsStorageQuotaWebhookDenial(t *testing.T) {
	quotaDenied := quotaWebhookDenial("persistentvolumeclaims", "validate-quota-on-create.k8s.io",
		insufficientQuotaMsg)
	otherWebhookDenied := quotaWebhookDenial("persistentvolumeclaims", "other.k8s.io", "no")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"quota webhook denial", quotaDenied, true},
		{"quota webhook denial for other reason", quotaWebhookDenial("persistentvolumeclaims",
			"validate-quota-on-create.k8s.io", scNotAssociatedMsg), true},
		{"quota update webhook denial", quotaWebhookDenial("persistentvolumeclaims",
			"validate-quota-on-update.k8s.io", insufficientQuotaMsg), true},
		{"wrapped quota webhook denial", fmt.Errorf("patch failed: %w", quotaDenied), true},
		{"other webhook denial", otherWebhookDenied, false},
		{"RBAC forbidden", forbiddenErr("persistentvolumeclaims", "persistentvolumeclaims is forbidden: "+
			"User \"system:serviceaccount:sv-ns:pvcsi\" cannot create resource \"persistentvolumeclaims\""), false},
		{"namespace terminating", forbiddenErr("persistentvolumeclaims", "persistentvolumeclaims \"obj\" is "+
			"forbidden: unable to create new content in namespace sv-ns because it is being terminated"), false},
		{"non forbidden error", apierrors.NewInternalError(errors.New(
			"failed calling webhook \"validate-quota-on-create.k8s.io\"")), false},
		{"plain error", errors.New("validate-quota-on-create.k8s.io denied the request"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isStorageQuotaWebhookDenial(tt.err); got != tt.want {
				t.Errorf("isStorageQuotaWebhookDenial() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStorageQuotaDeniedError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  []string
	}{
		{
			name: "insufficient quota",
			err: errors.New("Operation denied due to insufficient storage quota for storage policy " +
				"resource r1, namespace sv-ns"),
			wantCode: codes.ResourceExhausted,
			wantMsg:  []string{"insufficient storage quota in vSphere Namespace", "contact the vSphere administrator"},
		},
		{
			name:     "insufficient quota for StoragePolicyReservation",
			err:      errors.New("Insufficient storage quota for StorageClass gold, namespace sv-ns."),
			wantCode: codes.ResourceExhausted,
			wantMsg:  []string{"insufficient storage quota in vSphere Namespace"},
		},
		{
			name: "other webhook rejection",
			err: errors.New("admission webhook \"validate-quota-on-create.k8s.io\" denied the request: " +
				"storageclass gold is not associated with sv-ns namespace"),
			wantCode: codes.FailedPrecondition,
			wantMsg: []string{"rejected by the storage quota validation",
				"Reason: storageclass gold is not associated with sv-ns namespace"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storageQuotaDeniedError("Volume creation", "PVC default/pvc-1", "sv-ns", "gold", "1024Mi", tt.err)
			st, ok := status.FromError(err)
			if !ok || st.Code() != tt.wantCode {
				t.Fatalf("expected %v status, got %v", tt.wantCode, err)
			}
			for _, want := range append(tt.wantMsg, "Volume creation", "PVC default/pvc-1", "sv-ns", "gold",
				"1024Mi") {
				if !strings.Contains(st.Message(), want) {
					t.Errorf("message %q does not contain %q", st.Message(), want)
				}
			}
		})
	}
}

func TestStorageQuotaDeniedAnnotation(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason string
	}{
		{"insufficient quota", errors.New(insufficientQuotaMsg), "InsufficientQuota: "},
		{"insufficient quota for StoragePolicyReservation",
			errors.New("Insufficient storage quota for StorageClass gold, namespace sv-ns."), "InsufficientQuota: "},
		{"StorageClass not associated", errors.New(scNotAssociatedMsg), "StorageClassNotAssociated: "},
		{"StorageClass not associated for StoragePolicyReservation",
			errors.New("StorageClass gold is not associated with namespace sv-ns"), "StorageClassNotAssociated: "},
		{"other rejection", errors.New("Operation denied, StoragePolicyReservation spr-1 not found in " +
			"namespace sv-ns"), "QuotaValidationFailed: "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := storageQuotaDeniedAnnotation(tt.err)
			if !strings.HasPrefix(got, tt.wantReason) {
				t.Fatalf("annotation %q does not start with %q", got, tt.wantReason)
			}
			// The annotation must not include the supervisor details of the webhook error.
			if strings.Contains(got, "sv-ns") || strings.Contains(got, "gold") {
				t.Fatalf("annotation %q includes supervisor details", got)
			}
		})
	}
}

func TestGuestPVCProvisioningErrorAnnotation(t *testing.T) {
	ctx := context.Background()
	guestClient := testclient.NewSimpleClientset(&corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: "default",
			Annotations: map[string]string{"other": "value"}},
	})
	getAnnotations := func() map[string]string {
		pvc, err := guestClient.CoreV1().PersistentVolumeClaims("default").Get(ctx, "pvc-1", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get PVC: %v", err)
		}
		return pvc.Annotations
	}

	setGuestPVCProvisioningError(ctx, guestClient, "default", "pvc-1", "quota exceeded")
	if got := getAnnotations()[common.AnnKeyProvisioningError]; got != "quota exceeded" {
		t.Fatalf("expected annotation %q, got %q", "quota exceeded", got)
	}

	clearGuestPVCProvisioningError(ctx, guestClient, "default", "pvc-1")
	annotations := getAnnotations()
	if _, ok := annotations[common.AnnKeyProvisioningError]; ok {
		t.Fatalf("expected annotation %s to be removed", common.AnnKeyProvisioningError)
	}
	if annotations["other"] != "value" {
		t.Fatalf("expected other annotations to be retained, got %v", annotations)
	}

	// Clearing when the annotation is absent, or with missing PVC details, must not fail.
	clearGuestPVCProvisioningError(ctx, guestClient, "default", "pvc-1")
	clearGuestPVCProvisioningError(ctx, guestClient, "", "")
}

// quotaCreateRequest returns a block CreateVolumeRequest for the guest PVC used by the quota tests.
func quotaCreateRequest() *csi.CreateVolumeRequest {
	return fvsCreateRequest(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "")
}

// guestPVCProvisioningError returns the AnnKeyProvisioningError annotation on the guest PVC
// used by the quota tests, and whether it is set.
func guestPVCProvisioningError(t *testing.T, c *controller) (string, bool) {
	t.Helper()
	pvc, err := c.guestClient.CoreV1().PersistentVolumeClaims(fvsCreateGuestPVCNamespace).Get(
		context.Background(), fvsCreateGuestPVCName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get guest PVC: %v", err)
	}
	msg, ok := pvc.Annotations[common.AnnKeyProvisioningError]
	return msg, ok
}

// TestCreateVolume_SupervisorPVCCreateForbidden verifies the error code returned by CreateVolume
// and the guest PVC annotation when the supervisor PVC creation is rejected with Forbidden.
func TestCreateVolume_SupervisorPVCCreateForbidden(t *testing.T) {
	tests := []struct {
		name           string
		createErr      error
		wantCode       codes.Code
		wantAnnotation string // empty means the annotation must not be set
	}{
		{
			name: "insufficient quota",
			createErr: quotaWebhookDenial("persistentvolumeclaims", "validate-quota-on-create.k8s.io",
				insufficientQuotaMsg),
			wantCode:       codes.ResourceExhausted,
			wantAnnotation: "InsufficientQuota: ",
		},
		{
			name: "quota webhook rejection for other reason",
			createErr: quotaWebhookDenial("persistentvolumeclaims", "validate-quota-on-create.k8s.io",
				scNotAssociatedMsg),
			wantCode:       codes.FailedPrecondition,
			wantAnnotation: "StorageClassNotAssociated: ",
		},
		{
			name: "unrelated forbidden error",
			createErr: forbiddenErr("persistentvolumeclaims", "persistentvolumeclaims is forbidden: "+
				"User \"system:serviceaccount:sv-ns:pvcsi\" cannot create resource \"persistentvolumeclaims\""),
			wantCode: codes.Internal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newFVSCreateController(t, makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
				testStorageClass, ""))
			c.supervisorClient.(*testclient.Clientset).PrependReactor("create", "persistentvolumeclaims",
				func(action ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, tt.createErr
				})

			_, err := c.CreateVolume(context.Background(), quotaCreateRequest())
			if status.Code(err) != tt.wantCode {
				t.Fatalf("expected %v, got %v", tt.wantCode, err)
			}
			msg, ok := guestPVCProvisioningError(t, c)
			if tt.wantAnnotation == "" {
				if ok {
					t.Fatalf("expected annotation %s to be unset, got %q", common.AnnKeyProvisioningError, msg)
				}
				return
			}
			if !strings.HasPrefix(msg, tt.wantAnnotation) {
				t.Fatalf("annotation %q does not start with %q", msg, tt.wantAnnotation)
			}
		})
	}
}

// TestCreateVolume_SuccessClearsProvisioningError verifies that the provisioning error recorded on
// the guest PVC by an earlier rejected attempt is removed once the volume is created.
func TestCreateVolume_SuccessClearsProvisioningError(t *testing.T) {
	guestPVC := makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace, testStorageClass, "")
	guestPVC.Annotations = map[string]string{common.AnnKeyProvisioningError: "quota exceeded"}
	c := newFVSCreateController(t, guestPVC)

	if _, _, err := runCreateVolumeAndBindSupervisorPVC(t, c, quotaCreateRequest()); err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	if msg, ok := guestPVCProvisioningError(t, c); ok {
		t.Fatalf("expected annotation %s to be removed, got %q", common.AnnKeyProvisioningError, msg)
	}
}

// TestControllerExpandForFVSFileVolume_QuotaDenied verifies that a supervisor PVC patch rejected
// by the storage quota webhook is reported as ResourceExhausted and recorded on the guest PVC, and
// that the annotation is removed once the volume is expanded.
func TestControllerExpandForFVSFileVolume_QuotaDenied(t *testing.T) {
	ctx := context.Background()
	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "5Gi", "5Gi")
	pvc.Annotations = map[string]string{common.AnnKeyGuestClusterPvc: `{"name":"` + fvsCreateGuestPVCName +
		`","namespace":"` + fvsCreateGuestPVCNamespace + `"}`}
	c := fvsExpandController(t, pvc, pvc)
	c.guestClient = testclient.NewClientset(makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
		common.StorageClassVsanFileServicePolicy, ""))
	c.supervisorRuntimeClient = ctrlclientfake.NewClientBuilder().
		WithScheme(fvsExpandScheme(t)).
		WithObjects(pvc.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli ctrlclient.WithWatch, obj ctrlclient.Object,
				patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
				return quotaWebhookDenial("persistentvolumeclaims", "validate-quota-on-update.k8s.io",
					insufficientQuotaMsg)
			},
		}).
		Build()

	_, _, err := controllerExpandForFVSFileVolume(ctx, expandReqRWX(fvsExpandPVC, gi10), pvc.DeepCopy(), c)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
	if !strings.Contains(err.Error(), "Volume expansion for \"volume "+fvsExpandPVC+"\"") {
		t.Fatalf("unexpected error message: %v", err)
	}
	msg, ok := guestPVCProvisioningError(t, c)
	if !ok || !strings.HasPrefix(msg, "InsufficientQuota: ") {
		t.Fatalf("expected InsufficientQuota annotation, got %q (set: %v)", msg, ok)
	}

	// A later expansion that succeeds removes the annotation.
	expanded := pvc.DeepCopy()
	expanded.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("10Gi")
	expanded.Status.Capacity[corev1.ResourceStorage] = resource.MustParse("10Gi")
	if _, _, err := controllerExpandForFVSFileVolume(ctx, expandReqRWX(fvsExpandPVC, gi10), expanded, c); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg, ok := guestPVCProvisioningError(t, c); ok {
		t.Fatalf("expected annotation %s to be removed, got %q", common.AnnKeyProvisioningError, msg)
	}
}

// TestCreateSnapshot_QuotaDenied verifies that a supervisor VolumeSnapshot creation rejected by
// the storage quota webhook is reported as ResourceExhausted.
func TestCreateSnapshot_QuotaDenied(t *testing.T) {
	ctx := context.Background()
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to build fake CO: %v", err)
	}
	oldCO := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = co
	defer func() { commonco.ContainerOrchestratorUtility = oldCO }()

	sc := "gold"
	supervisorClient := testclient.NewClientset(&corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "source-pvc", Namespace: testNamespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	})
	snapshotClient := fakesnapshotclient.NewClientset()
	snapshotClient.PrependReactor("create", "volumesnapshots",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, quotaWebhookDenial("volumesnapshots", "validate-quota-on-create.k8s.io",
				insufficientQuotaMsg)
		})
	c := &controller{
		supervisorClient:            supervisorClient,
		supervisorSnapshotterClient: snapshotClient,
		supervisorNamespace:         testNamespace,
		tanzukubernetesClusterUID:   "tkc-uid",
	}

	_, err = c.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           "snapshot-mysnap",
		SourceVolumeId: "source-pvc",
		Parameters: map[string]string{
			common.VolumeSnapshotNameKey:      "mysnap",
			common.VolumeSnapshotNamespaceKey: "guest-ns",
		},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
	for _, want := range []string{"VolumeSnapshot guest-ns/mysnap", "\"gold\"", "1Gi"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err.Error(), want)
		}
	}
}
