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

package admissionhandler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// fakeSpreadFSS answers the two feature checks podVolumeSpreadFeatureState makes and records
// which were asked. Any other method of the interface panics, which is what we want: the
// function under test must not need anything else.
type fakeSpreadFSS struct {
	commonco.COCommonInterface
	pvcsiFSS, effective bool
	effectiveAsked      bool
	askedFor            []string
}

func (f *fakeSpreadFSS) IsPVCSIFSSEnabled(_ context.Context, featureName string) bool {
	f.askedFor = append(f.askedFor, featureName)
	return f.pvcsiFSS
}

func (f *fakeSpreadFSS) IsFSSEnabled(_ context.Context, featureName string) bool {
	f.askedFor = append(f.askedFor, featureName)
	f.effectiveAsked = true
	return f.effective
}

func TestPodVolumeSpreadFeatureState(t *testing.T) {
	tests := []struct {
		name                   string
		pvcsiFSS, effective    bool
		wantFSS, wantSupported bool
		wantCapabilityLookedUp bool
	}{
		{name: "FSS off", pvcsiFSS: false, effective: true},
		{name: "FSS on, capability missing", pvcsiFSS: true, effective: false,
			wantFSS: true, wantCapabilityLookedUp: true},
		{name: "FSS on, capability present", pvcsiFSS: true, effective: true,
			wantFSS: true, wantSupported: true, wantCapabilityLookedUp: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			co := &fakeSpreadFSS{pvcsiFSS: tc.pvcsiFSS, effective: tc.effective}
			gotFSS, gotSupported := podVolumeSpreadFeatureState(context.Background(), co)
			if gotFSS != tc.wantFSS || gotSupported != tc.wantSupported {
				t.Errorf("got (fss=%v, supported=%v), want (fss=%v, supported=%v)",
					gotFSS, gotSupported, tc.wantFSS, tc.wantSupported)
			}
			if co.effectiveAsked != tc.wantCapabilityLookedUp {
				t.Errorf("capability looked up = %v, want %v", co.effectiveAsked, tc.wantCapabilityLookedUp)
			}
			for _, name := range co.askedFor {
				if name != common.PodVolumeSpreadFSS {
					t.Errorf("asked for feature %q, want only %q", name, common.PodVolumeSpreadFSS)
				}
			}
		})
	}
}

// TestPodVolumeSpreadFSSIsPairedWithSharedDiskCapability pins the mapping that makes the
// effective state of the pvCSI FSS depend on the supervisor capability.
func TestPodVolumeSpreadFSSIsPairedWithSharedDiskCapability(t *testing.T) {
	got, ok := common.WCPFeatureStateAssociatedWithPVCSI[common.PodVolumeSpreadFSS]
	if !ok || got != common.SharedDiskFss {
		t.Errorf("WCPFeatureStateAssociatedWithPVCSI[%q] = %q, %v; want %q",
			common.PodVolumeSpreadFSS, got, ok, common.SharedDiskFss)
	}
	if _, ok := common.WCPFeatureStatesSupportsLateEnablement[common.SharedDiskFss]; !ok {
		t.Errorf("capability %q must support late enablement", common.SharedDiskFss)
	}
}

func setPodVolumeSpreadFlags(t *testing.T, fssEnabled, supported bool) {
	t.Helper()
	origFSS, origSupported := featureIsPodVolumeSpreadFSSEnabled, featureIsPodVolumeSpreadSupported
	featureIsPodVolumeSpreadFSSEnabled, featureIsPodVolumeSpreadSupported = fssEnabled, supported
	t.Cleanup(func() {
		featureIsPodVolumeSpreadFSSEnabled, featureIsPodVolumeSpreadSupported = origFSS, origSupported
	})
}

func podAdmissionRequest(t *testing.T, op admissionv1.Operation, labels map[string]string) admission.Request {
	t.Helper()
	pod := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "db-0", Namespace: "app", Labels: labels},
	}
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("failed to marshal pod: %v", err)
	}
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Kind:      metav1.GroupVersionKind{Version: "v1", Kind: "Pod"},
		Operation: op,
		Name:      pod.Name,
		Namespace: pod.Namespace,
		Object:    runtime.RawExtension{Raw: raw},
	}}
}

func TestGuestMutationWebhookPodVolumeSpread(t *testing.T) {
	spread := map[string]string{common.PodVolumePlacementLabel: common.PodVolumePlacementSpread}
	tests := []struct {
		name           string
		fss, supported bool
		op             admissionv1.Operation
		labels         map[string]string
		wantAllowed    bool
	}{
		{name: "FSS off: labelled pod is admitted, label ignored",
			op: admissionv1.Create, labels: spread, wantAllowed: true},
		{name: "FSS on, capability missing: labelled pod is rejected",
			fss: true, op: admissionv1.Create, labels: spread, wantAllowed: false},
		{name: "FSS on, capability present: labelled pod is admitted",
			fss: true, supported: true, op: admissionv1.Create, labels: spread, wantAllowed: true},
		{name: "FSS on, capability missing: pod without the label is admitted",
			fss: true, op: admissionv1.Create, labels: map[string]string{"app": "db"}, wantAllowed: true},
		{name: "FSS on, capability missing: pod with no labels is admitted",
			fss: true, op: admissionv1.Create, wantAllowed: true},
		{name: "FSS on, capability missing: another label value is admitted",
			fss: true, op: admissionv1.Create,
			labels: map[string]string{common.PodVolumePlacementLabel: "default"}, wantAllowed: true},
		{name: "FSS on, capability missing: updates are not checked",
			fss: true, op: admissionv1.Update, labels: spread, wantAllowed: true},
		{name: "FSS on, capability missing: deletes are not checked",
			fss: true, op: admissionv1.Delete, labels: spread, wantAllowed: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setPodVolumeSpreadFlags(t, tc.fss, tc.supported)
			resp := (&CSIGuestMutationWebhook{}).Handle(context.Background(),
				podAdmissionRequest(t, tc.op, tc.labels))
			if resp.Allowed != tc.wantAllowed {
				t.Fatalf("allowed = %v, want %v (result: %+v)", resp.Allowed, tc.wantAllowed, resp.Result)
			}
			if len(resp.Patches) != 0 || resp.Patch != nil {
				t.Errorf("phase 1 must not mutate the pod, got patches %v", resp.Patches)
			}
			if !tc.wantAllowed {
				if resp.Result == nil || !strings.Contains(resp.Result.Message, common.SharedDiskFss) ||
					!strings.Contains(resp.Result.Message, common.PodVolumePlacementLabel) {
					t.Errorf("rejection must name the label and the missing capability, got %+v", resp.Result)
				}
			}
		})
	}
}

func TestAdmitPodVolumeSpreadMalformedPod(t *testing.T) {
	setPodVolumeSpreadFlags(t, true, true)
	req := podAdmissionRequest(t, admissionv1.Create, nil)
	req.Object.Raw = []byte("{not json")
	resp := admitPodVolumeSpread(context.Background(), req)
	if resp.Allowed {
		t.Fatal("a pod that cannot be decoded must not be admitted")
	}
	if resp.Result == nil || resp.Result.Code != http.StatusBadRequest {
		t.Errorf("result = %+v, want HTTP 400", resp.Result)
	}
}

// TestGuestMutationWebhookOtherKindsUnaffected checks the pod branch does not change how
// other kinds are handled.
func TestGuestMutationWebhookOtherKindsUnaffected(t *testing.T) {
	setPodVolumeSpreadFlags(t, true, false)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Kind:      metav1.GroupVersionKind{Version: "v1", Kind: "ConfigMap"},
		Operation: admissionv1.Create,
	}}
	if resp := (&CSIGuestMutationWebhook{}).Handle(context.Background(), req); !resp.Allowed {
		t.Errorf("a ConfigMap must be admitted, got %+v", resp.Result)
	}
}

func TestAppendPodVolumeSpreadLateEnablement(t *testing.T) {
	existing := []string{common.LinkedCloneSupport}
	tests := []struct {
		name           string
		fss, supported bool
		want           []string
	}{
		{"FSS off", false, false, []string{common.LinkedCloneSupport}},
		{"FSS on, capability present", true, true, []string{common.LinkedCloneSupport}},
		{"FSS on, capability missing", true, false, []string{common.LinkedCloneSupport, common.SharedDiskFss}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := appendPodVolumeSpreadLateEnablement(append([]string{}, existing...), tc.fss, tc.supported)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
