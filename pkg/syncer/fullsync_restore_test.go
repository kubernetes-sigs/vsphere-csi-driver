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
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

const (
	testRestoredVolumeID = "3c6b9a1e-5d2f-4f4e-9b8a-2a71c0e5d9f4"
	testRestoreStamp     = "9b1f0c3d-4e5a-4b6c-8d7e-0f1a2b3c7a2e"
)

// restoreFullSyncService records the restore calls made by the full sync.
type restoreFullSyncService struct {
	cnsvolumeinfo.VolumeInfoService
	result      cnsvolumeinfo.RestoreResult
	handoverErr error
	markErr     error
	handovers   []cnsvolumeinfo.RestoredVolumeInfo
	marked      []string
}

func (s *restoreFullSyncService) CreateOrUpdateRestoredVolumeInfo(_ context.Context,
	info cnsvolumeinfo.RestoredVolumeInfo) (cnsvolumeinfo.RestoreResult, error) {
	s.handovers = append(s.handovers, info)
	return s.result, s.handoverErr
}

func (s *restoreFullSyncService) MarkRestoredSnapshotSizeMeasured(_ context.Context, volumeID string) error {
	s.marked = append(s.marked, volumeID)
	return s.markErr
}

func useRestoreFullSyncService(t *testing.T, s *restoreFullSyncService) {
	t.Helper()
	saved := volumeInfoService
	volumeInfoService = s
	t.Cleanup(func() { volumeInfoService = saved })
}

func testRestoredPV(annotations map[string]string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "static-pv-" + testRestoredVolumeID, Annotations: annotations},
		Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: testRestoredVolumeID},
		}},
	}
}

func restoredPVAnnotations() map[string]string {
	return map[string]string{
		cnsvolumeinfov1alpha1.AnnSnapServiceRestore:          "true",
		cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume: testRestoreStamp,
	}
}

func testRestoredPVC(storageClass *string) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-pvc", Namespace: "ns1"},
		Spec:       v1.PersistentVolumeClaimSpec{StorageClassName: storageClass},
	}
}

func TestIsRestoredPV(t *testing.T) {
	assert.True(t, isRestoredPV(testRestoredPV(restoredPVAnnotations())))
	assert.False(t, isRestoredPV(testRestoredPV(nil)))
	assert.False(t, isRestoredPV(testRestoredPV(map[string]string{
		cnsvolumeinfov1alpha1.AnnSnapServiceRestore: "false"})))
}

func TestHandoverRestoredVolumeInFullSync(t *testing.T) {
	ctx := context.Background()
	gold := "sc-gold"
	capacity := resource.MustParse("20Gi")
	vc := commontypes.NewFQDN("vc1.example.com")
	now := metav1.Now()
	deleting := testRestoredPV(restoredPVAnnotations())
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"kubernetes.io/pv-protection"}
	for _, tc := range []struct {
		name          string
		listed        *v1.PersistentVolume
		live          *v1.PersistentVolume
		storageClass  *string
		result        cnsvolumeinfo.RestoreResult
		handoverErr   error
		wantHandovers int
	}{
		{name: "runs the handover with the stamp from the PV",
			listed: testRestoredPV(restoredPVAnnotations()), live: testRestoredPV(restoredPVAnnotations()),
			storageClass: &gold, wantHandovers: 1,
			result: cnsvolumeinfo.RestoreResult{Condition: metav1.Condition{Status: metav1.ConditionFalse,
				Reason: cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound}}},
		{name: "handover error is retried by the next full sync",
			listed: testRestoredPV(restoredPVAnnotations()), live: testRestoredPV(restoredPVAnnotations()),
			storageClass: &gold, handoverErr: errors.New("CRD not served"), wantHandovers: 1},
		{name: "PV gone", listed: testRestoredPV(restoredPVAnnotations()), storageClass: &gold},
		{name: "PV being deleted", listed: testRestoredPV(restoredPVAnnotations()), live: deleting,
			storageClass: &gold},
		{name: "PV without stamp",
			listed:       testRestoredPV(map[string]string{cnsvolumeinfov1alpha1.AnnSnapServiceRestore: "true"}),
			live:         testRestoredPV(map[string]string{cnsvolumeinfov1alpha1.AnnSnapServiceRestore: "true"}),
			storageClass: &gold},
		{name: "PVC without storage class", listed: testRestoredPV(restoredPVAnnotations()),
			live: testRestoredPV(restoredPVAnnotations())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &restoreFullSyncService{result: tc.result, handoverErr: tc.handoverErr}
			useRestoreFullSyncService(t, service)
			k8sClient := k8sfake.NewClientset()
			if tc.live != nil {
				k8sClient = k8sfake.NewClientset(tc.live)
			}
			handoverRestoredVolumeInFullSync(ctx, k8sClient, vc, tc.listed, testRestoredPVC(tc.storageClass),
				"policy-gold", &capacity)
			require.Len(t, service.handovers, tc.wantHandovers)
			if tc.wantHandovers == 0 {
				return
			}
			assert.Equal(t, cnsvolumeinfo.RestoredVolumeInfo{
				VolumeID:         testRestoredVolumeID,
				Namespace:        "ns1",
				StoragePolicyID:  "policy-gold",
				StorageClassName: gold,
				VCenter:          vc.String(),
				Capacity:         &capacity,
				RestoredBy:       testRestoreStamp,
			}, service.handovers[0])
		})
	}
}

func TestMarkRestoredSnapshotSizesMeasured(t *testing.T) {
	withCondition := func(status metav1.ConditionStatus) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
		info := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
		if status != "" {
			info.Status.Conditions = []metav1.Condition{{
				Type: cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored, Status: status, Reason: "Reason",
			}}
		}
		return info
	}
	service := &restoreFullSyncService{markErr: errors.New("conflicts exhausted")}
	useRestoreFullSyncService(t, service)
	markRestoredSnapshotSizesMeasured(context.Background(), map[string]*cnsvolumeinfov1alpha1.CNSVolumeInfo{
		"false-1": withCondition(metav1.ConditionFalse),
		"false-2": withCondition(metav1.ConditionFalse),
		"true":    withCondition(metav1.ConditionTrue),
		"none":    withCondition(""),
		"migration": {Status: cnsvolumeinfov1alpha1.CNSVolumeInfoStatus{MigrationConditions: []metav1.Condition{{
			Type: cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored, Status: metav1.ConditionFalse}}}},
	})
	sort.Strings(service.marked)
	assert.Equal(t, []string{"false-1", "false-2"}, service.marked,
		"only CVIs with a False condition are checked, and one failure does not stop the others")
}
