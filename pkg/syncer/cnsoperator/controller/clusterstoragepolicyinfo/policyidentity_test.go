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

package clusterstoragepolicyinfo

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/pbm"
	pbmmethods "github.com/vmware/govmomi/pbm/methods"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25/soap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

type identityProfileTransport struct {
	t       *testing.T
	name    string
	missing bool
	err     error
	calls   int
}

func (r *identityProfileTransport) RoundTrip(_ context.Context, req, res soap.HasFault) error {
	r.calls++
	require.IsType(r.t, &pbmmethods.PbmQueryProfileDetailsBody{}, req)
	if r.err != nil {
		return r.err
	}
	response := &pbmtypes.PbmQueryProfileDetailsResponse{}
	if !r.missing {
		response.Returnval = []pbmtypes.PbmProfileDetails{{
			Profile: &pbmtypes.PbmCapabilityProfile{
				PbmProfile:       pbmtypes.PbmProfile{ProfileId: pbmtypes.PbmProfileId{UniqueId: "p1"}},
				K8sCompliantName: r.name, ProfileCategory: "REQUIREMENT",
			},
		}}
	}
	res.(*pbmmethods.PbmQueryProfileDetailsBody).Res = response
	return nil
}

func TestResolveClusterSPIPolicyIdentity(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	configPath := filepath.Join(t.TempDir(), "vsphere.conf")
	require.NoError(t, os.WriteFile(configPath, []byte(
		"[Global]\ncluster-id = \"test\"\n"+
			"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
			"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", configPath)
	model := simulator.VPX()
	require.NoError(t, model.Create())
	t.Cleanup(model.Remove)
	model.Service.TLS = new(tls.Config)
	model.Service.RegisterEndpoints = true
	server := model.Service.NewServer()
	t.Cleanup(server.Close)
	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)
	vc := &cnsvsphere.VirtualCenter{
		Config: &cnsvsphere.VirtualCenterConfig{
			Host: commontypes.NewFQDN(server.URL.Hostname()), Port: port, Insecure: true,
			Username: "user", Password: "pass",
		},
		ClientMutex: &sync.Mutex{},
	}
	require.NoError(t, vc.Connect(ctx))

	for _, tc := range []struct {
		name                string
		resolved            string
		missing             bool
		queryErr            error
		skip                bool
		wantErr             bool
		wantVolumeClassName string
		initialDeleted      bool
		noBackingResources  bool
		deletedBackingID    bool
	}{
		{name: "canonical", resolved: "gold", wantVolumeClassName: "gold"},
		{name: "full sync without backing resources", resolved: "gold",
			noBackingResources: true, wantVolumeClassName: "gold"},
		{name: "deleted full-sync policy without backing resources", missing: true,
			noBackingResources: true, skip: true},
		{name: "policy restored", resolved: "gold", wantVolumeClassName: "gold", initialDeleted: true},
		{name: "upgraded alias", resolved: "silver", skip: true, wantVolumeClassName: "silver"},
		{name: "deleted policy clears volume class name", missing: true, skip: true},
		{name: "reused compliant name does not recover deleted backing ID", resolved: "gold",
			deletedBackingID: true, initialDeleted: true, skip: true},
		{name: "reused compliant name marks old backing ID deleted", resolved: "gold",
			deletedBackingID: true, skip: true},
		{name: "query failure", queryErr: assert.AnError, wantErr: true, wantVolumeClassName: "old-name"},
		{name: "empty compliant name", wantErr: true, wantVolumeClassName: "old-name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := &identityProfileTransport{t: t, name: tc.resolved, missing: tc.missing, err: tc.queryErr}
			vc.PbmClient = &pbm.Client{RoundTripper: rt}
			instance := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
				ObjectMeta: metav1.ObjectMeta{Name: "gold", Annotations: map[string]string{
					"other": "preserved",
				}},
				Status: clusterspiv1alpha1.ClusterStoragePolicyInfoStatus{
					Error: "previous error", StoragePolicyDeleted: tc.initialDeleted, VolumeClassName: "old-name",
				},
			}
			cli := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(instance).
				WithStatusSubresource(instance).Build()
			require.NoError(t, cli.Get(ctx, types.NamespacedName{Name: "gold"}, instance))
			// No event recorder: the helper must leave success/error status
			// persistence and event recording to the caller.
			r := &ReconcileClusterStoragePolicyInfo{client: cli}
			origStatus := instance.Status.DeepCopy()
			backingPolicyID := "p1"
			if tc.deletedBackingID {
				backingPolicyID = "deleted-policy"
			}
			if tc.noBackingResources {
				backingPolicyID, err = storagePolicyIDFromBackingResources(nil, nil)
				require.NoError(t, err)
				require.Empty(t, backingPolicyID)
			}
			profile, skip, err := r.resolveClusterSPIPolicyIdentity(ctx, instance, backingPolicyID, vc)
			assert.Equal(t, 1, rt.calls)
			assert.Equal(t, tc.skip, skip)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if !tc.wantErr {
				assert.Equal(t, tc.missing || tc.deletedBackingID, instance.Status.StoragePolicyDeleted)
			}
			if !tc.wantErr && !tc.skip {
				require.NotNil(t, profile)
				assert.Equal(t, "p1", profile.ID)
			} else {
				assert.Nil(t, profile)
			}
			got := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
			require.NoError(t, cli.Get(ctx, types.NamespacedName{Name: "gold"}, got))
			assert.Equal(t, tc.wantVolumeClassName, instance.Status.VolumeClassName)
			assert.Equal(t, "old-name", got.Status.VolumeClassName, "helper must not persist status")
			assert.Equal(t, "preserved", got.Annotations["other"])
			assert.Equal(t, "previous error", got.Status.Error)
			assert.Equal(t, tc.initialDeleted, got.Status.StoragePolicyDeleted, "helper must not persist status")
			r.recorder = record.NewFakeRecorder(10)
			if tc.wantErr {
				require.NoError(t, r.setClusterSPIError(ctx, instance, origStatus, err.Error()))
			} else {
				require.NoError(t, r.setClusterSPISuccess(ctx, instance, origStatus, "identity resolved"))
			}
			require.NoError(t, cli.Get(ctx, types.NamespacedName{Name: "gold"}, got))
			assert.Equal(t, tc.wantVolumeClassName, got.Status.VolumeClassName)
			if !tc.wantErr {
				assert.Equal(t, tc.missing || tc.deletedBackingID, got.Status.StoragePolicyDeleted)
			}
		})
	}
}
