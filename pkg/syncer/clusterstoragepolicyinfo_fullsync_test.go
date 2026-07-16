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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func fullSyncTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, apis.SchemeBuilder.AddToScheme(s))
	return s
}

func TestEnsureClusterStoragePolicyInfoCRsExist_CreatesMissingCRs(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := fullSyncTestScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	profiles := []cnsvsphere.ProfileDetail{
		{ID: "policy-1", Name: "Policy One", K8sCompliantName: "policy-one"},
		{ID: "policy-2", Name: "Policy Two", K8sCompliantName: "policy-two"},
	}

	ensureClusterStoragePolicyInfoCRsExist(ctx, cli, profiles)

	for _, name := range []string{"policy-one", "policy-two"} {
		got := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
		err := cli.Get(ctx, apitypes.NamespacedName{Name: name}, got)
		require.NoError(t, err, "expected ClusterStoragePolicyInfo %q to be created", name)
		assert.Empty(t, got.OwnerReferences, "expected no owner references on a bare CR created from full sync")
	}
}

func TestEnsureClusterStoragePolicyInfoCRsExist_SkipsExistingCR(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := fullSyncTestScheme(t)

	existing := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name: "policy-one",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "storage.k8s.io/v1", Kind: "StorageClass", Name: "sc-1"},
			},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

	profiles := []cnsvsphere.ProfileDetail{
		{ID: "policy-1", Name: "Policy One", K8sCompliantName: "policy-one"},
	}

	ensureClusterStoragePolicyInfoCRsExist(ctx, cli, profiles)

	got := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
	require.NoError(t, cli.Get(ctx, apitypes.NamespacedName{Name: "policy-one"}, got))
	assert.Equal(t, existing.OwnerReferences, got.OwnerReferences,
		"existing CR with owner references must not be touched by full sync")
}

func TestEnsureClusterStoragePolicyInfoCRsExist_SkipsProfileWithoutK8sCompliantName(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := fullSyncTestScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	profiles := []cnsvsphere.ProfileDetail{
		{ID: "policy-1", Name: "Policy One", K8sCompliantName: ""},
	}

	ensureClusterStoragePolicyInfoCRsExist(ctx, cli, profiles)

	list := &clusterspiv1alpha1.ClusterStoragePolicyInfoList{}
	require.NoError(t, cli.List(ctx, list))
	assert.Empty(t, list.Items, "expected no CR to be created for a profile with no K8s compliant name")
}

// erroringCreateClient wraps a client.Client and forces Create calls to fail, to verify that a
// per-policy Create error does not stop processing of the remaining policies.
type erroringCreateClient struct {
	client.Client
	failNames map[string]bool
}

func (e *erroringCreateClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if e.failNames[obj.GetName()] {
		return assert.AnError
	}
	return e.Client.Create(ctx, obj, opts...)
}

func TestEnsureClusterStoragePolicyInfoCRsExist_ContinuesAfterPerPolicyCreateError(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := fullSyncTestScheme(t)
	base := fake.NewClientBuilder().WithScheme(scheme).Build()
	cli := &erroringCreateClient{Client: base, failNames: map[string]bool{"policy-one": true}}

	profiles := []cnsvsphere.ProfileDetail{
		{ID: "policy-1", Name: "Policy One", K8sCompliantName: "policy-one"},
		{ID: "policy-2", Name: "Policy Two", K8sCompliantName: "policy-two"},
	}

	ensureClusterStoragePolicyInfoCRsExist(ctx, cli, profiles)

	// The failing policy's CR should not exist...
	err := base.Get(ctx, apitypes.NamespacedName{Name: "policy-one"}, &clusterspiv1alpha1.ClusterStoragePolicyInfo{})
	assert.Error(t, err)

	// ...but the remaining policy should still have been processed and created.
	err = base.Get(ctx, apitypes.NamespacedName{Name: "policy-two"}, &clusterspiv1alpha1.ClusterStoragePolicyInfo{})
	assert.NoError(t, err)
}
