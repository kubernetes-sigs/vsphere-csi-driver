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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// clusterStoragePolicyInfoFullSync ensures a ClusterStoragePolicyInfo CR exists for every storage
// policy present on the VC, even if no StorageClass/VolumeAttributesClass references it yet.
// The reactive ClusterStoragePolicyInfo controller (pkg/syncer/cnsoperator/controller/
// clusterstoragepolicyinfo) continues to own attribute syncing, InfraStoragePolicyInfo creation,
// and owner-reference management once a CR exists; this function only guarantees the CR's presence.
func clusterStoragePolicyInfoFullSync(ctx context.Context, metadataSyncer *metadataSyncInformer,
	vc string, vcenter *cnsvsphere.VirtualCenter) {
	log := logger.GetLogger(ctx)

	if !metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.SupportsExposingStoragePolicyAttributes) {
		log.Debugf("clusterStoragePolicyInfoFullSync: capability %q is not activated, skipping",
			common.SupportsExposingStoragePolicyAttributes)
		return
	}

	if vcenter == nil {
		log.Errorf("clusterStoragePolicyInfoFullSync: no VirtualCenter instance provided for VC %s", vc)
		return
	}

	profiles, err := vcenter.QueryAllProfileDetails(ctx, "REQUIREMENT", false)
	if err != nil {
		log.Errorf("clusterStoragePolicyInfoFullSync: failed to query storage policies for VC %s. Err: %v",
			vc, err)
		return
	}

	ensureClusterStoragePolicyInfoCRsExist(ctx, metadataSyncer.cnsOperatorClient, profiles)
}

// ensureClusterStoragePolicyInfoCRsExist creates a bare ClusterStoragePolicyInfo CR (by
// K8sCompliantName, no owner references) for every profile that doesn't already have one.
// Owner references, InfraStoragePolicyInfo creation, and attribute syncing are left to the
// existing reactive reconcile loop, which is triggered automatically by the CR create event.
func ensureClusterStoragePolicyInfoCRsExist(ctx context.Context, cnsOperatorClient client.Client,
	profiles []cnsvsphere.ProfileDetail) {
	log := logger.GetLogger(ctx)

	for _, profile := range profiles {
		if profile.K8sCompliantName == "" {
			log.Debugf("clusterStoragePolicyInfoFullSync: skipping storage policy %q (ID %s) with no "+
				"K8s compliant name", profile.Name, profile.ID)
			continue
		}

		name := profile.K8sCompliantName
		existing := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
		err := cnsOperatorClient.Get(ctx, apitypes.NamespacedName{Name: name}, existing)
		if err == nil {
			continue
		}
		if !apierrors.IsNotFound(err) {
			log.Errorf("clusterStoragePolicyInfoFullSync: failed to get ClusterStoragePolicyInfo %q. Err: %v",
				name, err)
			continue
		}

		instance := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
			TypeMeta: metav1.TypeMeta{
				APIVersion: apis.SchemeGroupVersion.String(),
				Kind:       "ClusterStoragePolicyInfo",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
		}
		if err := cnsOperatorClient.Create(ctx, instance); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			log.Errorf("clusterStoragePolicyInfoFullSync: failed to create ClusterStoragePolicyInfo %q. Err: %v",
				name, err)
			continue
		}
		log.Infof("clusterStoragePolicyInfoFullSync: created ClusterStoragePolicyInfo %q for storage policy %q "+
			"(ID %s) ahead of any StorageClass", name, profile.Name, profile.ID)
	}
}
