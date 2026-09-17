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

package snapshotmetadataservice

import (
	"context"
	"fmt"
	"time"

	snapshotmetadatav1beta1 "github.com/kubernetes-csi/external-snapshot-metadata/client/apis/snapshotmetadataservice/v1beta1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wcpcapv1alph1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/wcpcapabilities/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// tkgServiceID owns the core_addon_management capability. It is a *service*
	// capability, so it lives under status.services on the Capabilities CR and is
	// invisible to the supervisor-scoped FSS helpers, which would always report false.
	tkgServiceID = "tkg.vsphere.vmware.com"

	// coreAddonManagementCapability gates the ConfigExport machinery: ConfigExport is
	// a core add-on management CRD, so exporting anything is pointless until it is on.
	coreAddonManagementCapability = "core_addon_management"

	// vksPublicNamespace is the namespace the addon framework reads from when
	// propagating configuration into VKS guest clusters.
	vksPublicNamespace = "vmware-system-vks-public"

	exportServiceAccountName    = "snapshotmetadataservice-cr-export-sa"
	exportReaderClusterRoleName = "snapshotmetadataservice-cr-reader"
	exportRoleBindingName       = "snapshotmetadataservice-cr-export-binding"
	exportConfigExportName      = "snapshotmetadataservice-cr"

	// crExportRequeueAfter paces retries of the CR export. It only applies while the
	// capability is activated but the export has not succeeded, so the polling is
	// bounded to that window rather than running in steady state.
	crExportRequeueAfter = 5 * time.Minute
)

// configExportGVK identifies the addon framework's ConfigExport CRD. No Go types are
// vendored for this group, so the CR is built and written as an unstructured object.
var configExportGVK = schema.GroupVersionKind{
	Group:   "addons.kubernetes.vmware.com",
	Version: "v1alpha1",
	Kind:    "ConfigExport",
}

// isCoreAddonManagementActivated reports whether the core_addon_management service
// capability is activated. A missing service or capability key yields the zero value,
// which is the desired "not activated" default.
func isCoreAddonManagementActivated(caps *wcpcapv1alph1.Capabilities) bool {
	return caps.Status.Services[tkgServiceID][coreAddonManagementCapability].Activated
}

// syncCRExportResources creates the CR export resources when core_addon_management is
// activated, and reports whether the caller should requeue to try again.
//
// Failures are never propagated as reconcile errors: the caller's primary job is keeping
// the SnapshotMetadataService CR in sync, and that must not be held up by the export.
// They do however require a requeue, because no further event is coming. The capability
// watch only fires on an activation change, and that change is what got us here — so a
// failure now would otherwise go unretried until a cert rotation, an LB IP change or a
// syncer restart, any of which may be a very long way off.
func (r *ReconcileSnapshotMetadataService) syncCRExportResources(ctx context.Context) (requeue bool) {
	log := logger.GetLogger(ctx)

	caps, err := r.getWcpCapabilities(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// A definitive answer rather than a failure: with no Capabilities CR there
			// is no activated capability. The watch reports a Create if one appears.
			log.Debugf("%q not found; skipping SnapshotMetadataService CR export",
				common.WCPCapabilitiesCRName)
			return false
		}
		log.Warnf("Failed to read %q; will retry SnapshotMetadataService CR export. Err: %+v",
			common.WCPCapabilitiesCRName, err)
		return true
	}

	// Steady state for a supervisor without VKS addon management. No requeue: the
	// capability watch reports the activation when and if it happens.
	if !isCoreAddonManagementActivated(caps) {
		log.Debugf("Capability %q is not activated; skipping SnapshotMetadataService CR export",
			coreAddonManagementCapability)
		return false
	}

	if err := ensureCRExportResources(ctx, r.apiClient); err != nil {
		if meta.IsNoMatchError(err) {
			// A VKS service upgrade activates core_addon_management and installs the
			// ConfigExport CRD, in no guaranteed order, so the CRD can legitimately be
			// missing for a while after the activation we just observed. Transient.
			log.Warnf("ConfigExport CRD is not installed yet; will retry "+
				"SnapshotMetadataService CR export. Err: %+v", err)
			return true
		}
		log.Warnf("Failed to create SnapshotMetadataService CR export resources; will retry. Err: %+v", err)
		return true
	}
	return false
}

// getWcpCapabilities reads the cluster-scoped supervisor-capabilities CR.
func (r *ReconcileSnapshotMetadataService) getWcpCapabilities(
	ctx context.Context) (*wcpcapv1alph1.Capabilities, error) {
	caps := &wcpcapv1alph1.Capabilities{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{Name: common.WCPCapabilitiesCRName}, caps); err != nil {
		return nil, err
	}
	return caps, nil
}

func exportServiceAccount() *v1.ServiceAccount {
	return &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      exportServiceAccountName,
			Namespace: targetNamespace,
		},
	}
}

func exportReaderClusterRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: exportReaderClusterRoleName},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{snapshotmetadatav1beta1.GroupName},
				Resources: []string{"snapshotmetadataservices"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}
}

func exportClusterRoleBinding() *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: exportRoleBindingName},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      exportServiceAccountName,
				Namespace: targetNamespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     exportReaderClusterRoleName,
			APIGroup: rbacv1.GroupName,
		},
	}
}

// exportConfigExport builds the ConfigExport that copies the SnapshotMetadataService
// CR's address, audience and caCert into a ConfigMap in the VKS public namespace.
func exportConfigExport() *unstructured.Unstructured {
	configExport := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name":      exportConfigExportName,
				"namespace": targetNamespace,
			},
			"spec": map[string]interface{}{
				"source": map[string]interface{}{
					"apiGroup": snapshotmetadatav1beta1.GroupName,
					"kind":     "SnapshotMetadataService",
					"name":     targetSMSName,
				},
				"extract": []interface{}{
					map[string]interface{}{"key": "address", "path": ".spec.address"},
					map[string]interface{}{"key": "audience", "path": ".spec.audience"},
					map[string]interface{}{"key": "caCert", "path": ".spec.caCert"},
				},
				"toNamespace":        vksPublicNamespace,
				"toName":             exportConfigExportName,
				"serviceAccountName": exportServiceAccountName,
			},
		},
	}
	configExport.SetGroupVersionKind(configExportGVK)
	return configExport
}

// ensureCRExportResources creates the resources that export the SnapshotMetadataService
// CR into the VKS public namespace, so pvCSI in a guest cluster can reach the supervisor
// CBT service. Idempotent: each object is created only when absent.
func ensureCRExportResources(ctx context.Context, c client.Client) error {
	log := logger.GetLogger(ctx)

	resources := []client.Object{
		exportServiceAccount(),
		exportReaderClusterRole(),
		exportClusterRoleBinding(),
		exportConfigExport(),
	}

	for _, resource := range resources {
		if err := createIfAbsent(ctx, c, resource); err != nil {
			return err
		}
	}

	log.Debugf("SnapshotMetadataService CR export resources are present")
	return nil
}

// createIfAbsent creates resource unless it already exists. A concurrent create by
// another replica surfaces as AlreadyExists and is treated as success.
func createIfAbsent(ctx context.Context, c client.Client, resource client.Object) error {
	log := logger.GetLogger(ctx)

	// Reuse the object's own type for the existence probe so unstructured reads carry
	// their GVK; DeepCopyObject keeps the desired state intact for the create below.
	existing, _ := resource.DeepCopyObject().(client.Object)
	err := c.Get(ctx, client.ObjectKeyFromObject(resource), existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for %T %q: %w", resource, resource.GetName(), err)
	}

	if err := c.Create(ctx, resource); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create %T %q: %w", resource, resource.GetName(), err)
	}
	log.Infof("Created SnapshotMetadataService CR export resource %T %q", resource, resource.GetName())
	return nil
}
