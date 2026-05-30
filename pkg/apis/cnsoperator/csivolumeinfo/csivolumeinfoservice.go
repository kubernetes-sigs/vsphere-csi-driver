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

// Package csivolumeinfo provides the service layer for CsiVolumeInfo CRs.
// CsiVolumeInfo is a namespaced CR that tracks the per-volume ownership
// lifecycle for the VM-owned volume attach/detach model.
package csivolumeinfo

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	// cviNamePrefix is the prefix used for CsiVolumeInfo CR names.
	// CR name = cviNamePrefix + volumeID.
	cviNamePrefix = "csi-volume-info-"

	// allowedRetries is the default number of patch retries on conflict.
	allowedRetries = 5
)

// CsiVolumeInfoService exposes CRUD operations on CsiVolumeInfo CRs.
// CRs are namespaced (they live in the PVC's namespace) and span the entire
// supervisor cluster. The service uses a typed controller-runtime client for
// type safety; callers provide the namespace on each operation.
type CsiVolumeInfoService interface {
	// CreateCsiVolumeInfo creates a new CsiVolumeInfo CR.
	// Returns nil if a CR with the same name already exists (idempotent).
	CreateCsiVolumeInfo(ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error

	// GetCsiVolumeInfo fetches a CsiVolumeInfo by deterministic name derived
	// from volumeID (csi-volume-info-<volumeID>) in the given namespace.
	GetCsiVolumeInfo(ctx context.Context, namespace, volumeID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error)

	// GetCsiVolumeInfoByDiskUUID fetches a CsiVolumeInfo by the
	// cns.vmware.com/disk-uuid label in the given namespace. Uses an
	// API-server-indexed label selector for O(1) lookup.
	// Returns nil and no error if not found.
	GetCsiVolumeInfoByDiskUUID(ctx context.Context, namespace, diskUUID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) //nolint:lll

	// UpdateCsiVolumeInfoStatus replaces the status subresource of the given
	// CsiVolumeInfo object. The object must have been fetched from the API
	// server (its ResourceVersion is used for optimistic concurrency).
	UpdateCsiVolumeInfoStatus(ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error

	// PatchCsiVolumeInfo applies a JSON merge-patch to the spec and metadata
	// (not status) of the CsiVolumeInfo identified by volumeID / namespace.
	// Retries up to allowedRetries times on conflict.
	PatchCsiVolumeInfo(ctx context.Context, namespace, volumeID string, patchBytes []byte) error

	// DeleteCsiVolumeInfo deletes the CsiVolumeInfo for the given volumeID in
	// the given namespace. Returns nil if the CR is already gone (idempotent).
	DeleteCsiVolumeInfo(ctx context.Context, namespace, volumeID string) error

	// CsiVolumeInfoExists reports whether a CsiVolumeInfo CR exists for the
	// given volumeID in the given namespace.
	CsiVolumeInfoExists(ctx context.Context, namespace, volumeID string) (bool, error)

	// GetCsiVolumeInfoByPVCName returns the CsiVolumeInfo whose spec.pvcName
	// matches the given PVC name in the given namespace. Returns nil and no
	// error when no matching CR is found.
	GetCsiVolumeInfoByPVCName(ctx context.Context, namespace, pvcName string) (
		*csivolumeinfov1alpha1.CsiVolumeInfo, error)
}

// csiVolumeInfoSvc is the concrete singleton implementing CsiVolumeInfoService.
type csiVolumeInfoSvc struct {
	k8sClient client.Client
}

var (
	// serviceInstance is the package-level singleton.
	serviceInstance *csiVolumeInfoSvc
)

// InitCsiVolumeInfoService initialises (idempotent) the CsiVolumeInfo service
// by building a controller-runtime client for the cns.vmware.com group.
//
// The CsiVolumeInfo CRD must already exist before calling this function.
// CRD creation is handled by InitCnsOperator (in init.go) which is
// gated behind both the Workload cluster flavor check and the
// VMOwnedVolumes FSS — ensuring this service is only initialised on
// supervisor clusters with the feature enabled.
//
// Callers must hold no lock; internal state is initialised once and is
// safe for concurrent reads afterward.
func InitCsiVolumeInfoService(ctx context.Context) (CsiVolumeInfoService, error) {
	log := logger.GetLogger(ctx)
	if serviceInstance != nil {
		return serviceInstance, nil
	}

	log.Info("Initializing CsiVolumeInfo service...")

	config, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		return nil, logger.LogNewErrorf(log,
			"failed to get kubeconfig for CsiVolumeInfo service. err: %v", err)
	}

	k8sClient, err := k8s.NewClientForGroup(ctx, config,
		csivolumeinfov1alpha1.GroupName)
	if err != nil {
		return nil, logger.LogNewErrorf(log,
			"failed to create k8s client for CsiVolumeInfo service. err: %v", err)
	}

	serviceInstance = &csiVolumeInfoSvc{k8sClient: k8sClient}
	log.Info("CsiVolumeInfo service initialized")
	return serviceInstance, nil
}

// GetCsiVolumeInfoCRName returns the deterministic CR name for a volumeID.
// Name format: csi-volume-info-<volumeID>.
func GetCsiVolumeInfoCRName(volumeID string) string {
	return cviNamePrefix + volumeID
}

// BuildCsiVolumeInfo constructs a CsiVolumeInfo object ready for creation.
// When pvUID is empty, no ownerReference is set; the caller is expected to
// patch it once the PersistentVolume is available.
// No cvi-protection finalizer is set because the volume starts in the
// CSI_MANAGED steady state where none is required.
func BuildCsiVolumeInfo(
	volumeID, pvcName, pvcNamespace, pvName, pvUID, diskUUID, diskPath string,
) *csivolumeinfov1alpha1.CsiVolumeInfo {
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetCsiVolumeInfoCRName(volumeID),
			Namespace: pvcNamespace,
			Labels: map[string]string{
				csivolumeinfov1alpha1.LabelDiskUUID: diskUUID,
			},
		},
		Spec: csivolumeinfov1alpha1.CsiVolumeInfoSpec{
			VolumeID: volumeID,
			PVCName:  pvcName,
			PVName:   pvName,
		},
		Status: csivolumeinfov1alpha1.CsiVolumeInfoStatus{
			OwnershipState: csivolumeinfov1alpha1.OwnershipStateCSIManaged,
			DiskUUID:       diskUUID,
			DiskPath:       diskPath,
		},
	}
	if pvUID != "" {
		controller := true
		blockOwnerDeletion := true
		cvi.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion:         "v1",
				Kind:               "PersistentVolume",
				Name:               pvName,
				UID:                k8stypes.UID(pvUID),
				Controller:         &controller,
				BlockOwnerDeletion: &blockOwnerDeletion,
			},
		}
	}
	return cvi
}

// CreateCsiVolumeInfo creates a new CsiVolumeInfo CR.
// AlreadyExists errors are treated as success (idempotent).
func (s *csiVolumeInfoSvc) CreateCsiVolumeInfo(
	ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx)
	log.Infof("Creating CsiVolumeInfo %s/%s", cvi.Namespace, cvi.Name)

	if err := s.k8sClient.Create(ctx, cvi); err != nil {
		if apierrors.IsAlreadyExists(err) {
			log.Infof("CsiVolumeInfo %s/%s already exists", cvi.Namespace, cvi.Name)
			return nil
		}
		return logger.LogNewErrorf(log,
			"failed to create CsiVolumeInfo %s/%s: %v", cvi.Namespace, cvi.Name, err)
	}
	log.Infof("Successfully created CsiVolumeInfo %s/%s", cvi.Namespace, cvi.Name)
	return nil
}

// GetCsiVolumeInfo fetches a CsiVolumeInfo by volumeID in the given namespace.
func (s *csiVolumeInfoSvc) GetCsiVolumeInfo(
	ctx context.Context, namespace, volumeID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	log := logger.GetLogger(ctx)
	name := GetCsiVolumeInfoCRName(volumeID)
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	if err := s.k8sClient.Get(ctx, k8stypes.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, cvi); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, logger.LogNewErrorf(log,
			"failed to get CsiVolumeInfo %s/%s: %v", namespace, name, err)
	}
	return cvi, nil
}

// GetCsiVolumeInfoByDiskUUID fetches a CsiVolumeInfo by the disk-uuid label in
// the given namespace. Returns nil if no CR is found.
// If multiple CRs carry the same label (should not happen by construction),
// the first result is returned and an error is logged.
func (s *csiVolumeInfoSvc) GetCsiVolumeInfoByDiskUUID(
	ctx context.Context, namespace, diskUUID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	log := logger.GetLogger(ctx)
	list := &csivolumeinfov1alpha1.CsiVolumeInfoList{}
	labelSelector := labels.SelectorFromSet(labels.Set{
		csivolumeinfov1alpha1.LabelDiskUUID: diskUUID,
	})
	if err := s.k8sClient.List(ctx, list,
		&client.ListOptions{
			Namespace:     namespace,
			LabelSelector: labelSelector,
		}); err != nil {
		return nil, logger.LogNewErrorf(log,
			"failed to list CsiVolumeInfo by diskUUID %q in namespace %q: %v",
			diskUUID, namespace, err)
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	if len(list.Items) > 1 {
		// Defensive: disk-uuid must be unique — log and use the first match.
		log.Errorf("found %d CsiVolumeInfo CRs with disk-uuid=%q in namespace %q; "+
			"using first match. This is a data integrity issue.",
			len(list.Items), diskUUID, namespace)
	}
	return &list.Items[0], nil
}

// UpdateCsiVolumeInfoStatus replaces the status subresource.
func (s *csiVolumeInfoSvc) UpdateCsiVolumeInfoStatus(
	ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error {
	log := logger.GetLogger(ctx)
	if err := s.k8sClient.Status().Update(ctx, cvi); err != nil {
		return logger.LogNewErrorf(log,
			"failed to update status of CsiVolumeInfo %s/%s: %v",
			cvi.Namespace, cvi.Name, err)
	}
	log.Infof("Successfully updated status of CsiVolumeInfo %s/%s (ownershipState=%s)",
		cvi.Namespace, cvi.Name, cvi.Status.OwnershipState)
	return nil
}

// PatchCsiVolumeInfo applies a JSON merge-patch to the CsiVolumeInfo for the
// given volumeID/namespace. Retries on conflict up to allowedRetries times.
func (s *csiVolumeInfoSvc) PatchCsiVolumeInfo(
	ctx context.Context, namespace, volumeID string, patchBytes []byte) error {
	log := logger.GetLogger(ctx)
	name := GetCsiVolumeInfoCRName(volumeID)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}

	var lastErr error
	for attempt := 1; attempt <= allowedRetries; attempt++ {
		err := s.k8sClient.Patch(ctx, cvi,
			client.RawPatch(k8stypes.MergePatchType, patchBytes))
		if err == nil {
			log.Infof("attempt %d: successfully patched CsiVolumeInfo %s/%s",
				attempt, namespace, name)
			return nil
		}
		lastErr = err
		log.Warnf("attempt %d: failed to patch CsiVolumeInfo %s/%s: %v",
			attempt, namespace, name, err)
		time.Sleep(100 * time.Millisecond)
	}
	return logger.LogNewErrorf(log,
		"failed to patch CsiVolumeInfo %s/%s after %d retries: %v",
		namespace, name, allowedRetries, lastErr)
}

// DeleteCsiVolumeInfo deletes the CsiVolumeInfo for the given volumeID.
// NotFound errors are treated as success (idempotent).
func (s *csiVolumeInfoSvc) DeleteCsiVolumeInfo(
	ctx context.Context, namespace, volumeID string) error {
	log := logger.GetLogger(ctx)
	name := GetCsiVolumeInfoCRName(volumeID)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	if err := s.k8sClient.Delete(ctx, cvi); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("CsiVolumeInfo %s/%s already deleted", namespace, name)
			return nil
		}
		return logger.LogNewErrorf(log,
			"failed to delete CsiVolumeInfo %s/%s: %v", namespace, name, err)
	}
	log.Infof("Successfully deleted CsiVolumeInfo %s/%s", namespace, name)
	return nil
}

// CsiVolumeInfoExists reports whether a CsiVolumeInfo CR exists for volumeID.
func (s *csiVolumeInfoSvc) CsiVolumeInfoExists(
	ctx context.Context, namespace, volumeID string) (bool, error) {
	cvi, err := s.GetCsiVolumeInfo(ctx, namespace, volumeID)
	if err != nil {
		return false, fmt.Errorf("CsiVolumeInfoExists: %w", err)
	}
	return cvi != nil, nil
}

// GetCsiVolumeInfoByPVCName lists all CsiVolumeInfo CRs in the given namespace
// and returns the one whose spec.pvcName matches pvcName. Returns nil and no
// error when no matching CR is found. If multiple CRs match (data integrity
// anomaly), the first is returned and the discrepancy is logged.
func (s *csiVolumeInfoSvc) GetCsiVolumeInfoByPVCName(
	ctx context.Context, namespace, pvcName string) (
	*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	log := logger.GetLogger(ctx)

	list := &csivolumeinfov1alpha1.CsiVolumeInfoList{}
	if err := s.k8sClient.List(ctx, list, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, logger.LogNewErrorf(log,
			"failed to list CsiVolumeInfo in namespace %q: %v", namespace, err)
	}

	var matches []csivolumeinfov1alpha1.CsiVolumeInfo
	for i := range list.Items {
		if list.Items[i].Spec.PVCName == pvcName {
			matches = append(matches, list.Items[i])
		}
	}

	if len(matches) == 0 {
		return nil, nil
	}
	if len(matches) > 1 {
		log.Errorf("found %d CsiVolumeInfo CRs with pvcName=%q in namespace %q; "+
			"using first match. This is a data integrity issue.",
			len(matches), pvcName, namespace)
	}
	return &matches[0], nil
}
