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
// CsiVolumeInfo CRs live in the vmware-system-csi namespace and track the
// per-volume ownership lifecycle for the VM-owned volume attach/detach model.
package csivolumeinfo

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	// allowedRetries is the default number of patch retries on conflict.
	allowedRetries = 5
)

// CsiVolumeInfoService exposes CRUD operations on CsiVolumeInfo CRs.
// All CRs live in the vmware-system-csi namespace (csivolumeinfov1alpha1.CVINamespace).
// The service uses a typed controller-runtime client for type safety.
type CsiVolumeInfoService interface {
	// CreateCsiVolumeInfo creates a new CsiVolumeInfo CR.
	// Returns nil if a CR with the same name already exists (idempotent).
	CreateCsiVolumeInfo(ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error

	// GetCsiVolumeInfo fetches a CsiVolumeInfo by deterministic name derived
	// from volumeID (cns-volume-<volumeID>) in the vmware-system-csi namespace.
	// Returns nil and no error if not found.
	GetCsiVolumeInfo(ctx context.Context, volumeID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error)

	// UpdateCsiVolumeInfoStatus replaces the status subresource of the given
	// CsiVolumeInfo object. The object must have been fetched from the API
	// server (its ResourceVersion is used for optimistic concurrency).
	UpdateCsiVolumeInfoStatus(ctx context.Context, cvi *csivolumeinfov1alpha1.CsiVolumeInfo) error

	// PatchCsiVolumeInfo applies a JSON merge-patch to the spec and metadata
	// (not status) of the CsiVolumeInfo identified by volumeID.
	// Retries up to allowedRetries times on conflict.
	//
	// It returns the metadata.generation of the object after the patch is
	// applied. Because a spec change increments generation, callers that need
	// to record observedGeneration must use this returned value (not the
	// pre-patch generation) so that observedGeneration reflects the spec the
	// controller actually acted on.
	PatchCsiVolumeInfo(ctx context.Context, volumeID string, patchBytes []byte) (int64, error)

	// PatchCsiVolumeInfoStatus applies a JSON merge-patch to the status
	// subresource of the CsiVolumeInfo identified by volumeID.
	// Retries up to allowedRetries times on conflict.
	PatchCsiVolumeInfoStatus(ctx context.Context, volumeID string, patchBytes []byte) error

	// DeleteCsiVolumeInfo deletes the CsiVolumeInfo for the given volumeID.
	// Returns nil if the CR is already gone (idempotent).
	DeleteCsiVolumeInfo(ctx context.Context, volumeID string) error

	// CsiVolumeInfoExists reports whether a CsiVolumeInfo CR exists for the
	// given volumeID.
	CsiVolumeInfoExists(ctx context.Context, volumeID string) (bool, error)

	// AddVolumeProtectionFinalizer adds the volume-protection finalizer to the
	// CsiVolumeInfo for the given volumeID. Idempotent: no-op if already present.
	AddVolumeProtectionFinalizer(ctx context.Context, volumeID string) error

	// RemoveVolumeProtectionFinalizer removes the volume-protection finalizer from the
	// CsiVolumeInfo for the given volumeID. Idempotent: no-op if already absent.
	RemoveVolumeProtectionFinalizer(ctx context.Context, volumeID string) error
}

// csiVolumeInfoSvc is the concrete singleton implementing CsiVolumeInfoService.
type csiVolumeInfoSvc struct {
	k8sClient client.Client
}

var (
	// serviceInstance is the package-level singleton.
	serviceInstance *csiVolumeInfoSvc
)

// GetCsiVolumeInfoCRName returns the deterministic CR name for a volumeID.
// Name format: cns-volume-<volumeID>.
func GetCsiVolumeInfoCRName(volumeID string) string {
	return csivolumeinfov1alpha1.CVINamePrefix + volumeID
}

// InitCsiVolumeInfoService initialises (idempotent) the CsiVolumeInfo service
// by building a controller-runtime client for the cns.vmware.com group.
//
// The CsiVolumeInfo CRD must already exist before calling this function.
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

// NewCsiVolumeInfoService creates a CsiVolumeInfoService backed by the given client.
// Use this constructor in tests and controllers where a client is already available.
func NewCsiVolumeInfoService(k8sClient client.Client) CsiVolumeInfoService {
	return &csiVolumeInfoSvc{k8sClient: k8sClient}
}

// CreateCsiVolumeInfo creates a new CsiVolumeInfo CR.
// AlreadyExists errors are treated as success (idempotent).
// Because the CRD declares a status subresource, the API server strips the
// status block on Create; callers that need initial status set should call
// PatchCsiVolumeInfoStatus after creation.
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

// GetCsiVolumeInfo fetches a CsiVolumeInfo by volumeID in the vmware-system-csi namespace.
// Returns nil and no error if the CR does not exist.
func (s *csiVolumeInfoSvc) GetCsiVolumeInfo(
	ctx context.Context, volumeID string) (*csivolumeinfov1alpha1.CsiVolumeInfo, error) {
	log := logger.GetLogger(ctx)
	name := GetCsiVolumeInfoCRName(volumeID)
	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{}
	if err := s.k8sClient.Get(ctx, k8stypes.NamespacedName{
		Namespace: csivolumeinfov1alpha1.CVINamespace,
		Name:      name,
	}, cvi); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, logger.LogNewErrorf(log,
			"failed to get CsiVolumeInfo %s/%s: %v", csivolumeinfov1alpha1.CVINamespace, name, err)
	}
	return cvi, nil
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
	log.Infof("Successfully updated status of CsiVolumeInfo %s/%s (ownership=%s)",
		cvi.Namespace, cvi.Name, cvi.Status.Ownership)
	return nil
}

// PatchCsiVolumeInfo applies a JSON merge-patch to the CsiVolumeInfo for the
// given volumeID. Only conflicts are retried (up to allowedRetries times); any
// other error is returned immediately because retrying it would not help.
//
// The returned int64 is the object's metadata.generation after the patch. The
// API server populates the response object on a successful patch, so this value
// reflects any generation increment caused by the spec change.
func (s *csiVolumeInfoSvc) PatchCsiVolumeInfo(
	ctx context.Context, volumeID string, patchBytes []byte) (int64, error) {
	log := logger.GetLogger(ctx)
	name := GetCsiVolumeInfoCRName(volumeID)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: csivolumeinfov1alpha1.CVINamespace,
			Name:      name,
		},
	}

	var lastErr error
	for attempt := 1; attempt <= allowedRetries; attempt++ {
		err := s.k8sClient.Patch(ctx, cvi,
			client.RawPatch(k8stypes.MergePatchType, patchBytes))
		if err == nil {
			log.Infof("attempt %d: successfully patched CsiVolumeInfo %s/%s (generation=%d)",
				attempt, csivolumeinfov1alpha1.CVINamespace, name, cvi.Generation)
			return cvi.Generation, nil
		}
		lastErr = err
		if !apierrors.IsConflict(err) {
			return 0, logger.LogNewErrorf(log,
				"failed to patch CsiVolumeInfo %s/%s: %v",
				csivolumeinfov1alpha1.CVINamespace, name, err)
		}
		log.Warnf("attempt %d: conflict patching CsiVolumeInfo %s/%s: %v",
			attempt, csivolumeinfov1alpha1.CVINamespace, name, err)
		time.Sleep(100 * time.Millisecond)
	}
	return 0, logger.LogNewErrorf(log,
		"failed to patch CsiVolumeInfo %s/%s after %d conflict retries: %v",
		csivolumeinfov1alpha1.CVINamespace, name, allowedRetries, lastErr)
}

// PatchCsiVolumeInfoStatus applies a JSON merge-patch to the status subresource
// of the CsiVolumeInfo for the given volumeID. Retries on conflict up to
// allowedRetries times.
func (s *csiVolumeInfoSvc) PatchCsiVolumeInfoStatus(
	ctx context.Context, volumeID string, patchBytes []byte) error {
	log := logger.GetLogger(ctx)
	name := GetCsiVolumeInfoCRName(volumeID)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: csivolumeinfov1alpha1.CVINamespace,
			Name:      name,
		},
	}

	var lastErr error
	for attempt := 1; attempt <= allowedRetries; attempt++ {
		err := s.k8sClient.Status().Patch(ctx, cvi,
			client.RawPatch(k8stypes.MergePatchType, patchBytes))
		if err == nil {
			log.Infof("attempt %d: successfully patched status of CsiVolumeInfo %s/%s",
				attempt, csivolumeinfov1alpha1.CVINamespace, name)
			return nil
		}
		lastErr = err
		if !apierrors.IsConflict(err) {
			return logger.LogNewErrorf(log,
				"failed to patch status of CsiVolumeInfo %s/%s: %v",
				csivolumeinfov1alpha1.CVINamespace, name, err)
		}
		log.Warnf("attempt %d: conflict patching status of CsiVolumeInfo %s/%s: %v",
			attempt, csivolumeinfov1alpha1.CVINamespace, name, err)
		time.Sleep(100 * time.Millisecond)
	}
	return logger.LogNewErrorf(log,
		"failed to patch status of CsiVolumeInfo %s/%s after %d conflict retries: %v",
		csivolumeinfov1alpha1.CVINamespace, name, allowedRetries, lastErr)
}

// DeleteCsiVolumeInfo deletes the CsiVolumeInfo for the given volumeID.
// NotFound errors are treated as success (idempotent).
func (s *csiVolumeInfoSvc) DeleteCsiVolumeInfo(
	ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)
	name := GetCsiVolumeInfoCRName(volumeID)

	cvi := &csivolumeinfov1alpha1.CsiVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: csivolumeinfov1alpha1.CVINamespace,
			Name:      name,
		},
	}
	if err := s.k8sClient.Delete(ctx, cvi); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("CsiVolumeInfo %s/%s already deleted",
				csivolumeinfov1alpha1.CVINamespace, name)
			return nil
		}
		return logger.LogNewErrorf(log,
			"failed to delete CsiVolumeInfo %s/%s: %v",
			csivolumeinfov1alpha1.CVINamespace, name, err)
	}
	log.Infof("Successfully deleted CsiVolumeInfo %s/%s",
		csivolumeinfov1alpha1.CVINamespace, name)
	return nil
}

// CsiVolumeInfoExists reports whether a CsiVolumeInfo CR exists for volumeID.
func (s *csiVolumeInfoSvc) CsiVolumeInfoExists(
	ctx context.Context, volumeID string) (bool, error) {
	cvi, err := s.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil {
		return false, fmt.Errorf("CsiVolumeInfoExists: %w", err)
	}
	return cvi != nil, nil
}

// AddVolumeProtectionFinalizer adds the volume-protection finalizer to the CsiVolumeInfo
// identified by volumeID. The operation is idempotent.
func (s *csiVolumeInfoSvc) AddVolumeProtectionFinalizer(
	ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)
	name := GetCsiVolumeInfoCRName(volumeID)

	cvi, err := s.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil {
		return fmt.Errorf("AddVolumeProtectionFinalizer: failed to fetch CsiVolumeInfo %s/%s: %w",
			csivolumeinfov1alpha1.CVINamespace, name, err)
	}
	if cvi == nil {
		return fmt.Errorf("AddVolumeProtectionFinalizer: CsiVolumeInfo %s/%s not found",
			csivolumeinfov1alpha1.CVINamespace, name)
	}
	if controllerutil.ContainsFinalizer(cvi, csivolumeinfov1alpha1.VolumeProtectionFinalizer) {
		log.Infof("AddVolumeProtectionFinalizer: finalizer already present on CsiVolumeInfo %s/%s",
			csivolumeinfov1alpha1.CVINamespace, name)
		return nil
	}

	patch := client.MergeFrom(cvi.DeepCopy())
	controllerutil.AddFinalizer(cvi, csivolumeinfov1alpha1.VolumeProtectionFinalizer)
	if err := s.k8sClient.Patch(ctx, cvi, patch); err != nil {
		return fmt.Errorf("AddVolumeProtectionFinalizer: failed to patch CsiVolumeInfo %s/%s: %w",
			csivolumeinfov1alpha1.CVINamespace, name, err)
	}
	log.Infof("AddVolumeProtectionFinalizer: added finalizer on CsiVolumeInfo %s/%s",
		csivolumeinfov1alpha1.CVINamespace, name)
	return nil
}

// RemoveVolumeProtectionFinalizer removes the volume-protection finalizer from the
// CsiVolumeInfo identified by volumeID. The operation is idempotent.
func (s *csiVolumeInfoSvc) RemoveVolumeProtectionFinalizer(
	ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)
	name := GetCsiVolumeInfoCRName(volumeID)

	cvi, err := s.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil {
		return fmt.Errorf("RemoveVolumeProtectionFinalizer: failed to fetch CsiVolumeInfo %s/%s: %w",
			csivolumeinfov1alpha1.CVINamespace, name, err)
	}
	if cvi == nil {
		log.Infof("RemoveVolumeProtectionFinalizer: CsiVolumeInfo %s/%s not found; no-op",
			csivolumeinfov1alpha1.CVINamespace, name)
		return nil
	}
	if !controllerutil.ContainsFinalizer(cvi, csivolumeinfov1alpha1.VolumeProtectionFinalizer) {
		log.Infof("RemoveVolumeProtectionFinalizer: finalizer absent on CsiVolumeInfo %s/%s; no-op",
			csivolumeinfov1alpha1.CVINamespace, name)
		return nil
	}

	patch := client.MergeFrom(cvi.DeepCopy())
	controllerutil.RemoveFinalizer(cvi, csivolumeinfov1alpha1.VolumeProtectionFinalizer)
	if err := s.k8sClient.Patch(ctx, cvi, patch); err != nil {
		return fmt.Errorf("RemoveVolumeProtectionFinalizer: failed to patch CsiVolumeInfo %s/%s: %w",
			csivolumeinfov1alpha1.CVINamespace, name, err)
	}
	log.Infof("RemoveVolumeProtectionFinalizer: removed finalizer from CsiVolumeInfo %s/%s",
		csivolumeinfov1alpha1.CVINamespace, name)
	return nil
}
