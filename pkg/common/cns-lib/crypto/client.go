/*
Copyright 2024 The Kubernetes Authors.

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

package crypto

import (
	"context"
	"fmt"
	"slices"

	byokv1 "github.com/vmware-tanzu/vm-operator/external/byok/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto/internal"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// Client providers functionality quering encryption information related to entities.
type Client interface {
	ctrlclient.Client
	// IsEncryptedStorageClass returns true if the provided StorageClass name was
	// marked as encrypted. If encryption is supported, the StorageClass's profile
	// ID is also returned.
	IsEncryptedStorageClass(ctx context.Context, name string) (bool, string, error)
	// IsEncryptedStorageProfile returns true if the provided storage profile ID was
	// marked as encrypted.
	IsEncryptedStorageProfile(ctx context.Context, profileID string) (bool, error)
	// MarkEncryptedStorageClass records the provided StorageClass as encrypted.
	MarkEncryptedStorageClass(ctx context.Context, storageClass *storagev1.StorageClass, encrypted bool) error
	// GetEncryptionClass retrieves the encryption class associated with a specific name
	// and namespace.
	GetEncryptionClass(ctx context.Context, name, namespace string) (*byokv1.EncryptionClass, error)
	// GetDefaultEncryptionClass retrieves the default encryption class in a namespace.
	GetDefaultEncryptionClass(ctx context.Context, namespace string) (*byokv1.EncryptionClass, error)
	// GetEncryptionClassForPVC retrieves the encryption class associated with a PersistentVolumeClaim (PVC).
	GetEncryptionClassForPVC(ctx context.Context, name, namespace string) (*byokv1.EncryptionClass, error)
}

// NewClient creates and returns a new instance of a crypto Client implementation based on
// existing Kubernetes client.
func NewClient(ctx context.Context, k8sClient ctrlclient.Client) Client {
	return &defaultClient{
		Client:       k8sClient,
		csiNamespace: cnsconfig.GetCSINamespace(),
	}
}

// NewClient creates and returns a new instance of a crypto Client implementation based on
// Kubernetes client config.
func NewClientWithConfig(ctx context.Context, config *rest.Config) (Client, error) {
	scheme, err := NewK8sScheme()
	if err != nil {
		return nil, err
	}

	k8sClient, err := ctrlclient.New(config, ctrlclient.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, err
	}

	return NewClient(ctx, k8sClient), nil
}

// NewClient creates and returns a new instance of a crypto Client implementation using
// default Kubernetes client config.
func NewClientWithDefaultConfig(ctx context.Context) (Client, error) {
	config, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}
	return NewClientWithConfig(ctx, config)
}

type defaultClient struct {
	ctrlclient.Client
	csiNamespace string
}

func (c *defaultClient) IsEncryptedStorageClass(ctx context.Context, name string) (bool, string, error) {
	var obj storagev1.StorageClass
	if err := c.Client.Get(
		ctx,
		ctrlclient.ObjectKey{Name: name},
		&obj); err != nil {

		return false, "", ctrlclient.IgnoreNotFound(err)
	}

	return c.isEncryptedStorageClass(ctx, &obj)
}

func (c *defaultClient) IsEncryptedStorageProfile(ctx context.Context, profileID string) (bool, error) {
	var obj storagev1.StorageClassList
	if err := c.Client.List(ctx, &obj); err != nil {
		return false, err
	}

	for i := range obj.Items {
		if pid := GetStoragePolicyID(&obj.Items[i]); pid == profileID {
			ok, _, err := c.isEncryptedStorageClass(ctx, &obj.Items[i])
			return ok, err
		}
	}

	return false, nil
}

func (c *defaultClient) MarkEncryptedStorageClass(
	ctx context.Context,
	storageClass *storagev1.StorageClass,
	encrypted bool) error {

	log := logger.GetLogger(ctx)

	var (
		obj = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: c.csiNamespace,
				Name:      internal.EncryptedStorageClassNamesConfigMapName,
			},
		}
		ownerRef = internal.GetOwnerRefForStorageClass(storageClass)
	)

	// Get the ConfigMap.
	err := c.Client.Get(ctx, ctrlclient.ObjectKeyFromObject(&obj), &obj)

	if err != nil {
		// Return any error other than 404.
		if !apierrors.IsNotFound(err) {
			return err
		}

		// The ConfigMap was not found.
		if !encrypted {
			// If the goal is to mark the StorageClass as not encrypted, then we
			// do not need to actually create the underlying ConfigMap if it
			// does not exist.
			return nil
		}

		// The ConfigMap does not already exist and the goal is to mark the
		// StorageClass as encrypted, so go ahead and create the ConfigMap and
		// return early.
		log.Infof("Creating config map %s for storing references to storage classes "+
			"with encryption capabilities",
			internal.EncryptedStorageClassNamesConfigMapName)
		obj.OwnerReferences = []metav1.OwnerReference{ownerRef}
		return c.Client.Create(ctx, &obj)
	}

	// The ConfigMap already exists, so check if it needs to be updated.
	storageClassIsOwner := slices.Contains(obj.OwnerReferences, ownerRef)

	switch {
	case encrypted && storageClassIsOwner:
		// The StorageClass should be marked encrypted, which means it should
		// be in the ConfigMap. Since the StorageClass is currently set in the
		// ConfigMap, we can return early.
		return nil
	case !encrypted && !storageClassIsOwner:
		// The StorageClass should be marked as not encrypted, which means it
		// should not be in the ConfigMap. Since the StorageClass is not
		// currently in the ConfigMap, we can return return early.
		return nil
	}

	// Create the patch used to update the ConfigMap.
	objPatch := ctrlclient.StrategicMergeFrom(obj.DeepCopyObject().(ctrlclient.Object))
	if encrypted {
		// Add the StorageClass as an owner of the ConfigMap.
		log.Infof("Marking StorageClass %s as encrypted", storageClass.Name)
		obj.OwnerReferences = append(obj.OwnerReferences, ownerRef)
	} else {
		// Remove the StorageClass as an owner of the ConfigMap.
		log.Infof("Unmarking StorageClass %s as encrypted", storageClass.Name)
		obj.OwnerReferences = slices.DeleteFunc(
			obj.OwnerReferences,
			func(o metav1.OwnerReference) bool { return o == ownerRef })
	}

	// Patch the ConfigMap with the change.
	return c.Client.Patch(ctx, &obj, objPatch)
}

func (c *defaultClient) isEncryptedStorageClass(
	ctx context.Context,
	storageClass *storagev1.StorageClass,
) (bool, string, error) {
	var (
		obj    corev1.ConfigMap
		objKey = ctrlclient.ObjectKey{
			Namespace: c.csiNamespace,
			Name:      internal.EncryptedStorageClassNamesConfigMapName,
		}
		ownerRef = internal.GetOwnerRefForStorageClass(storageClass)
	)

	if err := c.Client.Get(ctx, objKey, &obj); err != nil {
		return false, "", ctrlclient.IgnoreNotFound(err)
	}

	if slices.Contains(obj.OwnerReferences, ownerRef) {
		if profileID := GetStoragePolicyID(storageClass); profileID != "" {
			return true, profileID, nil
		}
	}

	return false, "", nil
}

func (c *defaultClient) GetEncryptionClass(
	ctx context.Context,
	name, namespace string,
) (*byokv1.EncryptionClass, error) {
	var obj byokv1.EncryptionClass
	key := ctrlclient.ObjectKey{Namespace: namespace, Name: name}
	err := c.Client.Get(ctx, key, &obj)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (c *defaultClient) GetDefaultEncryptionClass(
	ctx context.Context,
	namespace string,
) (*byokv1.EncryptionClass, error) {
	var list byokv1.EncryptionClassList
	if err := c.Client.List(
		ctx,
		&list,
		ctrlclient.InNamespace(namespace),
		ctrlclient.MatchingLabels{
			DefaultEncryptionClassLabelName: DefaultEncryptionClassLabelValue,
		}); err != nil {

		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, ErrDefaultEncryptionClassNotFound
	}
	if len(list.Items) > 1 {
		return nil, ErrMultipleDefaultEncryptionClasses
	}
	return &list.Items[0], nil
}

func (c *defaultClient) GetEncryptionClassForPVC(
	ctx context.Context,
	name, namespace string,
) (*byokv1.EncryptionClass, error) {
	var pvc corev1.PersistentVolumeClaim
	pvcKey := ctrlclient.ObjectKey{Namespace: namespace, Name: name}
	if err := c.Client.Get(ctx, pvcKey, &pvc); err != nil {
		return nil, err
	}

	encClassName := GetEncryptionClassNameForPVC(&pvc)
	if encClassName == "" {
		return nil, nil
	}

	return c.GetEncryptionClass(ctx, encClassName, namespace)
}
