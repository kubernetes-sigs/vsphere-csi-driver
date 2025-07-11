/*
Copyright 2025 The Kubernetes Authors.

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

package cnssharedblockvolumeinfo

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/cnssharedblockvolumeinfo/v1alpha1"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// SharedBlockVolumeInfo exposes an interface to support adding
// and removing information about attached VMs to a PVC.
type SharedBlockVolumeInfo interface {
	// Add adds the input VM UUID to the list of
	// attached VMs for the given shared block volume.
	AddVmToAttachedList(ctx context.Context, sharedBlockVolumeName, VmUUID string) error
	// RemoveVmFromAttachedList removes the input VM UUID from
	// the list of attached VMs for the given shared block volume.
	RemoveVmFromAttachedList(ctx context.Context, sharedBlockVolumeName, VmUUID string) error
}

// sharedBlockVolumeInfo maintains a client to the API
// server for operations on SharedBlockVolumeInfo instance.
// It also contains a per instance lock to handle
// concurrent operations.
type sharedBlockVolumeInfo struct {
	client client.Client
	// Per volume lock for concurrent access to SharedBlockVolumeInfo instances.
	// Keys are strings representing PVC names.
	// Values are individual sync.Mutex locks that need to be held
	// to make updates to the SharedBlockVolumeInfo instance on the API server.
	volumeLock *sync.Map
}

var (
	sharedBlockClientInstanceLock   sync.Mutex
	sharedBlockVolumeClientInstance *sharedBlockVolumeInfo
)

// Add adds the input VM UUID to the list of
// attached VMs for the given shared block volume.
// Callers need to specify sharedBlockVolumeName as a combination of
// "<SV-namespace>/<SV-PVC-name>". This combination is used to uniquely
// identify CnsSharedBlockVolumeInfo instances.
// The instance is created if it doesn't exist.
// Returns an error if the operation cannot be persisted on the API server.
func (f *sharedBlockVolumeInfo) AddVmToAttachedList(ctx context.Context,
	sharedBlockVolumeName, VmUUID string) error {
	log := logger.GetLogger(ctx)

	log.Infof("Adding VM %s to cnsSharedBlockVolumeInfo %s",
		VmUUID, sharedBlockVolumeName)
	actual, _ := f.volumeLock.LoadOrStore(sharedBlockVolumeName, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("failed to cast lock for cnsSharedBlockVolumeInfo instance: %s", sharedBlockVolumeName)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	instance := &v1alpha1.CnsSharedBlockVolumeInfo{}
	instanceNamespace, instanceName, err := cache.SplitMetaNamespaceKey(sharedBlockVolumeName)
	if err != nil {
		log.Errorf("failed to split key %s with error: %+v", sharedBlockVolumeName, err)
		return err
	}
	instanceKey := types.NamespacedName{
		Namespace: instanceNamespace,
		Name:      instanceName,
	}
	err = f.client.Get(ctx, instanceKey, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Create the instance as it does not exist on the API server.
			instance = &v1alpha1.CnsSharedBlockVolumeInfo{
				ObjectMeta: v1.ObjectMeta{
					Name:      instanceName,
					Namespace: instanceNamespace,
					// Add finalizer so that CnSharedBlockVolumeInfo instance doesn't get deleted abruptly
					Finalizers: []string{cnsoperatortypes.CNSFinalizer},
				},
				Spec: v1alpha1.CnsSharedBlockVolumeInfoSpec{
					AttachedVms: []string{
						VmUUID,
					},
				},
			}
			log.Debugf("Creating cnsSharedBlockVolumeInfo instance %s with spec: %+v", sharedBlockVolumeName, instance)
			err = f.client.Create(ctx, instance)
			if err != nil {
				log.Errorf("failed to create cnsSharedBlockVolumeInfo instance %s with error: %+v", sharedBlockVolumeName, err)
				return err
			}
			return nil
		}
		log.Errorf("failed to get cnsSharedBlockVolumeInfo instance %s with error: %+v", sharedBlockVolumeName, err)
		return err
	}

	// Verify if input VmUUID exists in existing AttachedVMs list.
	log.Debugf("Verifying if VM %s exists in current list of attached Vms. Current list: %+v",
		VmUUID, instance.Spec.AttachedVms)
	oldAttachedVmsList := instance.Spec.AttachedVms
	for _, oldAttachedVM := range oldAttachedVmsList {
		if oldAttachedVM == VmUUID {
			log.Debugf("Found VM %s in list. Returning.", VmUUID)
			return nil
		}
	}
	newAttachVmsList := append(oldAttachedVmsList, VmUUID)
	instance.Spec.AttachedVms = newAttachVmsList
	log.Debugf("Updating cnsSharedBlockVolumeInfo instance %s with spec: %+v", sharedBlockVolumeName, instance)
	err = f.client.Update(ctx, instance)
	if err != nil {
		log.Errorf("failed to update cnsSharedBlockVolumeInfo instance %s/%s with error: %+v", sharedBlockVolumeName, err)
	}
	return err
}

// RemoveVmFromAttachedList removes the input VM UUID from
// the list of attached VMs for the given shared block volume.
// Callers need to specify sharedBlockVolumeName as a combination of
// "<SV-namespace>/<SV-PVC-name>". This combination is used to uniquely
// identify CnsSharedBlockVolumeInfo instances.
// If the given VM was the last client for this file volume, the instance is
// deleted from the API server.
// Returns an error if the operation cannot be persisted on the API server.
func (f *sharedBlockVolumeInfo) RemoveVmFromAttachedList(ctx context.Context,
	sharedBlockVolumeName, VmUUID string) error {
	log := logger.GetLogger(ctx)
	log.Infof("Removing VmUUID %s from cnsSharedBlockVolumeInfo %s",
		VmUUID, sharedBlockVolumeName)
	actual, _ := f.volumeLock.LoadOrStore(sharedBlockVolumeName, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("failed to cast lock for cnsSharedBlockVolumeInfo instance: %s", sharedBlockVolumeName)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()
	instance := &v1alpha1.CnsSharedBlockVolumeInfo{}
	instanceNamespace, instanceName, err := cache.SplitMetaNamespaceKey(sharedBlockVolumeName)
	if err != nil {
		log.Errorf("failed to split key %s with error: %+v", sharedBlockVolumeName, err)
		return err
	}
	instanceKey := types.NamespacedName{
		Namespace: instanceNamespace,
		Name:      instanceName,
	}
	err = f.client.Get(ctx, instanceKey, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Infof("cnsSharedBlockVolumeInfo instance %s does not exist on API server", sharedBlockVolumeName)
			return nil
		}
		log.Errorf("failed to get cnsSharedBlockVolumeInfo instance %s with error: %+v", sharedBlockVolumeName, err)
		return err
	}

	log.Debugf("Verifying if VM UUID %s exists in list of already attached VMs. Current list: %+v",
		sharedBlockVolumeName, instance.Spec.AttachedVms)
	for index, existingAttachedtVM := range instance.Spec.AttachedVms {
		if sharedBlockVolumeName == existingAttachedtVM {
			log.Debugf("Removing VmUUID %s from Attached VMs list", VmUUID)
			instance.Spec.AttachedVms = append(
				instance.Spec.AttachedVms[:index],
				instance.Spec.AttachedVms[index+1:]...)
			if len(instance.Spec.AttachedVms) == 0 {
				log.Infof("Deleting cnsSharedBlockVolumeInfo instance %s from API server", sharedBlockVolumeName)
				// Remove finalizer from CnsSharedBlockVolumeInfo instance
				err = removeFinalizer(ctx, f.client, instance)
				if err != nil {
					log.Errorf("failed to remove finalizer from cnsSharedBlockVolumeInfo instance %s with error: %+v",
						sharedBlockVolumeName, err)
				}
				err = f.client.Delete(ctx, instance)
				if err != nil {
					// In case of namespace deletion, we will have deletion timestamp added on the
					// CnsSharedBlockVolumeInfo instance. So, as soon as we delete finalizer, instance might
					// get deleted immediately. In such cases we will get NotFound error here, return success
					// if instance is already deleted.
					if errors.IsNotFound(err) {
						log.Infof("cnsSharedBlockVolumeInfo instance %s seems to be already deleted.", sharedBlockVolumeName)
						f.volumeLock.Delete(sharedBlockVolumeName)
						return nil
					}
					log.Errorf("failed to delete cnsSharedBlockVolumeInfo instance %s with error: %+v", sharedBlockVolumeName, err)
					return err
				}
				f.volumeLock.Delete(sharedBlockVolumeName)
				return nil
			}
			log.Debugf("Updating cnsSharedBlockVolumeInfo instance %s with spec: %+v", sharedBlockVolumeName, instance)
			err = f.client.Update(ctx, instance)
			if err != nil {
				log.Errorf("failed to update cnsSharedBlockVolumeInfo instance %s with error: %+v", sharedBlockVolumeName, err)
			}
			return err
		}
	}
	log.Debugf("Could not find VM %s in list. Returning.", VmUUID)
	return nil
}

// removeFinalizer will remove the CNS Finalizer = cns.vmware.com,
// from a given CnsSharedBlockVolumeInfo instance.
func removeFinalizer(ctx context.Context, client client.Client,
	instance *v1alpha1.CnsSharedBlockVolumeInfo) error {
	if !controllerutil.ContainsFinalizer(instance, cnsoperatortypes.CNSFinalizer) {
		// Finalizer not present on instance. Nothing to do.
		return nil
	}

	if !controllerutil.AddFinalizer(instance, cnsoperatortypes.CNSFinalizer) {
		return fmt.Errorf("failed to add CNS finalizer %s to CnsSharedBlockVolumeInfo "+
			"instance %s in namespace %s", cnsoperatortypes.CNSFinalizer, instance.Name,
			instance.Namespace)
	}

	err := client.Update(ctx, instance)
	if err != nil {
		return fmt.Errorf("failed to update finalizer CnsSharedBlockVolumeInfo instance with name: %q on namespace: %q",
			instance.Name, instance.Namespace)
	}

	return nil
}
