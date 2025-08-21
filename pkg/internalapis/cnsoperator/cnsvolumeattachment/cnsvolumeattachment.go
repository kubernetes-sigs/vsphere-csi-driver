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

package cnsvolumeattachment

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
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/cnsvolumeattachment/v1alpha1"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

var (
	cnsVolumeAttachmentInstanceLock sync.Mutex
	cnsVolumeAttachmentInstance     *cnsVolumeAttachment
)

// CnsVolumeAttachment exposes an interface to support adding
// and removing information about attached VMs to a PVC.
type CnsVolumeAttachment interface {
	// AddVmToAttachedList adds the input VM instance UUID to the list of
	// attached VMs for the given volume.
	AddVmToAttachedList(ctx context.Context, volumeName, VmInstanceUUID string) error
	// RemoveVmFromAttachedList removes the input instance VM UUID from
	// the list of attached VMs for the given volume.
	RemoveVmFromAttachedList(ctx context.Context, volumeName, VmInstanceUUID string) (error, bool)
}

// cnsVolumeAttachment maintains a client to the API
// server for operations on CnsVolumeAttachment instance.
// It also contains a per instance lock to handle
// concurrent operations.
type cnsVolumeAttachment struct {
	client client.Client
	// Per volume lock for concurrent access to CnsVolumeAttachment instances.
	// Keys are strings representing PVC names.
	// Values are individual sync.Mutex locks that need to be held
	// to make updates to the CnsVolumeAttachment instance on the API server.
	volumeLock *sync.Map
}

// GetCnsVolumeAttachmentInstance returns a singleton of type CnsVolumeAttachment.
// Initializes the singleton if not already initialized.
func GetCnsVolumeAttachmentInstance(ctx context.Context) (CnsVolumeAttachment, error) {
	log := logger.GetLogger(ctx)

	cnsVolumeAttachmentInstanceLock.Lock()
	log.Infof("Acquired lock for cnsVolumeAttachmentInstanceLock")
	defer func() {
		cnsVolumeAttachmentInstanceLock.Unlock()
		log.Infof("Released lock for cnsVolumeAttachmentInstanceLock")
	}()

	if cnsVolumeAttachmentInstance == nil {
		config, err := k8s.GetKubeConfig(ctx)
		if err != nil {
			log.Errorf("failed to get kubeconfig. Err: %v", err)
			return nil, err
		}
		k8sclient, err := k8s.NewClientForGroup(ctx, config, internalapis.GroupName)
		if err != nil {
			log.Errorf("failed to create k8s client. Err: %v", err)
			return nil, err
		}
		cnsVolumeAttachmentInstance = &cnsVolumeAttachment{
			client:     k8sclient,
			volumeLock: &sync.Map{},
		}
	}

	return cnsVolumeAttachmentInstance, nil
}

// Add adds the input VM InstanceUUID to the list of
// attached VMs for the given volume.
// Callers need to specify cnsVolumeAttachment as a combination of
// "<SV-namespace>/<SV-PVC-name>". This combination is used to uniquely
// identify CnsVolumeAttachment instances.
// The instance is created if it doesn't exist.
// Returns an error if the operation cannot be persisted on the API server.
func (f *cnsVolumeAttachment) AddVmToAttachedList(ctx context.Context,
	volumeName, VmInstanceUUID string) error {
	log := logger.GetLogger(ctx)

	log.Infof("Adding VM %s to cnsVolumeAttachment %s",
		VmInstanceUUID, volumeName)
	actual, _ := f.volumeLock.LoadOrStore(volumeName, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("failed to cast lock for cnsVolumeAttachment instance: %s", volumeName)
	}
	instanceLock.Lock()
	log.Infof("Acquired lock for cnsVolumeAttachment instance %s", volumeName)
	defer func() {
		instanceLock.Unlock()
		log.Infof("Released lock for instance %s", volumeName)
	}()

	instance := &v1alpha1.CnsVolumeAttachment{}
	instanceNamespace, instanceName, err := cache.SplitMetaNamespaceKey(volumeName)
	if err != nil {
		log.Errorf("failed to split key %s with error: %+v", volumeName, err)
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
			instance = &v1alpha1.CnsVolumeAttachment{
				ObjectMeta: v1.ObjectMeta{
					Name:      instanceName,
					Namespace: instanceNamespace,
					// Add finalizer so that CnsVolumeAttachment instance doesn't get deleted abruptly
					Finalizers: []string{cnsoperatortypes.CNSFinalizer},
				},
				Spec: v1alpha1.CnsVolumeAttachmentSpec{
					AttachedVms: []string{
						VmInstanceUUID,
					},
				},
			}
			log.Debugf("Creating cnsVolumeAttachment instance %s with spec: %+v", volumeName, instance)
			err = f.client.Create(ctx, instance)
			if err != nil {
				log.Errorf("failed to create cnsVolumeAttachment instance %s with error: %+v", volumeName, err)
				return err
			}
			return nil
		}
		log.Errorf("failed to get cnsVolumeAttachment instance %s with error: %+v", volumeName, err)
		return err
	}

	// Verify if input VmInstanceUUID exists in existing AttachedVMs list.
	log.Debugf("Verifying if VM %s exists in current list of attached Vms. Current list: %+v",
		VmInstanceUUID, instance.Spec.AttachedVms)
	currentAttachedVmsList := instance.Spec.AttachedVms
	for _, currentAttachedVM := range currentAttachedVmsList {
		if currentAttachedVM == VmInstanceUUID {
			log.Debugf("Found VM %s in list. Returning.", VmInstanceUUID)
			return nil
		}
	}
	newAttachVmsList := append(currentAttachedVmsList, VmInstanceUUID)
	instance.Spec.AttachedVms = newAttachVmsList
	log.Debugf("Updating cnsVolumeAttachment instance %s with spec: %+v", volumeName, instance)
	err = f.client.Update(ctx, instance)
	if err != nil {
		log.Errorf("failed to update cnsVolumeAttachment instance %s/%s with error: %+v", volumeName, err)
	}
	return err
}

// RemoveVmFromAttachedList removes the input VM UUID from
// the list of attached VMs for the given volume.
// Callers need to specify volumeName as a combination of
// "<SV-namespace>/<SV-PVC-name>". This combination is used to uniquely
// identify CnsVolumeAttachment instances.
// If the given VM was the last client for this file volume, the instance is
// deleted from the API server.
// Returns an error if the operation cannot be persisted on the API server.
func (f *cnsVolumeAttachment) RemoveVmFromAttachedList(ctx context.Context,
	volumeName, VmInstanceUUID string) (error, bool) {
	log := logger.GetLogger(ctx)
	log.Infof("Removing VmInstanceUUID %s from cnsVolumeAttachment %s",
		VmInstanceUUID, volumeName)
	actual, _ := f.volumeLock.LoadOrStore(volumeName, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("failed to cast lock for cnsVolumeAttachment instance: %s", volumeName),
			false
	}
	instanceLock.Lock()
	log.Infof("Acquired lock for cnsVolumeAttachment instance %s", volumeName)
	defer func() {
		instanceLock.Unlock()
		log.Infof("Released lock for instance %s", volumeName)
	}()

	instance := &v1alpha1.CnsVolumeAttachment{}
	instanceNamespace, instanceName, err := cache.SplitMetaNamespaceKey(volumeName)
	if err != nil {
		log.Errorf("failed to split key %s with error: %+v", volumeName, err)
		return err, false
	}
	instanceKey := types.NamespacedName{
		Namespace: instanceNamespace,
		Name:      instanceName,
	}
	err = f.client.Get(ctx, instanceKey, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Infof("cnsVolumeAttachment instance %s does not exist on API server", volumeName)
			return nil, true
		}
		log.Errorf("failed to get cnsVolumeAttachment instance %s with error: %+v", volumeName, err)
		return err, false
	}

	log.Infof("Verifying if VM UUID %s exists in list of already attached VMs. Current list: %+v",
		volumeName, instance.Spec.AttachedVms)
	for index, existingAttachedVM := range instance.Spec.AttachedVms {
		if VmInstanceUUID != existingAttachedVM {
			continue
		}
		log.Infof("Removing VmUUID %s from Attached VMs list", VmInstanceUUID)
		instance.Spec.AttachedVms = append(
			instance.Spec.AttachedVms[:index],
			instance.Spec.AttachedVms[index+1:]...)
		if len(instance.Spec.AttachedVms) == 0 {
			log.Infof("Deleting cnsVolumeAttachment instance %s from API server", volumeName)
			// Remove finalizer from CnsVolumeAttachment instance
			err = removeFinalizer(ctx, f.client, instance)
			if err != nil {
				log.Errorf("failed to remove finalizer from cnsVolumeAttachment instance %s with error: %+v",
					volumeName, err)
				return err, false
			}
			err = f.client.Delete(ctx, instance)
			if err != nil {
				// In case of namespace deletion, we will have deletion timestamp added on the
				// CnsVolumeAttachment instance. So, as soon as we delete finalizer, instance might
				// get deleted immediately. In such cases we will get NotFound error here, return success
				// if instance is already deleted.
				if errors.IsNotFound(err) {
					log.Infof("cnsVolumeAttachment instance %s seems to be already deleted.", volumeName)
					f.volumeLock.Delete(volumeName)
					return nil, true
				}
				log.Errorf("failed to delete cnsVolumeAttachment instance %s with error: %+v", volumeName, err)
				return err, false
			}
			log.Infof("Successfully deleted cnsVolumeAttachment instance %s", volumeName)
			f.volumeLock.Delete(volumeName)
			return nil, true
		}
		log.Debugf("Updating cnsVolumeAttachment instance %s with spec: %+v", volumeName, instance)
		err = f.client.Update(ctx, instance)
		if err != nil {
			log.Errorf("failed to update cnsVolumeAttachment instance %s with error: %+v", volumeName, err)
		}
		return err, false
	}
	log.Infof("Could not find VM %s in list. Returning.", VmInstanceUUID)
	return nil, false
}

// removeFinalizer will remove the CNS Finalizer = cns.vmware.com,
// from a given CnsVolumeAttachment instance.
func removeFinalizer(ctx context.Context, client client.Client,
	instance *v1alpha1.CnsVolumeAttachment) error {
	log := logger.GetLogger(ctx)

	if !controllerutil.ContainsFinalizer(instance, cnsoperatortypes.CNSFinalizer) {
		// Finalizer not present on instance. Nothing to do.
		return nil
	}

	finalizersOnInstance := instance.Finalizers
	for i, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			log.Infof("Removing %q finalizer from CnsNodeVmBatchAttachment instance with name: %q on namespace: %q",
				cnsoperatortypes.CNSFinalizer, instance.Name, instance.Namespace)
			finalizersOnInstance = append(instance.Finalizers[:i], instance.Finalizers[i+1:]...)
			break
		}
	}
	return k8s.PatchFinalizers(ctx, client, instance, finalizersOnInstance)
}
