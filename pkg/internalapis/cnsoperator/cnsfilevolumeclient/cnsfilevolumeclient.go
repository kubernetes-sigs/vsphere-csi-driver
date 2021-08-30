/*
Copyright 2021 The Kubernetes Authors.

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

package cnsfilevolumeclient

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/cnsoperator/cnsfilevolumeclient/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

// FileVolumeClient exposes an interface to support
// configuration of CNS file volume ACL's.
type FileVolumeClient interface {
	// GetClientVMsFromIPList returns the list of client vms associated
	// with a given External IP address and CnsFileVolumeClient instance name
	GetClientVMsFromIPList(ctx context.Context, fileVolumeName string, clientVMIP string) ([]string, error)
	// AddClientVMToIPList adds the input clientVMName to the list of
	// clientVMNames that expose the same external clientVMIP for a
	// given file volume. fileVolumeName is used to uniquely
	// identify CnsFileVolumeClient instances.
	AddClientVMToIPList(ctx context.Context, fileVolumeName, clientVMName, clientVMIP string) error
	// RemoveClientVMFromIPList removes the input clientVMName from
	// the list of clientVMNames that expose the same external
	// clientVMIP for a given file volume. fileVolumeName is used
	// to uniquely identify CnsFileVolumeClient instances.
	RemoveClientVMFromIPList(ctx context.Context, fileVolumeName, clientVMName, clientVMIP string) error
}

// fileVolumeClient maintains a client to the API
// server for operations on CnsFileVolumeClient instance.
// It also contains a per instance lock to handle
// concurrent operations.
type fileVolumeClient struct {
	client client.Client
	// Per volume lock for concurrent access to CnsFileVolumeClient instances.
	// Keys are strings representing volume handles (or SV-PVC names).
	// Values are individual sync.Mutex locks that need to be held
	// to make updates to the CnsFileVolumeClient instance on the API server.
	volumeLock *sync.Map
}

var (
	fileVolumeClientInstanceLock sync.Mutex
	fileVolumeClientInstance     *fileVolumeClient
)

// GetFileVolumeClientInstance returns a singleton of type FileVolumeClient.
// Initializes the singleton if not already initialized.
func GetFileVolumeClientInstance(ctx context.Context) (FileVolumeClient, error) {
	fileVolumeClientInstanceLock.Lock()
	defer fileVolumeClientInstanceLock.Unlock()
	if fileVolumeClientInstance == nil {
		log := logger.GetLogger(ctx)
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
		fileVolumeClientInstance = &fileVolumeClient{
			client:     k8sclient,
			volumeLock: &sync.Map{},
		}
	}

	return fileVolumeClientInstance, nil
}

// GetClientVMsFromIPList returns the list of client vms associated with a
// given External IP address and CnsFileVolumeClient instance
// Callers need to specify fileVolumeName as a combination of
// "<SV-namespace>/<SV-PVC-name>". This combination is used to uniquely
// identify CnsFileVolumeClient instances.
// Returns an empty list if the instance doesnt exist OR if the
// input IP address is not present in this instance.
// Returns an error if any operations fails.
func (f *fileVolumeClient) GetClientVMsFromIPList(ctx context.Context,
	fileVolumeName string, clientVMIP string) ([]string, error) {
	log := logger.GetLogger(ctx)

	log.Infof("Fetching client VMs list from cnsfilevolumeclient %s for IP address %s", fileVolumeName, clientVMIP)
	actual, _ := f.volumeLock.LoadOrStore(fileVolumeName, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return nil, fmt.Errorf("failed to cast lock for cnsfilevolumeclient instance: %s", fileVolumeName)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	instance := &v1alpha1.CnsFileVolumeClient{}
	instanceNamespace, instanceName, err := cache.SplitMetaNamespaceKey(fileVolumeName)
	if err != nil {
		log.Errorf("failed to split key %s with error: %+v", fileVolumeName, err)
		return []string{}, err
	}
	instanceKey := types.NamespacedName{
		Namespace: instanceNamespace,
		Name:      instanceName,
	}
	err = f.client.Get(ctx, instanceKey, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// If the get() on the instance fails, then we return empty list.
			log.Infof("Cnsfilevolumeclient instance %s not found. Returning empty list", fileVolumeName)
			return []string{}, nil
		}
		log.Errorf("failed to get cnsfilevolumeclient instance %s with error: %+v", fileVolumeName, err)
		return []string{}, err
	}

	// Verify if input clientVMIP exists in Spec.ExternalIPtoClientVms
	log.Debugf("Verifying if ExternalIPtoClientVms list exists for IP address: %s", clientVMIP)
	clientVMsList, ok := instance.Spec.ExternalIPtoClientVms[clientVMIP]
	if ok {
		return clientVMsList, nil
	}
	return []string{}, nil
}

// AddClientVMToIPList adds the input clientVMName to the list of
// clientVMNames that expose the same external IP address for a
// given CnsFileVolumeClient instance.
// Callers need to specify fileVolumeName as a combination of
// "<SV-namespace>/<SV-PVC-name>". This combination is used to uniquely
// identify CnsFileVolumeClient instances.
// The instance is created if it doesn't exist.
// Returns an error if the operation cannot be persisted on the API server.
func (f *fileVolumeClient) AddClientVMToIPList(ctx context.Context,
	fileVolumeName, clientVMName, clientVMIP string) error {
	log := logger.GetLogger(ctx)

	log.Infof("Adding client VM %s to cnsfilevolumeclient %s list for IP address %s",
		clientVMName, fileVolumeName, clientVMIP)
	actual, _ := f.volumeLock.LoadOrStore(fileVolumeName, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("failed to cast lock for cnsfilevolumeclient instance: %s", fileVolumeName)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()

	instance := &v1alpha1.CnsFileVolumeClient{}
	instanceNamespace, instanceName, err := cache.SplitMetaNamespaceKey(fileVolumeName)
	if err != nil {
		log.Errorf("failed to split key %s with error: %+v", fileVolumeName, err)
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
			instance = &v1alpha1.CnsFileVolumeClient{
				ObjectMeta: v1.ObjectMeta{
					Name:      instanceName,
					Namespace: instanceNamespace,
				},
				Spec: v1alpha1.CnsFileVolumeClientSpec{
					ExternalIPtoClientVms: map[string][]string{
						clientVMIP: {
							clientVMName,
						},
					},
				},
			}
			log.Debugf("Creating cnsfilevolumeclient instance %s with spec: %+v", fileVolumeName, instance)
			err = f.client.Create(ctx, instance)
			if err != nil {
				log.Errorf("failed to create cnsfilevolumeclient instance %s with error: %+v", fileVolumeName, err)
				return err
			}
			return nil
		}
		log.Errorf("failed to get cnsfilevolumeclient instance %s with error: %+v", fileVolumeName, err)
		return err
	}

	// Verify if input clientVM exists in existing ExternalIPtoClientVms list
	// for input IP address.
	log.Debugf("Verifying if VM %s exists in ExternalIPtoClientVms list for IP address: %s. Current list: %+v",
		clientVMName, clientVMIP, instance.Spec.ExternalIPtoClientVms[clientVMIP])
	oldClientVMList := instance.Spec.ExternalIPtoClientVms[clientVMIP]
	for _, oldClientVM := range oldClientVMList {
		if oldClientVM == clientVMName {
			log.Debugf("Found VM %s in list. Returning.", clientVMName)
			return nil
		}
	}
	newClientVMList := append(oldClientVMList, clientVMName)
	instance.Spec.ExternalIPtoClientVms[clientVMIP] = newClientVMList
	log.Debugf("Updating cnsfilevolumeclient instance %s with spec: %+v", fileVolumeName, instance)
	err = f.client.Update(ctx, instance)
	if err != nil {
		log.Errorf("failed to update cnsfilevolumeclient instance %s/%s with error: %+v", fileVolumeName, err)
	}
	return err
}

// RemoveClientVMFromIPList removes the input clientVMName from
// the list of clientVMNames that expose the same external IP
// address for a given CnsFileVolumeClient instance.
// Callers need to specify fileVolumeName as a combination of
// "<SV-namespace>/<SV-PVC-name>". This combination is used to uniquely
// identify CnsFileVolumeClient instances.
// If the given VM was the last client for this file volume, the instance is
// deleted from the API server.
// Returns an error if the operation cannot be persisted on the API server.
func (f *fileVolumeClient) RemoveClientVMFromIPList(ctx context.Context,
	fileVolumeName, clientVMName, clientVMIP string) error {
	log := logger.GetLogger(ctx)
	log.Infof("Removing clientVM %s from cnsfilevolumeclient %s list for IP address %s",
		clientVMName, fileVolumeName, clientVMIP)
	actual, _ := f.volumeLock.LoadOrStore(fileVolumeName, &sync.Mutex{})
	instanceLock, ok := actual.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("failed to cast lock for cnsfilevolumeclient instance: %s", fileVolumeName)
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()
	instance := &v1alpha1.CnsFileVolumeClient{}
	instanceNamespace, instanceName, err := cache.SplitMetaNamespaceKey(fileVolumeName)
	if err != nil {
		log.Errorf("failed to split key %s with error: %+v", fileVolumeName, err)
		return err
	}
	instanceKey := types.NamespacedName{
		Namespace: instanceNamespace,
		Name:      instanceName,
	}
	err = f.client.Get(ctx, instanceKey, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Infof("cnsfilevolumeclient instance %s does not exist on API server", fileVolumeName)
			return nil
		}
		log.Errorf("failed to get cnsfilevolumeclient instance %s with error: %+v", fileVolumeName, err)
		return err
	}

	log.Debugf("Verifying if clientVM %s exists in ExternalIPtoClientVms list for IP address: %s. Current list: %+v",
		clientVMName, clientVMIP, instance.Spec.ExternalIPtoClientVms[clientVMIP])
	for index, existingClientVM := range instance.Spec.ExternalIPtoClientVms[clientVMIP] {
		if clientVMName == existingClientVM {
			log.Debugf("Removing clientVM %s from ExternalIPtoClientVms list", clientVMName)
			instance.Spec.ExternalIPtoClientVms[clientVMIP] = append(
				instance.Spec.ExternalIPtoClientVms[clientVMIP][:index],
				instance.Spec.ExternalIPtoClientVms[clientVMIP][index+1:]...)
			if len(instance.Spec.ExternalIPtoClientVms[clientVMIP]) == 0 {
				log.Debugf("Deleting entry for IP %s from spec.ExternalIPtoClientVms", clientVMIP)
				delete(instance.Spec.ExternalIPtoClientVms, clientVMIP)
			}
			if len(instance.Spec.ExternalIPtoClientVms) == 0 {
				log.Debugf("Deleting cnsfilevolumeclient instance %s from API server", fileVolumeName)
				err = f.client.Delete(ctx, instance)
				if err != nil {
					log.Errorf("failed to delete cnsfilevolumeclient instance %s with error: %+v", fileVolumeName, err)
					return err
				}
				f.volumeLock.Delete(fileVolumeName)
				return nil
			}
			log.Debugf("Updating cnsfilevolumeclient instance %s with spec: %+v", fileVolumeName, instance)
			err = f.client.Update(ctx, instance)
			if err != nil {
				log.Errorf("failed to update cnsfilevolumeclient instance %s with error: %+v", fileVolumeName, err)
			}
			return err
		}
	}
	log.Debugf("Could not find VM %s in list. Returning.", clientVMName)
	return nil
}
