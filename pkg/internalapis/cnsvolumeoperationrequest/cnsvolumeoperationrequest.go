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

package cnsvolumeoperationrequest

import (
	"context"
	"fmt"
	"reflect"

	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	cnsvolumeoperationrequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

// VolumeOperationRequest is an interface that supports handling idempotency
// in CSI volume manager. This interface persists operation details invoked
// on CNS and returns the persisted information to callers whenever it is requested.
type VolumeOperationRequest interface {
	// GetRequestDetails returns the details of the operation on the volume
	// that is persisted by the VolumeOperationRequest interface.
	// Returns an error if any error is encountered while attempting to
	// read the previously persisted information.
	// If no previous information has been persisted, it returns an empty object.
	GetRequestDetails(ctx context.Context, name string) (*VolumeOperationRequestDetails, error)
	// StoreRequestDetails persists the details of the operation taking
	// place on the volume.
	// Returns an error if any error is encountered. Clients must assume
	// that the attempt to persist the information failed if an error is returned.
	StoreRequestDetails(ctx context.Context, instance *VolumeOperationRequestDetails) error
}

// VolumeOperationRequestDetails stores details about a single operation
// on the given volume. These details are persisted by
// VolumeOperationRequestInterface and the persisted details will be
// returned by the interface on request by the caller.
type VolumeOperationRequestDetails struct {
}

// operationRequestStoreOnETCD implements the VolumeOperationsRequest interface.
// This implementation persists the operation information on etcd via a client
// to the API server. Reads are also done directly on etcd; there is no caching
// layer involved.
type operationRequestStoreOnETCD struct {
	k8sclient client.Client
}

const (
	// CRDName represent the name of cnsvolumeoperationrequest CRD
	CRDName = "cnsvolumeoperationrequests.cns.vmware.com"
	// CRDSingular represent the singular name of cnsvolumeoperationrequest CRD
	CRDSingular = "cnsvolumeoperationrequest"
	// CRDPlural represent the plural name of cnsvolumeoperationrequest CRD
	CRDPlural = "cnsvolumeoperationrequests"
)

// InitVolumeOperationRequestInterface creates the CnsVolumeOperationRequest
// definition on the API server and returns an implementation of
// VolumeOperationRequest interface. Clients are unaware of the implementation
// details to read and persist volume operation details.
// This function is not thread safe. Multiple serial calls to this function will
// return multiple new instances of the VolumeOperationRequest interface.
// TODO: Make this thread-safe and a singleton.
func InitVolumeOperationRequestInterface(ctx context.Context) (VolumeOperationRequest, error) {
	log := logger.GetLogger(ctx)

	// Create CnsVolumeOperationRequest definition on API server
	log.Info("Creating cnsvolumeoperationrequest definition on API server")
	err := k8s.CreateCustomResourceDefinitionFromSpec(ctx, CRDName, CRDSingular, CRDPlural,
		reflect.TypeOf(cnsvolumeoperationrequestv1alpha1.CnsVolumeOperationRequest{}).Name(), cnsvolumeoperationrequestv1alpha1.SchemeGroupVersion.Group, cnsvolumeoperationrequestv1alpha1.SchemeGroupVersion.Version, apiextensionsv1beta1.NamespaceScoped)
	if err != nil {
		log.Errorf("failed to create cnsvolumeoperationrequest CRD with error: %v", err)
	}

	// Get in cluster config for client to API server
	config, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("failed to get kubeconfig with error: %v", err)
		return nil, err
	}

	// Create client to API server
	k8sclient, err := k8s.NewClientForGroup(ctx, config, cnsvolumeoperationrequestv1alpha1.SchemeGroupVersion.Group)
	if err != nil {
		log.Errorf("failed to create k8sClient with error: %v", err)
		return nil, err
	}

	// Initialize the operationRequestStoreOnETCD implementation of VolumeOperationRequest
	// interface.
	// NOTE: Currently there is only a single implementation of this interface.
	// Future implementations will need modify this step.
	operationRequestStore := &operationRequestStoreOnETCD{
		k8sclient: k8sclient,
	}

	return operationRequestStore, nil
}

// GetRequestDetails returns the details of the operation on the volume
// that is persisted by the VolumeOperationRequest interface, by querying
// API server for a CnsVolumeOperationRequest instance with the given
// name.
// Returns an error if any error is encountered while attempting to
// read the previously persisted information from the API server.
// If no previous information has been persisted, it returns an empty object.
func (or *operationRequestStoreOnETCD) GetRequestDetails(ctx context.Context, name string) (*VolumeOperationRequestDetails, error) {
	log := logger.GetLogger(ctx)
	// TODO: Implement GetRequestDetails for operationRequestStoreOnETCD
	err := fmt.Errorf("GetRequestDetails not implemented for operationRequestStoreOnETCD")
	log.Error(err)
	return nil, err
}

// StoreRequestDetails persists the details of the operation taking
// place on the volume by storing it on the API server.
// Returns an error if any error is encountered. Clients must assume
// that the attempt to persist the information failed if an error is returned.
func (or *operationRequestStoreOnETCD) StoreRequestDetails(ctx context.Context, instance *VolumeOperationRequestDetails) error {
	log := logger.GetLogger(ctx)
	// TODO: Implement StoreRequestDetails for operationRequestStoreOnETCD
	err := fmt.Errorf("StoreRequestDetails not implemented for operationRequestStoreOnETCD")
	log.Error(err)
	return err
}
