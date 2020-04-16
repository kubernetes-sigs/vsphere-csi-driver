/*
Copyright 2019 The Kubernetes Authors.

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

package manager

import (
	"context"
	"fmt"
	"reflect"

	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"

	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsnodevmattachment/v1alpha1"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsvolumemetadata/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/controller"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

var (
	// Use localhost and port for metrics
	metricsHost       = "0.0.0.0"
	metricsPort int32 = 8383
)

// InitCnsOperator initializes the Cns Operator
func InitCnsOperator(configInfo *types.ConfigInfo) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	log.Infof("Initializing CNS Operator")
	vCenter, err := types.GetVirtualCenterInstance(ctx, configInfo)
	if err != nil {
		return err
	}
	volumeManager := volumes.GetManager(ctx, vCenter)

	// Get a config to talk to the apiserver
	cfg, err := config.GetConfig()
	if err != nil {
		log.Errorf("failed to get Kubernetes config. Err: %+v", err)
		return err
	}
	// TODO: Verify leader election for CNS Operator in multi-master mode
	// Create CnsNodeVmAttachment CRD
	crdKindNodeVMAttachment := reflect.TypeOf(cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}).Name()
	crdNameNodeVMAttachment := apis.CnsNodeVMAttachmentPlural + "." + apis.SchemeGroupVersion.Group
	err = k8s.CreateCustomResourceDefinition(ctx, crdKindNodeVMAttachment, apis.CnsNodeVMAttachmentSingular, apis.CnsNodeVMAttachmentPlural,
		crdNameNodeVMAttachment, apis.SchemeGroupVersion.Group, apis.SchemeGroupVersion.Version, apiextensionsv1beta1.NamespaceScoped)
	if err != nil {
		log.Errorf("failed to create %q CRD. Err: %+v", crdNameNodeVMAttachment, err)
		return err
	}

	// Create CnsVolumeMetadata CRD
	crdKindVolumeMetadata := reflect.TypeOf(cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}).Name()
	crdNameVolumeMetadata := apis.CnsVolumeMetadataPlural + "." + apis.SchemeGroupVersion.Group

	err = k8s.CreateCustomResourceDefinition(ctx, crdNameVolumeMetadata, apis.CnsVolumeMetadataSingular, apis.CnsVolumeMetadataPlural,
		crdKindVolumeMetadata, apis.SchemeGroupVersion.Group, apis.SchemeGroupVersion.Version, apiextensionsv1beta1.NamespaceScoped)
	if err != nil {
		log.Errorf("failed to create %q CRD. Err: %+v", crdKindVolumeMetadata, err)
		return err
	}

	err = createCustomResourceDefinitionFromManifest(ctx, apiextensionsClientSet, "cnsregistervolume_crd.yaml")
	if err != nil {
		log.Errorf("Failed to create %q CRD. Err: %+v", apis.CnsRegisterVolumePlural, err)
		return err
	}

	// Create a new operator to provide shared dependencies and start components
	// Setting namespace to empty would let operator watch all namespaces.
	mgr, err := manager.New(cfg, manager.Options{
		Namespace:          "",
		MetricsBindAddress: fmt.Sprintf("%s:%d", metricsHost, metricsPort),
	})
	if err != nil {
		log.Errorf("failed to create new Cns operator instance. Err: %+v", err)
		return err
	}

	log.Info("Registering Components for Cns Operator")

	// Setup Scheme for all resources
	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		log.Errorf("failed to set the scheme for Cns operator. Err: %+v", err)
		return err
	}

	// Setup all Controllers
	if err := controller.AddToManager(mgr, configInfo, volumeManager); err != nil {
		log.Errorf("failed to setup the controller for Cns operator. Err: %+v", err)
		return err
	}

	log.Info("Starting Cns Operator")

	// Start the operator
	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		log.Errorf("failed to start Cns operator. Err: %+v", err)
		return err
	}
	return nil
}
