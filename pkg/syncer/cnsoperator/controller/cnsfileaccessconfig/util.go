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

package cnsfileaccessconfig

import (
	"context"
	"fmt"
	"reflect"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// getVirtualMachine gets the virtual machine instance with a name on a SV
// namespace.
func getVirtualMachine(ctx context.Context, vmOperatorClient client.Client,
	vmName string, namespace string) (*vmoperatortypes.VirtualMachine, string, error) {
	log := logger.GetLogger(ctx)
	vmKey := apitypes.NamespacedName{
		Namespace: namespace,
		Name:      vmName,
	}
	virtualMachine, apiVersion, err := utils.GetVirtualMachineAllApiVersions(ctx,
		vmKey, vmOperatorClient)
	if err != nil {
		msg := fmt.Sprintf("Failed to get virtualmachine instance for the VM with name: %q. Error: %+v", vmName, err)
		log.Error(msg)
		return nil, apiVersion, err
	}
	return virtualMachine, apiVersion, nil
}

// setInstanceOwnerRef sets ownerRef on CnsFileAccessConfig instance to VM
// instance.
func setInstanceOwnerRef(instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig, vmName string,
	vmUID apitypes.UID, apiVersion string) {
	bController := true
	bOwnerDeletion := true
	kind := reflect.TypeOf(vmoperatortypes.VirtualMachine{}).Name()
	instance.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion:         apiVersion,
			Controller:         &bController,
			BlockOwnerDeletion: &bOwnerDeletion,
			Kind:               kind,
			Name:               vmName,
			UID:                vmUID,
		},
	}
}
