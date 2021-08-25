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
	"os"
	"reflect"
	"strconv"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator-api/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// getVirtualMachine gets the virtual machine instance with a name on a SV
// namespace.
func getVirtualMachine(ctx context.Context, vmOperatorClient client.Client,
	vmName string, namespace string) (*vmoperatortypes.VirtualMachine, error) {
	log := logger.GetLogger(ctx)
	virtualMachine := &vmoperatortypes.VirtualMachine{}
	vmKey := apitypes.NamespacedName{
		Namespace: namespace,
		Name:      vmName,
	}
	if err := vmOperatorClient.Get(ctx, vmKey, virtualMachine); err != nil {
		msg := fmt.Sprintf("Failed to get virtualmachine instance for the VM with name: %q. Error: %+v", vmName, err)
		log.Error(msg)
		return nil, err
	}
	return virtualMachine, nil
}

// setInstanceOwnerRef sets ownerRef on CnsFileAccessConfig instance to VM
// instance.
func setInstanceOwnerRef(instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig, vmName string,
	vmUID apitypes.UID) {
	bController := true
	bOwnerDeletion := true
	kind := reflect.TypeOf(vmoperatortypes.VirtualMachine{}).Name()
	instance.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion:         "v1",
			Controller:         &bController,
			BlockOwnerDeletion: &bOwnerDeletion,
			Kind:               kind,
			Name:               vmName,
			UID:                vmUID,
		},
	}
}

// getMaxWorkerThreadsToReconcileCnsFileAccessConfig returns the maximum number
// of worker threads which can be run to reconcile CnsFileAccessConfig instances.
// If environment variable WORKER_THREADS_FILE_ACCESS_CONFIG is set and valid,
// return the value read from environment variable otherwise, use the default
// value.
func getMaxWorkerThreadsToReconcileCnsFileAccessConfig(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForFileAccessConfig
	if v := os.Getenv("WORKER_THREADS_FILE_ACCESS_CONFIG"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_FILE_ACCESS_CONFIG %s is less than 1, will use the default value %d",
					v, defaultMaxWorkerThreadsForFileAccessConfig)
			} else if value > defaultMaxWorkerThreadsForFileAccessConfig {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_FILE_ACCESS_CONFIG %s is greater than %d, will use the default value %d",
					v, defaultMaxWorkerThreadsForFileAccessConfig, defaultMaxWorkerThreadsForFileAccessConfig)
			} else {
				workerThreads = value
				log.Debugf("Maximum number of worker threads to run to reconcile "+
					"CnsFileAccessConfig instances is set to %d", workerThreads)
			}
		} else {
			log.Warnf("Maximum number of worker threads to run set in env variable "+
				"WORKER_THREADS_FILE_ACCESS_CONFIG %s is invalid, will use the default value %d",
				v, defaultMaxWorkerThreadsForFileAccessConfig)
		}
	} else {
		log.Debugf("WORKER_THREADS_FILE_ACCESS_CONFIG is not set. Picking the default value %d",
			defaultMaxWorkerThreadsForFileAccessConfig)
	}
	return workerThreads
}
