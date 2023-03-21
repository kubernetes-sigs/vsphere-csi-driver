/*
Copyright 2023 The Kubernetes Authors.

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
package fault

import (
	"context"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

var (
	// VimNonStorageFaultsList contains the list of faults that needs to classified as non-storage faults.
	VimNonStorageFaultsList = []string{VimFaultInvalidHostState, VimFaultHostNotConnected}
)

// IsNonStorageFault checks if the fault type is in a pre-defined list of non-storage faults
// and returns a bool value accordingly.
func IsNonStorageFault(fault string) bool {
	for _, nonStorageFault := range VimNonStorageFaultsList {
		if nonStorageFault == fault {
			return true
		}
	}
	return false
}

// AddCsiNonStoragePrefix adds "csi.fault.nonstorage." prefix to the faults.
func AddCsiNonStoragePrefix(ctx context.Context, fault string) string {
	log := logger.GetLogger(ctx)
	if fault != "" {
		log.Infof("Adding %q prefix to fault %q", CSINonStorageFaultPrefix, fault)
		return CSINonStorageFaultPrefix + fault
	}
	return fault
}
