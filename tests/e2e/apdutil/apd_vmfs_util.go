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
package apdutil

import (
	"context"
	"fmt"
	"sync"

	"github.com/onsi/ginkgo/v2"
	"k8s.io/kubernetes/test/e2e/framework"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
)

// This function inject APD in given VMFS datastore
func InjectAPDToVMFS(ctx context.Context, e2eTestConfig *config.E2eTestConfig, scsi string, hostIp string) (success bool) {
	executed := false
	enableErrorInjectCmd := constants.VSISH_SET + constants.INJECT_ERROR
	injectAPDCmd := constants.VSISH_SET + constants.STORAGE_PATH + scsi + constants.ERROR + constants.INJECT_APD_CODE
	k8testutil.RunCommandOnHost(ctx, enableErrorInjectCmd,
		e2eTestConfig, hostIp)
	k8testutil.RunCommandOnHost(ctx, injectAPDCmd,
		e2eTestConfig, hostIp)
	return executed
}

func InjectAPDToVMFSWithWaitGroup(ctx context.Context, e2eTestConfig *config.E2eTestConfig, scsi string, hostIp string, wgMain *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wgMain.Done()
	framework.Logf("Injecting APD on Lun : %s  from host : %s ", scsi, hostIp)
	InjectAPDToVMFS(ctx, e2eTestConfig, scsi, hostIp)
}

func ClearAPDToVMFSWithWaitGroup(ctx context.Context, e2eTestConfig *config.E2eTestConfig, scsi string, hostIp string, wgMain *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wgMain.Done()
	framework.Logf("Clearing APD on Lun : %s  from host : %s ", scsi, hostIp)
	ClearAPDToVMFS(ctx, e2eTestConfig, scsi, hostIp)
}

// This function clear APD in given VMFS datastore
func ClearAPDToVMFS(ctx context.Context, e2eTestConfig *config.E2eTestConfig, scsi string, hostIp string) (success bool) {
	executed := false
	disableErrorInject := constants.VSISH_SET + constants.CLEAR_ERROR
	removeAPDCommand := constants.VSISH_SET + constants.STORAGE_PATH + scsi + constants.ERROR + constants.CLEAR_APD_CODE

	_, err := k8testutil.RunCommandOnHost(ctx, disableErrorInject,
		e2eTestConfig, hostIp)
	if err != nil {
		fmt.Printf("Got error while enabling error injection : %v\n", err)
		return executed
	}
	_, err = k8testutil.RunCommandOnHost(ctx, removeAPDCommand,
		e2eTestConfig, hostIp)
	if err != nil {
		fmt.Printf("Got error while enabling error injection : %v\n", err)
		return executed
	}
	return true
}
