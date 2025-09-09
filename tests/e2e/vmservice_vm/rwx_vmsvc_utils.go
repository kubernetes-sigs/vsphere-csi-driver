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

package vmservice_vm

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"

	vsantypes "github.com/vmware/govmomi/vsan/types"
	pkgtypes "k8s.io/apimachinery/pkg/types"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
)

// CreateCnsFileAccessConfigCRD creates CnsFileAccessConfigCRD using pvc and VMservice VM name
// in a given namespace
func CreateCnsFileAccessConfigCRD(ctx context.Context, restConfig *rest.Config, pvcName string,
	vmsvcVmName string, namespace string, crdName string) error {
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		return err
	}
	spec := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      crdName,
			Namespace: namespace},
		Spec: cnsfileaccessconfigv1alpha1.CnsFileAccessConfigSpec{
			VMName:  vmsvcVmName,
			PvcName: pvcName,
		},
	}

	if err := cnsOperatorClient.Create(ctx, spec); err != nil {
		return fmt.Errorf("failed to create CNSFileAccessConfig CRD: %s with err: %v", crdName, err)
	}
	return nil
}

// FetchNFSAccessPointFromCnsFileAccessConfigCRD returns a NFS access point from CnsFileAccessConfigCRD
func FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx context.Context, restConfig *rest.Config,
	cnsFileAccessConfigCRDName string, namespace string) (string, error) {
	cfc, err := GetCnsFileAccessConfigCRD(ctx, restConfig, cnsFileAccessConfigCRDName, namespace)
	if err != nil {
		return "", err
	}
	nfs4AccessPoint := cfc.Status.AccessPoints[constants.Nfs4Keyword]
	return nfs4AccessPoint, nil
}

// Helper function to run SSH and log result
func RunSSHFromVmServiceVmAndLog(vmIP, cmd string) error {
	output := execSshOnVmThroughGatewayVm(vmIP, []string{cmd})
	if output[0].Stderr != "" {
		return fmt.Errorf("command failed with error: %s", output[0].Stderr)
	} else {
		framework.Logf("Executed on %s: %s\nOutput: %v", vmIP, cmd, output[0].Stdout)
		return nil
	}
}

// MountRWXVolumeAndVerifyIO mounts RWX volume inside a given VMService VM using the NFS Access point
func MountRWXVolumeAndVerifyIO(vmIPs []string, nfsAccessPoint string, testDir string) error {
	filePrefix := "testfile"
	message := "Hello from"

	var err error
	for _, vmIP := range vmIPs {
		framework.Logf("Setting up NFS access on VM: %s", vmIP)

		setupCmds := []string{
			// Update package index
			"sudo apt-get update",

			// Install NFS utilities
			"sudo apt-get install -y nfs-common",

			// Prepare NFS mount
			fmt.Sprintf("sudo mkdir -p /mnt/nfs/%s", testDir),
			fmt.Sprintf("sudo mount -t nfs %s /mnt/nfs/%s", nfsAccessPoint, testDir),
			fmt.Sprintf("sudo chmod -R 777 /mnt/nfs/%s", testDir),
		}

		for _, cmd := range setupCmds {
			err = RunSSHFromVmServiceVmAndLog(vmIP, cmd)
			if err != nil && !strings.Contains(err.Error(), "does not have a Release file") {
				return err
			}

		}

		// Write a file
		fileName := fmt.Sprintf("%s-%s.txt", filePrefix, strings.ReplaceAll(vmIP, ".", "-"))
		writeCmd := fmt.Sprintf("echo '%s %s' > /mnt/nfs/%s/%s", message, vmIP, testDir, fileName)
		err = RunSSHFromVmServiceVmAndLog(vmIP, writeCmd)
		if err != nil {
			return err
		}
	}

	// Validate reads from each VM
	for _, readerVM := range vmIPs {
		framework.Logf("Reading files from VM: %s", readerVM)
		for _, writerVM := range vmIPs {
			fileName := fmt.Sprintf("%s-%s.txt", filePrefix, strings.ReplaceAll(writerVM, ".", "-"))
			readCmd := fmt.Sprintf("cat /mnt/nfs/%s/%s", testDir, fileName)
			output := execSshOnVmThroughGatewayVm(readerVM, []string{readCmd})
			if !strings.Contains(output[0].Stdout, writerVM) {
				return fmt.Errorf("VM %s failed to read file written by VM %s", readerVM, writerVM)
			} else {
				framework.Logf("VM %s successfully read file written by VM %s: %s", readerVM, writerVM, output[0].Stdout)
				return nil
			}
		}
	}
	return nil
}

// VerifyACLPermissionsOnFileShare verifies ACL permission on a fileshare with a given volume ID and the allowed IPs
func VerifyACLPermissionsOnFileShare(vsanFileShares []vsantypes.VsanFileShare,
	volumeID string, vmIPs []string) error {
	var ipsAllowed []string
	for _, fs := range vsanFileShares {
		if volumeID == fs.Uuid {
			permissions := fs.Config.Permission
			for _, perm := range permissions {
				if perm.Permissions != constants.ReadWritePermission {
					return fmt.Errorf("ACL permissions are incorrect on vsan file share")
				}
				ipsAllowed = append(ipsAllowed, perm.Ips)
			}
		}
	}
	if !k8testutil.CompareStringLists(vmIPs, ipsAllowed) {
		return fmt.Errorf("allowed IPs are not matching with VMService VM IPs")
	}
	framework.Logf("Successfully verified ACL permissions on given file shares")
	return nil
}

// DeleteCnsFileAccessConfig deletes CNSfileaccessconfig CRD with a given name and namespace
func DeleteCnsFileAccessConfig(ctx context.Context, restConfig *rest.Config,
	cnsFileAccessConfigInstanceName string, namespace string) error {
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		return err
	}

	cfc, err := GetCnsFileAccessConfigCRD(ctx, restConfig, cnsFileAccessConfigInstanceName, namespace)
	if err != nil {
		return err
	}

	if err := cnsOperatorClient.Delete(ctx, cfc); err != nil {
		return fmt.Errorf("failed to create CNSFileAccessConfig CRD: %s with err: %v", cnsFileAccessConfigInstanceName, err)
	}
	return nil
}

// GetCnsFileAccessConfigCRD fetches CNSfileaccessconfig CRD with a given name and namespace
func GetCnsFileAccessConfigCRD(ctx context.Context, restConfig *rest.Config,
	cnsFileAccessConfigCRDName string, namespace string) (*cnsfileaccessconfigv1alpha1.CnsFileAccessConfig, error) {
	cfc := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		return nil, err
	}
	err = cnsOperatorClient.Get(ctx, pkgtypes.NamespacedName{Name: cnsFileAccessConfigCRDName,
		Namespace: namespace}, cfc)
	if err != nil {
		return cfc, err
	}
	return cfc, nil
}
