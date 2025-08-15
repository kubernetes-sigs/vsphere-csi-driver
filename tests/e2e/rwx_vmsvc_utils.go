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

package e2e

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"

	pkgtypes "k8s.io/apimachinery/pkg/types"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
)

func createCnsFileAccessConfigCRD(ctx context.Context, restConfig *rest.Config, pvcName string,
	vmsvcVmName string, namespace string, crdName string) error {
	cnsOperatorClient, err := getCnsOperatorClient(ctx)
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

func fetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx context.Context,
	cnsFileAccessConfigCRDName string, namespace string) (string, error) {
	cnsOperatorClient, err := getCnsOperatorClient(ctx)
	if err != nil {
		return "", err
	}
	cfc := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	err = cnsOperatorClient.Get(ctx, pkgtypes.NamespacedName{Name: cnsFileAccessConfigCRDName,
		Namespace: namespace}, cfc)
	if err != nil {
		return "", err
	}
	nfs4AccessPoint := cfc.Status.AccessPoints[nfs4Keyword]
	return nfs4AccessPoint, nil
}

// Helper function to run SSH and log result
func runSSHFromVmServiceVmAndLog(vmIP, cmd string) error {
	output := execSshOnVmThroughGatewayVm(vmIP, []string{cmd})
	if output[0].Stderr != "" {
		return fmt.Errorf("Command failed with error: %s", output[0].Stderr)
	} else {
		framework.Logf("Executed on %s: %s\nOutput: %v", vmIP, cmd, output[0].Stdout)
		return nil
	}
}

func mountRWXVolumeAndVerifyIO(vmIPs []string, nfsAccessPoint string, testDir string) error {
	filePrefix := "testfile"
	message := "Hello from"

	var err error
	for _, vmIP := range vmIPs {
		framework.Logf("Setting up NFS access on VM: %s", vmIP)

		// Install nfs-common and prepare mount point
		/*setupCmds := []string{
			"sudo rm -f /etc/apt/sources.list.d/kubernetes.list",
			"curl -fsSL https://example.com/repo.gpg -o /usr/share/keyrings/example-archive-keyring.gpg",
			"echo 'deb [signed-by=/usr/share/keyrings/example-archive-keyring.gpg] https://example.com/debian stable main' | sudo tee /etc/apt/sources.list.d/example.list",
			fmt.Sprintf("sudo mkdir -p /mnt/nfs/%s", testDir),
			fmt.Sprintf("sudo mount -t nfs %s /mnt/nfs/%s", nfsAccessPoint, testDir),
			fmt.Sprintf("sudo chmod -R 777 /mnt/nfs/%s", testDir),
		}*/
		setupCmds := []string{
			// Clean up old repo if any
			"sudo rm -f /etc/apt/sources.list.d/kubernetes.list",

			// Add Kubernetes APT key
			"sudo mkdir -p /usr/share/keyrings",
			"sudo curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg",

			// Add official Kubernetes repo (kubernetes-xenial works on Ubuntu 20.04)
			`echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | sudo tee /etc/apt/sources.list.d/kubernetes.list`,

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
			err = runSSHFromVmServiceVmAndLog(vmIP, cmd)
			if err != nil {
				return err
			}

		}

		// Write a file
		fileName := fmt.Sprintf("%s-%s.txt", filePrefix, strings.ReplaceAll(vmIP, ".", "-"))
		writeCmd := fmt.Sprintf("echo '%s %s' > /mnt/nfs/%s/%s", message, vmIP, testDir, fileName)
		err = runSSHFromVmServiceVmAndLog(vmIP, writeCmd)
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
