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

package rwx_vmservice_vm

import (
	"context"
	"fmt"
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vsantypes "github.com/vmware/govmomi/vsan/types"
	pkgtypes "k8s.io/apimachinery/pkg/types"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vmservice_vm"
)

var e2eTestConfig *config.E2eTestConfig

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
	output := vmservice_vm.ExecSshOnVmThroughGatewayVm(vmIP, []string{cmd})
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
			if err != nil && !strings.Contains(err.Error(), "no longer has a Release file") {
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
			output := vmservice_vm.ExecSshOnVmThroughGatewayVm(readerVM, []string{readCmd})
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

// CreateCnsFileAccessConfigCRD creates CnsFileAccessConfigCRD using pvc and VMservice VM name
// in a given namespace with WaitGroup
func CreateCnsFileAccessConfigCRDWithWg(ctx context.Context, restConfig *rest.Config, pvcName string,
	vmsvcVmName string, namespace string, crdName string, wg *sync.WaitGroup) {
	defer wg.Done()
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		framework.Logf("failed to create CNSFileAccessConfig CRD: %s with err: %v", crdName, err)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func CreateMultiplePVCs(ctx context.Context, adminClient clientset.Interface, client clientset.Interface,
	vs *config.E2eTestConfig, namespace string, storageClassName string,
	labelsMap map[string]string, accessMode v1.PersistentVolumeAccessMode, pvcCount int) ([]*v1.PersistentVolumeClaim, []string, error) {

	var pvcList []*v1.PersistentVolumeClaim
	var volHandles []string

	// Get the storage class
	storageclass, err := adminClient.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get storage class: %v", err)
	}

	// Create PVCs
	for i := 0; i < pvcCount; i++ {
		pvc, pvs, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, vs, namespace, labelsMap, accessMode,
			constants.DiskSize, storageclass, true)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create PVC: %v", err)
		}
		pvcList = append(pvcList, pvc)
		volHandles = append(volHandles, pvs[0].Spec.CSI.VolumeHandle)
	}

	return pvcList, volHandles, nil
}

func CreateVmServiceVms(ctx context.Context, client clientset.Interface, vmopC ctlrclient.Client,
	namespace string, vmClass string, vmi string, storageClassName string,
	vmCount int) ([]*vmopv1.VirtualMachine, []string, *vmopv1.VirtualMachineService, string, error) {

	var vmIPs []string

	// Create VM bootstrap secret
	secretName := vmservice_vm.CreateBootstrapSecretForVmsvcVms(ctx, client, namespace)

	// Create VMs
	vms := vmservice_vm.CreateStandaloneVmServiceVm(
		ctx, vmopC, namespace, vmClass, vmi, storageClassName, secretName, vmopv1.VirtualMachinePoweredOn, vmCount)

	// Create load balancing service for SSH
	vmlbsvc := vmservice_vm.CreateService4Vm(ctx, vmopC, namespace, vms[0].Name)

	// Wait for VM IPs
	for _, vm := range vms {
		vmIp, err := vmservice_vm.WaitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		if err != nil {
			return nil, nil, nil, "", fmt.Errorf("failed to get IP for VM %s: %v", vm.Name, err)
		}
		vmIPs = append(vmIPs, vmIp)
	}

	return vms, vmIPs, vmlbsvc, secretName, nil
}

func CreateCnsFileAccessConfigCRDs(ctx context.Context, restConfig *rest.Config, namespace string,
	pvcs []*v1.PersistentVolumeClaim, vms []*vmopv1.VirtualMachine, createInParallel bool) ([]string, []string, error) {

	var (
		crdNames           []string
		nfsAccessPointList []string
		mu                 sync.Mutex
		wg                 sync.WaitGroup
		errChan            = make(chan error, len(pvcs)*len(vms))
	)

	createFunc := func(pvc *v1.PersistentVolumeClaim, vm *vmopv1.VirtualMachine) {
		defer wg.Done()

		crdInstanceName := pvc.Name + "-" + vm.Name

		fmt.Printf("Creating CNSFileAccessConfig CRD for PVC %s and VM %s\n", pvc.Name, vm.Name)
		if err := CreateCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vm.Name, namespace, crdInstanceName); err != nil {
			errChan <- fmt.Errorf("failed to create CNSFileAccessConfig CRD for %s: %w", crdInstanceName, err)
			return
		}

		fmt.Printf("Verifying CNSFileAccessConfig CRD %s in supervisor cluster\n", crdInstanceName)
		k8testutil.VerifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName,
			constants.CrdCNSFileAccessConfig, constants.CrdVersion, constants.CrdGroup, true)

		ginkgo.By(fmt.Sprintf("Fetching NFS access point for CRD %s", crdInstanceName))
		nfsAccessPoint, err := FetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, restConfig, crdInstanceName, namespace)
		if err != nil {
			errChan <- fmt.Errorf("failed to fetch NFS access point for %s: %w", crdInstanceName, err)
			return
		}

		mu.Lock()
		crdNames = append(crdNames, crdInstanceName)
		nfsAccessPointList = append(nfsAccessPointList, nfsAccessPoint)
		mu.Unlock()
	}

	for _, pvc := range pvcs {
		for _, vm := range vms {
			wg.Add(1)
			if createInParallel {
				go createFunc(pvc, vm)
			} else {
				createFunc(pvc, vm)
			}
		}
	}

	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		return nil, nil, <-errChan // return first error encountered
	}

	return crdNames, nfsAccessPointList, nil
}

func VerifyIOAcrossVMs(vmIPs []string, accessPoints []string, text string) error {
	fmt.Sprintf("Write IO to file volume through VMService VMs (%s)", text)
	for _, accessPoint := range accessPoints {
		if err := MountRWXVolumeAndVerifyIO(vmIPs, accessPoint, "foo"); err != nil {
			return fmt.Errorf("IO verification failed for access point %s: %w", accessPoint, err)
		}
	}
	return nil
}

func DetachPVCFromVMs(ctx context.Context, restConfig *rest.Config, namespace string, crdNames []string) error {
	for _, crdName := range crdNames {
		ginkgo.By(fmt.Sprintf("Detaching PVC by deleting CRD %s", crdName))
		if err := DeleteCnsFileAccessConfig(ctx, restConfig, crdName, namespace); err != nil {
			return fmt.Errorf("failed to delete CNSFileAccessConfig CRD %s: %w", crdName, err)
		}
	}
	return nil
}
