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
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"

	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

// creating pvc spec
func createPVCSpecWithDifferentConfigurations(namespace string, ds string, storageclass *storagev1.StorageClass,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode,
	volumeMode v1.PersistentVolumeMode) *v1.PersistentVolumeClaim {

	// Set default values if not provided
	if namespace == "" {
		namespace = "default"
	}
	if ds == "" {
		ds = "1Gi"
	}
	if accessMode == "" {
		accessMode = v1.ReadWriteOnce
	}

	// Construct the PVC spec without volumeMode
	pvcSpec := v1.PersistentVolumeClaimSpec{
		AccessModes: []v1.PersistentVolumeAccessMode{accessMode},
		Resources: v1.VolumeResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceStorage: resource.MustParse(ds),
			},
		},
	}

	// Set storageClassName if provided
	if storageclass != nil && storageclass.Name != "" {
		pvcSpec.StorageClassName = &storageclass.Name
	}

	// Only set volumeMode if it was passed explicitly
	if volumeMode != "" {
		pvcSpec.VolumeMode = &volumeMode
	}

	// Create the final PVC object
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
			Labels:       pvclaimlabels,
		},
		Spec: pvcSpec,
	}

	return claim
}

// creating pvcs using spec
func createPVCsOnScaleWithDifferentConfigurations(ctx context.Context,
	client clientset.Interface, namespace string, pvclaimlabels map[string]string,
	diskSize string, storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode,
	volumeMode v1.PersistentVolumeMode,
	createPvcCountLimit int, parallelVolumeCreation bool) ([]*v1.PersistentVolumeClaim, error) {

	ginkgo.By(fmt.Sprintf(
		"Creating PVC on Namespace: %q, with DiskSize: %q, with AccessMode: %q, with VolumeMode: %q, Labels set as: %+v, using StorageClass: %+v, with PVC Count of: %d, ",
		namespace, diskSize, accessMode, volumeMode, pvclaimlabels, storageclass.Name, createPvcCountLimit,
	))
	createdPVCs := make([]*v1.PersistentVolumeClaim, createPvcCountLimit)

	if parallelVolumeCreation {
		var wg sync.WaitGroup
		errCh := make(chan error, createPvcCountLimit)

		for i := 0; i < createPvcCountLimit; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				pvcspec := createPVCSpecWithDifferentConfigurations(
					namespace, diskSize, storageclass, pvclaimlabels, accessMode, volumeMode,
				)

				ginkgo.By(fmt.Sprintf(
					"Creating PVC [%d/%d] (parallel) with StorageClass: %s, DiskSize: %q",
					i+1, createPvcCountLimit, storageclass.Name, diskSize,
				))

				pvc, err := fpv.CreatePVC(ctx, client, namespace, pvcspec)
				if err != nil {
					errCh <- fmt.Errorf("failed to create PVC [%d/%d]: %v", i+1, createPvcCountLimit, err)
					return
				}

				framework.Logf("PVC created: %s in namespace: %s", pvc.Name, namespace)

				createdPVCs[i] = pvc
			}(i)
		}

		wg.Wait()
		close(errCh)

		if len(errCh) > 0 {
			return nil, <-errCh
		}

	} else {
		for i := 0; i < createPvcCountLimit; i++ {
			pvcspec := createPVCSpecWithDifferentConfigurations(
				namespace, diskSize, storageclass, pvclaimlabels, accessMode, volumeMode,
			)

			pvc, err := fpv.CreatePVC(ctx, client, namespace, pvcspec)
			if err != nil {
				return nil, fmt.Errorf("failed to create PVC [%d/%d]: %v", i+1, createPvcCountLimit, err)
			}

			framework.Logf("PVC created: %s in namespace: %s", pvc.Name, namespace)
			createdPVCs[i] = pvc
		}
	}

	return createdPVCs, nil
}

func verifyPVCsBoundStateAndQueryVolumeInCNS(ctx context.Context, client clientset.Interface,
	pvclaims []*v1.PersistentVolumeClaim) ([]*v1.PersistentVolumeClaim, []*v1.PersistentVolume, error) {

	// Wait for all PVCs to be bound
	boundPVs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout*2)
	if err != nil {
		return pvclaims, boundPVs, fmt.Errorf("failed to wait for PVCs to bind: %w", err)
	}

	// Verify each PVC's volume in CNS
	for i, pv := range boundPVs {
		volHandle := pv.Spec.CSI.VolumeHandle
		if volHandle == "" {
			return pvclaims, boundPVs, fmt.Errorf("volume handle is empty for PVC %q", pvclaims[i].Name)
		}

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC %q with VolumeID: %s", pvclaims[i].Name, volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		if err != nil {
			return pvclaims, boundPVs, fmt.Errorf("failed to query CNS volume for PVC %q: %w", pvclaims[i].Name, err)
		}

		if len(queryResult.Volumes) == 0 || queryResult.Volumes[0].VolumeId.Id != volHandle {
			return pvclaims, boundPVs, fmt.Errorf("CNS query returned unexpected result for PVC %q", pvclaims[i].Name)
		}
	}

	return pvclaims, boundPVs, nil
}

func createVmServiceSpecWithDifferentConfigurations(
	ctx context.Context, c ctlrclient.Client, namespace string,
	vmClass string, pvcs []*v1.PersistentVolumeClaim, vmi string,
	storageClassName string, secretName string, sharingMode string,
	diskMode string, busSharing string,
	controllerType string, controllerNumber int32,
	unitNumber int32) *vmopv1.VirtualMachine {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vmName := fmt.Sprintf("csi-test-vm-%d", r.Intn(10000))
	vols := []vmopv1.VirtualMachineVolume{}

	for _, pvc := range pvcs {
		pvcSource := &vmopv1.PersistentVolumeClaimVolumeSource{
			PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvc.Name,
			},
		}

		// Optional fields
		// if sharingMode != "" {
		// 	pvcSource.Sharing = sharingMode
		// }
		// if diskMode != "" {
		// 	pvcSource.DiskMode = diskMode
		// }
		// if busSharing != "" || controllerType != "" || controllerNumber != 0 || unitNumber != 0 {
		// 	pvcSource.Controller = &vmopv1.VirtualDiskController{
		// 		BusSharing:       busSharing,
		// 		Type:             controllerType,
		// 		ControllerNumber: controllerNumber,
		// 		UnitNumber:       unitNumber,
		// 	}
		// }

		vols = append(vols, vmopv1.VirtualMachineVolume{
			Name:                  pvc.Name,
			PersistentVolumeClaim: pvcSource,
		})
	}

	vm := vmopv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmName,
			Namespace: namespace,
		},
		Spec: vmopv1.VirtualMachineSpec{
			PowerState:   vmopv1.VirtualMachinePoweredOn,
			ImageName:    vmi,
			ClassName:    vmClass,
			StorageClass: storageClassName,
			Volumes:      vols,
			VmMetadata: &vmopv1.VirtualMachineMetadata{
				Transport:  cloudInitLabel,
				SecretName: secretName,
			},
		},
	}

	err := c.Create(ctx, &vm)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return waitNgetVmsvcVM(ctx, c, namespace, vmName)
}

func createVmServiceOnScaleWithDifferentSettings(
	ctx context.Context, client clientset.Interface, vmopC ctlrclient.Client,
	cnsopC ctlrclient.Client, namespace string, pvclaims []*v1.PersistentVolumeClaim,
	vmClass string, storageClassName string, vmCount int, parallel bool,
	sharingMode string, diskMode string, busSharing string,
	controllerType string, controllerNumber int32,
	unitNumber int32) (string, []*vmopv1.VirtualMachine, *vmopv1.VirtualMachineService, error) {

	var err error
	var secretName string
	var vmlbsvc *vmopv1.VirtualMachineService
	vms := make([]*vmopv1.VirtualMachine, vmCount)

	// Fetch VM image name
	vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
	vmi := waitNGetVmiForImageName(ctx, vmopC, vmImageName)

	// Create bootstrap secret
	secretName = createBootstrapSecretForVmsvcVms(ctx, client, namespace)

	if parallel {
		var wg sync.WaitGroup
		errCh := make(chan error, vmCount)

		for i := 0; i < vmCount; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				// Increment unit number for each VM
				thisUnitNumber := unitNumber + int32(i)

				vm := createVmServiceSpecWithDifferentConfigurations(
					ctx, vmopC, namespace, vmClass, pvclaims, vmi,
					storageClassName, secretName, sharingMode, diskMode,
					busSharing, controllerType, controllerNumber, thisUnitNumber,
				)

				// LB for first VM
				if i == 0 {
					vmlbsvc = createService4Vm(ctx, vmopC, namespace, vm.Name)
				}

				if _, ipErr := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name); ipErr != nil {
					errCh <- fmt.Errorf("failed to get VM IP for %s: %w", vm.Name, ipErr)
					return
				}

				if pvcErr := waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm, pvclaims); pvcErr != nil {
					errCh <- fmt.Errorf("PVCs not attached to VM %s: %w", vm.Name, pvcErr)
					return
				}

				refreshed, getErr := getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name)
				if getErr != nil {
					errCh <- getErr
					return
				}
				vms[i] = refreshed
			}(i)
		}

		wg.Wait()
		close(errCh)
		if len(errCh) > 0 {
			return "", nil, nil, <-errCh
		}

	} else {
		for i := 0; i < vmCount; i++ {
			// Increment unit number for each VM
			thisUnitNumber := unitNumber + int32(i)

			vm := createVmServiceSpecWithDifferentConfigurations(
				ctx, vmopC, namespace, vmClass, pvclaims, vmi,
				storageClassName, secretName, sharingMode, diskMode,
				busSharing, controllerType, controllerNumber, thisUnitNumber,
			)

			vms[i] = vm

			if i == 0 {
				vmlbsvc = createService4Vm(ctx, vmopC, namespace, vm.Name)
			}

			if _, err = waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name); err != nil {
				return "", nil, nil, fmt.Errorf("failed to get VM IP for %s: %w", vm.Name, err)
			}

			if err = waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm, pvclaims); err != nil {
				return "", nil, nil, fmt.Errorf("PVCs not attached to VM %s: %w", vm.Name, err)
			}

			vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name)
			if err != nil {
				return "", nil, nil, err
			}
			vms[i] = vm
		}
	}

	return secretName, vms, vmlbsvc, nil
}

// verifyRawBlockVolumeAccessibilityAndDataIntegrityOnVM
// Verifies that each attached raw block volume is accessible from within the Ubuntu VM
// and checks data integrity by writing and reading 100MB of random data.
func verifyRawBlockVolumeAccessibilityAndDataIntegrityOnVM(
	ctx context.Context, vm *vmopv1.VirtualMachine,
	vmopC ctlrclient.Client, namespace string) error {

	// Get VM IP
	vmIp, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
	if err != nil {
		return fmt.Errorf("failed to get VM IP: %w", err)
	}

	// Refresh VM info
	vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, vol := range vm.Status.Volumes {
		devPath := findRawBlockDevice(vol.DiskUuid, vmIp)
		verifyRawBlockDataIntegrity(vmIp, devPath)
	}
	return nil
}

// findRawBlockDevice locates the raw block device path inside the VM based on disk UUID
func findRawBlockDevice(diskUuid, vmIp string) string {
	diskById := "/dev/disk/by-id/wwn-0x" + strings.ReplaceAll(strings.ToLower(diskUuid), "-", "")
	fmt.Println("Checking raw block device path:", diskById)

	// Check if disk exists
	diskCheckResults := execSshOnVmThroughGatewayVm(vmIp, []string{"ls -l " + diskById})
	if strings.Contains(diskCheckResults[0].Stderr, "No such file or directory") {
		fmt.Printf("Disk %s not found. Rescanning SCSI devices.\n", diskById)
		execSshOnVmThroughGatewayVm(vmIp, []string{
			"echo '- - -' | sudo tee /sys/class/scsi_host/host*/scan",
			"ls -l " + diskById,
		})
		diskCheckResults = execSshOnVmThroughGatewayVm(vmIp, []string{"ls -l " + diskById})
	}

	if strings.Contains(diskCheckResults[0].Stderr, "No such file or directory") {
		framework.Failf("Raw block disk %s not found on VM %s", diskById, vmIp)
	}

	// Extract /dev/sdX from ls output
	parts := strings.Split(strings.TrimSpace(diskCheckResults[0].Stdout), "/")
	if len(parts) < 7 {
		framework.Failf("Unexpected ls output: %s", diskCheckResults[0].Stdout)
	}
	dev := "/dev/" + parts[6]
	fmt.Println("Raw block device found:", dev)

	return dev
}

// verifyRawBlockDataIntegrity writes and reads raw data to verify integrity
func verifyRawBlockDataIntegrity(vmIp, devPath string) {
	cmds := []string{
		"sudo umount " + devPath + "* || true", // Ensure not mounted
		"dd if=/dev/urandom of=/tmp/file1 bs=1M count=100 status=progress",
		"sudo dd if=/tmp/file1 of=" + devPath + " bs=1M count=100 oflag=direct status=progress",
		"sudo dd if=" + devPath + " of=/tmp/file2 bs=1M count=100 iflag=direct status=progress",
		"md5sum /tmp/file1 /tmp/file2",
	}
	results := execSshOnVmThroughGatewayVm(vmIp, cmds)

	// Compare MD5 checksums
	lines := strings.Split(results[len(results)-1].Stdout, "\n")
	gomega.Expect(strings.Fields(lines[0])[0]).To(gomega.Equal(strings.Fields(lines[1])[0]),
		"Raw block volume data mismatch")
}

func getTestWcpNs(vcRestSessionId string, namespace string) string {
	vcIp := e2eVSphere.Config.Global.VCenterHostname
	nsGetUrl := "https://" + vcIp + "/api/vcenter/namespaces/instances/" + namespace
	resp, statusCode := invokeVCRestAPIGetRequest(vcRestSessionId, nsGetUrl)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	// Define struct locally inside the function
	type VcenterNamespaceInstance struct {
		ConfigStatus string `json:"config_status"`
	}

	var nsInfo VcenterNamespaceInstance
	gomega.Expect(json.Unmarshal(resp, &nsInfo)).NotTo(gomega.HaveOccurred())

	framework.Logf("Namespace %s Config Status: %s", namespace, nsInfo.ConfigStatus)

	return nsInfo.ConfigStatus
}

/*
apiVersion: vmoperator.vmware.com/v1alpha1
kind: VirtualMachine
metadata:
  name: testvm-1
  namespace: testns
spec:
  className: best-effort-small # kubectl get vmclass -n <ns>
  imageName: vmi-fca50b872a65bc686  # kubectl get vmi -n <ns>
  storageClass: wcpglobal_storage_profile
  powerState: poweredOn
  volumes:
    - name: my-pvc
      persistentVolumeClaim:
        claimName: my-pvc
        # Added fields with defaults
        sharingMode: None
        diskMode: independent_persistent
        controller:
          busSharing: Physical
          type: PVSCSI
          controllerNumber: 0
          unitNumber: 0
  vmMetadata:
    transport: CloudInit
    secretName: my-vm-bootstrap-data

*/

/*
apiVersion: vmoperator.vmware.com/v1alpha1
kind: VirtualMachine
metadata:
  name: testvm-1
  namespace: testns
spec:
  className: best-effort-small
  imageName: vmi-fca50b872a65bc686
  storageClass: wcpglobal_storage_profile
  powerState: poweredOn
  volumes:
    - name: my-pvc
      persistentVolumeClaim:
        claimName: my-pvc
        sharingMode: MultiWriter
        diskMode: independent_persistent
        controller:
          busSharing: Physical
          type: PVSCSI
          controllerNumber: 0
          unitNumber: 0
  vmMetadata:
    transport: CloudInit
    secretName: my-vm-bootstrap-data
---
apiVersion: vmoperator.vmware.com/v1alpha1
kind: VirtualMachine
metadata:
  name: testvm-2
  namespace: testns
spec:
  className: best-effort-small
  imageName: vmi-fca50b872a65bc686
  storageClass: wcpglobal_storage_profile
  powerState: poweredOn
  volumes:
    - name: my-pvc
      persistentVolumeClaim:
        claimName: my-pvc
        sharingMode: MultiWriter
        diskMode: independent_persistent
        controller:
          busSharing: Physical
          type: PVSCSI
          controllerNumber: 0
          unitNumber: 1
  vmMetadata:
    transport: CloudInit
    secretName: my-vm-bootstrap-data
---
apiVersion: vmoperator.vmware.com/v1alpha1
kind: VirtualMachine
metadata:
  name: testvm-3
  namespace: testns
spec:
  className: best-effort-small
  imageName: vmi-fca50b872a65bc686
  storageClass: wcpglobal_storage_profile
  powerState: poweredOn
  volumes:
    - name: my-pvc
      persistentVolumeClaim:
        claimName: my-pvc
        sharingMode: MultiWriter
        diskMode: independent_persistent
        controller:
          busSharing: Physical
          type: PVSCSI
          controllerNumber: 0
          unitNumber: 2
  vmMetadata:
    transport: CloudInit
    secretName: my-vm-bootstrap-data
---
apiVersion: vmoperator.vmware.com/v1alpha1
kind: VirtualMachine
metadata:
  name: testvm-4
  namespace: testns
spec:
  className: best-effort-small
  imageName: vmi-fca50b872a65bc686
  storageClass: wcpglobal_storage_profile
  powerState: poweredOn
  volumes:
    - name: my-pvc
      persistentVolumeClaim:
        claimName: my-pvc
        sharingMode: MultiWriter
        diskMode: independent_persistent
        controller:
          busSharing: Physical
          type: PVSCSI
          controllerNumber: 0
          unitNumber: 3
  vmMetadata:
    transport: CloudInit
    secretName: my-vm-bootstrap-data
---
apiVersion: vmoperator.vmware.com/v1alpha1
kind: VirtualMachine
metadata:
  name: testvm-5
  namespace: testns
spec:
  className: best-effort-small
  imageName: vmi-fca50b872a65bc686
  storageClass: wcpglobal_storage_profile
  powerState: poweredOn
  volumes:
    - name: my-pvc
      persistentVolumeClaim:
        claimName: my-pvc
        sharingMode: MultiWriter
        diskMode: independent_persistent
        controller:
          busSharing: Physical
          type: PVSCSI
          controllerNumber: 0
          unitNumber: 4
  vmMetadata:
    transport: CloudInit
    secretName: my-vm-bootstrap-data

*/

/*
# 1. Check available devices by ID
ls -l /dev/disk/by-id/

# 2. Identify your raw block PVC device (example WWN shown, replace with yours)
ls -l /dev/disk/by-id/wwn-0x6000c29ca04105fa728926afa87e3102

# 3. Assign to variable for convenience
RAW_DEV="/dev/disk/by-id/wwn-0x6000c29ca04105fa728926afa87e3102"

# 4. Create a test file with random data (100 MB)
dd if=/dev/urandom of=/tmp/file1 bs=1M count=100 status=progress

# 5. Write data directly to the raw block device
sudo dd if=/tmp/file1 of="$RAW_DEV" bs=1M count=100 oflag=direct status=progress

# 6. Read the data back from the raw block device into another file
sudo dd if="$RAW_DEV" of=/tmp/file2 bs=1M count=100 iflag=direct status=progress

# 7. Verify the written and read files are identical
md5sum /tmp/file1 /tmp/file2

*/

/*
# List all device symlinks
ls -l /dev/disk/by-id/

# Example: show the specific device info for a known WWN
ls -l /dev/disk/by-id/wwn-0x6000c29ca04105fa728926afa87e3102

# Ensure the device is not mounted
sudo umount /dev/sdc* || true

# (No partitioning, no mkfs for raw block devices)

# Identify the device size and details
lsblk -l
sudo blockdev --getsize64 /dev/sdc
sudo blockdev --getss /dev/sdc

# Create a temp test file of 100MB random data
dd if=/dev/urandom of=/tmp/file1 bs=1M count=100 status=progress

# Write directly to the raw block device
sudo dd if=/tmp/file1 of=/dev/sdc bs=1M count=100 oflag=direct status=progress

# Read back from the raw block device to a new file
sudo dd if=/dev/sdc of=/tmp/file2 bs=1M count=100 iflag=direct status=progress

# Compare checksums to verify integrity
md5sum /tmp/file1 /tmp/file2

# (Optional) Overwrite the start of the device with zeros to clean it
# sudo dd if=/dev/zero of=/dev/sdc bs=1M count=10 oflag=direct status=progress
sync

*/
