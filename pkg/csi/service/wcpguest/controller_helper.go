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

package wcpguest

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	vmoperatortypes "gitlab.eng.vmware.com/core-build/vm-operator-client/pkg/apis/vmoperator/v1alpha1"
	vmoperatorclient "gitlab.eng.vmware.com/core-build/vm-operator-client/pkg/client/clientset/versioned/typed/vmoperator/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachineryTypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
)

const (
	// default timeout for provision, used unless overridden by user in csi-controller YAML
	defaultProvisionTimeoutInMin = 4

	// timeout for attach and detach operation for watching on VirtualMachines instances, used unless overridden by user in csi-controller YAML
	defaultAttacherTimeoutInMin = 4
)

// validateGuestClusterCreateVolumeRequest is the helper function to validate
// CreateVolumeRequest for Guest Cluster CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateGuestClusterCreateVolumeRequest(req *csi.CreateVolumeRequest) error {
	// Validate Name length of volumeName is > 4, eg: pvc-xxxxx
	if len(req.Name) <= 4 {
		msg := fmt.Sprintf("Volume name %s is not valid", req.Name)
		return status.Error(codes.InvalidArgument, msg)
	}
	// Get create params
	var supervisorStorageClass string
	params := req.GetParameters()
	for param := range params {
		paramName := strings.ToLower(param)
		if paramName != common.AttributeSupervisorStorageClass {
			msg := fmt.Sprintf("Volume parameter %s is not a valid GC CSI parameter", param)
			return status.Error(codes.InvalidArgument, msg)
		} else {
			supervisorStorageClass = req.Parameters[param]
		}
	}
	// Validate if the req contains non-empty common.AttributeSupervisorStorageClass
	if supervisorStorageClass == "" {
		msg := fmt.Sprintf("Volume parameter %s is not set in the req", common.AttributeSupervisorStorageClass)
		return status.Error(codes.InvalidArgument, msg)
	}
	// Fail file volume creation
	if common.IsFileVolumeRequest(req.GetVolumeCapabilities()) {
		return status.Error(codes.InvalidArgument, "File volume not supported.")
	}
	return common.ValidateCreateVolumeRequest(req)
}

// validateGuestClusterDeleteVolumeRequest is the helper function to validate
// DeleteVolumeRequest for pvCSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateGuestClusterDeleteVolumeRequest(req *csi.DeleteVolumeRequest) error {
	return common.ValidateDeleteVolumeRequest(req)
}

func validateGuestClusterControllerPublishVolumeRequest(req *csi.ControllerPublishVolumeRequest) error {
	return common.ValidateControllerPublishVolumeRequest(req)
}

// getAccessMode returns the PersistentVolumeAccessMode for the PVC Spec given VolumeCapability_AccessMode
func getAccessMode(accessMode csi.VolumeCapability_AccessMode_Mode) v1.PersistentVolumeAccessMode {
	switch accessMode {
	case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER:
		return v1.ReadWriteOnce
	case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
		return v1.ReadWriteMany
	case csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:
		return v1.ReadOnlyMany
	default:
		return v1.ReadWriteOnce
	}
}

// getPersistentVolumeClaimSpecWithStorageClass return the PersistentVolumeClaim spec with specified storage class
func getPersistentVolumeClaimSpecWithStorageClass(pvcName string, namespace string, diskSize string, storageClassName string, pvcAccessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {

	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				pvcAccessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(diskSize),
				},
			},
			StorageClassName: &storageClassName,
		},
	}
	return claim
}

// isPVCInSupervisorClusterBound return true if the PVC is bound in the supervisor cluster before timeout, otherwise return false
func isPVCInSupervisorClusterBound(client clientset.Interface, claim *v1.PersistentVolumeClaim, timeout time.Duration) (bool, error) {
	pvcName := claim.Name
	ns := claim.Namespace
	Poll := 1 * time.Second
	klog.V(2).Infof("Waiting up to %v for PersistentVolumeClaims %v on namespace %s to have phase %s", timeout, pvcName, ns, v1.ClaimBound)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		pvc, err := client.CoreV1().PersistentVolumeClaims(ns).Get(pvcName, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Failed to get claim %q on namespace %s, retrying in %v. Error: %v", pvcName, ns, Poll, err)
			continue
		} else {
			if pvc.Status.Phase == v1.ClaimBound {
				klog.V(2).Infof("PersistentVolumeClaim %s found on namespace %s and phase=%s (%v)", pvcName, ns, v1.ClaimBound, time.Since(start))
				return true, nil
			} else {
				klog.V(3).Infof("PersistentVolumeClaim %s found on namespace %s but phase is %s instead of %s.", pvcName, ns, pvc.Status.Phase, v1.ClaimBound)
			}
		}
	}
	return false, fmt.Errorf("PersistentVolumeClaim %v on namespace %s not in phase %s within %v", pvcName, ns, v1.ClaimBound, timeout)
}

// getProvisionTimeoutInMin() return the timeout for volume provision.
// If environment variable PROVISION_TIMEOUT_MINUTES is set and valid,
// return the interval value read from environment variable
// otherwise, use the default timeout 5 mins
func getProvisionTimeoutInMin() int {
	provisionTimeoutInMin := defaultProvisionTimeoutInMin
	if v := os.Getenv("PROVISION_TIMEOUT_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				klog.Warningf(" provisionTimeout set in env variable PROVISION_TIMEOUT_MINUTES %s is equal or less than 0, will use the default timeout", v)
			} else {
				provisionTimeoutInMin = value
				klog.V(2).Infof("provisionTimeout is set to %d minutes", provisionTimeoutInMin)
			}
		} else {
			klog.Warningf("provisionTimeout set in env variable PROVISION_TIMEOUT_MINUTES %s is invalid, will use the default timeout", v)
		}
	}
	return provisionTimeoutInMin
}

// getAttacherTimeoutInMin() return the timeout for volume attach and detach.
// If environment variable ATTACHER_TIMEOUT_MINUTES is set and valid,
// return the interval value read from environment variable
// otherwise, use the default timeout 5 mins
func getAttacherTimeoutInMin() int {
	attacherTimeoutInMin := defaultAttacherTimeoutInMin
	if v := os.Getenv("ATTACHER_TIMEOUT_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				klog.Warningf("attacherTimeout set in env variable ATTACHER_TIMEOUT_MINUTES %s is equal or less than 0, will use the default timeout", v)
			} else {
				attacherTimeoutInMin = value
				klog.V(2).Infof("attacherTimeout is set to %d minutes", attacherTimeoutInMin)
			}
		} else {
			klog.Warningf("attacherTimeout set in env variable ATTACHER_TIMEOUT_MINUTES %s is invalid, will use the default timeout", v)
		}
	}
	return attacherTimeoutInMin
}

// patchVirtualMachineVolumes patches VirtualMachine instance with spec changes supplied in the newVirtualMachine instance.
func patchVirtualMachineVolumes(client *vmoperatorclient.VmoperatorV1alpha1Client, oldVirtualMachine *vmoperatortypes.VirtualMachine, newVirtualMachine *vmoperatortypes.VirtualMachine) (*vmoperatortypes.VirtualMachine, error) {
	oldVMData, err := json.Marshal(oldVirtualMachine)
	if err != nil {
		msg := fmt.Sprintf("Failed to marshal virtualMachine: %v. Error: %+v", oldVirtualMachine, err)
		klog.Error(msg)
		return nil, err
	}
	newVMData, err := json.Marshal(newVirtualMachine)
	if err != nil {
		msg := fmt.Sprintf("Failed to marshal virtualMachine: %v. Error: %+v", newVirtualMachine, err)
		klog.Error(msg)
		return nil, err
	}
	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldVMData, newVMData, oldVirtualMachine)
	if err != nil {
		msg := fmt.Sprintf("CreateTwoWayMergePatch failed for virtualMachine %q with Error: %v", oldVirtualMachine.Name, err)
		klog.Error(msg)
		return nil, err
	}
	updatedVirtualMachine, err := client.VirtualMachines(oldVirtualMachine.Namespace).Patch(oldVirtualMachine.Name, apimachineryTypes.StrategicMergePatchType, patchBytes)
	if err != nil {
		msg := fmt.Sprintf("patch failed for virtualMachine %q with Error: %v", oldVirtualMachine.Name, err)
		klog.Error(msg)
		return nil, err
	}
	return updatedVirtualMachine, nil
}
