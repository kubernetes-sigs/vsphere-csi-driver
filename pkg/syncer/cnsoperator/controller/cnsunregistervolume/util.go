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

package cnsunregistervolume

import (
	"context"
	"errors"
	"strings"

	snapshotclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

type volumeUsageInfo struct {
	isInUse         bool
	pods            []string
	snapshots       []string
	virtualMachines []string
	guestClusters   []string
}

func (v volumeUsageInfo) String() string {
	if !v.isInUse {
		return "volume is not in use by any resources"
	}

	msg := "volume is in use by the following resources:\n "
	if len(v.pods) > 0 {
		msg += "Pods: " + strings.Join(v.pods, ", ") + "\n"
	}
	if len(v.snapshots) > 0 {
		msg += "Snapshots: " + strings.Join(v.snapshots, ", ") + "\n"
	}
	if len(v.virtualMachines) > 0 {
		msg += "Virtual Machines: " + strings.Join(v.virtualMachines, ", ") + "\n"
	}
	if len(v.guestClusters) > 0 {
		msg += "Guest Clusters: " + strings.Join(v.guestClusters, ", ") + "\n"
	}
	return strings.TrimSuffix(msg, "\n")
}

var getVolumeUsageInfo = _getVolumeUsageInfo

// getVolumeUsageInfo checks if the PVC is in use by any resources in the specified namespace.
// For the sake of efficiency, the function returns as soon as it finds that the volume is in use by any resource.
// If ignoreVMUsage is set to true, the function skips checking if the volume is in use by any virtual machines.
func _getVolumeUsageInfo(ctx context.Context, pvcName string, pvcNamespace string,
	ignoreVMUsage bool) (*volumeUsageInfo, error) {
	log := logger.GetLogger(ctx)

	var volumeUsageInfo volumeUsageInfo
	if pvcName == "" {
		log.Debug("PVC name is empty. Nothing to do.")
		return &volumeUsageInfo, nil
	}

	var err error
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("failed to create k8s client. Error: %v", err)
		return nil, err
	}

	volumeUsageInfo.pods, volumeUsageInfo.isInUse, err = getPodsForPVC(ctx, pvcName, pvcNamespace, k8sClient)
	if err != nil {
		return nil, err
	}

	if volumeUsageInfo.isInUse {
		return &volumeUsageInfo, nil
	}

	cfg, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		return nil, err
	}

	volumeUsageInfo.guestClusters, volumeUsageInfo.isInUse, err = getGuestClustersForPVC(
		ctx, pvcName, pvcNamespace, *cfg)
	if err != nil {
		return nil, err
	}

	if volumeUsageInfo.isInUse {
		return &volumeUsageInfo, nil
	}

	volumeUsageInfo.snapshots, volumeUsageInfo.isInUse, err = getSnapshotsForPVC(ctx, pvcName, pvcNamespace, *cfg)
	if err != nil {
		return nil, err
	}

	if volumeUsageInfo.isInUse {
		return &volumeUsageInfo, nil
	}

	if ignoreVMUsage {
		log.Debugf("Skipping check for virtual machines using PVC %q in namespace %q as ignoreVMUsage is set to true",
			pvcName, pvcNamespace)
		return &volumeUsageInfo, nil
	}

	volumeUsageInfo.virtualMachines, volumeUsageInfo.isInUse, err = getVMsForPVC(ctx, pvcName, pvcNamespace, *cfg)
	if err != nil {
		return nil, err
	}

	return &volumeUsageInfo, nil
}

var getPVName = _getPVName

func _getPVName(ctx context.Context, volumeID string) (string, error) {
	log := logger.GetLogger(ctx)
	if commonco.ContainerOrchestratorUtility == nil {
		err := errors.New("ContainerOrchestratorUtility is not initialized")
		log.Warn(err)
		return "", err
	}

	pv, ok := commonco.ContainerOrchestratorUtility.GetPVNameFromCSIVolumeID(volumeID)
	if !ok {
		log.Infof("no PV found for volumeID %q", volumeID)
	} else {
		log.Infof("PV %q found for volumeID %q", pv, volumeID)
	}
	return pv, nil
}

var getPVCName = _getPVCName

func _getPVCName(ctx context.Context, volumeID string) (string, string, error) {
	log := logger.GetLogger(ctx)
	if commonco.ContainerOrchestratorUtility == nil {
		err := errors.New("ContainerOrchestratorUtility is not initialized")
		log.Warn(err)
		return "", "", err
	}

	pvc, ns, ok := commonco.ContainerOrchestratorUtility.GetPVCNameFromCSIVolumeID(volumeID)
	if !ok {
		log.Infof("no PVC found for volumeID %q", volumeID)
	} else {
		log.Infof("PVC %q found for volumeID %q in namespace %q", pvc, volumeID, ns)
	}
	return pvc, ns, nil
}

var getVolumeID = _getVolumeID

func _getVolumeID(ctx context.Context, pvcName, namespace string) (string, error) {
	log := logger.GetLogger(ctx)
	if commonco.ContainerOrchestratorUtility == nil {
		err := errors.New("ContainerOrchestratorUtility is not initialized")
		log.Warn(err)
		return "", err
	}

	volID, ok := commonco.ContainerOrchestratorUtility.GetVolumeIDFromPVCName(namespace, pvcName)
	if !ok {
		log.Infof("no volumeID found for PVC %q", pvcName)
	} else {
		log.Infof("volumeID %q found for PVC %q", volID, pvcName)
	}
	return volID, nil
}

// getPodsForPVC returns a list of pods that are using the specified PVC.
func getPodsForPVC(ctx context.Context, pvcName string, pvcNamespace string,
	k8sClient clientset.Interface) ([]string, bool, error) {
	log := logger.GetLogger(ctx)
	// TODO: check if we can use informer cache
	list, err := k8sClient.CoreV1().Pods(pvcNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Warnf("Failed to list pods in namespace %q for PVC %q. Error: %q",
			pvcNamespace, pvcName, err.Error())
		return nil, false, errors.New("failed to list pods")
	}

	var pods []string
	for _, pod := range list.Items {
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil ||
				vol.PersistentVolumeClaim.ClaimName != pvcName {
				continue
			}

			// We do not support a PVC being used by multiple pods.
			// So, for now, we can exit early if we find a pod using the PVC.
			pods = append(pods, pod.Name)
			return pods, true, nil
		}
	}

	return pods, false, nil
}

// getSnapshotsForPVC returns a list of snapshots that are created for the specified PVC.
func getSnapshotsForPVC(ctx context.Context, pvcName string, pvcNamespace string,
	cfg rest.Config) ([]string, bool, error) {
	log := logger.GetLogger(ctx)
	c, err := snapshotclient.NewForConfig(&cfg)
	if err != nil {
		log.Warnf("Failed to create snapshot client for PVC %q in namespace %q. Error: %q",
			pvcName, pvcNamespace, err.Error())
		return nil, false, errors.New("failed to create snapshot client")
	}

	// TODO: check if we can use informer cache
	list, err := c.SnapshotV1().VolumeSnapshots(pvcNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Warnf("Failed to list VolumeSnapshots in namespace %q for PVC %q. Error: %q",
			pvcNamespace, pvcName, err.Error())
		return nil, false, errors.New("failed to list VolumeSnapshots")
	}

	var snapshots []string
	for _, snap := range list.Items {
		if snap.Spec.Source.PersistentVolumeClaimName == nil ||
			*snap.Spec.Source.PersistentVolumeClaimName != pvcName {
			continue
		}

		snapshots = append(snapshots, snap.Name)
	}

	return snapshots, len(snapshots) > 0, nil
}

// getGuestClustersForPVC returns a list of guest clusters that are using the specified PVC.
func getGuestClustersForPVC(ctx context.Context, pvcName, pvcNamespace string,
	cfg rest.Config) ([]string, bool, error) {
	log := logger.GetLogger(ctx)
	c, err := k8s.NewClientForGroup(ctx, &cfg, apis.GroupName)
	if err != nil {
		return nil, false, err
	}

	// CNSVolumeMetadata objects are created by the guest cluster controller with
	// the name of the PVC as the name of the CnsVolumeMetadata object in the same namespace.
	cnsVolumeMetadata := v1alpha1.CnsVolumeMetadata{}
	err = c.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: pvcNamespace}, &cnsVolumeMetadata)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Debugf("CnsVolumeMetadata %q not found in namespace %q. Volume is not in use by any guest cluster",
				pvcName, pvcNamespace)
			return nil, false, nil
		}

		log.Warnf("Failed to get CnsVolumeMetadata %q in namespace %q. Error: %q",
			pvcName, pvcNamespace, err.Error())
		return nil, false, errors.New("failed to get CnsVolumeMetadata")
	}

	var gcs []string
	for _, ownerRef := range cnsVolumeMetadata.GetOwnerReferences() {
		if ownerRef.Kind != "TanzuKubernetesCluster" {
			continue
		}

		gcs = append(gcs, ownerRef.Name)
	}
	return gcs, true, nil
}

// getVMsForPVC returns a list of virtual machines that are using the specified PVC.
func getVMsForPVC(ctx context.Context, pvcName string, pvcNamespace string,
	cfg rest.Config) ([]string, bool, error) {
	c, err := k8s.NewClientForGroup(ctx, &cfg, vmoperatortypes.GroupName)
	if err != nil {
		return nil, false, errors.New("failed to create client for virtual machine group")
	}

	// TODO: check if we can use informer cache
	list, err := utils.ListVirtualMachines(ctx, c, pvcNamespace)
	if err != nil {
		return nil, false, errors.New("failed to list virtual machines")
	}

	var vms []string
	for _, vm := range list.Items {
		for _, vmVol := range vm.Spec.Volumes {
			if vmVol.PersistentVolumeClaim == nil ||
				vmVol.PersistentVolumeClaim.ClaimName != pvcName {
				continue
			}

			// If the volume is specified in the virtual machine's spec, then it is
			// either, in the process of being attached to the VM or is already attached.
			// In either case, we can consider the volume to be in use.
			vms = append(vms, vm.Name)
		}
	}
	return vms, len(vms) > 0, nil
}
