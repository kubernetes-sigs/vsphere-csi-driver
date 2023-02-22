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

package k8scloudoperator

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	api "k8s.io/kubernetes/pkg/apis/core"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"

	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	vmUUIDLabel                 = "vmware-system-vm-uuid"
	defaultPodPollIntervalInSec = 2
	spTypePrefix                = "cns.vmware.com/"
	spTypeAnnotationKey         = spTypePrefix + "StoragePoolTypeHint"
	// PVC annotation key to specify the node to which PV should be affinitized.
	nodeAffinityAnnotationKey = "failure-domain.beta.vmware.com/node"
	vsanDirectType            = spTypePrefix + "vsanD"
	vsanSnaType               = spTypePrefix + "vsan-sna"
	spTypeLabelKey            = spTypePrefix + "StoragePoolType"
	diskDecommissionModeField = "decommMode"
)

type k8sCloudOperator struct {
	k8sClient clientset.Interface
}

// initK8sCloudOperatorType initializes the k8sCloudOperator struct.
func initK8sCloudOperatorType(ctx context.Context) (*k8sCloudOperator, error) {
	var err error
	k8sCloudOperator := k8sCloudOperator{}
	log := logger.GetLogger(ctx)

	// Create the kubernetes client from config.
	k8sCloudOperator.k8sClient, err = k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return nil, err
	}
	return &k8sCloudOperator, nil
}

// InitK8sCloudOperatorService initializes the K8s Cloud Operator Service.
func InitK8sCloudOperatorService(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	log.Infof("Trying to initialize the K8s Cloud Operator gRPC service")
	k8sCloudOperatorServicePort := common.GetK8sCloudOperatorServicePort(ctx)
	log.Debugf("K8s Cloud Operator Service will be running on port %d", k8sCloudOperatorServicePort)
	port := flag.Int("port", k8sCloudOperatorServicePort, "The k8s cloud operator service port")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Errorf("failed to listen. Err: %v", err)
		return err
	}
	grpcServer := grpc.NewServer()
	server, err := initK8sCloudOperatorType(ctx)
	if err != nil {
		return err
	}
	RegisterK8SCloudOperatorServer(grpcServer, server)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Errorf("Failed to accept incoming connections on k8s Cloud Operator gRPC server. Err: %+v", err)
		return err
	}
	log.Infof("Successfully initialized the K8s Cloud Operator gRPC service")
	return nil
}

// GetPodVMUUIDAnnotation provide the implementation the GetPodVMUUIDAnnotation
// interface method. The method return codes.NotFound only when VMUUID annotation
// is not found. For other errors like Pod not found or volume not found,
// the method will return Internal or custom error messages
func (k8sCloudOperator *k8sCloudOperator) GetPodVMUUIDAnnotation(ctx context.Context,
	req *PodListenerRequest) (*PodListenerResponse, error) {
	var (
		vmuuid   string
		err      error
		timeout  = 4 * time.Minute
		pollTime = time.Duration(getPodPollIntervalInSecs(ctx)) * time.Second
		volumeID = req.VolumeID
		nodeName = req.NodeName
	)

	log := logger.GetLogger(ctx)
	pv, err := k8sCloudOperator.getPVWithVolumeID(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	if pv.Spec.ClaimRef == nil {
		return nil, logger.LogNewErrorf(log, "No Claim ref found for this PV with volumeID: %s", volumeID)
	}
	log.Infof("PV for volumeID: %s has claim: %s, claimNamespace: %s",
		volumeID, pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace)

	var podName string
	var podNamespace string
	var vmUuidAnnotationexists bool
	// Use vmUuidNotFoundError to set to false for all error paths except
	// vm-uuid annotation not found on the pod
	vmUuidNotFoundError := true
	err = wait.Poll(pollTime, timeout, func() (bool, error) {
		var pod *v1.Pod
		// Retrieve the pod name and namespace only if they are non-empty.
		// If they are already pre-populated, use them to get the pod
		// info from API server.
		if podName == "" || podNamespace == "" {
			pod, err = k8sCloudOperator.getPod(ctx, pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace, nodeName)
			if err != nil {
				log.Infof("retrying the getPod operation again with PVC: %s on namespace: %s on node: %s. Err: %+v",
					pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace, nodeName, err)
				return false, nil
			}
			if pod != nil {
				podName = pod.Name
				podNamespace = pod.Namespace
			} else {
				log.Infof("pod info is empty for PVC: %s. Retrying the operation again.",
					pv.Spec.ClaimRef.Name)
				return false, nil
			}
		}
		if pod == nil {
			pod, err = k8sCloudOperator.k8sClient.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					// When the pod itself is not found in the api server, we still mark the error as
					// vmUuidNotFoundError because from GetPodVMUUIDAnnotation's perspective the final error is that
					// it could not find the annotation. Also, there are no distinct notFound error codes
					// in grpc error codes. Refer - go/pkg/mod/google.golang.org/grpc@v1.27.1/codes/codes.go
					vmUuidNotFoundError = true
				} else {
					// Set vmUuidNotFoundError as false to indicate other errors
					vmUuidNotFoundError = false
				}
				return false, logger.LogNewErrorf(log,
					"Failed to get the pod with name: %s on namespace: %s using K8s Cloud Operator informer. Err: %+v",
					podName, podNamespace, err)
			}
		}
		annotations := pod.Annotations
		vmuuid, vmUuidAnnotationexists = annotations[vmUUIDLabel]
		if !vmUuidAnnotationexists {
			log.Debugf("Waiting for %s annotation in Pod: %s", vmUUIDLabel, spew.Sdump(pod))
			return false, nil
		}
		vmUuidNotFoundError = false
		log.Debugf("%s annotation with value: %s found in Pod: %s", vmUUIDLabel, vmuuid, spew.Sdump(pod))
		return true, nil
	})
	if err != nil {
		if podName == "" {
			return nil, logger.LogNewErrorf(log,
				"failed to get Pod with PVC: %s mapping to volumeID: %s"+
					" on namespace: %s on node: %s. Err: %+v",
				pv.Spec.ClaimRef.Name, volumeID, pv.Spec.ClaimRef.Namespace,
				nodeName, err)
		}
		// vmUuidNotFoundError not set indicates errors other than annotation not found
		if vmUuidNotFoundError && !vmUuidAnnotationexists {
			return nil, logger.LogNewErrorCodef(log, codes.NotFound,
				"Unable to find %s annotation on pod with name: %s in namespace: %s. Err: %+v",
				vmUUIDLabel, podName, podNamespace, err)
		}
		return nil, logger.LogNewErrorf(log,
			"Unable to find annotation %s on pod %s in namespace %s in timeout: %d. Err: %+v",
			vmUUIDLabel, podName, podNamespace, timeout, err)
	}

	log.Infof("Found the %s: %s annotation on Pod: %s referring to VolumeID: %s running on node: %s",
		vmUUIDLabel, vmuuid, podName, volumeID, nodeName)
	response := PodListenerResponse{VmuuidAnnotation: vmuuid}
	return &response, nil
}

// getPodPollIntervalInSecs return the Poll interval in secs to query the
// Pod Info from API server.
// If environment variable POD_POLL_INTERVAL_SECONDS is set and valid,
// return the interval value read from environment variable
// otherwise, use the default value 30 minutes
func getPodPollIntervalInSecs(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	podPollIntervalInSec := defaultPodPollIntervalInSec
	if v := os.Getenv("POD_POLL_INTERVAL_SECONDS"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Poll Interval to query Pod Info: POD_POLL_INTERVAL_SECONDS=%s is invalid, use default %d",
					v, defaultPodPollIntervalInSec)
			} else {
				podPollIntervalInSec = value
				log.Debugf("Poll Interval to query the Pod Info from API server is set to %d seconds", podPollIntervalInSec)
			}
		} else {
			log.Warnf("Poll Interval to query Pod Info: PODVM_POLL_INTERVAL_SECONDS=%s is invalid, use default", v)
		}
	}
	return podPollIntervalInSec
}

// getPVWithVolumeID queries API server to get PV referring to the given
// volumeID.
func (k8sCloudOperator *k8sCloudOperator) getPVWithVolumeID(ctx context.Context,
	volumeID string) (*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)
	allPVs, err := k8sCloudOperator.k8sClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("failed to retrieve all PVs from API server")
		return nil, err
	}
	for _, pv := range allPVs.Items {
		// Verify if it is vsphere block driver and volumehandle matches the
		// volume ID.
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csitypes.Name && pv.Spec.CSI.VolumeHandle == volumeID {
			log.Debugf("Found PV: %+v referring to volume ID: %s", pv, volumeID)
			return &pv, nil
		}
	}
	return nil, logger.LogNewErrorf(log, "failed to find PV referring to volume ID: %s", volumeID)
}

// getPod returns the pod spec for the pod satisfying the below conditions.
//  1. Pod Scheduled on node with name "nodeName".
//  2. Pod is in pending state in the same namespace as pvc specified using
//     "pvcNamespace".
//  3. Pod has a volume with name "pvcName" associated with it.
func (k8sCloudOperator *k8sCloudOperator) getPod(ctx context.Context, pvcName string, pvcNamespace string,
	nodeName string) (*v1.Pod, error) {
	log := logger.GetLogger(ctx)
	pods, err := k8sCloudOperator.k8sClient.CoreV1().Pods(pvcNamespace).List(ctx, metav1.ListOptions{
		FieldSelector: fields.AndSelectors(
			fields.SelectorFromSet(fields.Set{"spec.nodeName": string(nodeName)}),
			fields.SelectorFromSet(fields.Set{"status.phase": string(api.PodPending)})).String(),
	})

	if err != nil {
		return nil, logger.LogNewErrorf(log, "Cannot find pod with namespace: %s running on node: %s with error %+v",
			pvcNamespace, nodeName, err)
	}
	log.Debugf("Returned pods: %+v with namespace: %s running on node: %s", spew.Sdump(pods), pvcNamespace, nodeName)

	// Identify the pod that a volume with name "pvcName" associated with it.
	for _, pod := range pods.Items {
		for _, volume := range pod.Spec.Volumes {
			pvClaim := volume.VolumeSource.PersistentVolumeClaim
			if pvClaim != nil && pvClaim.ClaimName == pvcName {
				log.Debugf("Returned pod: %s", spew.Sdump(&pod))
				log.Infof("Returned pod: %s/%s with pvClaim name: %s running on node: %s",
					pod.Name, pod.Namespace, pvcName, nodeName)
				return &pod, nil
			}
		}
	}

	return nil, logger.LogNewErrorf(log, "Cannot find pod with pvClaim name: %s in namespace: %s running on node: %s",
		pvcName, pvcNamespace, nodeName)
}

// GetHostAnnotation provide the implementation for the GetHostAnnotation
// interface method.
func (k8sCloudOperator *k8sCloudOperator) GetHostAnnotation(ctx context.Context,
	req *HostAnnotationRequest) (*HostAnnotationResponse, error) {
	var (
		key      = req.AnnotationKey
		nodeName = req.HostName
	)
	log := logger.GetLogger(ctx)
	node, err := k8sCloudOperator.k8sClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("failed to get the node object for node %s: %s", nodeName, err)
		return nil, err
	}
	hostMoid, ok := node.Annotations[key]
	if !ok {
		log.Errorf("failed to get the node annotation for the key %s: %s", key, err)
		return nil, err
	}
	log.Infof("Found the %s: %s annotation on Node: %s", key, hostMoid, nodeName)
	response := &HostAnnotationResponse{
		AnnotationValue: hostMoid,
	}
	return response, nil
}

// Response of PlacePersistenceVolumeClaim RPC call include only success tag.
// Case 1: pvc does not have a storage pool annotation and does not need a
//
//	storage pool annotation.
//
// Case 2: pvc already has a annotation.
// Case 3: pvc needs a storage pool annotation and we cant find one.
// Case 4: pvc needs an annotation and we can find one.
// Everything other than case 3 is success.
func (k8sCloudOperator *k8sCloudOperator) PlacePersistenceVolumeClaim(ctx context.Context,
	req *PVCPlacementRequest) (*PVCPlacementResponse, error) {

	log := logger.GetLogger(ctx)
	out := &PVCPlacementResponse{
		PlaceSuccess: false,
	}
	if req == nil || req.Name == "" || req.Namespace == "" {
		log.Errorf("no right inputs given to PlacePersistenceVolumeClaim")
		return out, nil
	}

	pvc, err := k8sCloudOperator.k8sClient.CoreV1().PersistentVolumeClaims(
		req.Namespace).Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Fail to retrieve targeted PVC %s from API server with error %s", pvc, err)
		return out, err
	}

	scName, err := GetSCNameFromPVC(pvc)
	if err != nil {
		log.Errorf("Fail to get Storage class name from PVC with +v", err)
		return out, err
	}

	sc, err := k8sCloudOperator.k8sClient.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
	if err != nil {
		return out, err
	}

	spTypes, present := sc.Annotations[spTypeAnnotationKey]
	if !present || (!strings.Contains(spTypes, vsanDirectType) && !strings.Contains(spTypes, vsanSnaType)) {
		log.Debug("storage class is not of type vsan direct or vsan-sna, aborting placement")
		return out, nil
	}

	// Abort placement if the storage class has Immediate volume binding and
	// nodeAffinity PVC annotation is not specified.
	if *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
		if _, ok := pvc.ObjectMeta.Annotations[nodeAffinityAnnotationKey]; !ok {
			log.Debugf("Aborting placement for PVC[%s] as neither volume binding of the storage class is [%s] "+
				"nor nodeAffinity PVC annotation is specified", pvc.Name, storagev1.VolumeBindingWaitForFirstConsumer)
			return out, nil
		}
	}

	log.Debugf("Enter placementEngine %s", req)
	err = PlacePVConStoragePool(ctx, k8sCloudOperator.k8sClient, req.AccessibilityRequirements, pvc, spTypes)
	if err != nil {
		log.Errorf("Failed to place this PVC on sp with error %s", err)
		return out, err
	}

	log.Debugf("End placementEngine")
	out.PlaceSuccess = true
	return out, err
}

// GetStorageVMotionPlan provide the implementation for the GetHostAnnotation
// interface method. It creates a storage vMotion plan as a map where keys are
// PVs residing in the specified vSAN Direct Datastore and values are other
// vSAN Direct Datastores into which the PV should be migrated.
func (k8sCloudOperator *k8sCloudOperator) GetStorageVMotionPlan(ctx context.Context,
	req *StorageVMotionRequest) (*StorageVMotionResponse, error) {
	log := logger.GetLogger(ctx)
	out := &StorageVMotionResponse{
		SvMotionPlan: nil,
	}
	if req == nil || req.StoragePoolName == "" {
		log.Errorf("no right inputs given to GetStorageVMotionPlan")
		return out, fmt.Errorf("malformed request provided to GetStorageVMotionPlan")
	}
	log.Debugf("received GetStorageVMotionPlan for StoragePool %v and maintenance mode %v",
		req.StoragePoolName, req.MaintenanceMode)
	svMotionPlan, err := GetSVMotionPlan(ctx, k8sCloudOperator.k8sClient, req.StoragePoolName, req.MaintenanceMode)
	if err != nil {
		log.Errorf("Failed to get SvMotion plan. Error: %v", err)
		return out, err
	}
	out.SvMotionPlan = svMotionPlan
	return out, nil
}
