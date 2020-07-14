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

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	"github.com/davecgh/go-spew/spew"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	api "k8s.io/kubernetes/pkg/apis/core"

	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

const (
	vmUUIDLabel                 = "vmware-system-vm-uuid"
	defaultPodPollIntervalInSec = 2
	spTypePrefix                = "cns.vmware.com/"
	spTypeAnnotationKey         = spTypePrefix + "StoragePoolTypeHint"
	vsanDirectType              = spTypePrefix + "vsanD"
	spTypeLabelKey              = spTypePrefix + "StoragePoolType"
)

type k8sCloudOperator struct {
	k8sClient clientset.Interface
}

// initK8sCloudOperatorType initializes the k8sCloudOperator struct
func initK8sCloudOperatorType(ctx context.Context) (*k8sCloudOperator, error) {
	var err error
	k8sCloudOperator := k8sCloudOperator{}
	log := logger.GetLogger(ctx)

	// Create the kubernetes client from config
	k8sCloudOperator.k8sClient, err = k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return nil, err
	}
	return &k8sCloudOperator, nil
}

// InitK8sCloudOperatorService initializes the K8s Cloud Operator Service
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

/*
 * GetPodVMUUIDAnnotation provide the implementation the GetPodVMUUIDAnnotation interface method
 */
func (k8sCloudOperator *k8sCloudOperator) GetPodVMUUIDAnnotation(ctx context.Context, req *PodListenerRequest) (*PodListenerResponse, error) {
	var (
		vmuuid   string
		err      error
		timeout  = 5 * time.Minute
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
		errMsg := fmt.Sprintf("No Claim ref found for this PV with volumeID: %s", volumeID)
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	podResult, err := k8sCloudOperator.getPod(ctx, pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace, nodeName)
	if err != nil {
		return nil, err
	}
	podName := podResult.Name
	podNamespace := podResult.Namespace
	err = wait.Poll(pollTime, timeout, func() (bool, error) {
		var exists bool
		pod, err := k8sCloudOperator.k8sClient.CoreV1().Pods(podNamespace).Get(podName, metav1.GetOptions{})
		if err != nil {
			log.Errorf("Failed to get the pod with name: %s on namespace: %s using K8s Cloud Operator informer. Error: %+v", podName, podNamespace, err)
			return false, err
		}
		annotations := pod.Annotations
		vmuuid, exists = annotations[vmUUIDLabel]
		if !exists {
			log.Debugf("Waiting for %s annotation in Pod: %s", vmUUIDLabel, spew.Sdump(pod))
			return false, nil
		}
		log.Debugf("%s annotation with value: %s found in Pod: %s", vmUUIDLabel, vmuuid, spew.Sdump(pod))
		return true, nil
	})
	if err != nil {
		errMsg := fmt.Sprintf("Unable to find pod with name: %s and annotation: %s on namespace: %s in timeout: %d period. Error: %+v", podName, vmUUIDLabel, podNamespace, timeout, err)
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	log.Infof("Found the %s: %s annotation on Pod: %s referring to VolumeID: %s running on node: %s", vmUUIDLabel, vmuuid, podName, volumeID, nodeName)
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
				log.Warnf("Poll Interval to query the Pod Info from API server set in env variable POD_POLL_INTERVAL_SECONDS %s is equal or less than 0, will use the default interval %d", v, defaultPodPollIntervalInSec)
			} else {
				podPollIntervalInSec = value
				log.Debugf("Poll Interval to query the Pod Info from API server is set to %d seconds", podPollIntervalInSec)
			}
		} else {
			log.Warnf("Poll Interval to query the Pod Info from API server set in env variable PODVM_POLL_INTERVAL_SECONDS %s is invalid, will use the default interval", v)
		}
	}
	return podPollIntervalInSec
}

/*
 * getPVWithVolumeID queries API server to get PV
 * referring to the given volumeID
 */
func (k8sCloudOperator *k8sCloudOperator) getPVWithVolumeID(ctx context.Context, volumeID string) (*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)
	allPVs, err := k8sCloudOperator.k8sClient.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		log.Errorf("failed to retrieve all PVs from API server")
		return nil, err
	}
	for _, pv := range allPVs.Items {
		// Verify if it is vsphere block driver and volumehandle matches the volume ID
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csitypes.Name && pv.Spec.CSI.VolumeHandle == volumeID {
			log.Debugf("Found PV: %+v referring to volume ID: %s", pv, volumeID)
			return &pv, nil
		}
	}
	errMsg := fmt.Sprintf("failed to find PV referring to volume ID: %s", volumeID)
	log.Errorf(errMsg)
	return nil, fmt.Errorf(errMsg)
}

/*
 * getPod returns the pod spec for the pod satisfying the below conditions
 * 1. Pod Scheduled on node with name "nodeName"
 * 2. Pod is in pending state in the same namespace as pvc specified using "pvcNamespace"
 * 3. Pod has a volume with name "pvcName" associated with it
 */
func (k8sCloudOperator *k8sCloudOperator) getPod(ctx context.Context, pvcName string, pvcNamespace string,
	nodeName string) (*v1.Pod, error) {
	log := logger.GetLogger(ctx)
	pods, err := k8sCloudOperator.k8sClient.CoreV1().Pods(pvcNamespace).List(metav1.ListOptions{
		FieldSelector: fields.AndSelectors(fields.SelectorFromSet(fields.Set{"spec.nodeName": string(nodeName)}), fields.SelectorFromSet(fields.Set{"status.phase": string(api.PodPending)})).String(),
	})

	if err != nil {
		errMsg := fmt.Sprintf("Cannot find pod with namespace: %s running on node: %s with error %+v", pvcNamespace, nodeName, err)
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	log.Debugf("Returned pods: %+v with namespace: %s running on node: %s", spew.Sdump(pods), pvcNamespace, nodeName)

	// Identify the pod that a volume with name "pvcName" associated with it
	for _, pod := range pods.Items {
		for _, volume := range pod.Spec.Volumes {
			pvClaim := volume.VolumeSource.PersistentVolumeClaim
			if pvClaim != nil && pvClaim.ClaimName == pvcName {
				log.Debugf("Returned pod: %s with pvClaim name: %s and namespace: %s running on node: %s",
					spew.Sdump(&pod), pvcName, pvcNamespace, nodeName)
				return &pod, nil
			}
		}
	}

	errMsg := fmt.Sprintf("Cannot find pod with pvClaim name: %s in namespace: %s running on node: %s", pvcName, pvcNamespace, nodeName)
	log.Error(errMsg)
	return nil, fmt.Errorf(errMsg)
}

/*
 * GetHostAnnotation provide the implementation for the GetHostAnnotation interface method
 */
func (k8sCloudOperator *k8sCloudOperator) GetHostAnnotation(ctx context.Context,
	req *HostAnnotationRequest) (*HostAnnotationResponse, error) {
	var (
		key      = req.AnnotationKey
		nodeName = req.HostName
	)
	log := logger.GetLogger(ctx)
	node, err := k8sCloudOperator.k8sClient.CoreV1().Nodes().Get(nodeName, metav1.GetOptions{})
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

//response of PlacePersistenceVolumeClaim RPC call include only success tag
//pvc does not have a storage pool annotation and does not need a storage pool annotation — that is case 1
//pvc already has a annotation  - case 2
//pvc needs a storage pool annotation and we cant find one - case 3
//pvc needs an annotation and we can find one - case 4
//everything other than case 3 is success
func (k8sCloudOperator *k8sCloudOperator) PlacePersistenceVolumeClaim(ctx context.Context,
	req *PVCPlacementRequest) (*PVCPlacementResponse, error) {

	log := logger.GetLogger(ctx)
	out := &PVCPlacementResponse{
		PlaceSuccess: false,
	}
	if req == nil || req.AccessibilityRequirements == nil || req.Name == "" || req.Namespace == "" {
		return out, fmt.Errorf("no right inputs given to PlacePersistenceVolumeClaim")
	}
	log.Infof("Get info of topology from input %s", req.AccessibilityRequirements)

	pvc, err := k8sCloudOperator.k8sClient.CoreV1().PersistentVolumeClaims(req.Namespace).Get(req.Name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Fail to retrieve targeted PVC %s from API server with error %s", pvc, err)
		return out, err
	}

	scName := pvc.Spec.StorageClassName
	if scName == nil || *scName == "" {
		return out, nil
	}
	sc, err := k8sCloudOperator.k8sClient.StorageV1().StorageClasses().Get(*scName, metav1.GetOptions{})
	if err != nil {
		return out, err
	}
	spTypes, present := sc.Annotations[spTypeAnnotationKey]
	if !present || !strings.Contains(spTypes, vsanDirectType) {
		log.Debug("storage class is not of type vsan direct, aborting placement")
		return out, nil
	}

	log.Debugf("Enter placementEngine %s", req)
	err = PlacePVConStoragePool(ctx, k8sCloudOperator.k8sClient, req.AccessibilityRequirements, pvc)
	if err != nil {
		log.Errorf("Failed to place this PVC on sp with error %s", err)
		return out, err
	}

	log.Debugf("End placementEngine")
	out.PlaceSuccess = true
	return out, err
}
