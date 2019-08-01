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

package podlistener

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	csictx "github.com/rexray/gocsi/context"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	api "k8s.io/kubernetes/pkg/apis/core"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

const (
	vmUUIDLabel                   = "vmware-system-vm-uuid"
	defaultPodPollIntervalInSec   = 2
	defaultPodListenerServicePort = 10000
)

type podListener struct {
	types.Commontypes
	k8sClient clientset.Interface
}

// initPodListenerType initializes the pod listener struct
func initPodListenerType() (*podListener, error) {
	var err error
	podListener := podListener{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfgPath := csictx.Getenv(ctx, cnsconfig.EnvCloudConfig)
	if cfgPath == "" {
		cfgPath = cnsconfig.DefaultCloudConfigPath
	}
	podListener.Cfg, err = cnsconfig.GetCnsconfig(cfgPath)
	if err != nil {
		klog.Errorf("Failed to parse config. Err: %v", err)
		return nil, err
	}

	// Create the kubernetes client from config
	podListener.k8sClient, err = k8s.NewClient()
	if err != nil {
		klog.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return nil, err
	}
	return &podListener, nil
}

// InitPodListenerService initializes the Pod Listener Service
func InitPodListenerService() error {
	podListenerServicePort := getPodListenerServicePort()
	port := flag.Int("port", podListenerServicePort, "The Pod Listener service port")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		klog.Errorf("Failed to listen. Err: %v", err)
		return err
	}
	grpcServer := grpc.NewServer()
	server, err := initPodListenerType()
	if err != nil {
		return err
	}
	RegisterPodListenerServer(grpcServer, server)
	err = grpcServer.Serve(lis)
	if err != nil {
		klog.Errorf("Failed to accept incoming connections on pod listener gRPC server. Err: %v", err)
		return err
	}
	klog.V(2).Infof("Successfully initialized the Pod Listener gRPC service")
	return nil
}

/*
 * GetPodVMUUIDAnnotation provide the implementation the GetPodVMUUIDAnnotation interface method
 */
func (podListener *podListener) GetPodVMUUIDAnnotation(ctx context.Context, req *PodListenerRequest) (*PodListenerResponse, error) {
	var (
		vmuuid   string
		err      error
		timeout  = 5 * time.Minute
		pollTime = time.Duration(getPodPollIntervalInSecs()) * time.Second
		volumeID = req.VolumeID
		nodeName = req.NodeName
	)

	pv, err := podListener.getPVWithVolumeID(volumeID)
	if err != nil {
		return nil, err
	}
	if pv.Spec.ClaimRef == nil {
		errMsg := fmt.Sprintf("No Claim ref found for this PV with volumeID: %s", volumeID)
		klog.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	podResult, err := podListener.getPod(pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace, nodeName)
	if err != nil {
		return nil, err
	}
	podName := podResult.Name
	podNamespace := podResult.Namespace
	err = wait.Poll(pollTime, timeout, func() (bool, error) {
		var exists bool
		pod, err := podListener.k8sClient.CoreV1().Pods(podNamespace).Get(podName, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Failed to get the pod with name: %s on namespace: %s using Podlister informer. Error: %+v", podName, podNamespace, err)
			return false, err
		}
		annotations := pod.Annotations
		vmuuid, exists = annotations[vmUUIDLabel]
		if !exists {
			klog.V(4).Infof("Waiting for %s annotation in Pod: %s", vmUUIDLabel, spew.Sdump(pod))
			return false, nil
		}
		klog.V(3).Infof("%s annotation with value: %s found in Pod: %s", vmUUIDLabel, vmuuid, spew.Sdump(pod))
		return true, nil
	})
	if err != nil {
		errMsg := fmt.Sprintf("Unable to find pod with name: %s and annotation: %s on namespace: %s in timeout: %d period. Error: %+v", podName, vmUUIDLabel, podNamespace, timeout, err)
		klog.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	klog.V(2).Infof("Found the %s: %s annotation on Pod: %s referring to VolumeID: %s running on node: %s", vmUUIDLabel, vmuuid, podName, volumeID, nodeName)
	response := PodListenerResponse{VmuuidAnnotation: vmuuid}
	return &response, nil
}

// getPodListenerServicePort return the port where the Pod Listener gRPC service
// will be running.
// If environment variable X_CSI_POD_LISTENER_SERVICE_PORT is set and valid,
// return the interval value read from enviroment variable
// otherwise, use the default port
func getPodListenerServicePort() int {
	podListenerServicePort := defaultPodListenerServicePort
	if v := os.Getenv("X_CSI_POD_LISTENER_SERVICE_PORT"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				klog.Warningf("Pod Listener Service Port set in env variable X_CSI_POD_LISTENER_SERVICE_PORT %s is equal or less than 0, will use the default port %d", v, defaultPodListenerServicePort)
			} else {
				podListenerServicePort = value
				klog.V(4).Infof("Pod Listener Service will be running on port %d", podListenerServicePort)
			}
		} else {
			klog.Warningf("Pod Listener Service port set in env variable X_CSI_POD_LISTENER_SERVICE_PORT %s is invalid, will use the default port %d", v, defaultPodListenerServicePort)
		}
	}
	return podListenerServicePort
}

// getPodPollIntervalInSecs return the Poll interval in secs to query the
// Pod Info from API server.
// If environment variable X_CSI_POD_POLL_INTERVAL_SECONDS is set and valid,
// return the interval value read from enviroment variable
// otherwise, use the default value 30 minutes
func getPodPollIntervalInSecs() int {
	podPollIntervalInSec := defaultPodPollIntervalInSec
	if v := os.Getenv("X_CSI_POD_POLL_INTERVAL_SECONDS"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				klog.Warningf("Poll Interval to query the Pod Info from API server set in env variable X_CSI_POD_POLL_INTERVAL_SECONDS %s is equal or less than 0, will use the default interval %d", v, defaultPodPollIntervalInSec)
			} else {
				podPollIntervalInSec = value
				klog.V(4).Infof("Poll Interval to query the Pod Info from API server is set to %d seconds", podPollIntervalInSec)
			}
		} else {
			klog.Warningf("Poll Interval to query the Pod Info from API server set in env variable X_CSI_PODVM_POLL_INTERVAL_SECONDS %s is invalid, will use the default interval", v)
		}
	}
	return podPollIntervalInSec
}

/*
 * getPVWithVolumeID queries API server to get PV
 * referring to the given volumeID
 */
func (podListener *podListener) getPVWithVolumeID(volumeID string) (*v1.PersistentVolume, error) {
	allPVs, err := podListener.k8sClient.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to retrieve all PVs from API server")
		return nil, err
	}
	for _, pv := range allPVs.Items {
		// Verify if it is vsphere block driver and volumehandle matches the volume ID
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == types.VSphereCSIDriverName && pv.Spec.CSI.VolumeHandle == volumeID {
			klog.V(4).Infof("Found PV: %+v referring to volume ID: %s", pv, volumeID)
			return &pv, nil
		}
	}
	errMsg := fmt.Sprintf("Failed to find PV referring to volume ID: %s", volumeID)
	klog.Errorf(errMsg)
	return nil, fmt.Errorf(errMsg)
}

/*
 * getPod returns the pod spec for the pod satisfying the below conditions
 * 1. Pod Scheduled on node with name "nodeName"
 * 2. Pod is in pending state in the same namespace as pvc specified using "pvcNamespace"
 * 3. Pod has a volume with name "pvcName" associated with it
 */
func (podListener *podListener) getPod(pvcName string, pvcNamespace string,
	nodeName string) (*v1.Pod, error) {
	var podResult *v1.Pod
	pods, err := podListener.k8sClient.CoreV1().Pods(pvcNamespace).List(metav1.ListOptions{
		FieldSelector: fields.AndSelectors(fields.SelectorFromSet(fields.Set{"spec.nodeName": string(nodeName)}), fields.SelectorFromSet(fields.Set{"status.phase": string(api.PodPending)})).String(),
	})

	if err != nil {
		errMsg := fmt.Sprintf("Cannot find pod with namespace: %s running on node: %s with error %+v", pvcNamespace, nodeName, err)
		klog.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	klog.V(4).Infof("Returned pods: %+v with namespace: %s running on node: %s", spew.Sdump(pods), pvcNamespace, nodeName)

	// Identify the pod that a volume with name "pvcName" associated with it
	for _, pod := range pods.Items {
		if pod.Spec.Volumes != nil {
			for _, volume := range pod.Spec.Volumes {
				pvClaim := volume.VolumeSource.PersistentVolumeClaim
				if pvClaim != nil && pvClaim.ClaimName == pvcName {
					podResult = &pod
					break
				}
			}
		}
	}

	if podResult == nil {
		errMsg := fmt.Sprintf("Cannot find pod with pvClaim name: %s in namespace: %s running on node: %s", pvcName, pvcNamespace, nodeName)
		klog.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	klog.V(3).Infof("Returned pod: %s with pvClaim name: %s and namespace: %s running on node: %s",
		spew.Sdump(podResult), pvcName, pvcNamespace, nodeName)
	return podResult, nil
}
