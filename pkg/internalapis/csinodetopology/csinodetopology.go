/*
Copyright 2021 The Kubernetes Authors.

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

package csinodetopology

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/internalapis/csinodetopology/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

const (
	// CRDSingular represents the singular name of csinodetopology CRD.
	CRDSingular = "csinodetopology"
	// CRDPlural represents the plural name of csinodetopology CRD.
	CRDPlural = "csinodetopologies"
	// defaultTopologyCRWatcherTimeoutInMin is the default duration for which
	// the topology service client will watch on the CSINodeTopology instance to check
	// if the Status has been updated successfully.
	defaultTopologyCRWatcherTimeoutInMin = 1
	// maxTopologyCRWatcherTimeoutInMin is the maximum duration for which
	// the topology service client will watch on the CSINodeTopology instance to check
	// if the Status has been updated successfully.
	maxTopologyCRWatcherTimeoutInMin = 2
)

// NodeInfo contains the information required for the TopologyService to
// identify a node and retrieve its topology information.
type NodeInfo struct {
	// NodeName uniquely identifies the node in kubernetes.
	NodeName string
	// NodeID is a unique identifier of the NodeVM in vSphere.
	NodeID string
}

// TopologyService is an interface which exposes functionality related to topology feature.
type TopologyService interface {
	// GetNodeTopologyLabels fetches the topology labels on the ancestors of NodeVM given the NodeInfo.
	GetNodeTopologyLabels(ctx context.Context, info *NodeInfo) (map[string]string, error)
}

// volumeTopology stores information required for the TopologyService interface.
type volumeTopology struct {
	// k8sClient helps operate on CSINodeTopology custom resource.
	k8sClient client.Client
	// csiNodeTopologyWatcher is a watcher instance on the CSINodeTopology custom resource.
	csiNodeTopologyWatcher *cache.ListWatch
}

var (
	// volumeTopologyInstance is instance of volumeTopology and implements TopologyService interface.
	volumeTopologyInstance *volumeTopology
	// volumeTopologyInstanceLock is used for handling race conditions during read, write on volumeTopologyInstance
	volumeTopologyInstanceLock = &sync.RWMutex{}
)

// InitTopologyServiceInterface returns a singleton implementation of the
// TopologyService interface.
func InitTopologyServiceInterface(ctx context.Context) (TopologyService, error) {
	log := logger.GetLogger(ctx)

	volumeTopologyInstanceLock.RLock()
	if volumeTopologyInstance == nil {
		volumeTopologyInstanceLock.RUnlock()
		volumeTopologyInstanceLock.Lock()
		defer volumeTopologyInstanceLock.Unlock()
		if volumeTopologyInstance == nil {

			// Get in cluster config for client to API server.
			config, err := k8s.GetKubeConfig(ctx)
			if err != nil {
				log.Errorf("failed to get kubeconfig with error: %v", err)
				return nil, err
			}

			// Create client to API server.
			k8sclient, err := k8s.NewClientForGroup(ctx, config, csinodetopologyv1alpha1.GroupName)
			if err != nil {
				log.Errorf("failed to create k8sClient with error: %v", err)
				return nil, err
			}

			// Create watcher for CSINodeTopology instances
			crWatcher, err := k8s.NewCSINodeTopologyWatcher(ctx, config)
			if err != nil {
				log.Errorf("failed to create a watcher for CSINodeTopology CR. Error: %+v", err)
				return nil, err
			}

			volumeTopologyInstance = &volumeTopology{
				k8sClient:              k8sclient,
				csiNodeTopologyWatcher: crWatcher,
			}
		}
	} else {
		volumeTopologyInstanceLock.RUnlock()
	}

	return volumeTopologyInstance, nil
}

// GetNodeTopologyLabels uses the CSINodeTopology CR to retrieve topology information of a node.
func (volTopology *volumeTopology) GetNodeTopologyLabels(ctx context.Context, info *NodeInfo) (
	map[string]string, error) {
	log := logger.GetLogger(ctx)

	// Fetch node object to set owner ref.
	nodeObj, err := getNodeObject(ctx, info.NodeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// Create spec for CSINodeTopology.
	csiNodeTopologySpec := &csinodetopologyv1alpha1.CSINodeTopology{
		ObjectMeta: metav1.ObjectMeta{
			Name: info.NodeName,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Node",
					Name:       nodeObj.Name,
					UID:        nodeObj.UID,
				},
			},
		},
		Spec: csinodetopologyv1alpha1.CSINodeTopologySpec{
			NodeID: info.NodeID,
		},
	}
	// Create CSINodeTopology CR for the node.
	err = volTopology.k8sClient.Create(ctx, csiNodeTopologySpec)
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to create CSINodeTopology CR. Error: %+v", err)
		}
	} else {
		log.Infof("Successfully created a CSINodeTopology instance for NodeName: %q", info.NodeName)
	}

	timeoutSeconds := int64((time.Duration(getCSINodeTopologyWatchTimeoutInMin(ctx)) * time.Minute).Seconds())
	watchCSINodeTopology, err := volTopology.csiNodeTopologyWatcher.Watch(metav1.ListOptions{
		FieldSelector:  fields.OneTermEqualSelector("metadata.name", info.NodeName).String(),
		TimeoutSeconds: &timeoutSeconds,
		Watch:          true,
	})
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to watch on CSINodeTopology instance with name %q. Error: %+v", info.NodeName, err)
	}
	defer watchCSINodeTopology.Stop()

	// Check if status gets updated in the instance within the given timeout seconds.
	for event := range watchCSINodeTopology.ResultChan() {
		csiNodeTopologyInstance, ok := event.Object.(*csinodetopologyv1alpha1.CSINodeTopology)
		if !ok {
			log.Warnf("Received unidentified object - %+v", event.Object)
			continue
		}
		if csiNodeTopologyInstance.Name != info.NodeName {
			continue
		}
		switch csiNodeTopologyInstance.Status.Status {
		case csinodetopologyv1alpha1.CSINodeTopologySuccess:
			accessibleTopology := make(map[string]string)
			for _, label := range csiNodeTopologyInstance.Status.TopologyLabels {
				accessibleTopology[label.Key] = label.Value
			}
			return accessibleTopology, nil
		case csinodetopologyv1alpha1.CSINodeTopologyError:
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to retrieve topology information for Node: %q. Error: %q", info.NodeName,
				csiNodeTopologyInstance.Status.ErrorMessage)
		}
	}
	return nil, logger.LogNewErrorCodef(log, codes.Internal,
		"timed out while waiting for topology labels to be updated in %q CSINodeTopology instance.",
		info.NodeName)
}

// getNodeObject fetches the node instance given the nodeName
func getNodeObject(ctx context.Context, nodeName string) (*v1.Node, error) {
	log := logger.GetLogger(ctx)

	// Create the kubernetes client.
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("failed to create K8s client. Error: %v", err)
		return nil, err
	}
	// Fetch node object.
	nodeObj, err := k8sClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("failed to fetch node object with name %q. Error: %v", nodeName, err)
		return nil, err
	}
	return nodeObj, nil
}

// getCSINodeTopologyWatchTimeoutInMin returns the timeout for watching
// on CSINodeTopology instances for any updates.
// If environment variable NODEGETINFO_WATCH_TIMEOUT_MINUTES is set and
// has a valid value between [1, 2], return the interval value read
// from environment variable. Otherwise, use the default timeout of 1 min.
func getCSINodeTopologyWatchTimeoutInMin(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	watcherTimeoutInMin := defaultTopologyCRWatcherTimeoutInMin
	if v := os.Getenv("NODEGETINFO_WATCH_TIMEOUT_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			switch {
			case value <= 0:
				log.Warnf("Timeout set in env variable NODEGETINFO_WATCH_TIMEOUT_MINUTES %q is equal or "+
					"less than 0, will use the default timeout of %d minute(s)", v, watcherTimeoutInMin)
			case value > maxTopologyCRWatcherTimeoutInMin:
				log.Warnf("Timeout set in env variable NODEGETINFO_WATCH_TIMEOUT_MINUTES %q is greater than "+
					"%d, will use the default timeout of %d minute(s)", v, maxTopologyCRWatcherTimeoutInMin,
					watcherTimeoutInMin)
			default:
				watcherTimeoutInMin = value
				log.Infof("Timeout is set to %d minute(s)", watcherTimeoutInMin)
			}
		} else {
			log.Warnf("Timeout set in env variable NODEGETINFO_WATCH_TIMEOUT_MINUTES %q is invalid, "+
				"using the default timeout of %d minute(s)", v, watcherTimeoutInMin)
		}
	}
	return watcherTimeoutInMin
}
