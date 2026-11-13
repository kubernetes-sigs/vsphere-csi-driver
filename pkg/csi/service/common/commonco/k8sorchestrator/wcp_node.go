/*
Copyright 2023 The Kubernetes Authors.

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

package k8sorchestrator

import (
	"context"
	"os"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

var clusterComputeResourceMoIds []string

// InitializeCSINodes listens on node Add events to create CSINode instance for each K8s node object.
func (c *K8sOrchestrator) InitializeCSINodes(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	var err error
	clusterComputeResourceMoIds, _, err = common.GetClusterComputeResourceMoIds(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to get clusterComputeResourceMoIds. Error: %v", err)
	}
	err = c.informerManager.AddNodeListener(
		ctx,
		func(obj interface{}) { // Add.
			c.createCSINode(obj)
		},
		nil,
		nil)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to listen on nodes. Error: %+v", err)
	}
	log.Infof("Started informer to listen on nodes and create corresponding CSINodes")
	return nil
}

// createCSINode creates CSINode instances for each K8s node object.
func (c *K8sOrchestrator) createCSINode(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	node, ok := obj.(*corev1.Node)
	if node == nil || !ok {
		log.Warnf("createCSINode: unrecognized object %+v found", obj)
		return
	}

	// Check if CreateCSINodeAnnotation is present on the node. Spherelet adds this annotation on
	// the node to communicate to CSI driver to create CSINode instance for each node.
	csiNodeAnnotation, exists := node.ObjectMeta.Annotations[common.CreateCSINodeAnnotation]
	if val, _ := strconv.ParseBool(csiNodeAnnotation); !exists || !val {
		log.Infof("createCSINode: %s annotation exists: %t with value: %t on the node %s",
			common.CreateCSINodeAnnotation, exists, val, node.Name)
		return
	}

	var topologyKeysList []string
	isWorkloadDomainIsolationEnabled := c.IsFSSEnabled(ctx, common.WorkloadDomainIsolation)
	if isWorkloadDomainIsolationEnabled {
		// Publish zone and host standard topology keys for all types of supervisor clusters.
		topologyKeysList = append(topologyKeysList, corev1.LabelTopologyZone, corev1.LabelHostname)
	} else {
		if len(clusterComputeResourceMoIds) > 1 {
			// Publish zone and host standard topology keys for stretch supervisor.
			topologyKeysList = append(topologyKeysList, corev1.LabelTopologyZone, corev1.LabelHostname)
		} else {
			topologyKeysList = append(topologyKeysList, corev1.LabelHostname)
		}
	}

	// NOTE: As this is a WCP node, we can safely assume that only vSphere CSI driver will be run on it.
	// If spherelet is not creating the CSINodes, we will create them and ignore the error if it is already present.
	csiNodeSpec := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(node.Name),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Node",
					Name:       node.Name,
					UID:        node.UID,
				},
			},
		},
		Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{
				{
					Name:         common.VSphereCSIDriverName,
					NodeID:       node.Name,
					TopologyKeys: topologyKeysList,
				},
			},
		},
	}
	csinode, err := c.k8sClient.StorageV1().CSINodes().Get(ctx, node.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = c.k8sClient.StorageV1().CSINodes().Create(ctx, csiNodeSpec, metav1.CreateOptions{})
			if err != nil {
				if apierrors.IsAlreadyExists(err) {
					log.Warnf("CSINode object for node %q already exists.", node.Name)
					return
				}
				log.Errorf("failed to create CSINode object %+v. Error: %v", csiNodeSpec, err)
				os.Exit(1)
			} else {
				log.Infof("Created CSINode object %+v for node %q", csinode, node.Name)
				return
			}
		} else {
			log.Errorf("failed to get CSINode object %+v. Error: %v", csiNodeSpec, err)
			os.Exit(1)
		}
	} else {
		driverIdx := slices.IndexFunc(csinode.Spec.Drivers, func(d storagev1.CSINodeDriver) bool {
			return d.Name == common.VSphereCSIDriverName
		})
		if driverIdx < 0 {
			// The CSINode object exists but carries no entry for the vSphere CSI driver. kubelet creates
			// the CSINode when the node registers and only adds a driver entry once that node plugin
			// registers itself, so a missing entry means the CSI node daemonset is not running here. This
			// is the case on an annotated control plane node. Do not add the entry: it would advertise the
			// driver to the scheduler and to the attacher on a node where nothing can format or mount a
			// volume, leaving pods scheduled there stuck in NodeStageVolume.
			log.Warnf("createCSINode: CSINode object for node %q has no entry for driver %q. Skipping, as "+
				"the CSI node daemonset does not appear to be running on this node.",
				node.Name, common.VSphereCSIDriverName)
			return
		}
		// If CSINode object is present, re-create CSINode object if zone topology key is not present on it.
		// Topology keys of an existing driver entry are immutable, hence the delete and re-create.
		if isWorkloadDomainIsolationEnabled {
			if !slices.Contains(csinode.Spec.Drivers[driverIdx].TopologyKeys, corev1.LabelTopologyZone) {
				log.Infof("topology key: %q not present on CSINodes object for node: %q. Re-creating CSINodes object.",
					corev1.LabelTopologyZone, node.Name)
				err = c.k8sClient.StorageV1().CSINodes().Delete(ctx, csiNodeSpec.Name, metav1.DeleteOptions{})
				if err != nil {
					log.Errorf("failed to delete CSINode object for node: %q. Error: %v", csiNodeSpec.Name, err)
					os.Exit(1)
				}
				_, err = c.k8sClient.StorageV1().CSINodes().Create(ctx, csiNodeSpec, metav1.CreateOptions{})
				if err == nil {
					log.Infof("re-created CSINodes object %+v for the node: %q with zonal topology keys",
						csinode, csiNodeSpec.Name)
					return
				} else {
					log.Errorf("failed to create CSINode object %+v. Error: %v", csiNodeSpec, err)
					os.Exit(1)
				}
			}
		}
	}
}
