/*
Copyright 2020 The Kubernetes Authors.

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

package storagepool

import (
	"context"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

var defaultNodeAnnotationListener NodeAnnotationListener

// NodeAnnotationListener listens to Nodes in this WCP cluster looking for "vmware-system-esxi-node-moid" annotation
type NodeAnnotationListener struct {
	informerManager *k8s.InformerManager
	scWatch         *StorageClassWatch
	spController    *SpController
}

// InitNodeAnnotationListener initializes a listener that listens to Nodes in a WCP cluster.
func InitNodeAnnotationListener(ctx context.Context, informerManager *k8s.InformerManager,
	scWatch *StorageClassWatch, spController *SpController) error {
	log := logger.GetLogger(ctx)
	defaultNodeAnnotationListener = NodeAnnotationListener{
		informerManager: informerManager,
		scWatch:         scWatch,
		spController:    spController,
	}
	defaultNodeAnnotationListener.informerManager.AddNodeListener(
		nil,
		defaultNodeAnnotationListener.nodeUpdated, // Update
		nil)
	log.Infof("NodeAnnotationListener initialized.")
	<-defaultNodeAnnotationListener.informerManager.Listen()
	return nil
}

func (l *NodeAnnotationListener) nodeUpdated(oldObj interface{}, newObj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	oldNode, ok := oldObj.(*v1.Node)
	if oldNode == nil || !ok {
		log.Warnf("nodeUpdated: unrecognized old object %+v", oldObj)
		return
	}
	newNode, ok := newObj.(*v1.Node)
	if newNode == nil || !ok {
		log.Warnf("nodeUpdated: unrecognized new object %+v", newObj)
		return
	}
	oldMoid, oldOk := oldNode.ObjectMeta.Annotations[nodeMoidAnnotation]
	newMoid, newOk := newNode.ObjectMeta.Annotations[nodeMoidAnnotation]
	if oldMoid != newMoid || oldOk != newOk {
		log.Infof("Change in node annotation for %s from %s to %s. Starting ReconcileAllStoragePools...",
			newNode.Name, oldMoid, newMoid)
		err := ReconcileAllStoragePools(ctx, l.scWatch, l.spController)
		if err != nil {
			log.Errorf("ReconcileAllStoragePools failed. err: %v", err)
		}
		log.Debugf("Done reconciling all StoragePools for node annotation change for %s", newNode.Name)
	}
}
