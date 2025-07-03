/*
Copyright 2024 The Kubernetes Authors.

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

package syncer

import (
	"context"
	"fmt"

	vmoperatorv1alpha4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

type ObjectToBeProcess struct {
	vmSnapshotObj    *vmoperatorv1alpha4.VirtualMachineSnapshot
	vmSnapshotOldObj *vmoperatorv1alpha4.VirtualMachineSnapshot
	isDeleted        bool
}

// VirtualMachineSnapshotReconciler is the interface for VirtualMachineSnapshot reconciler.
type VirtualMachineSnapshotReconciler interface {
	// Run starts the reconciler.
	Run(ctx context.Context, workers int)
}

type virtualMachineSnapshotReconciler struct {
	// metadataSyncer informer to get FSS information.
	metadataSyncerInformer *metadataSyncInformer
	// VirtualMachineSnapshot queue to add/delete VirtualMachineSnapshot CRs.
	svcVmsnapshotOpsQueue workqueue.TypedRateLimitingInterface[any]
	// VirtualMachineSnapshot Synced.
	svcQuotaSynced cache.InformerSynced
}

// newVirtualMachineSnapshotReconciler returns a VirtualMachineSnapshotReconciler.
func newVirtualMachineSnapshotReconciler(
	ctx context.Context,
	metadataSyncerInformer *metadataSyncInformer,
	svcVirtualMachineSnapshotRateLimiter workqueue.TypedRateLimiter[any],
	stopCh <-chan struct{}) (*virtualMachineSnapshotReconciler, error) {

	log := logger.GetLogger(ctx)
	// Create an informer for VirtualMachineSnapshot instances.
	k8sConfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "newVirtualMachineSnapshotReconciler: failed to get kubeconfig with error: %v", err)
	}
	dynamicInformerFactory, err := k8s.NewDynamicInformerFactory(ctx, k8sConfig, metav1.NamespaceAll, true)
	if err != nil {
		log.Errorf("newVirtualMachineSnapshotReconciler: could not retrieve dynamic informer factory. Error: %+v", err)
		return nil, err
	}
	// Return informer from shared dynamic informer factory for input resource.
	gvr := schema.GroupVersionResource{Group: cnsoperatorv1alpha1.GroupName, Version: cnsoperatorv1alpha1.Version,
		Resource: cnsoperatorv1alpha1.CnsVirtualMachineSnapshotPlural}
	virtualMachineSnapshotInformer := dynamicInformerFactory.ForResource(gvr)
	svcVmsnapshotOpsQueue := workqueue.NewTypedRateLimitingQueueWithConfig(
		svcVirtualMachineSnapshotRateLimiter, workqueue.TypedRateLimitingQueueConfig[any]{
			Name: "storage-policy-quota-ops",
		})

	rc := &virtualMachineSnapshotReconciler{
		metadataSyncerInformer: metadataSyncerInformer,
		svcQuotaSynced:         virtualMachineSnapshotInformer.Informer().HasSynced,
		svcVmsnapshotOpsQueue:  svcVmsnapshotOpsQueue,
	}

	_, err = virtualMachineSnapshotInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    rc.addVirtualMachineSnapshot,
			DeleteFunc: rc.delVirtualMachineSnapshot,
			UpdateFunc: rc.updateVirtualMachineSnapshot,
		})
	if err != nil {
		return nil, logger.LogNewErrorf(log, "newVirtualMachineSnapshotReconciler: failed to add event handler on "+
			"VirtualMachineSnapshot informer. Error: %v", err)
	}

	// Start VirtualMachineSnapshot Informer.
	dynamicInformerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, rc.svcQuotaSynced) {
		return nil, fmt.Errorf("newVirtualMachineSnapshotReconciler: cannot sync VirtualMachineSnapshot cache")
	}

	return rc, nil
}

func (rc *virtualMachineSnapshotReconciler) addVirtualMachineSnapshot(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	policyQuotaObj := &vmoperatorv1alpha4.VirtualMachineSnapshot{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object,
		policyQuotaObj)
	if err != nil {
		log.Errorf("addVirtualMachineSnapshot: failed to cast object %+v to %s. Error: %v", obj,
			cnsoperatorv1alpha1.CnsVirtualMachineSnapshotSingular, err)
		return
	}
	objToBeProcess := &ObjectToBeProcess{
		vmSnapshotObj: policyQuotaObj,
		isDeleted:     false,
	}
	rc.svcVmsnapshotOpsQueue.Add(objToBeProcess)
}

func (rc *virtualMachineSnapshotReconciler) updateVirtualMachineSnapshot(oldObj interface{}, newObj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	vmSnapshotObj := &vmoperatorv1alpha4.VirtualMachineSnapshot{}
	vmSnapshotOldObj := &vmoperatorv1alpha4.VirtualMachineSnapshot{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(oldObj.(*unstructured.Unstructured).Object,
		vmSnapshotOldObj)
	if err != nil {
		log.Errorf("addVirtualMachineSnapshot: failed to cast object %+v to %s. Error: %v", newObj,
			cnsoperatorv1alpha1.CnsVirtualMachineSnapshotSingular, err)
		return
	}

	err = runtime.DefaultUnstructuredConverter.FromUnstructured(newObj.(*unstructured.Unstructured).Object,
		vmSnapshotObj)
	if err != nil {
		log.Errorf("addVirtualMachineSnapshot: failed to cast object %+v to %s. Error: %v", newObj,
			cnsoperatorv1alpha1.CnsVirtualMachineSnapshotSingular, err)
		return
	}
	objToBeProcess := &ObjectToBeProcess{
		vmSnapshotObj:    vmSnapshotObj,
		vmSnapshotOldObj: vmSnapshotOldObj,
		isDeleted:        false,
	}
	rc.svcVmsnapshotOpsQueue.Add(objToBeProcess)
}

func (rc *virtualMachineSnapshotReconciler) delVirtualMachineSnapshot(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	policyQuotaObj := &vmoperatorv1alpha4.VirtualMachineSnapshot{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object,
		policyQuotaObj)
	if err != nil {
		log.Errorf("delVirtualMachineSnapshot: failed to cast object %+v to %s. Error: %v", obj,
			cnsoperatorv1alpha1.CnsVirtualMachineSnapshotSingular, err)
		return
	}
	objToBeProcess := &ObjectToBeProcess{
		vmSnapshotObj: policyQuotaObj,
		isDeleted:     true,
	}
	rc.svcVmsnapshotOpsQueue.Add(objToBeProcess)
}

// Run starts the reconciler.
func (rc *virtualMachineSnapshotReconciler) Run(
	ctx context.Context, workers int) {
	log := logger.GetLogger(ctx)
	defer rc.svcVmsnapshotOpsQueue.ShutDown()

	log.Infof("Starting VirtualMachineSnapshot reconciler")
	defer log.Infof("Shutting down VirtualMachineSnapshot reconciler after draining")

	stopCh := ctx.Done()

	for i := 0; i < workers; i++ {
		go wait.Until(func() { rc.processVirtualMachineSnapshots(ctx) }, 0, stopCh)
	}
	<-stopCh
}

// syncVirtualMachineSnapshots is the main worker to add/delete VirtualMachineSnapshot for given VirtualMachineSnapshot.
func (rc *virtualMachineSnapshotReconciler) processVirtualMachineSnapshots(ctx context.Context) {
	obj, quit := rc.svcVmsnapshotOpsQueue.Get()
	if quit {
		return
	}
	defer rc.svcVmsnapshotOpsQueue.Done(obj)
	objToBeProcess := obj.(*ObjectToBeProcess)
	policyQuotaObj := objToBeProcess.vmSnapshotObj
	// If object has been deleted, process deletion of corresponding VirtualMachineSnapshot(s).
	// Otherwise process the object for addition of corresponding VirtualMachineSnapshot(s).
	if !objToBeProcess.isDeleted {
		if err := rc.processVirtualMachineSnapshot(ctx, policyQuotaObj); err != nil {
			// Put VirtualMachineSnapshot back to the queue so that we can retry later.
			rc.svcVmsnapshotOpsQueue.AddRateLimited(obj)
		} else {
			rc.svcVmsnapshotOpsQueue.Forget(obj)
		}
	} else {
		if err := rc.processVirtualMachineSnapshot(ctx, policyQuotaObj); err != nil {
			// Put VirtualMachineSnapshot back to the queue so that we can retry later.
			rc.svcVmsnapshotOpsQueue.AddRateLimited(obj)
		} else {
			rc.svcVmsnapshotOpsQueue.Forget(obj)
		}
	}
}

// processVirtualMachineSnapshot processes one VirtualMachineSnapshot CR added or updated to cluster
func (rc *virtualMachineSnapshotReconciler) processVirtualMachineSnapshot(ctx context.Context,
	virtualMachineSnapshot *vmoperatorv1alpha4.VirtualMachineSnapshot) error {
	log := logger.GetLogger(ctx)
	log.Infof("processVirtualMachineSnapshot: Started VirtualMachineSnapshot processing %s/%s",
		virtualMachineSnapshot.Namespace, virtualMachineSnapshot.Name)
	// Check for the annotation "csi.vsphere.volume.sync: Requested"
	annotationValue := virtualMachineSnapshot.Annotations["csi.vsphere.volume.sync"]
	if annotationValue == "Requested" {
		// if found fetch vmsnapshot and pvcs and pvs
		vmKey := types.NamespacedName{
			Namespace: virtualMachineSnapshot.Namespace,
			Name:      virtualMachineSnapshot.Spec.VMRef.Name,
		}
		err := validateStorageQuotaforVMSnapshot(ctx, vmKey, rc.metadataSyncerInformer)
		if err != nil {
			log.Errorf("processVirtualMachineSnapshotAddCase: Could not create VirtualMachineSnapshot CR for %s/%s err: %v",
				virtualMachineSnapshot.Namespace, virtualMachineSnapshot.Name, err)
			return err
		}
	}
	// Update VMSnapshot CR annotation to "csi.vsphere.volume.sync: Completed"
	virtualMachineSnapshot.Annotations["csi.vsphere.volume.sync"] = "Completed"
	err := utils.UpdateVirtualMachineSnapshot(ctx, MetadataSyncer.cnsOperatorClient, virtualMachineSnapshot)
	if err != nil {
		return err
	}
	return nil
}

// // processVirtualMachineSnapshotDelCase processes one VirtualMachineSnapshot CR deleted from cluster
// func (rc *virtualMachineSnapshotReconciler) processVirtualMachineSnapshotDelCase(ctx context.Context,
// 	virtualMachineSnapshot *vmoperatorv1alpha4.VirtualMachineSnapshot) error {
// 	log := logger.GetLogger(ctx)
// 	log.Debugf("processVirtualMachineSnapshotDelCase: Started VirtualMachineSnapshot processing %s/%s",
// 		virtualMachineSnapshot.Namespace, virtualMachineSnapshot.Name)

// 	// err := deleteStoragePolicyUsageCR(ctx, virtualMachineSnapshot.Name,
// 	// 	virtualMachineSnapshot.Namespace, rc.metadataSyncerInformer)
// 	// if err != nil {
// 	// 	log.Errorf("processVirtualMachineSnapshotDelCase: Could not delete VirtualMachineSnapshot CR for %s/%s err: %v",
// 	// 		virtualMachineSnapshot.Namespace, virtualMachineSnapshot.Name, err)
// 	// 	return err
// 	// }

// 	return nil
// }
