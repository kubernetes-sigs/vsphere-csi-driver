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

package cnsnodevmattachment

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	"gitlab.eng.vmware.com/hatchway/govmomi/object"
	vimtypes "gitlab.eng.vmware.com/hatchway/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	cnsnode "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/node"
	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsnodevmattachment/v1alpha1"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

// Add creates a new CnsNodeVmAttachment Controller and adds it to the Manager, ConfigInfo
// and VirtualCenterTypes. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, configInfo *types.ConfigInfo, volumeManager volumes.Manager) error {
	return add(mgr, newReconciler(mgr, configInfo, volumeManager))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, configInfo *types.ConfigInfo, volumeManager volumes.Manager) reconcile.Reconciler {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	return &ReconcileCnsNodeVMAttachment{client: mgr.GetClient(), scheme: mgr.GetScheme(), configInfo: configInfo, volumeManager: volumeManager, nodeManager: cnsnode.GetManager(ctx)}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Create a new controller
	c, err := controller.New("cnsnodevmattachment-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		log.Errorf("Failed to create new CnsNodeVmAttachment controller with error: %+v", err)
		return err
	}

	// Watch for changes to primary resource CnsNodeVmAttachment
	err = c.Watch(&source.Kind{Type: &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		log.Errorf("Failed to watch for changes to CnsNodeVmAttachment resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that ReconcileCnsNodeVMAttachment implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileCnsNodeVMAttachment{}

// ReconcileCnsNodeVMAttachment reconciles a CnsNodeVmAttachment object
type ReconcileCnsNodeVMAttachment struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *types.ConfigInfo
	volumeManager volumes.Manager
	nodeManager   cnsnode.Manager
}

// Reconcile reads that state of the cluster for a CnsNodeVMAttachment object and makes changes based on the state read
// and what is in the CnsNodeVMAttachment.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileCnsNodeVMAttachment) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	log.Infof("Reconciling CnsNodeVmAttachment with Request.Name: %q and Request.Namespace: %q", request.Namespace, request.Name)
	// Fetch the CnsNodeVmAttachment instance
	instance := &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Error("CnsNodeVmAttachment resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CnsNodeVmAttachment with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// If the CnsNodeVMAttachment instance is already attached and
	// not deleted by the user, remove the instance from the queue.
	if instance.Status.Attached && instance.DeletionTimestamp == nil {
		return reconcile.Result{}, nil
	}

	vcdcMap, err := getVCDatacentersFromConfig(r.configInfo.Cfg)
	if err != nil {
		log.Errorf("Failed to find datacenter moref from config for CnsNodeVmAttachment request with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		instance.Status.Error = err.Error()
		updateCnsNodeVMAttachment(ctx, r.client, instance)
		return reconcile.Result{}, err
	}
	var host, dcMoref string
	for key, value := range vcdcMap {
		host = key
		dcMoref = value[0]
	}
	// Get node VM by nodeUUID
	var dc *vsphere.Datacenter
	vcenter, err := types.GetVirtualCenterInstance(ctx, r.configInfo)
	if err != nil {
		log.Errorf("Failed to get virtual center instance with error: %v", err)
		return reconcile.Result{}, err
	}
	dc = &vsphere.Datacenter{
		Datacenter: object.NewDatacenter(vcenter.Client.Client,
			vimtypes.ManagedObjectReference{
				Type:  "Datacenter",
				Value: dcMoref,
			}),
		VirtualCenterHost: host,
	}
	nodeUUID := instance.Spec.NodeUUID
	if !instance.Status.Attached && instance.DeletionTimestamp == nil {
		nodeVM, err := dc.GetVirtualMachineByUUID(ctx, nodeUUID, false)
		if err != nil {
			log.Errorf("Failed to find the VM with UUID: %q for CnsNodeVmAttachment request with name: %q on namespace: %q. Err: %+v",
				nodeUUID, request.Name, request.Namespace, err)
			instance.Status.Error = fmt.Sprintf("Failed to find the VM with UUID: %q", nodeUUID)
			updateCnsNodeVMAttachment(ctx, r.client, instance)
			return reconcile.Result{}, err
		}
		volumeID, err := getVolumeID(ctx, r.client, instance.Spec.VolumeName, instance.Namespace)
		if err != nil {
			log.Errorf("Failed to get volumeID from volumeName: %q for CnsNodeVmAttachment request with name: %q on namespace: %q. Error: %+v",
				instance.Spec.VolumeName, request.Name, request.Namespace, err)
			instance.Status.Error = err.Error()
			updateCnsNodeVMAttachment(ctx, r.client, instance)
			return reconcile.Result{}, err
		}
		cnsFinalizerExists := false
		// Check if finalizer already exists.
		for _, finalizer := range instance.Finalizers {
			if finalizer == cnsoperatortypes.CNSFinalizer {
				cnsFinalizerExists = true
				break
			}
		}
		// Update finalizer and attachmentMetadata together in CnsNodeVMAttachment.
		if !cnsFinalizerExists {
			// Add finalizer.
			instance.Finalizers = append(instance.Finalizers, cnsoperatortypes.CNSFinalizer)
			/*
				Add the CNS volume ID in the attachment metadata. This is used later to detach the CNS volume on
				deletion of CnsNodeVMAttachment instance. Note that the supervisor PVC can be deleted due to following:
				1. Bug in external provisioner(https://github.com/kubernetes/kubernetes/issues/84226) where DeleteVolume
				   could be invoked in pvcsi before ControllerUnpublishVolume. This causes supervisor PVC to be deleted.
				2. Supervisor namespace user deletes PVC used by a guest cluster.
				3. Supervisor namespace is deleted
				Basically, we cannot rely on the existence of PVC in supervisor cluster for detaching the volume from
				guest cluster VM. So, the logic stores the CNS volume ID in attachmentMetadata itself which is used
				during detach.
			*/
			attachmentMetadata := make(map[string]string)
			attachmentMetadata[cnsnodevmattachmentv1alpha1.AttributeCnsVolumeID] = volumeID
			instance.Status.AttachmentMetadata = attachmentMetadata
			err = updateCnsNodeVMAttachment(ctx, r.client, instance)
			if err != nil {
				log.Errorf("Failed to update CnsNodeVmAttachment instance: %q on namespace: %q. Error: %+v",
					request.Name, request.Namespace, err)
				return reconcile.Result{}, err
			}
		}

		log.Debugf("vSphere CNS driver is attaching volume: %q to nodevm: %+v for CnsNodeVmAttachment request with name: %q on namespace: %q",
			volumeID, nodeVM, request.Name, request.Namespace)
		diskUUID, attachErr := volumes.GetManager(ctx, vcenter).AttachVolume(ctx, nodeVM, volumeID)
		if attachErr != nil {
			log.Errorf("Failed to attach disk: %q to nodevm: %+v for CnsNodeVmAttachment request with name: %q on namespace: %q. Err: %+v",
				volumeID, nodeVM, request.Name, request.Namespace, attachErr)
		}

		if !cnsFinalizerExists {
			// Read the CnsNodeVMAttachment instance again because the instance is already modified
			err = r.client.Get(ctx, request.NamespacedName, instance)
			if err != nil {
				log.Errorf("Error reading the CnsNodeVmAttachment with name: %q on namespace: %q. Err: %+v",
					request.Name, request.Namespace, err)
				// Error reading the object - requeue the request.
				return reconcile.Result{}, err
			}
		}

		if attachErr != nil {
			// Update CnsNodeVMAttachment instance with attach error message
			instance.Status.Error = attachErr.Error()
		} else {
			// Update CnsNodeVMAttachment instance with attached status set to true
			// and attachment metadata
			instance.Status.AttachmentMetadata[cnsnodevmattachmentv1alpha1.AttributeFirstClassDiskUUID] = diskUUID
			instance.Status.Attached = true
			// Clear the error message
			instance.Status.Error = ""
		}

		err = updateCnsNodeVMAttachment(ctx, r.client, instance)
		if err != nil {
			log.Errorf("Failed to update attach status on CnsNodeVmAttachment instance: %q on namespace: %q. Error: %+v",
				request.Name, request.Namespace, err)
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, attachErr
	}

	if instance.DeletionTimestamp != nil {
		nodeVM, err := dc.GetVirtualMachineByUUID(ctx, nodeUUID, false)
		if err != nil {
			log.Errorf("Failed to find the VM with UUID: %q for CnsNodeVmAttachment request with name: %q on namespace: %q. Err: %+v",
				nodeUUID, request.Name, request.Namespace, err)
			// TODO : Need to check for VirtualMachine CRD instance existence.
			// This check is needed in scenarios where VC inventory is stale due to upgrade or back-up and restore
			removeFinalizerFromCRDInstance(ctx, instance, request)
			updateCnsNodeVMAttachment(ctx, r.client, instance)
			return reconcile.Result{}, nil
		}
		var cnsVolumeID string
		var ok bool
		if cnsVolumeID, ok = instance.Status.AttachmentMetadata[cnsnodevmattachmentv1alpha1.AttributeCnsVolumeID]; !ok {
			errMsg := fmt.Sprintf("CnsNodeVmAttachment does not have CNS volume ID. AttachmentMetadata: %+v", instance.Status.AttachmentMetadata)
			log.Error(errMsg)
			return reconcile.Result{}, errors.New(errMsg)
		}
		log.Debugf("vSphere CNS driver is detaching volume: %q to nodevm: %+v for CnsNodeVmAttachment request with name: %q on namespace: %q",
			cnsVolumeID, nodeVM, request.Name, request.Namespace)
		detachErr := volumes.GetManager(ctx, vcenter).DetachVolume(ctx, nodeVM, cnsVolumeID)
		if detachErr != nil {
			if vsphere.IsManagedObjectNotFound(detachErr) {
				log.Errorf("Found a managed object not found fault for vm: %+v", nodeVM)
				removeFinalizerFromCRDInstance(ctx, instance, request)
				updateCnsNodeVMAttachment(ctx, r.client, instance)
				return reconcile.Result{}, nil
			}
			log.Errorf("Failed to detach disk: %q from nodevm: %+v for CnsNodeVmAttachment request with name: %q on namespace: %q. Err: %+v",
				cnsVolumeID, nodeVM, request.Name, request.Namespace, detachErr)
			// Update CnsNodeVMAttachment instance with detach error message
			instance.Status.Error = detachErr.Error()
		} else {
			removeFinalizerFromCRDInstance(ctx, instance, request)
		}
		err = updateCnsNodeVMAttachment(ctx, r.client, instance)
		if err != nil {
			log.Errorf("Failed to update detach status on CnsNodeVmAttachment instance: %q on namespace: %q. Error: %+v",
				request.Name, request.Namespace, err)
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, detachErr
	}
	return reconcile.Result{}, nil
}

// removeFinalizerFromCRDInstance will remove the CNS Finalizer = cns.vmware.com, from a given nodevmattachment instance
func removeFinalizerFromCRDInstance(ctx context.Context, instance *cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment, request reconcile.Request) {
	log := logger.GetLogger(ctx)
	for i, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			log.Debugf("Removing %q finalizer from CnsNodeVmAttachment instance with name: %q on namespace: %q",
				cnsoperatortypes.CNSFinalizer, request.Name, request.Namespace)
			instance.Finalizers = append(instance.Finalizers[:i], instance.Finalizers[i+1:]...)
		}
	}
}

// getVCDatacenterFromConfig returns datacenter registered for each vCenter
func getVCDatacentersFromConfig(cfg *config.Config) (map[string][]string, error) {
	var err error
	vcdcMap := make(map[string][]string)
	for key, value := range cfg.VirtualCenter {
		dcList := strings.Split(value.Datacenters, ",")
		for _, dc := range dcList {
			dcMoID := strings.TrimSpace(dc)
			if dcMoID != "" {
				vcdcMap[key] = append(vcdcMap[key], dcMoID)
			}
		}
	}
	if len(vcdcMap) == 0 {
		err = errors.New("Unable get vCenter datacenters from vsphere config")
	}
	return vcdcMap, err
}

// getVolumeID gets the volume ID from the PV that is bound to PVC by pvcName
func getVolumeID(ctx context.Context, client client.Client, pvcName string, namespace string) (string, error) {
	log := logger.GetLogger(ctx)
	// Get PVC by pvcName from namespace
	pvc := &v1.PersistentVolumeClaim{}
	err := client.Get(ctx, k8stypes.NamespacedName{Name: pvcName, Namespace: namespace}, pvc)
	if err != nil {
		log.Errorf("Failed to get PVC with volumename: %q on namespace: %q. Err: %+v",
			pvcName, namespace, err)
		return "", err
	}

	// Get PV by name
	pv := &v1.PersistentVolume{}
	err = client.Get(ctx, k8stypes.NamespacedName{Name: pvc.Spec.VolumeName, Namespace: ""}, pv)
	if err != nil {
		log.Errorf("Failed to get PV with name: %q for PVC: %q. Err: %+v",
			pvc.Spec.VolumeName, pvcName, err)
		return "", err
	}
	return pv.Spec.CSI.VolumeHandle, nil
}


func updateCnsNodeVMAttachment(ctx context.Context, client client.Client, instance *cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		log.Errorf("Failed to update CnsNodeVmAttachment instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, err)
	}
	return err
}
