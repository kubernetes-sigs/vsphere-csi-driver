/*
Copyright 2020 VMware, Inc.

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
	"encoding/json"
	"os"
	"strings"

	vimtypes "github.com/vmware/govmomi/vim25/types"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

const (
	// We are still deciding where exactly to publish the storage policy
	// content, so keep this off for now.
	featureSwitchStorageClassContentAnnotation = false

	policyContentAnnotationKey = "cns.vmware.com/policy"
)

// StorageClassWatch keeps state to watch storage classes and keep
// an in-memory cache of datastore / storage pool accessibility
type StorageClassWatch struct {
	scWatch       watch.Interface
	clientset     *kubernetes.Clientset
	vc            *cnsvsphere.VirtualCenter
	policyIds     []string
	policyToScMap map[string]*storagev1.StorageClass
	clusterID     string

	dsPolicyCompatMapCache map[string][]string
}

// Watch storage classes that pertain to our CSI driver
//
// The goal is two fold:
// a) Keep a in-memory cache of all storage classes so when we remediate
//    other things, like StoragePool, we have cheap access to the
//    classes.
// b) Put annotations on the StorageClass that contain information
//    about the underlying Storage Policy in VC
//
// This function starts a go-routine which processes watch fires
func startStorageClassWatch(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string, cfg *rest.Config) (*StorageClassWatch, error) {
	log := logger.GetLogger(ctx)
	w := &StorageClassWatch{}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Errorf("Failed to create K8s client using config. Err: %+v", err)
		return nil, err
	}
	w.clientset = clientset
	w.vc = vc
	w.clusterID = clusterID

	// Refresh our cache once now, so any other code that wants to access
	// the cache doesn't need to syncronize with the watch firing
	err = w.refreshStorageClassCache(ctx)
	if err != nil {
		return nil, err
	}

	err = renewStorageClassWatch(w)
	if err != nil {
		return nil, err
	}

	go w.watchStorageClass(ctx)
	return w, nil
}

// As our watch can and will expire, we need a helper to renew it
// Note that after we re-new it, we will get a bunch of ADDED events, triggering
// a full remediation
func renewStorageClassWatch(w *StorageClassWatch) error {
	var err error
	scClient := w.clientset.StorageV1().StorageClasses()
	// This means every 24h our watch may expire and require to be re-created.
	// When that happens, we need to do a full remediation, hence we change
	// from 30m (default) to 24h.
	timeout := int64(60 * 60 * 24) // 24h
	w.scWatch, err = scClient.Watch(metav1.ListOptions{
		TimeoutSeconds: &timeout,
	})
	return err
}

// Handler for storage class watch firing. Mostly handles shutdown and
// watch renewal, and on all storage class changes triggers a full
// remediation via refreshStorageClassCache().
func (w *StorageClassWatch) watchStorageClass(ctx context.Context) {
	log := logger.GetLogger(ctx)

	done := false
	for !done {
		select {
		case <-ctx.Done():
			log.Info("watchStorageClass shutdown", "ctxErr", ctx.Err())
			done = true
		case e, ok := <-w.scWatch.ResultChan():
			if !ok {
				log.Info("watchStorageClass watch not ok")

				err := renewStorageClassWatch(w)
				if err != nil {
					// XXX: Not sure how to handle this, as we need this watch.
					// So crash and let restart perform any remediation required
					log.Error(err, "Fatal event, couldn't renew storage class watch")
					os.Exit(3)
					return
				}
				continue
			}
			switch e.Object.(type) {
			case *storagev1.StorageClass:
				w.refreshStorageClassCache(ctx)
			}
		}
	}
	log.Info("watchStorageClass ends")
}

// Refresh the storage class cache and do a full remediation
// The cache contains a convenient lookup table mapping from policyId to storage class content
// we also ensure to put our annotation on the storage classes
func (w *StorageClassWatch) refreshStorageClassCache(ctx context.Context) error {
	log := logger.GetLogger(ctx)

	policyIds := make([]string, 0)
	policyToSCMap := make(map[string]*storagev1.StorageClass)
	scClient := w.clientset.StorageV1().StorageClasses()
	scList, err := scClient.List(metav1.ListOptions{})
	if err != nil {
		log.Errorf("Failed to query Storage Classes. Err: %+v", err)
		return err
	}
	for idx, sc := range scList.Items {
		if sc.Provisioner != "csi.vsphere.vmware.com" {
			continue
		}
		// VMware CSI supports case insensitive parameters
		for key, value := range sc.Parameters {
			if strings.ToLower(key) == "storagepolicyid" {
				policyID := value
				policyIds = append(policyIds, policyID)
				policyToSCMap[policyID] = &scList.Items[idx]
			}
		}
	}

	w.policyToScMap = policyToSCMap
	w.policyIds = policyIds
	ReconcileStoragePools(ctx, w, w.vc, w.clusterID, true)
	w.addStorageClassPolicyAnnotations(ctx)

	return nil
}

// We want each StorageClass to have an annotation capturing the content of the
// SPBM storage policy. This function makes it so.
func (w *StorageClassWatch) addStorageClassPolicyAnnotation(ctx context.Context, profile cnsvsphere.SpbmPolicyContent) error {
	log := logger.GetLogger(ctx)

	sc := w.policyToScMap[profile.ID]
	profileBytes, err := json.Marshal(profile)
	if err != nil {
		log.Errorf("Failed to marshal policy: %s", err)
		return err
	}
	value, exists := sc.Annotations[policyContentAnnotationKey]
	if exists && value == string(profileBytes) {
		return nil
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				policyContentAnnotationKey: string(profileBytes),
			},
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Errorf("Failed to marshal patch: %s", err)
		return err
	}

	if !featureSwitchStorageClassContentAnnotation {
		return nil
	}

	scClient := w.clientset.StorageV1().StorageClasses()
	_, err = scClient.Patch(sc.Name, k8stypes.MergePatchType, patchBytes)
	if err != nil {
		log.Errorf("Failed to patch: %s", err)
		return err
	}

	return nil
}

// Perform a remediation, get policy content for all storage classes, and update
// their annotation if necessary
func (w *StorageClassWatch) addStorageClassPolicyAnnotations(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	profiles, err := w.vc.PbmRetrieveContent(ctx, w.policyIds)
	if err != nil {
		log.Errorf("Failed to retrieve policy content: %s", err)
		return err
	}
	for _, profile := range profiles {
		w.addStorageClassPolicyAnnotation(ctx, profile)
	}
	return nil
}

// Query SPBM to get the list of policies that the given list of datastores
// is compatible with. Returns a dict of format:
// datastoreMoId -> []string of policyIds
//
// If storage classes haven't recently changed, and if forceRefreshforceRefresh is false,
// will return cached data.
func (w *StorageClassWatch) getDatastoreToPolicyCompatibility(ctx context.Context, datastores []*cnsvsphere.DatastoreInfo, forceRefresh bool) (map[string][]string, error) {
	log := logger.GetLogger(ctx)
	if !forceRefresh {
		return w.dsPolicyCompatMapCache, nil
	}
	datastoreToPoliciesMap := make(map[string][]string)
	datastoreMorList := make([]vimtypes.ManagedObjectReference, 0)
	for _, ds := range datastores {
		datastoreMorList = append(datastoreMorList, ds.Datastore.Reference())
		datastoreToPoliciesMap[ds.Datastore.Reference().Value] = make([]string, 0)
	}

	for _, policyID := range w.policyIds {
		compat, err := w.vc.PbmCheckCompatibility(ctx, datastoreMorList, policyID)
		if err != nil {
			log.Errorf("Failed to check PBM compatibility: %s %s", datastoreMorList, policyID)
			return datastoreToPoliciesMap, err
		}

		for _, ds := range compat.CompatibleDatastores() {
			datastoreToPoliciesMap[ds.HubId] = append(datastoreToPoliciesMap[ds.HubId], policyID)
		}
	}

	w.dsPolicyCompatMapCache = datastoreToPoliciesMap

	return datastoreToPoliciesMap, nil
}
