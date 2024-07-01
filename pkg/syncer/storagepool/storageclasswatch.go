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
	"os"
	"reflect"
	"strings"

	"github.com/vmware/govmomi/vim25/soap"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// StorageClassWatch keeps state to watch storage classes and keep
// an in-memory cache of datastore / storage pool accessibility.
type StorageClassWatch struct {
	scWatch        watch.Interface
	clientset      *kubernetes.Clientset
	vc             *cnsvsphere.VirtualCenter
	policyIDs      []string
	policyToScMap  map[string]map[string]*storagev1.StorageClass
	isHostLocalMap map[string]bool
	clusterIDs     []string
	spController   *SpController

	dsPolicyCompatMapCache map[string][]string
}

// Watch storage classes that pertain to our CSI driver.
//
// The goal is two fold:
// a) Keep a in-memory cache of all storage classes so when we remediate
//
//	other things, like StoragePool, we have cheap access to the
//	classes.
//
// b) Put annotations on the StorageClass that contain information
//
//	about the underlying Storage Policy in VC.
//
// This function starts a go-routine which processes watch fires.
func startStorageClassWatch(ctx context.Context,
	spController *SpController, cfg *rest.Config) (*StorageClassWatch, error) {
	log := logger.GetLogger(ctx)
	w := &StorageClassWatch{}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Errorf("Failed to create K8s client using config. Err: %+v", err)
		return nil, err
	}
	w.clientset = clientset
	w.vc = spController.vc
	w.clusterIDs = spController.clusterIDs
	w.spController = spController
	w.isHostLocalMap = make(map[string]bool)

	err = renewStorageClassWatch(ctx, w)
	if err != nil {
		return nil, err
	}

	go w.watchStorageClass(ctx)
	return w, nil
}

// As our watch can and will expire, we need a helper to renew it.
// Note that after we re-new it, we will get a bunch of ADDED events, triggering
// a full remediation.
func renewStorageClassWatch(ctx context.Context, w *StorageClassWatch) error {
	var err error
	scClient := w.clientset.StorageV1().StorageClasses()
	// This means every 24h our watch may expire and require to be re-created.
	// When that happens, we need to do a full remediation, hence we change
	// from 30m (default) to 24h.
	timeout := int64(60 * 60 * 24) // 24h
	w.scWatch, err = scClient.Watch(
		ctx,
		metav1.ListOptions{
			TimeoutSeconds: &timeout,
		})
	return err
}

// Handler for storage class watch firing. Mostly handles shutdown and
// watch renewal, and on all storage class changes triggers a full
// remediation via refreshStorageClassCache().
// The watch gets triggered immediately if there is any storage class present,
// and hence the cache gets updated on startup and is ready to serve
// getDatastoreToPolicyCompatibility() from the cache.
func (w *StorageClassWatch) watchStorageClass(ctx context.Context) {
	log := logger.GetLogger(ctx)

	done := false
	for !done {
		select {
		case <-ctx.Done():
			log.Infof("watchStorageClass shutdown. ctxErr: %+v", ctx.Err())
			done = true
		case e, ok := <-w.scWatch.ResultChan():
			if !ok {
				log.Info("watchStorageClass watch not ok")

				err := renewStorageClassWatch(ctx, w)
				if err != nil {
					// XXX: Not sure how to handle this, as we need this watch.
					// So crash and let restart perform any remediation required.
					log.Error(err, "Fatal event, couldn't renew storage class watch")
					os.Exit(3)
					return
				}
				continue
			}
			// Run this task on a separate log context that has a separate TraceId
			// for every invocation.
			taskCtx := logger.NewContextWithLogger(ctx)
			if sc, ok := e.Object.(*storagev1.StorageClass); ok && w.needsRefreshStorageClassCache(taskCtx, sc, e.Type) {
				err := w.refreshStorageClassCache(taskCtx)
				if err != nil {
					log.Errorf("refreshStorageClassCache failed. err: %v", err)
				}
			}
		}
	}
	log.Info("watchStorageClass ends")
}

// isHostLocalProfile checks whether this profile has a subprofile which has
// the vSAN Host Local policy rule.
func isHostLocalProfile(profile cnsvsphere.SpbmPolicyContent) bool {
	for _, sub := range profile.Profiles {
		for _, role := range sub.Rules {
			if role.Ns == "VSAN" && role.CapID == "locality" && role.Value == "HostLocal" {
				return true
			}
		}
	}
	return false
}

// isHostLocal uses the cached info to know if a profileID refers to a policy
// with vSAN Host Local policy rule.
func (w *StorageClassWatch) isHostLocal(scName string) bool {
	return w.isHostLocalMap[scName]
}

// Returns true if the given StorageClass is not present in the cache yet, false
// otherwise.
func (w *StorageClassWatch) needsRefreshStorageClassCache(ctx context.Context, sc *storagev1.StorageClass,
	eventType watch.EventType) bool {
	log := logger.GetLogger(ctx)

	thisStoragePolicyID := getStoragePolicyIDFromSC(sc)
	if thisStoragePolicyID == "" {
		// Our cache only has StorageClasses created by vSphere.
		return false
	}
	// Lookup StorageClass from our cache.
	cachedSc, found := w.policyToScMap[thisStoragePolicyID][sc.Name]
	switch eventType {
	case watch.Added, watch.Modified:
		// Need to refresh our cache if this StorageClass is missing or anything
		// has changed in it.
		if !found || !reflect.DeepEqual(sc, cachedSc) {
			log.Infof("StorageClassWatch cache refresh due to %s", sc.Name)
			return true
		}
	case watch.Deleted:
		// Need to refresh our cache since this StorageClass is going away.
		if found {
			log.Infof("StorageClassWatch cache refresh due to delete of %s", sc.Name)
			return true
		}
	}
	return false
}

// Refresh the storage class cache and do a full remediation.
// The cache contains a convenient lookup table mapping from policyId to
// storage class content. We also ensure to put our annotation on the storage
// classes.
func (w *StorageClassWatch) refreshStorageClassCache(ctx context.Context) error {
	log := logger.GetLogger(ctx)

	scList, err := w.clientset.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("Failed to query Storage Classes. Err: %+v", err)
		return err
	}

	policyIDs := make([]string, 0)
	policyToSCMap := make(map[string]map[string]*storagev1.StorageClass)
	for idx, sc := range scList.Items {
		policyID := getStoragePolicyIDFromSC(&sc)
		if policyID == "" {
			continue
		}

		if _, ok := policyToSCMap[policyID]; !ok {
			policyToSCMap[policyID] = make(map[string]*storagev1.StorageClass)
			policyIDs = append(policyIDs, policyID)
		}
		policyToSCMap[policyID][scList.Items[idx].Name] = &scList.Items[idx]
	}
	w.policyToScMap = policyToSCMap
	w.policyIDs = policyIDs

	profiles, err := w.fetchPolicies(ctx)
	if err != nil {
		log.Errorf("fetchPolicies failed. err: %v", err)
		return err
	}

	isHostLocalMap := make(map[string]bool)
	for _, profile := range profiles {
		for _, sc := range w.policyToScMap[profile.ID] {
			isHostLocalMap[sc.Name] = isHostLocalProfile(profile)
			log.Infof("sc %s is hostLocal: %t", sc.Name, w.isHostLocalMap[sc.Name])
		}
	}
	w.isHostLocalMap = isHostLocalMap

	err = ReconcileAllStoragePools(ctx, w, w.spController)
	if err != nil {
		log.Errorf("ReconcileAllStoragePools failed. err: %v", err)
	}

	return nil
}

// fetchPolicies for known valid policyIDs from SPBM. The PbmRetrieveContent()
// is a batch API that returns the profile contents for given profileIds. If
// any of the input profile IDs are invalid, then it returns an empty list of
// profiles and an error that lists all the invalid profile IDs. In such a case,
// this function removes invalid profileIds from the input and makes a second
// call to PbmRetrieveContent() with just the valid ones.
func (w *StorageClassWatch) fetchPolicies(ctx context.Context) ([]cnsvsphere.SpbmPolicyContent, error) {
	log := logger.GetLogger(ctx)
	var profiles []cnsvsphere.SpbmPolicyContent
	var err error
	policyIds := w.policyIDs
	for i := 0; i < 2; i++ {
		profiles, err = w.vc.PbmRetrieveContent(ctx, policyIds)
		if err != nil {
			// Ignore non-existent stale profileIDs and continue with those that
			// were fetched.
			isInvalidProfileErr, invalidProfiles := isInvalidProfileErr(ctx, err)
			if !isInvalidProfileErr {
				log.Errorf("Failed to retrieve policy contents for invalid profiles: %v", invalidProfiles)
				return nil, err
			}
			if len(invalidProfiles) == 0 {
				log.Errorf("Did not get the invalid profile IDs in error from SPBM. err: %v", err)
				break
			}
			// Remove the invalid profiles from policyIds and fetch valid profiles
			// once again.
			log.Infof("Removing invalid profileIds %v from cache: %v", invalidProfiles, w.policyIDs)
			invalidProfileMap := make(map[string]bool, len(invalidProfiles))
			for _, p := range invalidProfiles {
				invalidProfileMap[p] = true
			}
			validProfileIds := make([]string, 0)
			for _, p := range w.policyIDs {
				if !invalidProfileMap[p] {
					validProfileIds = append(validProfileIds, p)
				}
			}
			policyIds = validProfileIds
		} else {
			log.Debugf("Successfully retrieved content of policies %v", policyIds)
			break
		}
	}
	return profiles, err
}

// isInvalidProfileErr returns whether the given error is an InvalidArgument
// for the profileId returned by SPBM. If yes, this function also returns the
// list of profileIds that are invalid.
func isInvalidProfileErr(ctx context.Context, err error) (bool, []string) {
	log := logger.GetLogger(ctx)
	if !soap.IsSoapFault(err) {
		return false, nil
	}
	soapFault := soap.ToSoapFault(err)
	vimFault, isInvalidArgumentErr := soapFault.VimFault().(vimtypes.InvalidArgument)
	if isInvalidArgumentErr && vimFault.InvalidProperty == "profileId" {
		// Parse the profile IDs from the error message.
		log.Errorf("Invalid profile error: %+v", soapFault.String)
		if strings.HasPrefix(soapFault.String, "Profile not found. Id:") {
			profiles := strings.TrimPrefix(soapFault.String, "Profile not found. Id:")
			split := strings.Split(profiles, ",")
			profilesSlice := make([]string, 0)
			for i := range split {
				s := strings.TrimSpace(split[i])
				if s != "" {
					profilesSlice = append(profilesSlice, s)
				}
			}
			log.Debugf("isInvalidProfileErr %v", profilesSlice)
			return true, profilesSlice
		}
		// If the error returned from SPBM does not have profileIDs, return nil.
		return true, nil
	}
	return false, nil
}

// Query SPBM to get the list of policies that the given list of datastores
// is compatible with. Returns a dict of format:
// datastoreMoId -> []string of policyIds
//
// If storage classes haven't recently changed, and if forceRefresh
// is false, will return cached data.
func (w *StorageClassWatch) getDatastoreToPolicyCompatibility(ctx context.Context,
	datastores []*cnsvsphere.DatastoreInfo, forceRefresh bool) (map[string][]string, error) {
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

	for _, policyID := range w.policyIDs {
		compat, err := w.vc.PbmCheckCompatibility(ctx, datastoreMorList, policyID)
		if err != nil {
			if isInvalidProfileErr, _ := isInvalidProfileErr(ctx, err); isInvalidProfileErr {
				// Stale policyIDs can be skipped safely.
				log.Infof("Skipping non-existent policy %s that failed in check PBM compatibility %v with error %v",
					policyID, datastoreMorList, err)
				continue
			}

			return datastoreToPoliciesMap, err
		}

		for _, ds := range compat.CompatibleDatastores() {
			datastoreToPoliciesMap[ds.HubId] = append(datastoreToPoliciesMap[ds.HubId], policyID)
		}
	}

	w.dsPolicyCompatMapCache = datastoreToPoliciesMap
	return datastoreToPoliciesMap, nil
}
