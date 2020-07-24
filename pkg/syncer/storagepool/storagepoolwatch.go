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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

// SPWatcher watches for events on StoragePool CRD and for each event determines
// if the events is caused due to external changes to StoragePool or not.
// If yes then it triggers a reconciliation of StoragePool. It compares generation
// number of a resource in the event with the generation number stored in
// SPNameToGenerationNumberMap to determine external change.
type SPWatcher struct {
	spWatch                     watch.Interface
	// A successful update operation to a resource instance increases its generation number by 1.
	// So all create, delete and update operation to StoragePool should also revise gen num in this map.
	SPNameToGenerationNumberMap map[string]int64
}

// newStoragePoolWatch initialises a SPWatcher instance.
func newStoragePoolWatch() *SPWatcher {
	w := &SPWatcher{}
	w.SPNameToGenerationNumberMap = make(map[string]int64)
	return w
}

// StartStoragePoolWatch starts a go-routine which processes watch events
func (w *SPWatcher) StartStoragePoolWatch(ctx context.Context, scWatchCntlr *StorageClassWatch, spCtl *SpController) error {
	err := w.renewStoragePoolWatch(ctx)
	if err != nil {
		return err
	}
	go w.watchStoragePool(ctx, scWatchCntlr, spCtl)
	return nil
}

// As our watch can and will expire, we need a helper to renew it
// Note that after we re-new it, we will get a bunch of ADDED events, which may
// trigger a full remediation
func (w *SPWatcher) renewStoragePoolWatch(ctx context.Context) error {
	var err error
	spClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return err
	}
	// This means every 24h our watch may expire and require to be re-created.
	// When that happens, we may need to do a full remediation, hence we change
	// from 30m (default) to 24h.
	timeout := int64(60 * 60 * 24) // 24h
	w.spWatch, err = spClient.Resource(*spResource).Watch(ctx, metav1.ListOptions{
		TimeoutSeconds: &timeout,
	})
	return err
}

// Handler for storage class watch firing. Handles shutdown and
// watch renewal, and if it detects any external changes made to StoragePool
// CRD instance it triggers a full remediation via ReconcileAllStoragePools.
// external changes are those write operation which are not performed by SPController
// but by some other entity for example k8s admin user.
func (w *SPWatcher) watchStoragePool(ctx context.Context, scWatchCntlr *StorageClassWatch, spCtl *SpController) {
	log := logger.GetLogger(ctx)

	done := false
	for !done {
		select {
		case <-ctx.Done():
			log.Info("watchStoragePool shutdown", "ctxErr", ctx.Err())
			done = true
		case e, ok := <-w.spWatch.ResultChan():
			if !ok {
				log.Info("StoragePool watch not ok")
				err := w.renewStoragePoolWatch(ctx)
				if err != nil {
					// XXX: Not sure how to handle this, as we need this watch.
					// So crash and let restart perform any remediation required
					log.Error(err, "Fatal event, couldn't renew StoragePool watch")
					os.Exit(4)
					return
				}
				continue
			}
			// run this task on a separate log context that has a separate TraceId for every invocation
			taskCtx := logger.NewContextWithLogger(ctx)
			if w.isExternalChange(ctx, e) {
				log.Info("External change to StoragePool instance detected. Reconciling all StoragePools.")
				err := ReconcileAllStoragePools(taskCtx, scWatchCntlr, spCtl)
				if err != nil {
					log.Errorf("Failed to correct external changes to StoragePool instance. err: %v", err)
				}
			}
		}
	}
	log.Info("watchStoragePool ends")
}

func (w *SPWatcher) isExternalChange(ctx context.Context, e watch.Event) bool {
	log := logger.GetLogger(ctx)
	sp, ok := e.Object.(*unstructured.Unstructured)
	if !ok {
		log.Debugf("Object in StoragePool watch event is not of type *unstructured.Unstructured, but of type %T", e.Object)
		return false
	}
	driver, found, err := unstructured.NestedString(sp.Object, "spec", "driver")
	if !found || err != nil || driver != csitypes.Name {
		log.Debugf("StoragePool watch event does not correspond to %v driver.", csitypes.Name)
		return false
	}
	spName := sp.GetName()
	objGenerationNum := sp.GetGeneration()
	savedGenerationNum, exists := w.SPNameToGenerationNumberMap[spName]
	if e.Type == watch.Deleted {
		if !exists || savedGenerationNum != -1 {
			log.Debugf("Expected -1 as generation number for %s but have %v", spName, savedGenerationNum)
			return true
		}
		// delete spName key if it exists to prevent any memory buildup
		delete(w.SPNameToGenerationNumberMap, spName)
		return false

	} else if !exists || objGenerationNum > savedGenerationNum {
		log.Debugf("Got %v as generation number for %s but expected <= %v", objGenerationNum, spName, savedGenerationNum)
		return true
	}
	return false
}
