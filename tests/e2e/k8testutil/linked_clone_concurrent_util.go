/*
Copyright 2025 The Kubernetes Authors.
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

package k8testutil

import (
	"context"
	"fmt"
	"sync"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/onsi/ginkgo/v2"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	v1 "k8s.io/api/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
)

func CreateMultiplePvcPod(ctx context.Context, e2eTestConfig *config.E2eTestConfig, client clientset.Interface, namespace string, storageclass *v1.StorageClass, doCreatePod bool, doCreateDep bool, numOfPVc int) map[*corev1.PersistentVolumeClaim][]*corev1.PersistentVolume {

	volumeMap := map[*corev1.PersistentVolumeClaim][]*corev1.PersistentVolume{}

	// Create PVC and verify PVC is bound
	for i := 0; i < numOfPVc; i++ {
		ginkgo.By(fmt.Sprintf("Creating PVC in iteration: %v",
			i))
		pvclaim, pv := createAndValidatePvc(ctx, client, namespace, storageclass)
		volumeMap[pvclaim] = pv

		// Create Pod and attach to PVC
		if doCreatePod || doCreateDep {
			CreatePodForPvc(ctx, e2eTestConfig, client, namespace, pvclaim, doCreatePod, doCreateDep)
		}
	}

	return volumeMap
}

func CreateSnapshotInParallel(ctx context.Context, e2eTestConfig *config.E2eTestConfig, namespace string, volumeMap map[*corev1.PersistentVolumeClaim][]*corev1.PersistentVolume) chan *snapV1.VolumeSnapshot {

	var wg sync.WaitGroup
	snapshots := make(chan *snapV1.VolumeSnapshot, len(volumeMap))

	for pvclaim, pvList := range volumeMap {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snapshot := CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, pvList)
			snapshots <- snapshot
		}()
	}

	wg.Wait()
	close(snapshots)
	fmt.Println("All threads completed")
	return snapshots
}

// Expected to create few linked clones before calling this method
func CreateDeleteLinkedClonesInParallel(ctx context.Context, client clientset.Interface, namespace string, storageclass *storagev1.StorageClass, snapshot *snapV1.VolumeSnapshot, pvcList []*corev1.PersistentVolumeClaim, iteration int) {
	var wg sync.WaitGroup
	for i := 0; i < iteration; i++ {
		wg.Add(2)
		go createLinkedClonePvc(ctx, client, namespace, storageclass, snapshot.Name)
		go fpv.DeletePersistentVolumeClaim(ctx, client, pvcList[i].Name, namespace)
	}

	wg.Wait()
}
