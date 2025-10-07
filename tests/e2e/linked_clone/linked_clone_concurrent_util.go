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

package linked_clone

import (
	"context"
	"fmt"
	"sync"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

/*
This method will create and delete the linked clone as per given iteration in parallel
Expected to create few linked clones before calling this method
It returns PersistentVolumeClaim and PersistentVolume chan
*/
func CreateDeleteLinkedClonesInParallel(
	ctx context.Context,
	client clientset.Interface,
	namespace string,
	storageclass *v1.StorageClass,
	snapshot *snapV1.VolumeSnapshot,
	pvcList []*corev1.PersistentVolumeClaim,
	iteration int,
) (
	chan *corev1.PersistentVolumeClaim,
	chan []*corev1.PersistentVolume,
) {
	var wg sync.WaitGroup

	lcPvcCreated := make(chan *corev1.PersistentVolumeClaim, iteration)
	lcPvCreated := make(chan []*corev1.PersistentVolume, iteration)

	for i := 0; i < iteration; i++ {
		fmt.Printf("Iteration %d\n", i)

		wg.Add(2) // We're launching 2 goroutines per iteration

		go func(id int) {
			defer wg.Done()
			linkdeClonePvc, lcPv := CreateAndValidateLinkedClone(ctx, client, namespace, storageclass, snapshot.Name)
			lcPvcCreated <- linkdeClonePvc
			lcPvCreated <- lcPv
		}(i)

		go func(id int) {
			defer wg.Done()
			_ = fpv.DeletePersistentVolumeClaim(ctx, client, pvcList[i].Name, namespace)
		}(i)
	}

	wg.Wait()
	close(lcPvcCreated)
	close(lcPvCreated)

	return lcPvcCreated, lcPvCreated
}
