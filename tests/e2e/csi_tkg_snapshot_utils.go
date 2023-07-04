/*
Copyright 2022 The Kubernetes Authors.

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

package e2e

import (
	"context"
	"fmt"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
	ginkgo "github.com/onsi/ginkgo/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

func changeDeletionPolicyOfVolumeSnapshotContentOnGuest(ctx context.Context,
	snapshotContent *snapV1.VolumeSnapshotContent, snapc *snapclient.Clientset,
	namespace string) (*snapV1.VolumeSnapshotContent, error) {

	// Retrieve the latest version of the object
	latestSnapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx, snapshotContent.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// Apply changes to the latest version
	latestSnapshotContent.Spec.DeletionPolicy = snapV1.VolumeSnapshotContentRetain

	// Update the object
	updatedSnapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Update(ctx, latestSnapshotContent, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}

	return updatedSnapshotContent, nil
}

func deleteVolumeSnapshotContentOnGuest(ctx context.Context, updatedSnapshotContent *snapV1.VolumeSnapshotContent,
	snapc *snapclient.Clientset, namespace string, pandoraSyncWaitTime int) error {

	framework.Logf("Delete volume snapshot content")
	deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc, updatedSnapshotContent.Name, pandoraSyncWaitTime)

	framework.Logf("Wait till the volume snapshot content is deleted")
	err := waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, updatedSnapshotContent.Name)
	if err != nil {
		return err
	}
	return nil
}

func createPreProvisionedSnapshotForGuestCluster(ctx context.Context, volumeSnapshot *snapV1.VolumeSnapshot,
	updatedSnapshotContent *snapV1.VolumeSnapshotContent,
	snapc *snapclient.Clientset, namespace string, pandoraSyncWaitTime int,
	svcVolumeSnapshotName string) (*snapV1.VolumeSnapshotContent, *snapV1.VolumeSnapshot, error) {

	framework.Logf("Change the deletion policy of VolumeSnapshotContent from Delete to Retain " +
		"in Guest Cluster")
	updatedSnapshotContent, err := changeDeletionPolicyOfVolumeSnapshotContentOnGuest(ctx, updatedSnapshotContent,
		snapc, namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to change deletion policy of VolumeSnapshotContent: %v", err)
	}

	framework.Logf("Delete dynamic volume snapshot from Guest Cluster")
	deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

	framework.Logf("Delete VolumeSnapshotContent from Guest Cluster explicitly")
	err = deleteVolumeSnapshotContentOnGuest(ctx, updatedSnapshotContent, snapc, namespace, pandoraSyncWaitTime)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to delete VolumeSnapshotContent: %v", err)
	}

	framework.Logf(fmt.Sprintf("Creating static VolumeSnapshotContent in Guest Cluster using "+
		"supervisor VolumeSnapshotName %s", svcVolumeSnapshotName))
	staticSnapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
		getVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Delete"), svcVolumeSnapshotName,
			"static-vs", namespace), metav1.CreateOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create static VolumeSnapshotContent: %v", err)
	}

	framework.Logf("Verify VolumeSnapshotContent is created or not in Guest Cluster")
	staticSnapshotContent, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
		staticSnapshotContent.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get static VolumeSnapshotContent: %v", err)
	}
	framework.Logf("Snapshotcontent name is  %s", staticSnapshotContent.ObjectMeta.Name)

	if !*staticSnapshotContent.Status.ReadyToUse {
		return nil, nil, fmt.Errorf("VolumeSnapshotContent is not ready to use")
	}

	ginkgo.By("Create a static volume snapshot by static snapshotcontent")
	staticVolumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
		getVolumeSnapshotSpecByName(namespace, "static-vs",
			staticSnapshotContent.ObjectMeta.Name), metav1.CreateOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create static volume snapshot: %v", err)
	}
	framework.Logf("Volume snapshot name is : %s", staticVolumeSnapshot.Name)

	ginkgo.By("Verify static volume snapshot is created")
	staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, staticVolumeSnapshot.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to wait for volume snapshot: %v", err)
	}
	if staticSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize)) != 0 {
		return nil, nil, fmt.Errorf("expected RestoreSize does not match")
	}
	framework.Logf("Snapshot details is %+v", staticSnapshot)

	return staticSnapshotContent, staticSnapshot, nil
}
