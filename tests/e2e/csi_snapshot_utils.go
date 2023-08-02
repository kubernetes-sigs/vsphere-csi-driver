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

package e2e

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"
)

// getVolumeSnapshotClassSpec returns a spec for the volume snapshot class
func getVolumeSnapshotClassSpec(deletionPolicy snapV1.DeletionPolicy,
	parameters map[string]string) *snapV1.VolumeSnapshotClass {
	var volumesnapshotclass = &snapV1.VolumeSnapshotClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshotClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "volumesnapshot-",
		},
		Driver:         e2evSphereCSIDriverName,
		DeletionPolicy: deletionPolicy,
	}

	volumesnapshotclass.Parameters = parameters
	return volumesnapshotclass
}

// getVolumeSnapshotSpec returns a spec for the volume snapshot
func getVolumeSnapshotSpec(namespace string, snapshotclassname string, pvcName string) *snapV1.VolumeSnapshot {
	var volumesnapshotSpec = &snapV1.VolumeSnapshot{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshot",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "snapshot-",
			Namespace:    namespace,
		},
		Spec: snapV1.VolumeSnapshotSpec{
			VolumeSnapshotClassName: &snapshotclassname,
			Source: snapV1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}
	return volumesnapshotSpec
}

// waitForVolumeSnapshotReadyToUse waits for the volume's snapshot to be in ReadyToUse
func waitForVolumeSnapshotReadyToUse(client snapclient.Clientset, ctx context.Context, namespace string,
	name string) (*snapV1.VolumeSnapshot, error) {
	var volumeSnapshot *snapV1.VolumeSnapshot
	var err error
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		volumeSnapshot, err = client.SnapshotV1().VolumeSnapshots(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("error fetching volumesnapshot details : %v", err)
		}
		if volumeSnapshot.Status != nil && *volumeSnapshot.Status.ReadyToUse {
			return true, nil
		}
		return false, nil
	})
	return volumeSnapshot, waitErr
}

// waitForVolumeSnapshotContentToBeDeleted wait till the volume snapshot content is deleted
func waitForVolumeSnapshotContentToBeDeleted(client snapclient.Clientset, ctx context.Context,
	name string) error {
	var err error
	waitErr := wait.PollImmediate(poll, 2*pollTimeout, func() (bool, error) {
		_, err = client.SnapshotV1().VolumeSnapshotContents().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			} else {
				return false, fmt.Errorf("error fetching volumesnapshotcontent details : %v", err)
			}
		}
		return false, nil
	})
	return waitErr
}

// deleteVolumeSnapshotWithPandoraWait deletes Volume Snapshot with Pandora wait for CNS to sync
func deleteVolumeSnapshotWithPandoraWait(ctx context.Context, snapc *snapclient.Clientset,
	namespace string, snapshotName string, pandoraSyncWaitTime int) {
	err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, snapshotName,
		metav1.DeleteOptions{})
	if !apierrors.IsNotFound(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)
}

// deleteVolumeSnapshotContentWithPandoraWait deletes Volume Snapshot Content with Pandora wait for CNS to sync
func deleteVolumeSnapshotContentWithPandoraWait(ctx context.Context, snapc *snapclient.Clientset,
	snapshotContentName string, pandoraSyncWaitTime int) {
	err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx, snapshotContentName, metav1.DeleteOptions{})
	if !apierrors.IsNotFound(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

	err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc, snapshotContentName, pandoraSyncWaitTime)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// waitForVolumeSnapshotContentToBeDeletedWithPandoraWait wait till the volume snapshot content is deleted
func waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx context.Context, snapc *snapclient.Clientset,
	name string, pandoraSyncWaitTime int) error {
	var err error
	waitErr := wait.PollImmediate(poll, 2*pollTimeout, func() (bool, error) {
		_, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
				time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)
				return true, nil
			} else {
				return false, fmt.Errorf("error fetching volumesnapshotcontent details : %v", err)
			}
		}
		return false, err
	})
	return waitErr
}

// waitForCNSSnapshotToBeDeleted wait till the give snapshot is deleted from CNS
func waitForCNSSnapshotToBeDeleted(volumeId string, snapshotId string) error {
	var err error
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		err = verifySnapshotIsDeletedInCNS(volumeId, snapshotId, false)
		if err != nil {
			if strings.Contains(err.Error(), "snapshot entry is still present") {
				return false, nil
			}
			return false, err
		}
		framework.Logf("Snapshot with ID: %v for volume with ID: %v is deleted from CNS now...", snapshotId, volumeId)
		return true, nil
	})
	return waitErr
}

// getVolumeSnapshotContentSpec returns a spec for the volume snapshot content
func getVolumeSnapshotContentSpec(deletionPolicy snapV1.DeletionPolicy, snapshotHandle string,
	futureSnapshotName string, namespace string) *snapV1.VolumeSnapshotContent {
	var volumesnapshotContentSpec = &snapV1.VolumeSnapshotContent{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshotContent",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "snapshotcontent-",
		},
		Spec: snapV1.VolumeSnapshotContentSpec{
			DeletionPolicy: deletionPolicy,
			Driver:         e2evSphereCSIDriverName,
			Source: snapV1.VolumeSnapshotContentSource{
				SnapshotHandle: &snapshotHandle,
			},
			VolumeSnapshotRef: v1.ObjectReference{
				Name:      futureSnapshotName,
				Namespace: namespace,
			},
		},
	}
	return volumesnapshotContentSpec
}

// getVolumeSnapshotSpecByName returns a spec for the volume snapshot by name
func getVolumeSnapshotSpecByName(namespace string, snapshotName string,
	snapshotcontentname string) *snapV1.VolumeSnapshot {
	var volumesnapshotSpec = &snapV1.VolumeSnapshot{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshot",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: namespace,
		},
		Spec: snapV1.VolumeSnapshotSpec{
			Source: snapV1.VolumeSnapshotSource{
				VolumeSnapshotContentName: &snapshotcontentname,
			},
		},
	}
	return volumesnapshotSpec
}

// getVolumeSnapshotSpecWithoutSC returns a spec for the volume snapshot
func getVolumeSnapshotSpecWithoutSC(namespace string, pvcName string) *snapV1.VolumeSnapshot {
	var volumesnapshotSpec = &snapV1.VolumeSnapshot{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshot",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "snapshot-",
			Namespace:    namespace,
		},
		Spec: snapV1.VolumeSnapshotSpec{
			Source: snapV1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}
	return volumesnapshotSpec
}

// createSnapshotInParallel creates snapshot for a given pvc
// in a given namespace
func createSnapshotInParallel(ctx context.Context, namespace string,
	snapc *snapclient.Clientset, pvcName string, volumeSnapClassName string,
	ch chan *snapV1.VolumeSnapshot, lock *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	framework.Logf("Waiting for a few seconds for IO to happen to pod")
	time.Sleep(time.Duration(10) * time.Second)
	volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
		getVolumeSnapshotSpec(namespace, volumeSnapClassName, pvcName), metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)
	lock.Lock()
	ch <- volumeSnapshot
	lock.Unlock()
}

// getSnapshotHandleFromSupervisorCluster fetches the SnapshotHandle from Supervisor Cluster
func getSnapshotHandleFromSupervisorCluster(ctx context.Context,
	volumeSnapshotClass *snapV1.VolumeSnapshotClass, snapshothandle string) (string, string, string, error) {
	var snapc *snapclient.Clientset
	var err error
	if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		restConfig, err := clientcmd.BuildConfigFromFlags("", k8senv)
		if err != nil {
			return "", "", "", err
		}
		snapc, err = snapclient.NewForConfig(restConfig)
		if err != nil {
			return "", "", "", err
		}
	}

	svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)

	volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(svNamespace).Get(ctx, snapshothandle,
		metav1.GetOptions{})
	if err != nil {
		return "", "", "", err
	}

	snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
		*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
	if err != nil {
		return "", "", "", err
	}

	svcSnapshotHandle := *snapshotContent.Status.SnapshotHandle
	snapshotID := strings.Split(svcSnapshotHandle, "+")[1]

	svcVolumeSnapshotName := volumeSnapshot.Name

	return snapshotID, svcSnapshotHandle, svcVolumeSnapshotName, nil
}

// getRestConfigClient returns  rest config client for Guest Cluster
func getRestConfigClientForGuestCluster(guestClusterRestConfig *rest.Config) *rest.Config {
	var err error
	if guestClusterRestConfig == nil {
		if k8senv := GetAndExpectStringEnvVar("KUBECONFIG"); k8senv != "" {
			guestClusterRestConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	}
	return guestClusterRestConfig
}

// deleteVolumeSnapshot deletes volume snapshot from K8s side and CNS side
func deleteVolumeSnapshot(ctx context.Context, snapc *snapclient.Clientset, namespace string,
	volumeSnapshot *snapV1.VolumeSnapshot, pandoraSyncWaitTime int,
	volHandle string, snapshotID string) (bool, bool, error) {
	var err error

	framework.Logf("Delete volume snapshot and verify the snapshot content is deleted")
	deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
	snapshotCreated := false

	framework.Logf("Wait until the volume snapshot is deleted")
	err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
	if err != nil {
		return snapshotCreated, false, err
	}
	snapshotContentCreated := false

	framework.Logf("Verify snapshot entry %v is deleted from CNS for volume %v", snapshotID, volHandle)
	err = waitForCNSSnapshotToBeDeleted(volHandle, snapshotID)
	if err != nil {
		return snapshotCreated, snapshotContentCreated, err
	}

	framework.Logf("Verify snapshot entry is deleted from CNS")
	err = verifySnapshotIsDeletedInCNS(volHandle, snapshotID, false)
	if err != nil {
		return snapshotCreated, snapshotContentCreated, err
	}

	framework.Logf("Deleting volume snapshot again to check 'Not found' error")
	deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

	return snapshotCreated, snapshotContentCreated, nil
}

// getVolumeSnapshotIdFromSnapshotHandle fetches VolumeSnapshotId From SnapshotHandle
func getVolumeSnapshotIdFromSnapshotHandle(ctx context.Context, snapshotContent *snapV1.VolumeSnapshotContent,
	volumeSnapshotClass *snapV1.VolumeSnapshotClass, volHandle string) (string, error) {
	var snapshotID string
	var err error
	var pandoraSyncWaitTime int
	if vanillaCluster {
		snapshotHandle := *snapshotContent.Status.SnapshotHandle
		snapshotID = strings.Split(snapshotHandle, "+")[1]
	} else if guestCluster {
		snapshotHandle := *snapshotContent.Status.SnapshotHandle
		snapshotID, _, _, err = getSnapshotHandleFromSupervisorCluster(ctx, volumeSnapshotClass, snapshotHandle)
		if err != nil {
			return "", err
		}
	}

	if os.Getenv(envPandoraSyncWaitTime) != "" {
		pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		pandoraSyncWaitTime = defaultPandoraSyncWaitTime
	}

	ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

	ginkgo.By("Query CNS and check the volume snapshot entry")
	err = verifySnapshotIsCreatedInCNS(volHandle, snapshotID, false)
	if err != nil {
		return "", err
	}
	return snapshotID, nil
}

// createVolumeSnapshotClass creates VSC for a Vanilla cluster and fetches VSC for a Guest Cluster
func createVolumeSnapshotClass(ctx context.Context, snapc *snapclient.Clientset,
	deletionPolicy string) (*snapV1.VolumeSnapshotClass, error) {
	var volumeSnapshotClass *snapV1.VolumeSnapshotClass
	var err error
	if vanillaCluster {
		volumeSnapshotClass, err = snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy(deletionPolicy), nil), metav1.CreateOptions{})
		if err != nil {
			return nil, err
		}
	} else if guestCluster || supervisorCluster {
		volumeSnapshotClassName := GetAndExpectStringEnvVar(envVolSnapClassDel)
		volumeSnapshotClass, err = snapc.SnapshotV1().VolumeSnapshotClasses().Get(ctx,
			volumeSnapshotClassName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
	}
	return volumeSnapshotClass, nil
}

// createDynamicVolumeSnapshot util creates dynamic volume snapshot for a volume
func createDynamicVolumeSnapshot(ctx context.Context, namespace string,
	snapc *snapclient.Clientset, volumeSnapshotClass *snapV1.VolumeSnapshotClass,
	pvclaim *v1.PersistentVolumeClaim, diskSize string) (*snapV1.VolumeSnapshot, *snapV1.VolumeSnapshotContent,
	bool, bool, error) {

	volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
		getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
	if err != nil {
		return nil, nil, false, false, err
	}
	framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)

	ginkgo.By("Verify volume snapshot is created")
	volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
	if err != nil {
		return nil, nil, false, false, err
	}
	snapshotCreated := true
	if volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize)) != 0 {
		return nil, nil, false, false, fmt.Errorf("unexpected restore size")
	}

	ginkgo.By("Verify volume snapshot content is created")
	snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
		*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, false, false, err
	}
	snapshotContentCreated := true
	if !*snapshotContent.Status.ReadyToUse {
		return nil, nil, false, false, fmt.Errorf("volume snapshot content is not ready to use")
	}

	return volumeSnapshot, snapshotContent, snapshotCreated, snapshotContentCreated, nil
}

// getPersistentVolumeClaimSpecWithDatasource return the PersistentVolumeClaim
// spec with specified storage class.
func getPersistentVolumeClaimSpecWithDatasource(namespace string, ds string, storageclass *storagev1.StorageClass,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode,
	datasourceName string, snapshotapigroup string) *v1.PersistentVolumeClaim {
	disksize := diskSize
	if ds != "" {
		disksize = ds
	}
	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteOnce
	}
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(disksize),
				},
			},
			StorageClassName: &(storageclass.Name),
			DataSource: &v1.TypedLocalObjectReference{
				APIGroup: &snapshotapigroup,
				Kind:     "VolumeSnapshot",
				Name:     datasourceName,
			},
		},
	}

	if pvclaimlabels != nil {
		claim.Labels = pvclaimlabels
	}

	return claim
}

/*
changeDeletionPolicyOfVolumeSnapshotContentOnGuest changes the deletion policy
of volume snapshot content from delete to retain in Guest Cluster
*/
func changeDeletionPolicyOfVolumeSnapshotContent(ctx context.Context,
	snapshotContent *snapV1.VolumeSnapshotContent, snapc *snapclient.Clientset,
	namespace string, policyName snapV1.DeletionPolicy) (*snapV1.VolumeSnapshotContent, error) {

	// Retrieve the latest version of the object
	latestSnapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx, snapshotContent.Name,
		metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// Apply changes to the latest version
	latestSnapshotContent.Spec.DeletionPolicy = policyName

	// Update the object
	updatedSnapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Update(ctx,
		latestSnapshotContent, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}

	return updatedSnapshotContent, nil
}

/* deleteVolumeSnapshotContent deletes volume snapshot content explicitly  on Guest cluster */
func deleteVolumeSnapshotContent(ctx context.Context, updatedSnapshotContent *snapV1.VolumeSnapshotContent,
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

/* createPreProvisionedSnapshotInGuestCluster created pre-provisioned snaphot  on Guest cluster */
func createPreProvisionedSnapshotInGuestCluster(ctx context.Context, volumeSnapshot *snapV1.VolumeSnapshot,
	updatedSnapshotContent *snapV1.VolumeSnapshotContent,
	snapc *snapclient.Clientset, namespace string, pandoraSyncWaitTime int,
	svcVolumeSnapshotName string, diskSize string) (*snapV1.VolumeSnapshotContent,
	*snapV1.VolumeSnapshot, bool, bool, error) {

	framework.Logf("Change the deletion policy of VolumeSnapshotContent from Delete to Retain " +
		"in Guest Cluster")
	updatedSnapshotContent, err := changeDeletionPolicyOfVolumeSnapshotContent(ctx, updatedSnapshotContent,
		snapc, namespace, snapV1.VolumeSnapshotContentRetain)
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("failed to change deletion policy of VolumeSnapshotContent: %v", err)
	}

	framework.Logf("Delete dynamic volume snapshot from Guest Cluster")
	deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

	framework.Logf("Delete VolumeSnapshotContent from Guest Cluster explicitly")
	err = deleteVolumeSnapshotContent(ctx, updatedSnapshotContent, snapc, namespace, pandoraSyncWaitTime)
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("failed to delete VolumeSnapshotContent: %v", err)
	}

	framework.Logf(fmt.Sprintf("Creating static VolumeSnapshotContent in Guest Cluster using "+
		"supervisor VolumeSnapshotName %s", svcVolumeSnapshotName))
	staticSnapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
		getVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Delete"), svcVolumeSnapshotName,
			"static-vs", namespace), metav1.CreateOptions{})
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("failed to create static VolumeSnapshotContent: %v", err)
	}

	framework.Logf("Verify VolumeSnapshotContent is created or not in Guest Cluster")
	staticSnapshotContent, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
		staticSnapshotContent.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("failed to get static VolumeSnapshotContent: %v", err)
	}
	framework.Logf("Snapshotcontent name is  %s", staticSnapshotContent.ObjectMeta.Name)

	if !*staticSnapshotContent.Status.ReadyToUse {
		return nil, nil, false, false, fmt.Errorf("VolumeSnapshotContent is not ready to use")
	}
	staticSnapshotContentCreated := true

	ginkgo.By("Create a static volume snapshot by static snapshotcontent")
	staticVolumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
		getVolumeSnapshotSpecByName(namespace, "static-vs",
			staticSnapshotContent.ObjectMeta.Name), metav1.CreateOptions{})
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("failed to create static volume snapshot: %v", err)
	}
	framework.Logf("Volume snapshot name is : %s", staticVolumeSnapshot.Name)

	ginkgo.By("Verify static volume snapshot is created")
	staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, staticVolumeSnapshot.Name)
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("volumeSnapshot is still not ready to use: %v", err)
	}
	if staticSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize)) != 0 {
		return nil, nil, false, false, fmt.Errorf("expected RestoreSize does not match")
	}
	framework.Logf("Snapshot details is %+v", staticSnapshot)
	staticSnapshotCreated := true

	return staticSnapshotContent, staticSnapshot, staticSnapshotContentCreated, staticSnapshotCreated, nil
}
