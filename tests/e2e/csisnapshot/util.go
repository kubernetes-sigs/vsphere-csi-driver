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

package csisnapshot

import (
	"context"
	"fmt"
	"strings"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

// waitForVolumeSnapshotContentReadyToUse waits for the volume's snapshot content to be in ReadyToUse
func WaitForVolumeSnapshotContentReadyToUse(client snapclient.Clientset, ctx context.Context,
	name string) (*snapV1.VolumeSnapshotContent, error) {
	var volumeSnapshotContent *snapV1.VolumeSnapshotContent
	var err error

	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeout*2, true,
		func(ctx context.Context) (bool, error) {
			volumeSnapshotContent, err = client.SnapshotV1().VolumeSnapshotContents().Get(ctx, name, metav1.GetOptions{})
			framework.Logf("volumesnapshotcontent details: %v", volumeSnapshotContent)
			if err != nil {
				return false, fmt.Errorf("error fetching volumesnapshotcontent details : %v", err)
			}
			if volumeSnapshotContent.Status != nil && *volumeSnapshotContent.Status.ReadyToUse {
				framework.Logf("%s volume snapshotContent is in ready state", name)
				return true, nil
			}
			return false, nil
		})
	return volumeSnapshotContent, waitErr
}

// waitForCNSSnapshotToBeCreated wait till the give snapshot is created in CNS
func WaitForCNSSnapshotToBeCreated(vs *config.E2eTestConfig, volumeId string, snapshotId string) error {
	var err error
	waitErr := wait.PollUntilContextTimeout(context.Background(), constants.Poll, constants.PollTimeout*2, true,
		func(ctx context.Context) (bool, error) {
			err = vcutil.VerifySnapshotIsCreatedInCNS(vs, volumeId, snapshotId)
			if err != nil {
				if strings.Contains(err.Error(), "snapshot entry is not present in CNS") {
					return false, nil
				}
				return false, err
			}
			framework.Logf("Snapshot with ID: %v for volume with ID: %v is created in CNS now...", snapshotId, volumeId)
			return true, nil
		})
	return waitErr
}

// getVolumeSnapshotSpec returns a spec for the volume snapshot
func GetVolumeSnapshotSpec(namespace string, snapshotclassname string, pvcName string) *snapV1.VolumeSnapshot {
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
func WaitForVolumeSnapshotReadyToUse(client snapclient.Clientset, ctx context.Context, namespace string,
	name string) (*snapV1.VolumeSnapshot, error) {
	var volumeSnapshot *snapV1.VolumeSnapshot
	var err error
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeout*2, true,
		func(ctx context.Context) (bool, error) {
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

// getVolumeSnapshotIdFromSnapshotHandle fetches VolumeSnapshotId From SnapshotHandle
func GetVolumeSnapshotIdFromSnapshotHandle(ctx context.Context, vs *config.E2eTestConfig,
	snapshotContent *snapV1.VolumeSnapshotContent) (string, string, error) {
	var snapshotID string
	var snapshotHandle string
	var err error

	if vs.TestInput.ClusterFlavor.VanillaCluster || vs.TestInput.ClusterFlavor.SupervisorCluster {
		snapshotHandle = *snapshotContent.Status.SnapshotHandle
		snapshotID = strings.Split(snapshotHandle, "+")[1]
	} else if vs.TestInput.ClusterFlavor.GuestCluster {
		snapshotHandle = *snapshotContent.Status.SnapshotHandle
		snapshotID, _, _, err = GetSnapshotHandleFromSupervisorCluster(ctx, snapshotHandle)
		if err != nil {
			return snapshotID, snapshotHandle, err
		}
	}
	return snapshotID, snapshotHandle, nil
}

// getSnapshotHandleFromSupervisorCluster fetches the SnapshotHandle from Supervisor Cluster
func GetSnapshotHandleFromSupervisorCluster(ctx context.Context,
	snapshothandle string) (string, string, string, error) {
	var snapc *snapclient.Clientset
	var err error
	if k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		restConfig, err := clientcmd.BuildConfigFromFlags("", k8senv)
		if err != nil {
			return "", "", "", err
		}
		snapc, err = snapclient.NewForConfig(restConfig)
		if err != nil {
			return "", "", "", err
		}
	}

	svNamespace := env.GetAndExpectStringEnvVar(constants.EnvSupervisorClusterNamespace)

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

// createDynamicVolumeSnapshot util creates dynamic volume snapshot for a volume
func CreateDynamicVolumeSnapshot(ctx context.Context, vs *config.E2eTestConfig, namespace string,
	snapc *snapclient.Clientset, volumeSnapshotClass *snapV1.VolumeSnapshotClass,
	pvclaim *v1.PersistentVolumeClaim, volHandle string, diskSize string,
	performCnsQueryVolumeSnapshot bool) (*snapV1.VolumeSnapshot,
	*snapV1.VolumeSnapshotContent, bool, bool, string, string, error) {

	volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
		GetVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
	if err != nil {
		return volumeSnapshot, nil, false, false, "", "", err
	}
	framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)

	ginkgo.By("Verify volume snapshot is created")
	volumeSnapshot, err = WaitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
	if err != nil {
		return volumeSnapshot, nil, false, false, "", "", err
	}

	snapshotCreated := true
	if volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize)) != 0 {
		return volumeSnapshot, nil, false, false, "", "", fmt.Errorf("unexpected restore size")
	}

	ginkgo.By("Verify volume snapshot content is created")
	snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
		*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
	if err != nil {
		return volumeSnapshot, snapshotContent, false, false, "", "", err
	}
	snapshotContentCreated := true
	snapshotContent, err = WaitForVolumeSnapshotContentReadyToUse(*snapc, ctx, snapshotContent.Name)
	if err != nil {
		return volumeSnapshot, snapshotContent, false, false, "", "",
			fmt.Errorf("volume snapshot content is not ready to use")
	}

	framework.Logf("Get volume snapshot ID from snapshot handle")
	snapshotId, snapshotHandle, err := GetVolumeSnapshotIdFromSnapshotHandle(ctx, vs, snapshotContent)
	if err != nil {
		return volumeSnapshot, snapshotContent, false, false, snapshotId, "", err
	}

	if performCnsQueryVolumeSnapshot {
		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = WaitForCNSSnapshotToBeCreated(vs, volHandle, snapshotId)
		if err != nil {
			framework.Logf("no error")
			return volumeSnapshot, snapshotContent, false, false, snapshotId, "", err
		}
	}

	return volumeSnapshot, snapshotContent, snapshotCreated, snapshotContentCreated, snapshotId, snapshotHandle, nil
}

// waitForVolumeSnapshotContentToBeDeletedWithPandoraWait wait till the volume snapshot content is deleted
func WaitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx context.Context, snapc *snapclient.Clientset,
	name string, pandoraSyncWaitTime int) error {
	var err error
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, 2*constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
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

// deleteVolumeSnapshotContentWithPandoraWait deletes Volume Snapshot Content with Pandora wait for CNS to sync
func DeleteVolumeSnapshotContentWithPandoraWait(ctx context.Context, snapc *snapclient.Clientset,
	snapshotContentName string, pandoraSyncWaitTime int) {
	err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx, snapshotContentName, metav1.DeleteOptions{})
	if !apierrors.IsNotFound(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

	err = WaitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc, snapshotContentName, pandoraSyncWaitTime)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

/* deleteVolumeSnapshotContent deletes volume snapshot content explicitly  on Guest cluster */
func DeleteVolumeSnapshotContent(ctx context.Context, updatedSnapshotContent *snapV1.VolumeSnapshotContent,
	snapc *snapclient.Clientset, pandoraSyncWaitTime int) error {

	framework.Logf("Delete volume snapshot content")
	DeleteVolumeSnapshotContentWithPandoraWait(ctx, snapc, updatedSnapshotContent.Name, pandoraSyncWaitTime)

	framework.Logf("Wait till the volume snapshot content is deleted")
	err := WaitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, updatedSnapshotContent.Name)
	if err != nil {
		return err
	}
	return nil
}

// getVolumeSnapshotClassSpec returns a spec for the volume snapshot class
func GetVolumeSnapshotClassSpec(deletionPolicy snapV1.DeletionPolicy,
	parameters map[string]string) *snapV1.VolumeSnapshotClass {
	var volumesnapshotclass = &snapV1.VolumeSnapshotClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshotClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "volumesnapshot-",
		},
		Driver:         constants.E2evSphereCSIDriverName,
		DeletionPolicy: deletionPolicy,
	}

	volumesnapshotclass.Parameters = parameters
	return volumesnapshotclass
}

// getVolumeSnapshotClass retrieves the existing VolumeSnapshotClass (VSC)
func GetVolumeSnapshotClass(ctx context.Context, snapc *snapclient.Clientset,
	name string) (*snapV1.VolumeSnapshotClass, error) {

	var volumeSnapshotClass *snapV1.VolumeSnapshotClass
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			var err error
			volumeSnapshotClass, err = snapc.SnapshotV1().VolumeSnapshotClasses().Get(ctx,
				name, metav1.GetOptions{})
			framework.Logf("volumesnapshotclass %v, err:%v", volumeSnapshotClass, err)
			if !apierrors.IsNotFound(err) && err != nil {
				return false, fmt.Errorf("couldn't find "+
					"snapshotclass: %s due to error: %v", name, err)
			}
			if volumeSnapshotClass.Name != "" {
				framework.Logf("Found volumesnapshotclass %s", name)
				return true, nil
			}
			framework.Logf("waiting to get volumesnapshotclass %s", name)
			return false, nil
		})
	if wait.Interrupted(waitErr) {
		return nil, fmt.Errorf("couldn't find volumesnapshotclass: %s", name)
	}
	return volumeSnapshotClass, nil
}

// createVolumeSnapshotClass creates VSC for a Vanilla cluster and
// fetches VSC for a Guest or Supervisor Cluster
func CreateVolumeSnapshotClass(ctx context.Context, vs *config.E2eTestConfig, snapc *snapclient.Clientset,
	deletionPolicy string) (*snapV1.VolumeSnapshotClass, error) {
	var volumeSnapshotClass *snapV1.VolumeSnapshotClass
	var err error
	if vs.TestInput.ClusterFlavor.VanillaCluster {
		volumeSnapshotClass, err = snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			GetVolumeSnapshotClassSpec(snapV1.DeletionPolicy(deletionPolicy), nil), metav1.CreateOptions{})
		if err != nil {
			return nil, err
		}
	} else if vs.TestInput.ClusterFlavor.GuestCluster || vs.TestInput.ClusterFlavor.SupervisorCluster {
		var volumeSnapshotClassName string
		if deletionPolicy == "Delete" {
			volumeSnapshotClassName = env.GetAndExpectStringEnvVar(constants.EnvVolSnapClassDel)
		} else {
			framework.Failf("%s volume snapshotclass is not supported"+
				" in Supervisor or Guest Cluster", deletionPolicy)
		}
		volumeSnapshotClass, err = GetVolumeSnapshotClass(ctx, snapc, volumeSnapshotClassName)
		if err != nil {
			return nil, err
		}
	}
	return volumeSnapshotClass, nil
}

// deleteVolumeSnapshotWithPandoraWait deletes Volume Snapshot with Pandora wait for CNS to sync
func DeleteVolumeSnapshotWithPandoraWait(ctx context.Context, snapc *snapclient.Clientset,
	namespace string, snapshotName string, pandoraSyncWaitTime int) {
	err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, snapshotName,
		metav1.DeleteOptions{})
	if !apierrors.IsNotFound(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)
}

// waitForVolumeSnapshotContentToBeDeleted wait till the volume snapshot content is deleted
func WaitForVolumeSnapshotContentToBeDeleted(client snapclient.Clientset, ctx context.Context,
	name string) error {
	var err error
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, 2*constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			_, err = client.SnapshotV1().VolumeSnapshotContents().Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					framework.Logf("VolumeSnapshotContent: %s is deleted", name)
					return true, nil
				} else {
					return false, fmt.Errorf("error fetching volumesnapshotcontent details : %v", err)
				}
			}
			return false, nil
		})
	return waitErr
}
