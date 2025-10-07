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
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
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

// GetRestConfigClientForGuestCluster can be used to get the KUBECONFIG
func GetRestConfigClientForGuestCluster(guestClusterRestConfig *rest.Config) *rest.Config {
	var err error
	if guestClusterRestConfig == nil {
		if k8senv := env.GetAndExpectStringEnvVar("KUBECONFIG"); k8senv != "" {
			guestClusterRestConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	}
	return guestClusterRestConfig
}

// verifyVolumeRestoreOperation verifies if volume(PVC) restore from given snapshot
// and creates pod and checks attach volume operation if verifyPodCreation is set to true
func VerifyVolumeRestoreOperation(ctx context.Context, e2eTestConfig *config.E2eTestConfig, client clientset.Interface,
	namespace string, storageclass *storagev1.StorageClass,
	volumeSnapshot *snapV1.VolumeSnapshot, diskSize string,
	verifyPodCreation bool) (*v1.PersistentVolumeClaim, []*v1.PersistentVolume, *v1.Pod) {

	ginkgo.By("Create PVC from snapshot")
	pvcSpec := GetPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
		v1.ReadWriteOnce, volumeSnapshot.Name, constants.Snapshotapigroup)

	pvclaim2, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
		[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
	if e2eTestConfig.TestInput.ClusterFlavor.GuestCluster {
		volHandle2 = k8testutil.GetVolumeIDFromSupervisorCluster(volHandle2)
	}
	gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

	var pod *v1.Pod
	if verifyPodCreation {
		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err = k8testutil.CreatePod(ctx, e2eTestConfig, client, namespace, nil,
			[]*v1.PersistentVolumeClaim{pvclaim2}, false, constants.ExecRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var vmUUID string
		var exists bool
		nodeName := pod.Spec.NodeName

		if e2eTestConfig.TestInput.ClusterFlavor.VanillaCluster {
			vmUUID = k8testutil.GetNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if e2eTestConfig.TestInput.ClusterFlavor.GuestCluster {
			vmUUID, err = vcutil.GetVMUUIDFromNodeName(e2eTestConfig, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			annotations := pod.Annotations
			vmUUID, exists = annotations[constants.VmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", constants.VmUUIDLabel))
			_, err := vcutil.GetVMByUUID(ctx, e2eTestConfig, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle2, nodeName))
		isDiskAttached, err := vcutil.IsVolumeAttachedToVM(client, e2eTestConfig, volHandle2, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		var cmd []string
		if e2eTestConfig.TestInput.TestBedInfo.WindowsEnv {
			cmd = []string{"exec", pod.Name, "--namespace=" + namespace, "powershell.exe", "cat ", constants.FilePathPod1}
		} else {
			cmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat ", constants.FilePathPod1}
		}
		output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		var wrtiecmd []string
		if e2eTestConfig.TestInput.TestBedInfo.WindowsEnv {
			wrtiecmd = []string{"exec", pod.Name, "--namespace=" + namespace, "powershell.exe",
				"Add-Content /mnt/volume1/Pod1.html 'Hello message from test into Pod1'"}
		} else {
			wrtiecmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"echo 'Hello message from test into Pod1' >> /mnt/volume1/Pod1.html"}
		}
		e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
		return pvclaim2, persistentvolumes2, pod
	}
	return pvclaim2, persistentvolumes2, pod
}

/* createPreProvisionedSnapshotInGuestCluster created pre-provisioned snaphot  on Guest cluster */
func CreatePreProvisionedSnapshotInGuestCluster(ctx context.Context, volumeSnapshot *snapV1.VolumeSnapshot,
	updatedSnapshotContent *snapV1.VolumeSnapshotContent,
	snapc *snapclient.Clientset, namespace string, pandoraSyncWaitTime int,
	svcVolumeSnapshotName string, diskSize string) (*snapV1.VolumeSnapshotContent,
	*snapV1.VolumeSnapshot, bool, bool, error) {

	framework.Logf("Change the deletion policy of VolumeSnapshotContent from Delete to Retain " +
		"in Guest Cluster")

	updatedSnapshotContent, err := ChangeDeletionPolicyOfVolumeSnapshotContent(ctx, updatedSnapshotContent,
		snapc, snapV1.VolumeSnapshotContentRetain)
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("failed to change deletion policy of VolumeSnapshotContent: %v", err)
	}

	framework.Logf("Delete dynamic volume snapshot from Guest Cluster")
	DeleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

	framework.Logf("Delete VolumeSnapshotContent from Guest Cluster explicitly")
	err = DeleteVolumeSnapshotContent(ctx, updatedSnapshotContent, snapc, pandoraSyncWaitTime)
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("failed to delete VolumeSnapshotContent: %v", err)
	}

	framework.Logf("Creating static VolumeSnapshotContent in Guest Cluster using "+
		"supervisor VolumeSnapshotName %s", svcVolumeSnapshotName)
	staticSnapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
		GetVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Delete"), svcVolumeSnapshotName,
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

	staticSnapshotContent, err = WaitForVolumeSnapshotContentReadyToUse(*snapc, ctx, staticSnapshotContent.Name)
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("volume snapshot content is not ready to use")
	}
	staticSnapshotContentCreated := true

	ginkgo.By("Create a static volume snapshot by static snapshotcontent")
	staticVolumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
		GetVolumeSnapshotSpecByName(namespace, "static-vs",
			staticSnapshotContent.ObjectMeta.Name), metav1.CreateOptions{})
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("failed to create static volume snapshot: %v", err)
	}
	framework.Logf("Volume snapshot name is : %s", staticVolumeSnapshot.Name)

	ginkgo.By("Verify static volume snapshot is created")
	staticSnapshot, err := WaitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, staticVolumeSnapshot.Name)
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

/*
changeDeletionPolicyOfVolumeSnapshotContentOnGuest changes the deletion policy
of volume snapshot content from delete to retain in Guest Cluster
*/
func ChangeDeletionPolicyOfVolumeSnapshotContent(ctx context.Context,
	snapshotContent *snapV1.VolumeSnapshotContent, snapc *snapclient.Clientset,
	policyName snapV1.DeletionPolicy) (*snapV1.VolumeSnapshotContent, error) {

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

// getVolumeSnapshotContentSpec returns a spec for the volume snapshot content
func GetVolumeSnapshotContentSpec(deletionPolicy snapV1.DeletionPolicy, snapshotHandle string,
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
			Driver:         constants.E2evSphereCSIDriverName,
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
func GetVolumeSnapshotSpecByName(namespace string, snapshotName string,
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

// GetPersistentVolumeClaimSpecWithDatasource return the PersistentVolumeClaim
// spec with specified storage class.
func GetPersistentVolumeClaimSpecWithDatasource(namespace string, ds string, storageclass *storagev1.StorageClass,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode,
	datasourceName string, snapshotapigroup string) *v1.PersistentVolumeClaim {
	disksize := constants.DiskSize
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
			Resources: v1.VolumeResourceRequirements{
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
