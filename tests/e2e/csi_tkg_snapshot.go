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
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
)

var _ = ginkgo.Describe("[tkg-snapshot] Volume Snapshot Basic Test", func() {
	f := framework.NewDefaultFramework("volume-snapshot")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                 clientset.Interface
		namespace              string
		scParameters           map[string]string
		datastoreURL           string
		pandoraSyncWaitTime    int
		restConfig             *restclient.Config
		guestClusterRestConfig *restclient.Config
		snapc                  *snapclient.Clientset
		storagePolicyName      string
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		//Get snapshot client using the rest config
		if !guestCluster {
			restConfig = getRestConfigClient()
			snapc, err = snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			guestClusterRestConfig = getRestConfigClientForGuestCluster(guestClusterRestConfig)
			snapc, err = snapclient.NewForConfig(guestClusterRestConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
	})

	ginkgo.AfterEach(func() {
		if guestCluster {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			framework.Logf("Collecting supervisor PVC events before performing PV/PVC cleanup")
			eventList, err := svcClient.CoreV1().Events(svcNamespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, item := range eventList.Items {
				framework.Logf(fmt.Sprintf(item.Message))
			}
		}
	})

	/* Create/Delete snapshot via k8s API using VolumeSnapshotContent (Pre-Provisioned Snapshots)

	//Steps
	1. In this approach create a dynamic VolumeSnapshot in Guest with a VolumeSnapshotClass with “Delete”
	deletion policy.
	2. Note the VolumeSnapshot name created on the Supervisor.
	3. Explicitly change the deletionPolicy of VolumeSnapshotContent on Guest to “Retain”.
	4. Delete the VolumeSnapshot. This will leave the VolumeSnapshotContent on the Guest as is,
	since deletionPolicy was “Retain”
	5. Explicitly delete the VolumeSnapshotContent.
	6. In this approach, we now have Supervisor VolumeSnapshot that doesn’t have a corresponding
	VolumeSnapshot-VolumeSnapshotContent on Guest.
	7. Create a VolumeSnapshotContent that points to the Supervisor VolumeSnapshot, and create a
	VolumeSnapshot on Guest that point to the VolumeSnapshotContent.

			Create/Delete snapshot via k8s API using VolumeSnapshotContent (Pre-Provisioned Snapshots)
		1. Create a storage class (eg: vsan default) and create a pvc using this sc
		2. The volumesnapshotclass is set to delete
		3. Create a VolumeSnapshotContent using snapshot-handle
		   a. get snapshotHandle by referring to an existing volume snapshot
		   b. this snapshot will be created dynamically, and the snapshot-content that is
		      created by that will be referred to get the snapshotHandle
		4. Create a volume snapshot using source set to volumeSnapshotContentName above
				5. Ensure the snapshot is created, verify using get VolumeSnapshot
		6. Verify the restoreSize on the snapshot and the snapshotcontent is set to same as that of the pvcSize
		7. Delete the above snapshot, run a get from k8s side and ensure its removed
		8. Run QuerySnapshot from CNS side, the backend snapshot should be deleted
		9. Also ensure that the VolumeSnapshotContent is deleted along with the
		   volume snapshot as the policy is delete
		10. Cleanup the pvc

	*/
	ginkgo.It("[tkg-snapshot] TC2Verify Preprovisioned snapshot workflow", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var volHandle string
		var pvclaims []*v1.PersistentVolumeClaim

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle = persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dyanmic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, diskSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		framework.Logf("Get volume snapshot handle from Supervisor Cluster")
		snapshotId, _, svcVolumeSnapshotName, err := getSnapshotHandleFromSupervisorCluster(ctx, volumeSnapshotClass,
			*snapshotContent.Status.SnapshotHandle)

		ginkgo.By("Create Preprovisioned snapshot")
		_, staticSnapshot, err := createPreProvisionedSnapshotForGuestCluster(ctx, volumeSnapshot, snapshotContent,
			snapc, namespace, pandoraSyncWaitTime, svcVolumeSnapshotName)

		ginkgo.By("Delete Preprovisioned snapshot")
		staticSnapshotCreated, StaticSnapshotContentCreated, err := deleteDynamicVolumeSnapshot(ctx, snapc, namespace,
			staticSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if staticSnapshotCreated {
				framework.Logf("Deleting static volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if StaticSnapshotContentCreated {
				framework.Logf("Deleting static volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
	})

	/*
		Volume restore using snapshot (a) dynamic snapshot (b) pre-provisioned snapshot
		1. Create a sc, a pvc and attach the pvc to a pod, write a file
		2. Create pre-provisioned and dynamically provisioned snapshots using this pvc
		3. Create new volumes (pvcFromPreProvSS and pvcFromDynamicSS) using these
			snapshots as source, use the same sc
		4. Ensure the pvc gets provisioned and is Bound
		5. Attach the pvc to a pod and ensure data from snapshot is available
		   (file that was written in step.1 should be available)
		6. And also write new data to the restored volumes and it should succeed
		7. Delete the snapshots and pvcs/pods created in steps 1,2,3
		8. Continue to write new data to the restore volumes and it should succeed
		9. Create new snapshots on restore volume and verify it succeeds
		10. Run cleanup: Delete snapshots, restored-volumes, pods
	*/

	ginkgo.It("TC5Volume restore using dynamic snapshot and pre-provisioned snapshot", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dyanmic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, diskSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Get volume snapshot ID from snapshot handle")
		snapshotId, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, snapshotContent, volumeSnapshotClass,
			volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restore PVC using dynamic volume snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolume2, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim2},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := persistentvolume2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle2))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volHandle2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle2))

		ginkgo.By("Creating Pod and attach it to restored PVC")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var vmUUID string
		nodeName := pod.Spec.NodeName

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle2, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle2, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		framework.Logf("Get volume snapshot handle from Supervisor Cluster")
		snapshotId, _, svcVolumeSnapshotName, err := getSnapshotHandleFromSupervisorCluster(ctx, volumeSnapshotClass,
			*snapshotContent.Status.SnapshotHandle)

		ginkgo.By("Create Preprovisioned snapshot")
		_, staticSnapshot, err := createPreProvisionedSnapshotForGuestCluster(ctx, volumeSnapshot, snapshotContent,
			snapc, namespace, pandoraSyncWaitTime, svcVolumeSnapshotName)

		ginkgo.By("Restore PVC using pre-provisioned snapshot")
		pvcSpec = getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, staticSnapshot.Name, snapshotapigroup)

		pvclaim3, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolume3, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim3},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle3 := persistentvolume3[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle3).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle3 = getVolumeIDFromSupervisorCluster(volHandle3)
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod to attach PV to the node")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false,
			execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var vmUUID2 string
		nodeName2 := pod2.Spec.NodeName

		if vanillaCluster {
			vmUUID2 = getNodeUUID(ctx, client, pod2.Spec.NodeName)
		} else if guestCluster {
			vmUUID2, err = getVMUUIDFromNodeName(pod2.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle3, nodeName2))
		isDiskAttached2, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle3, vmUUID2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached2).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod2.html "}
		output2 := framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output2, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd2...)
		output2 = framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output2, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
		ginkgo.By("Delete Preprovisioned snapshot")
		staticSnapshotCreated, StaticSnapshotContentCreated, err := deleteDynamicVolumeSnapshot(ctx, snapc, namespace,
			staticSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if staticSnapshotCreated {
				framework.Logf("Deleting static volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if StaticSnapshotContentCreated {
				framework.Logf("Deleting static volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a new volume snapshot from the restored PVC created from dynamic snapshot")
		volumeSnapshot3, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim2.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot3.Name)
		snapshotCreated3 := true
		snapshotContentCreated3 := true

		defer func() {
			if snapshotContentCreated3 {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated3 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot3, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot3.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated3 = true
		gomega.Expect(volumeSnapshot3.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent3, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot3.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated3 = true
		gomega.Expect(*snapshotContent3.Status.ReadyToUse).To(gomega.BeTrue())

		ginkgo.By("Get volume snapshot ID from snapshot handle")
		snapshotId3, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, snapshotContent, volumeSnapshotClass,
			volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleted volume snapshot is created above")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot3.Name, pandoraSyncWaitTime)
		snapshotCreated3 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContent3.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated3 = false

		ginkgo.By("Delete dyanmic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteDynamicVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Delete the namespace hosting the pvcs and volume-snapshots and
			recover the data using snapshot-content
		1. Create a sc, create a pvc using this sc on a non-default namesapce
		2. create a dynamic snapshot using the pvc as source
		3. verify volume-snapshot is ready-to-use and volumesnapshotcontent is auto-created
		4. Delete the non-default namespace which should delete all namespaced objects such as pvc, volume-snapshot
		5. Ensure the volumesnapshotcontent object which is cluster-scoped does not get deleted
		6. Also verify we can re-provision a snapshot and restore a volume using
			this object on another namespace (could be default too)
		7. This VolumeSnapshotContent is dynamically created. we can't use it for pre-provisioned snapshot.
			we would be creating a new VolumeSnapshotContent pointing to the same snapshotHandle
			and then create a new VolumeSnapshot to bind with it
		8. Ensure the pvc with source as snapshot creates successfully and is bound
		9. Cleanup the snapshot, pvcs and ns
	*/
	ginkgo.It("TC11Delete the namespace hosting the pvcs and "+
		"volume-snapshots and recover the data using snapshot-content", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false

		ginkgo.By("Creating new namespace for the test")
		namespace1, err := framework.CreateTestingNS(f.BaseName, client, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		newNamespaceName := namespace1.Name
		isNamespaceDeleted := false

		defer func() {
			if !isNamespaceDeleted {
				ginkgo.By("Delete namespace")
				err = client.CoreV1().Namespaces().Delete(ctx, newNamespaceName, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isNamespaceDeleted = true
			}
		}()

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			newNamespaceName, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			if !isNamespaceDeleted {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, newNamespaceName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Retain"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete volume snapshot class")
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx,
				volumeSnapshotClass.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a volume snapshot")
		snapshot1, err := snapc.SnapshotV1().VolumeSnapshots(newNamespaceName).Create(ctx,
			getVolumeSnapshotSpec(newNamespaceName, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", snapshot1.Name)
		snapshotCreated := true
		var snapshotContent1Name = ""
		defer func() {
			if !isNamespaceDeleted {
				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot1.Name, pandoraSyncWaitTime)
				}
				if snapshotContentCreated {
					framework.Logf("Deleting volume snapshot content")
					deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
						*snapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

					framework.Logf("Wait till the volume snapshot content is deleted")
					err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *snapshot1.Status.BoundVolumeSnapshotContentName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					snapshotContent1Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot content is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *snapshot1.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot is Ready to use")
		snapshot1_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, newNamespaceName, snapshot1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(snapshot1_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent1, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*snapshot1_updated.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		snapshotContent1Name = snapshotContent1.Name

		gomega.Expect(*snapshotContent1.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle := *snapshotContent1.Status.SnapshotHandle
		snapshotId := strings.Split(snapshothandle, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete namespace")
		err = client.CoreV1().Namespaces().Delete(ctx, newNamespaceName, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isNamespaceDeleted = true

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Verify volume snapshot is deleted")
		_, err = snapc.SnapshotV1().VolumeSnapshots(newNamespaceName).Get(ctx,
			snapshot1.Name, metav1.GetOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		snapshotCreated = false

		_, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotContent1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating another new namespace for the test")
		namespace2, err := framework.CreateTestingNS(f.BaseName, client, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		f.AddNamespacesToDelete(namespace2)
		namespace2Name := namespace2.Name

		snapshotHandle := volHandle + "+" + snapshotId
		ginkgo.By(fmt.Sprintf("Creating volume snapshot content by snapshotHandle %s", snapshotHandle))
		snapshotcontent2, err := snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
			getVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Delete"), snapshotHandle,
				"static-vs-cns", namespace2Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume snapshot content is created or not")
		snapshotContent2_updated, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotcontent2.ObjectMeta.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 := true
		framework.Logf("Snapshotcontent name is  %s", snapshotContent2_updated.ObjectMeta.Name)

		defer func() {
			if snapshotContentCreated2 {
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					snapshotContent2_updated.ObjectMeta.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Create a volume snapshot by snapshotcontent")
		volumeSnapshot2, err := snapc.SnapshotV1().VolumeSnapshots(namespace2Name).Create(ctx,
			getVolumeSnapshotSpecByName(namespace2Name, "static-vs-cns",
				snapshotContent2_updated.ObjectMeta.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot2.Name)
		snapshotCreated2 := true

		defer func() {
			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot2")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				err = e2eVSphere.deleteVolumeSnapshotInCNS(volHandle, volumeSnapshot2.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot2 is created and Ready to use")
		volumeSnapshot2_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace2Name,
			volumeSnapshot2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(volumeSnapshot2_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
		framework.Logf("Snapshot details is %+v", volumeSnapshot2_updated)

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace2Name, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot2.Name, snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(client, namespace2Name, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot2")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace2Name, volumeSnapshot2.Name, pandoraSyncWaitTime)

				err = e2eVSphere.deleteVolumeSnapshotInCNS(volHandle, volumeSnapshot2.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				snapshotCreated2 = false
			}
			framework.Logf("Deleting restored PVC")
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace2Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Deleted volume snapshot is created above")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace2Name, volumeSnapshot2.Name, pandoraSyncWaitTime)
		snapshotCreated2 = false

		framework.Logf("Wait till the volume snapshot content is deleted")
		deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
			volumeSnapshot2_updated.ObjectMeta.Name, pandoraSyncWaitTime)

		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, volumeSnapshot2_updated.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false

		framework.Logf("Deleting volume snapshot content 1")
		deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
			snapshotContent1Name, pandoraSyncWaitTime)

		err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
			snapshot1_updated.ObjectMeta.Name, pandoraSyncWaitTime)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Create a pre-provisioned snapshot using VolumeSnapshotContent as source
			(use VSC which is auto-created by a dynamic provisioned snapshot)
		1. create a sc, and pvc using this sc
		2. create a dynamic snapshot using above pvc as source
		3. verify that it auto-created a VolumeSnapshotContent object
		4. create a pre-provisioned snapshot (which uses VolumeSnapshotContent as source) using the VSC from step(3)
		5. Ensure this provisioning fails with appropriate error: SnapshotContentMismatch error
	*/
	ginkgo.It("TC8Create a pre-provisioned snapshot using VolumeSnapshotContent as source", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var snapshotCreated = false

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)

		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = true
		gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		gomega.Expect(*snapshotContent.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle := *snapshotContent.Status.SnapshotHandle
		snapshotId := strings.Split(snapshothandle, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a volume snapshot2")
		volumeSnapshot2, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpecByName(namespace, "static-vs", snapshotContent.ObjectMeta.Name),
			metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated2 := true

		defer func() {
			if snapshotCreated2 {
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, "static-vs", pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot 2 is creation failed")
		staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot2.Name)
		gomega.Expect(err).To(gomega.HaveOccurred())
		framework.Logf("Snapshot details is %+v", staticSnapshot)

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Pre-provisioned snapshot using incorrect/non-existing static snapshot
		1. Create a sc, and pvc using this sc
		2. Create a snapshot for this pvc (use CreateSnapshot CNS API)
		3. Create a VolumeSnapshotContent CR using above snapshot-id, by passing the snapshotHandle
		4. Create a VolumeSnapshot using above content as source
		5. VolumeSnapshot and VolumeSnapshotContent should be created successfully and readToUse set to True
		6. Delete the snapshot created in step-2 (use deleteSnapshots CNS API)
		7. VolumeSnapshot and VolumeSnapshotContent will still have readyToUse set to True
		8. Restore: Create a volume using above pre-provisioned snapshot k8s object
			(note the snapshotHandle its pointing to has been deleted)
		9. Volume Create should fail with an appropriate error on k8s side
	*/
	ginkgo.It("TC9Pre-provisioned snapshot using incorrect/non-existing static snapshot", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx,
				volumeSnapshotClass.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot in CNS")
		snapshotId, err := e2eVSphere.createVolumeSnapshotInCNS(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Successfully created a snapshot in CNS %s", snapshotId)
		snapshotCreated1 := true

		defer func() {
			if snapshotCreated1 {
				err = e2eVSphere.deleteVolumeSnapshotInCNS(volHandle, snapshotId)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		snapshotHandle := volHandle + "+" + snapshotId
		ginkgo.By(fmt.Sprintf("Creating volume snapshot content by snapshotHandle %s", snapshotHandle))
		snapshotContentNew, err := snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
			getVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Delete"), snapshotHandle,
				"static-vs-cns", namespace), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume snapshot content is created or not")
		snapshotContentNew, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotContentNew.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 := true
		framework.Logf("Snapshotcontent name is  %s", snapshotContentNew.ObjectMeta.Name)

		defer func() {
			if snapshotContentCreated2 {
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					snapshotContentNew.ObjectMeta.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Create a volume snapshot by snapshotcontent")
		volumeSnapshot2, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpecByName(namespace, "static-vs-cns",
				snapshotContentNew.ObjectMeta.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot2.Name)
		snapshotCreated2 := true

		defer func() {
			if snapshotCreated2 {
				err = e2eVSphere.deleteVolumeSnapshotInCNS(volHandle, volumeSnapshot2.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(staticSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
		framework.Logf("Snapshot details is %+v", staticSnapshot)

		ginkgo.By("Deleted volume snapshot is created above")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, staticSnapshot.Name, pandoraSyncWaitTime)
		snapshotCreated1 = false
		snapshotCreated2 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContentNew.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false

		ginkgo.By("Create PVC using the snapshot deleted")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, "static-vs-cns", snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim2},
			framework.ClaimProvisionShortTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())

		expectedErrMsg := "error getting handle for DataSource Type VolumeSnapshot by Name"
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim2.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(),
			fmt.Sprintf("Expected pvc creation failure with error message: %s", expectedErrMsg))

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		Create a volume from a snapshot that is still not ready-to-use
		1. Create a pre-provisioned snapshot pointing to a VolumeSnapshotContent
			which is still not provisioned (or does not exist)
		2. The snapshot will have status.readyToUse: false and snapshot is in Pending state
		3. Create a volume using the above snapshot as source and ensure the provisioning fails with error:
			ProvisioningFailed | snapshot <> not bound
		4. pvc is stuck in Pending
		5. Once the VolumeSnapshotContent is created, snapshot should have status.readyToUse: true
		6. The volume should now get provisioned successfully
		7. Validate the pvc is Bound
		8. Cleanup the snapshot and pvc
	*/
	ginkgo.It("TC10Create a volume from a snapshot that is still not ready-to-use", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx,
				volumeSnapshotClass.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot in CNS")
		snapshotId, err := e2eVSphere.createVolumeSnapshotInCNS(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Successfully created a snapshot in CNS %s", snapshotId)
		snapshotCreated1 := true

		defer func() {
			if snapshotCreated1 {
				err = e2eVSphere.deleteVolumeSnapshotInCNS(volHandle, snapshotId)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		snapshotHandle := volHandle + "+" + snapshotId
		ginkgo.By(fmt.Sprintf("Creating volume snapshot content by snapshotHandle %s", snapshotHandle))
		snapshotContentNew, err := snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
			getVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Delete"), snapshotHandle,
				"static-vs-cns", namespace), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume snapshot content is created or not")
		snapshotContentNew, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotContentNew.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 := true
		framework.Logf("Snapshotcontent name is  %s", snapshotContentNew.ObjectMeta.Name)

		defer func() {
			if snapshotContentCreated2 {
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					snapshotContentNew.ObjectMeta.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Create a volume snapshot by snapshotcontent")
		volumeSnapshot2, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpecByName(namespace, "static-vs-cns",
				snapshotContentNew.ObjectMeta.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot2.Name)
		snapshotCreated2 := true

		defer func() {
			if snapshotCreated2 {
				err = e2eVSphere.deleteVolumeSnapshotInCNS(volHandle, volumeSnapshot2.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create PVC while snapshot is still provisioning")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, "static-vs-cns", snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume snapshot is created")
		staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(staticSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
		framework.Logf("Snapshot details is %+v", staticSnapshot)

		ginkgo.By("Deleted volume snapshot is created above")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, staticSnapshot.Name, pandoraSyncWaitTime)
		snapshotCreated1 = false
		snapshotCreated2 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContentNew.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false
	})

	/*
		Create snapshots using default VolumeSnapshotClass
		1. Create a VolumeSnapshotClass and set it as default
		2. Create a snapshot without providing the snapshotClass input and
			ensure the default class is picked and honored for snapshot creation
		3. Validate the fields after snapshot creation succeeds (snapshotClass, retentionPolicy)
	*/
	ginkgo.It("TC14Create snapshots using default VolumeSnapshotClass", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var snapshotCreated = false

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class")
		vscSpec := getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil)
		vscSpec.ObjectMeta.Annotations = map[string]string{
			"snapshot.storage.kubernetes.io/is-default-class": "true",
		}
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			vscSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpecWithoutSC(namespace, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)

		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = true
		gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		gomega.Expect(*snapshotContent.Status.ReadyToUse).To(gomega.BeTrue())
		gomega.Expect(*snapshotContent.Spec.VolumeSnapshotClassName).To(gomega.Equal(volumeSnapshotClass.Name))

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle := *snapshotContent.Status.SnapshotHandle
		snapshotId := strings.Split(snapshothandle, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* Table4TC1
		Create snapshot on Supervisor cluster
		    Create a Storage Class, and a PVC using this SC in Supervisor cluster.
	    Create a dynamic snapshot in supervisor using above PVC as source
	    Snapshot creation should fail with appropriate error message.
	    Cleanup the snapshots, PVC and SC
	*/

	ginkgo.It("[tkg-snapshot] TC1Table4Verify snapshot creation on Supervisor cluster", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var volHandle string
		var pvclaims []*v1.PersistentVolumeClaim

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle = persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dyanmic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, diskSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Get volume snapshot ID from snapshot handle")
		snapshotId, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, snapshotContent, volumeSnapshotClass,
			volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dyanmic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteDynamicVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* PVC → Snapshot → RestoreVolume → Snapshot → Restore Vol again
		    Create a Storage Class, a PVC and attach the PVC to a Pod, write a file
	    Create dynamically provisioned snapshots using this PVC
	    Create new volume using this snapshots as source, use the same SC
	    Ensure the PVC gets provisioned and is Bound
	    Attach the PVC to a Pod and ensure data from snapshot is available (file that was written in step.1 should be available)
	    And also write new data to the restored volumes and it should succeed
	    Take a snapshot of restore volume created in step #3.
	    Create new volume using the snapshot as source use the same SC created in step #7
	    Ensure the PVC gets provisioned and is Bound
	    Attach the PVC to a Pod and ensure data from snapshot is available (file that was written in step.1 and step 5 should be available)
	    And also write new data to the restored volumes and it should succeed
	    Run cleanup: Delete snapshots, restored-volumes, pods.
	*/

	/*
	 */
})
