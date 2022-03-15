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
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
)

var _ = ginkgo.Describe("[block-vanilla-snapshot] Volume Snapshot Basic Test", func() {
	f := framework.NewDefaultFramework("volume-snapshot")
	var (
		client       clientset.Interface
		namespace    string
		scParameters map[string]string
		datastoreURL string
		pvclaims     []*v1.PersistentVolumeClaim
		restConfig   *restclient.Config
		snapc        *snapclient.Clientset
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
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Create/Delete snapshot via k8s API using PVC (Dynamic Provisioning)

	   1. Create a storage class (eg: vsan default) and create a pvc using this sc
	   2. Create a VolumeSnapshot class with snapshotter as vsphere-csi-driver and set deletionPolicy to Delete
	   3. Create a volume-snapshot with labels, using the above snapshot-class and pvc (from step-1) as source
	   4. Ensure the snapshot is created, verify using get VolumeSnapshot
	   5. Also verify that VolumeSnapshotContent is auto-created
	   6. Verify the references to pvc and volume-snapshot on this object
	   7. Verify that the VolumeSnapshot has ready-to-use set to True
	   8. Verify that the Restore Size set on the snapshot is same as that of the source volume size
	   9. Query the snapshot from CNS side using volume id - should pass and return the snapshot entry
	   10. Delete the above snapshot from k8s side using kubectl delete, run a get and ensure it is removed
	   11. Also ensure that the VolumeSnapshotContent is deleted along with the
	       volume snapshot as the policy is delete
	   12. Query the snapshot from CNS side - should return 0 entries
	   13. Cleanup: Delete PVC, SC (validate they are removed)
	*/
	ginkgo.It("Verify snapshot dynamic provisioning workflow", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
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
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
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
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
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
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
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
	ginkgo.It("Verify snapshot static provisioning through K8s API workflow", func() {
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
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
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
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By(fmt.Sprintf("Creating volume snapshot content by snapshotHandle %s", snapshothandle))
		snapshotContent2, err := snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
			getVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Delete"), snapshothandle,
				"static-vs", namespace), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume snapshot content is created or not")
		snapshotContentNew, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotContent2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 := true
		framework.Logf("Snapshotcontent name is  %s", snapshotContentNew.ObjectMeta.Name)

		defer func() {
			if snapshotContentCreated2 {
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					snapshotContentNew.ObjectMeta.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot by snapshotcontent")
		volumeSnapshot2, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpecByName(namespace, "static-vs",
				snapshotContentNew.ObjectMeta.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot2.Name)
		snapshotCreated2 := true

		defer func() {
			if snapshotCreated2 {
				err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, "static-vs", metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot 2 is created")
		staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(staticSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
		framework.Logf("Snapshot details is %+v", staticSnapshot)

		ginkgo.By("Deleted volume snapshot is created above")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, staticSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated2 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContentNew.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false

	})

	/*
		Create pre-provisioned snapshot using backend snapshot (create via CNS)
		1. Create a storage-class and create a pvc using this sc
		2. Call CNS CreateSnapshot API on the above volume
		3. Call CNS QuerySnapshot API on the above volume to get the snapshotHandle id
		4. Use this snapshotHandle to create the VolumeSnapshotContent
		5. Create a volume snapshot using source set to volumeSnapshotContentName above
		6. Ensure the snapshot is created, verify using get VolumeSnapshot
		7. Verify the restoreSize on the snapshot and the snapshotcontent is set to same as that of the pvcSize
		8. Delete the above snapshot, run a get from k8s side and ensure its removed
		9. Run QuerySnapshot from CNS side, the backend snapshot should be deleted
		10. Also ensure that the VolumeSnapshotContent is deleted along with the
		    volume snapshot as the policy is delete
		11. The snapshot that was created via CNS in step-2 should be deleted as part of k8s snapshot delete
		12. Delete the pvc
	*/
	ginkgo.It("Verify snapshot static provisioning via CNS", func() {
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
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
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
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					snapshotContentNew.ObjectMeta.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, staticSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated1 = false
		snapshotCreated2 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContentNew.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false
	})

	/*
		Create/Delete snapshot via k8s API using VolumeSnapshotContent (Pre-Provisioned Snapshots)
		1. Create a storage class (eg: vsan default) and create a pvc using this sc
		2. The volumesnapshotclass is set to delete
		3. Create a VolumeSnapshotContent using snapshot-handle with deletion policy Retain
		   a. get snapshotHandle by referring to an existing volume snapshot
		   b. this snapshot will be created dynamically, and the snapshot-content that is
		      created by that will be referred to get the snapshotHandle
		4. Create a volume snapshot using source set to volumeSnapshotContentName above
		5. Ensure the snapshot is created, verify using get VolumeSnapshot
		6. Verify the restoreSize on the snapshot and the snapshotcontent is set to same as that of the pvcSize
		7. Delete the above snapshot, run a get from k8s side and ensure its removed
		8. Run QuerySnapshot from CNS side, the backend snapshot should be deleted
		9. Verify the volume snaphsot content is not deleted
		10. Delete dynamically created volume snapshot
		11. Verify the volume snapshot content created by snapshot 1 is deleted automatically
		12. Delete volume snapshot content 2
		13. Cleanup the pvc, volume snapshot class and storage class
	*/
	ginkgo.It("Verify snapshot static provisioning with deletion policy Retain", func() {
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
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
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
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ns, err := framework.CreateTestingNS(f.BaseName, client, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Creating volume snapshot content by snapshotHandle %s", snapshothandle))
		snapshotContent2, err := snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
			getVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Retain"), snapshothandle,
				"static-vs", ns.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume snapshot content is created or not")
		snapshotContentNew, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotContent2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 := true
		framework.Logf("Snapshotcontent name is  %s", snapshotContentNew.ObjectMeta.Name)

		defer func() {
			if snapshotContentCreated2 {
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					snapshotContentNew.ObjectMeta.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot by snapshotcontent")
		volumeSnapshot2, err := snapc.SnapshotV1().VolumeSnapshots(ns.Name).Create(ctx,
			getVolumeSnapshotSpecByName(ns.Name, "static-vs",
				snapshotContentNew.ObjectMeta.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot2.Name)
		snapshotCreated2 := true

		defer func() {
			if snapshotCreated2 {
				err = snapc.SnapshotV1().VolumeSnapshots(ns.Name).Delete(ctx, "static-vs", metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot 2 is created")
		staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, ns.Name, volumeSnapshot2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(staticSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
		framework.Logf("Snapshot details is %+v", staticSnapshot)

		ginkgo.By("Deleted volume snapshot is created above")
		err = snapc.SnapshotV1().VolumeSnapshots(ns.Name).Delete(ctx, staticSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated2 = false

		ginkgo.By("Verify volume snapshot content is not deleted")
		snapshotContentGetResult, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotContent2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(snapshotContentGetResult.Name).Should(gomega.Equal(snapshotContent2.Name))
		framework.Logf("Snapshotcontent name is  %s", snapshotContentGetResult.ObjectMeta.Name)

		framework.Logf("Deleting volume snapshot 1 ")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContent.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		framework.Logf("Delete volume snapshot content 2")
		err = snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
			snapshotContentGetResult.ObjectMeta.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false
	})
})
