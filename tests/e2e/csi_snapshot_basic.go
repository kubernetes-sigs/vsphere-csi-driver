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
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
)

var _ = ginkgo.Describe("Volume Snapshot Basic Test", func() {
	f := framework.NewDefaultFramework("volume-snapshot")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                     clientset.Interface
		clientNewGc                clientset.Interface
		c                          clientset.Interface
		namespace                  string
		scParameters               map[string]string
		datastoreURL               string
		pandoraSyncWaitTime        int
		volumeOpsScale             int
		restConfig                 *restclient.Config
		guestClusterRestConfig     *restclient.Config
		snapc                      *snapclient.Clientset
		storagePolicyName          string
		clientIndex                int
		defaultDatastore           *object.Datastore
		defaultDatacenter          *object.Datacenter
		isVsanHealthServiceStopped bool
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

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		if os.Getenv("VOLUME_OPS_SCALE") != "" {
			volumeOpsScale, err = strconv.Atoi(os.Getenv(envVolumeOperationsScale))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			if vanillaCluster {
				volumeOpsScale = 25
			}
		}
		framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)

		controllerClusterConfig := os.Getenv(contollerClusterKubeConfig)
		c = client
		if controllerClusterConfig != "" {
			framework.Logf("Creating client for remote kubeconfig")
			remoteC, err := createKubernetesClientFromConfig(controllerClusterConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			c = remoteC
		}

		if guestCluster {
			storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)

			datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
			var datacenters []string
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finder := find.NewFinder(e2eVSphere.Client.Client, false)
			cfg, err := getConfig()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			dcList := strings.Split(cfg.Global.Datacenters, ",")
			for _, dc := range dcList {
				dcName := strings.TrimSpace(dc)
				if dcName != "" {
					datacenters = append(datacenters, dcName)
				}
			}

			for _, dc := range datacenters {
				defaultDatacenter, err = finder.Datacenter(ctx, dc)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				finder.SetDatacenter(defaultDatacenter)
				defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	ginkgo.AfterEach(func() {
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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

		if isVsanHealthServiceStopped {
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}
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
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] Verify snapshot dynamic provisioning "+
		"workflow", ginkgo.Label(p0, block, tkg, vanilla, snapshot, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var volHandle string
		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle = persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
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
	ginkgo.It("[block-vanilla-snapshot] Verify snapshot static provisioning through K8s "+
		"API workflow", ginkgo.Label(p0, block, vanilla, snapshot, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false

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
		snapshotCreated := true

		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
			}

			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
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
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					snapshotContentNew.ObjectMeta.Name, pandoraSyncWaitTime)
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
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, "static-vs", pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot 2 is created")
		staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(staticSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
		framework.Logf("Snapshot details is %+v", staticSnapshot)

		ginkgo.By("Deleted volume snapshot is created above")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, staticSnapshot.Name, pandoraSyncWaitTime)
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
	ginkgo.It("[block-vanilla-snapshot] Verify snapshot static provisioning "+
		"via CNS", ginkgo.Label(p0, block, vanilla, snapshot, stable), func() {

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
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
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
	ginkgo.It("[block-vanilla-snapshot] Verify snapshot static provisioning with "+
		"deletion policy Retain", ginkgo.Label(p0, block, vanilla, snapshot, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false

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
		snapshotCreated := true

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
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
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
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					snapshotContentNew.ObjectMeta.Name, pandoraSyncWaitTime)
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

		framework.Logf("Deleting volume snapshot 1")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContent.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		framework.Logf("Delete volume snapshot content 2")
		deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
			snapshotContentGetResult.ObjectMeta.Name, pandoraSyncWaitTime)
		snapshotContentCreated2 = false
	})

	/*
		Create/Delete volume snapshot with snapshot-class retention set to Retain
		1. Create a storage class and create a pvc
		2. Create a VolumeSnapshot class with deletionPoloicy to retain
		3. Create a volume-snapshot using the above snapshot-class and pvc (from step-1) as source
		4. Verify the VolumeSnashot and VolumeSnapshotClass are created
		5. Query the Snapshot from CNS side - should pass
		6. Delete the above snasphot, run a get from k8s side and ensure its removed
		7. Verify that the underlying VolumeSnapshotContent is still not deleted
		8. The backend CNS snapshot should still be present
		9. Query the Snasphot from CNS side using the volumeId
		10. Cleanup the snapshot and delete the volume
	*/
	ginkgo.It("[block-vanilla-snapshot] Verify snapshot static provisioning with deletion "+
		"policy Retain - test2", ginkgo.Label(p0, block, vanilla, snapshot, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var contentName string

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
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Retain"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a volume snapshot")
		snapshot1, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", snapshot1.Name)
		snapshotCreated := true

		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot1.Name, pandoraSyncWaitTime)
			}

			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					contentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, contentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		snapshot1_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(snapshot1_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		content, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*snapshot1_updated.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		gomega.Expect(*content.Status.ReadyToUse).To(gomega.BeTrue())
		contentName = content.Name

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle := *content.Status.SnapshotHandle
		snapshotId := strings.Split(snapshothandle, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Deleting volume snapshot 1 " + snapshot1.Name)
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot1.Name, pandoraSyncWaitTime)
		snapshotCreated = false

		_, err = snapc.SnapshotV1().VolumeSnapshots(namespace).Get(ctx, snapshot1.Name,
			metav1.GetOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Delete snapshot entry from CNS")
		err = e2eVSphere.deleteVolumeSnapshotInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume snapshot content is not deleted")
		snapshotContentGetResult, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			content.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(snapshotContentGetResult.Name).Should(gomega.Equal(content.Name))
		framework.Logf("Snapshotcontent name is  %s", snapshotContentGetResult.Name)

		framework.Logf("Delete volume snapshot content")
		deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
			snapshotContentGetResult.Name, pandoraSyncWaitTime)
		snapshotContentCreated = false

		framework.Logf("Wait till the volume snapshot content is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContentGetResult.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[block-vanilla-snapshot] Volume restore using snapshot a dynamic snapshot b "+
		"pre-provisioned snapshot", ginkgo.Label(p0, block, vanilla, snapshot, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false

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
		snapshotCreated := true

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
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
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
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					snapshotContentNew.ObjectMeta.Name, pandoraSyncWaitTime)
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
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, "static-vs", pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot 2 is created")
		staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(staticSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
		framework.Logf("Snapshot details is %+v", staticSnapshot)

		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolume2, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim2},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := persistentvolume2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvcSpec2 := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot2.Name, snapshotapigroup)

		pvclaim3, err := fpv.CreatePVC(client, namespace, pvcSpec2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolume3, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim3},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle3 := persistentvolume3[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle3).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		var pod *v1.Pod
		if windowsEnv {
			pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false,
				windowsExecRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false,
				execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var vmUUID string
		nodeName := pod.Spec.NodeName

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle2, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle2, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		var pod2 *v1.Pod
		if windowsEnv {
			pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false,
				windowsExecRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false,
				execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var vmUUID2 string
		nodeName2 := pod2.Spec.NodeName

		if vanillaCluster {
			vmUUID2 = getNodeUUID(ctx, client, pod2.Spec.NodeName)
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle3, nodeName2))
		isDiskAttached2, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle3, vmUUID2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached2).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		var cmd []string
		if windowsEnv {
			cmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "Powershell.exe", "-Command",
				"cat C:\\mnt\\volume1\\Pod1.html"}
		} else {
			cmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat /mnt/volume1/Pod1.html "}
		}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		var writecmd []string
		if windowsEnv {
			writecmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "Powershell.exe", "-Command",
				"Add-Content C:\\mnt\\volume1\\Pod1.html 'Hello message from test into Pod1'"}
		} else {
			writecmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		}
		framework.RunKubectlOrDie(namespace, writecmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		var cmd2 []string
		if windowsEnv {
			cmd2 = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "Powershell.exe", "-Command",
				"cat C:\\mnt\\volume1\\Pod1.html"}
		} else {
			cmd2 = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat /mnt/volume1/Pod1.html "}
		}
		output2 := framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output2, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		var writecmd2 []string
		if windowsEnv {
			writecmd2 = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "Powershell.exe", "-Command",
				"Add-Content C:\\mnt\\volume1\\Pod1.html 'Hello message from test into Pod1'"}
		} else {
			writecmd2 = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		}
		framework.RunKubectlOrDie(namespace, writecmd2...)
		output2 = framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output2, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Create a volume snapshot")
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

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle3 := *snapshotContent3.Status.SnapshotHandle
		snapshotId3 := strings.Split(snapshothandle3, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle2, snapshotId3, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleted volume snapshot is created above")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot3.Name, pandoraSyncWaitTime)
		snapshotCreated3 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContent3.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated3 = false

		ginkgo.By("Deleted volume snapshot is created above")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, staticSnapshot.Name, pandoraSyncWaitTime)
		snapshotCreated2 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContentNew.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*

	   Snapshot creation and restore workflow verification with xfs filesystem
	   1. Create a storage class with fstype set to XFS and create a pvc using this sc
	   2. Create a pod which uses above PVC
	   3. Create file1.txt data at mountpath
	   4. Create a VolumeSnapshotClass with snapshotter as vsphere-csi-driver and set deletionPolicy to Delete
	   5. Create a VolumeSnapshot with labels, using the above snapshot-class and pvc (from step-1) as source
	   6. Ensure the snapshot is created, verify using get VolumeSnapshot
	   7. Also verify that VolumeSnapshotContent is auto created
	   8. Verify that the VolumeSnapshot has ReadyToUse set to True
	   9. Query the snapshot from CNS side using volume id to ensure that snapshot is created
	   10. Create new PVC using above snapshot as source (restore operation)
	   11. Ensure the PVC gets provisioned and is Bound
	   12. Attach this PVC to a pod on the same node where source volume is mounted
	   13. Ensure that file1.txt from snapshot is available
	   14. And write new file file2.txt to the restored volume and it should succeed
	   15. Delete the VolumeSnapshot, PVCs and pods created in above steps and ensure it is removed
	   16. Query the snapshot from CNS side - it shouldn't be available
	   17. Delete SC and VolumeSnapshotClass
	*/
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] Volume snapshot creation and restoration workflow "+
		"with xfs filesystem", ginkgo.Label(p0, block, vanilla, tkg, snapshot, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		scParameters[scParamFsType] = xfsFSType

		ginkgo.By("Create storage class with xfs filesystem and create PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "", diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

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

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify volume is attached to the node
		var vmUUID string
		nodeName := pod.Spec.NodeName

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Verify filesystem used to mount volume inside pod is xfs
		ginkgo.By("Verify that filesystem type is xfs as expected")
		_, err = framework.LookForStringInPodExec(namespace, pod.Name, []string{"/bin/cat", "/mnt/volume1/fstype"},
			xfsFSType, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create file1.txt at mountpath inside pod
		ginkgo.By(fmt.Sprintf("Creating file file1.txt at mountpath inside pod: %v", pod.Name))
		data1 := "This file file1.txt is written by Pod1"
		filePath1 := "/mnt/volume1/file1.txt"
		writeDataOnFileFromPod(namespace, pod.Name, filePath1, data1)

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Restore snapshot to new PVC")
		pvclaim2, persistentVolumes2, _ := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, false)
		volHandle2 := persistentVolumes2[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
		}
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating a pod to attach restored PV on the same node where earlier pod is running")
		nodeSelector := make(map[string]string)
		nodeSelector["kubernetes.io/hostname"] = nodeName
		pod2, err := createPod(client, namespace, nodeSelector, []*v1.PersistentVolumeClaim{pvclaim2}, false,
			execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify that new pod is scheduled on same node where earlier pod is running")
		nodeName2 := pod2.Spec.NodeName
		gomega.Expect(nodeName == nodeName2).To(gomega.BeTrue(), "Pod is not scheduled on expected node")

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod2.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod2.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle2, nodeName2))
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volHandle2, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Verify filesystem used to mount volume inside pod is xfs
		ginkgo.By("Verify that filesystem type is xfs inside pod which is using restored PVC")
		_, err = framework.LookForStringInPodExec(namespace, pod2.Name, []string{"/bin/cat", "/mnt/volume1/fstype"},
			xfsFSType, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Ensure that file1.txt is available as expected on the restored PVC
		ginkgo.By("Verify that file1.txt data is available as part of snapshot")
		output := readFileFromPod(namespace, pod2.Name, filePath1)
		gomega.Expect(output == data1+"\n").To(gomega.BeTrue(),
			"Pod2 is not able to read file1.txt written before snapshot creation")

		// Create new file file2.txt at mountpath inside pod
		ginkgo.By(fmt.Sprintf("Creating file file2.txt at mountpath inside pod: %v", pod2.Name))
		data2 := "This file file2.txt is written by Pod2"
		filePath2 := "/mnt/volume1/file2.txt"
		writeDataOnFileFromPod(namespace, pod2.Name, filePath2, data2)

		ginkgo.By("Verify that file2.txt data can be successfully read")
		output = readFileFromPod(namespace, pod2.Name, filePath2)
		gomega.Expect(output == data2+"\n").To(gomega.BeTrue(), "Pod2 is not able to read file2.txt")

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Volume restore using snapshot on a different storageclass
		1. Create a sc with thin-provisioned spbm policy, create a pvc and attach the pvc to a pod
		2. Create a dynamically provisioned snapshots using this pvc
		3. create another sc pointing to a different spbm policy (say thick)
		4. Run a restore workflow by giving a different storageclass in the pvc spec
		5. the new storageclass would point to a thick provisioned spbm plocy,
			while the source pvc was created usig thin provisioned psp-operatorlicy
		6. cleanup spbm policies, sc's, pvc's
	*/
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] Volume restore using snapshot on a different "+
		"storageclass", ginkgo.Label(p0, block, vanilla, snapshot, tkg, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		scParameters1 := make(map[string]string)
		scParameters1[scParamStoragePolicyName] = "Management Storage Policy - Regular"

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "snapshot" + curtimestring + val
		var storageclass1 *storagev1.StorageClass

		if vanillaCluster {
			storageclass1, err = createStorageClass(client, scParameters1, nil, "", "", false, scName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if guestCluster {
			scName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores2)
			storageclass1, err = client.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
		}

		pvclaim2, persistentVolumes2, _ := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass1, volumeSnapshot, diskSize, false)
		volHandle2 := persistentVolumes2[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
		}
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Testcase-11
		Delete the namespace hosting the pvcs and volume-snapshots and recover the data using snapshot-content
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
	ginkgo.It("[block-vanilla-snapshot][tkg-snapshot] Delete the namespace hosting the pvcs and "+
		"volume-snapshots and recover the data "+
		"using snapshot-content", ginkgo.Label(p0, block, vanilla, snapshot, tkg, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var snapshotContentCreated2 = false
		var snapshotId, snapshotHandle, claimPolicy string
		var snapshotcontent2 *snapV1.VolumeSnapshotContent
		var svcVolumeSnapshotName, staticSnapshotId string

		ginkgo.By("Creating new namespace for the test")
		namespace1, err := framework.CreateTestingNS(f.BaseName, client, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		newNamespaceName := namespace1.Name
		isNamespaceDeleted := false

		defer func() {
			if !isNamespaceDeleted {
				framework.Logf("Collecting new GC namespace before performing PV/PVC cleanup")
				newEventList, err := client.CoreV1().Events(newNamespaceName).List(ctx, metav1.ListOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, item := range newEventList.Items {
					framework.Logf(fmt.Sprintf(item.Message))
				}
				ginkgo.By("Delete namespace")
				err = client.CoreV1().Namespaces().Delete(ctx, newNamespaceName, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isNamespaceDeleted = true
			}
		}()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		if vanillaCluster {
			ginkgo.By("Create storage class and PVC")
			scParameters[scParamDatastoreURL] = datastoreURL
			storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			ginkgo.By("Get storage class and create PVC")
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By("Create PVC")
		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, newNamespaceName, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			if !isNamespaceDeleted {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, newNamespaceName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		if vanillaCluster {
			claimPolicy = retainClaimPolicy
		} else if guestCluster {
			claimPolicy = deletionPolicy
		}

		ginkgo.By("Create/Get volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, claimPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if vanillaCluster {
				ginkgo.By("Delete volume snapshot class")
				err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx,
					volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, newNamespaceName, snapc,
			volumeSnapshotClass, pvclaim, volHandle, diskSize, true)
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)
		snapshotContentName := snapshotContent.Name

		if guestCluster {
			framework.Logf("Get volume snapshot handle from Supervisor Cluster")
			staticSnapshotId, _, svcVolumeSnapshotName, err = getSnapshotHandleFromSupervisorCluster(ctx, volumeSnapshotClass,
				*snapshotContent.Status.SnapshotHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("svcVolumeSnapshotName: %s", svcVolumeSnapshotName)
			framework.Logf("Change the deletion policy of VolumeSnapshotContent from Delete to Retain " +
				"in Guest Cluster")
			refreshedSnapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx, snapshotContent.Name,
				metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotContent, err = changeDeletionPolicyOfVolumeSnapshotContent(ctx, refreshedSnapshotContent,
				snapc, snapV1.VolumeSnapshotContentRetain)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			if !isNamespaceDeleted {
				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, newNamespaceName, volumeSnapshot.Name, pandoraSyncWaitTime)
				}
				if snapshotContentCreated {
					framework.Logf("Deleting volume snapshot content")
					deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
						*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

					framework.Logf("Wait till the volume snapshot content is deleted")
					err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		ginkgo.By("Delete namespace")
		err = client.CoreV1().Namespaces().Delete(ctx, newNamespaceName, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isNamespaceDeleted = true

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Verify volume snapshot is deleted")
		_, err = snapc.SnapshotV1().VolumeSnapshots(newNamespaceName).Get(ctx,
			volumeSnapshot.Name, metav1.GetOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		snapshotCreated = false

		if vanillaCluster {
			_, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				snapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if guestCluster {
			framework.Logf("Delete VolumeSnapshotContent from Guest Cluster explicitly")
			err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, newNamespaceName, pandoraSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating another new namespace for the test")
		namespace2, err := framework.CreateTestingNS(f.BaseName, client, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		f.AddNamespacesToDelete(namespace2)
		namespace2Name := namespace2.Name

		if vanillaCluster {
			snapshotHandle = volHandle + "+" + snapshotId
			ginkgo.By(fmt.Sprintf("Creating volume snapshot content by snapshotHandle %s", snapshotHandle))
			snapshotcontent2, err = snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
				getVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Delete"), snapshotHandle,
					"static-vsc-cns", namespace2Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if guestCluster {
			framework.Logf(fmt.Sprintf("Creating static VolumeSnapshotContent in Guest Cluster using "+
				"supervisor VolumeSnapshotName %s", svcVolumeSnapshotName))
			snapshotcontent2, err = snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
				getVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Delete"), svcVolumeSnapshotName,
					"static-vsc-cns", namespace2Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify volume snapshot content is created or not")
		snapshotcontent2, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotcontent2.ObjectMeta.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = true
		framework.Logf("Snapshotcontent name is  %s", snapshotcontent2.ObjectMeta.Name)

		snapshotcontent2, err = waitForVolumeSnapshotContentReadyToUse(*snapc, ctx, snapshotcontent2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if snapshotContentCreated2 {
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					snapshotcontent2.ObjectMeta.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Create a volume snapshot by snapshotcontent")
		volumeSnapshot2, err := snapc.SnapshotV1().VolumeSnapshots(namespace2Name).Create(ctx,
			getVolumeSnapshotSpecByName(namespace2Name, "static-vsc-cns",
				snapshotcontent2.Name), metav1.CreateOptions{})
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
		volumeSnapshot2, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace2Name,
			volumeSnapshot2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(volumeSnapshot2.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
		framework.Logf("Snapshot details is %+v", volumeSnapshot2)
		snapshotContentCreated2 = true

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

		/*defer func() {
			framework.Logf("Deleting restored PVC")
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace2Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()*/

		if vanillaCluster {
			ginkgo.By("Deleted volume snapshot is created above")
			deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace2Name, volumeSnapshot2.Name, pandoraSyncWaitTime)
			snapshotCreated2 = false

			framework.Logf("Wait till the volume snapshot content is deleted")
			deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
				volumeSnapshot2.ObjectMeta.Name, pandoraSyncWaitTime)

			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, volumeSnapshot2.ObjectMeta.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotContentCreated2 = false

			framework.Logf("Deleting volume snapshot content 1")
			deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
				snapshotContent.Name, pandoraSyncWaitTime)

			err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
				snapshotContent.ObjectMeta.Name, pandoraSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotContentCreated = false

			ginkgo.By("Verify snapshot entry is deleted from CNS")
			err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if guestCluster {
			framework.Logf("Deleting volume snapshot 2: %s", volumeSnapshot2.Name)
			snapshotCreated2, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshot2, pandoraSyncWaitTime, volHandle, staticSnapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

	/*
		Delete a non-existent snapshot
		1. Create pvc and volumesnapshot using the pvc
		2. Verify that a snapshot using CNS querySnapshots API
		3. Delete the snapshot using CNS deleteSnapshots API
		4. Try deleting the volumesnapshot from k8s side
		5. Delete should fail with appropriate error as the backend snapshot is missing
		6. Delete would return a pass from CSI side (this is expected because CSI is
			designed to return success even though it cannot find a snapshot in the backend)
	*/
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] Delete a non-existent snapshot", ginkgo.Label(p0, block,
		vanilla, snapshot, tkg, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Delete snapshot from CNS")
		err = e2eVSphere.deleteVolumeSnapshotInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = false

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Create snapshots using default VolumeSnapshotClass
		1. Create a VolumeSnapshotClass and set it as default
		2. Create a snapshot without providing the snapshotClass input and
			ensure the default class is picked and honored for snapshot creation
		3. Validate the fields after snapshot creation succeeds (snapshotClass, retentionPolicy)
	*/
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] Create snapshots using default "+
		"VolumeSnapshotClass", ginkgo.Label(p0, block, vanilla, snapshot, tkg), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var volumeSnapshotClass *snapV1.VolumeSnapshotClass

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		if vanillaCluster {
			vscSpec := getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil)
			vscSpec.ObjectMeta.Annotations = map[string]string{
				"snapshot.storage.kubernetes.io/is-default-class": "true",
			}
			volumeSnapshotClass, err = snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
				vscSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if guestCluster {
			restConfig = getRestConfigClient()
			snapc, err = snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vscSpec := getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil)
			vscSpec.ObjectMeta.Annotations = map[string]string{
				"snapshot.storage.kubernetes.io/is-default-class": "true",
			}
			volumeSnapshotClass, err = snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
				vscSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}
		defer func() {
			if guestCluster {
				restConfig = getRestConfigClient()
				snapc, err = snapclient.NewForConfig(restConfig)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		if guestCluster {
			guestClusterRestConfig = getRestConfigClientForGuestCluster(guestClusterRestConfig)
			snapc, err = snapclient.NewForConfig(guestClusterRestConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Create Volume from snapshot with different size (high and low)
		1. Restore operation (or creation of volume from snapshot) should pass
			only if the volume size is same as that of the snapshot being used
		2. If a different size (high or low) is provided the pvc creation should fail with error
		3. Verify the error
		4. Create with exact size and ensure it succeeds
	*/
	ginkgo.It("[block-vanilla-snapshot][tkg-snapshot] Create Volume from snapshot with "+
		"different size", ginkgo.Label(p1, block, vanilla, snapshot, tkg, stable, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Create PVC using the higher size")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, defaultrqLimit, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)
		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expecting the volume bound to fail")
		_, err = fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim2},
			framework.ClaimProvisionShortTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())
		pvc2Deleted := false
		defer func() {
			if !pvc2Deleted {
				ginkgo.By("Delete the PVC in defer func")
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete the PVC-2")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc2Deleted = true

		if guestCluster {
			framework.Logf("Deleting pending PVCs from SVC namespace")
			pvcList := getAllPVCFromNamespace(svcClient, svcNamespace)
			for _, pvc := range pvcList.Items {
				if pvc.Status.Phase == v1.ClaimPending {
					framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(svcClient, pvc.Name, svcNamespace),
						"Failed to delete PVC", pvc.Name)
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Snapshot workflow for statefulsets
	   1. Create a statefulset with 3 replicas using a storageclass with volumeBindingMode set to Immediate
	   2. Wait for pvcs to be in Bound state
	   3. Wait for pods to be in Running state
	   4. Create snapshot on 3rd replica's pvc (pvc as source)
	   5. Scale down the statefulset to 2
	   6. Delete the pvc on which snapshot was created
	   7. PVC delete succeeds but PV delete will fail as there is snapshot - expected
	   8. Create a new PVC with same name (using the snapshot from step-4 as source) - verify a new PV is created
	   9. Scale up the statefulset to 3
	   10. Verify if the new pod attaches to the PV created in step-8
	   11. Cleanup the sts and the snapshot + pv that was left behind in step-7
	*/
	ginkgo.It("[block-vanilla-snapshot][tkg-snapshot] Snapshot workflow for statefulsets", ginkgo.Label(p0, block,
		vanilla, snapshot, tkg), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "nginx-sc-default-" + curtimestring + val
		var snapshotId1, snapshotId2 string
		var snapshotContentCreated, snapshotCreated bool

		if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		scSpec := getVSphereStorageClassSpec(scName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &scName
		*statefulset.Spec.Replicas = 2
		if windowsEnv {
			statefulset.Spec.Template.Spec.Containers[0].Image = windowsLTSC2019Image
			statefulset.Spec.Template.Spec.Containers[0].Command = []string{"Powershell.exe"}
			statefulset.Spec.Template.Spec.Containers[0].Args = []string{"-Command", windowsPodCmd}
		}
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		if windowsEnv {
			framework.Logf("Not required for windows")
		} else {
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		}
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		framework.Logf("Fetching pod 1, pvc1 and pv1 details")
		pod1, err := client.CoreV1().Pods(namespace).Get(ctx,
			ssPodsBeforeScaleDown.Items[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc1 := pod1.Spec.Volumes[0].PersistentVolumeClaim

		pvclaim1, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
			pvc1.ClaimName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv1 := getPvFromClaim(client, statefulset.Namespace, pvc1.ClaimName)
		volHandle1 := pv1.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle1).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle1 = getVolumeIDFromSupervisorCluster(volHandle1)
		}

		// Verify the attached volume match the one in CNS cache
		if !guestCluster {
			err = verifyVolumeMetadataInCNS(&e2eVSphere, pv1.Spec.CSI.VolumeHandle,
				pvc1.ClaimName, pv1.ObjectMeta.Name, pod1.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		framework.Logf("Fetching pod 2, pvc2 and pv2 details")
		pod2, err := client.CoreV1().Pods(namespace).Get(ctx,
			ssPodsBeforeScaleDown.Items[1].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc2 := pod2.Spec.Volumes[0].PersistentVolumeClaim

		pvclaim2, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
			pvc2.ClaimName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv2 := getPvFromClaim(client, statefulset.Namespace, pvc2.ClaimName)
		volHandle2 := pv2.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
		}

		// Verify the attached volume match the one in CNS cache
		if !guestCluster {
			err = verifyVolumeMetadataInCNS(&e2eVSphere, pv2.Spec.CSI.VolumeHandle,
				pvc2.ClaimName, pv2.ObjectMeta.Name, pod2.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot - 1")
		volumeSnapshot1, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim1.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot1.Name)
		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot1.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot1, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = true
		gomega.Expect(volumeSnapshot1.Status.RestoreSize.Cmp(resource.MustParse("1Gi"))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent1, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		gomega.Expect(*snapshotContent1.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		if vanillaCluster {
			snapshothandle1 := *snapshotContent1.Status.SnapshotHandle
			snapshotId1 = strings.Split(snapshothandle1, "+")[1]

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = verifySnapshotIsCreatedInCNS(volHandle1, snapshotId1, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if guestCluster {
			snapshothandle1 := *snapshotContent1.Status.SnapshotHandle
			snapshotId1, _, _, err = getSnapshotHandleFromSupervisorCluster(ctx, volumeSnapshotClass,
				snapshothandle1)
		}

		ginkgo.By("Create a volume snapshot - 2")
		volumeSnapshot2, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim2.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot2.Name)

		var snapshotCreated2 bool
		var snapshotContentCreated2 bool
		defer func() {
			if snapshotContentCreated2 {
				framework.Logf("Deleting volume snapshot content")
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot2.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot2, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated2 = true
		gomega.Expect(volumeSnapshot2.Status.RestoreSize.Cmp(resource.MustParse("1Gi"))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent2, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = true
		gomega.Expect(*snapshotContent2.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		if vanillaCluster {
			snapshothandle2 := *snapshotContent2.Status.SnapshotHandle
			snapshotId2 = strings.Split(snapshothandle2, "+")[1]

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = verifySnapshotIsCreatedInCNS(volHandle2, snapshotId2, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if guestCluster {
			snapshothandle2 := *snapshotContent2.Status.SnapshotHandle
			snapshotId2, _, _, err = getSnapshotHandleFromSupervisorCluster(ctx, volumeSnapshotClass,
				snapshothandle2)
		}

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas-1))
		_, scaledownErr := fss.Scale(client, statefulset, replicas-1)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas-1)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas-1)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
		replicas -= 1

		var pvcToDelete *v1.PersistentVolumeClaim
		var snapshotToBeDeleted *snapV1.VolumeSnapshot

		//var volId string
		//Find the missing pod and check if the cnsvolumemetadata is deleted or not
		if ssPodsAfterScaleDown.Items[0].Name == pod1.Name {
			pvcToDelete = pvclaim2
			snapshotToBeDeleted = volumeSnapshot2

		} else {
			pvcToDelete = pvclaim1
			snapshotToBeDeleted = volumeSnapshot1
		}

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
			snapshotToBeDeleted.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Wait till the volume snapshot content is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*snapshotToBeDeleted.Status.BoundVolumeSnapshotContentName)
		if err != nil {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		err = fpv.DeletePersistentVolumeClaim(client, pvcToDelete.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create a new PVC")
		pvcSpec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "1Gi", sc, nil, v1.ReadWriteOnce)

		pvcSpec.Name = pvcToDelete.Name
		pvclaim3, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expecting the volume to bound")
		newPV, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim3},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volHandleOfNewPV := newPV[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandleOfNewPV).NotTo(gomega.BeEmpty())

		replicas += 1
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		time.Sleep(5 * time.Minute)
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)

		ginkgo.By("Delete volume snapshot 1 and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
			volumeSnapshot1.Name, metav1.DeleteOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		snapshotCreated = false

		ginkgo.By("Delete volume snapshot 2 and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
			volumeSnapshot2.Name, metav1.DeleteOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		snapshotCreated2 = false

		framework.Logf("Wait till the volume snapshot content is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot1.Status.BoundVolumeSnapshotContentName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		snapshotContentCreated = false

		framework.Logf("Wait till the volume snapshot content 1 is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot2.Status.BoundVolumeSnapshotContentName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		snapshotContentCreated2 = false

		if !guestCluster {
			ginkgo.By("Verify snapshot 1 entry is deleted from CNS")
			err = verifySnapshotIsDeletedInCNS(volHandle1, snapshotId1, false)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

			ginkgo.By("Verify snapshot 2 entry is deleted from CNS")
			err = verifySnapshotIsDeletedInCNS(volHandle2, snapshotId2, false)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}
	})

	/*
		Volume deletion with existing snapshots
		1. Create a sc, and a pvc using this sc
		2. Create a dynamic snapshot using above pvc as source
		3. Delete this pvc, expect the pvc to be deleted successfully
		4. Underlying pv should not be deleted and should have a valid error
			calling out that the volume has active snapshots
			(note: the storageclass here is set to Delete retentionPolicy)
		5. Expect VolumeFailedDelete error with an appropriate err-msg
		6. Run cleanup - delete the snapshots and then delete pv
	*/
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] Volume deletion with existing snapshots", ginkgo.Label(p0,
		block, vanilla, snapshot, tkg, stable, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentvolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolume(client, persistentvolumes[0].Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Delete PVC before deleting the snapshot")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Get PV and check the PV is still not deleted")
		_, err = client.CoreV1().PersistentVolumes().Get(ctx, persistentvolumes[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete PV")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
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
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] Create a pre-provisioned snapshot using "+
		"VolumeSnapshotContent as source", ginkgo.Label(p1, block, vanilla, snapshot, tkg, stable,
		negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "", diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
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
	ginkgo.It("[block-vanilla-snapshot] Pre-provisioned snapshot using incorrect/non-existing "+
		"static snapshot", ginkgo.Label(p0, block, vanilla, snapshot, negative), func() {

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
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
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
	ginkgo.It("[block-vanilla-snapshot] Create a volume from a snapshot that is still not "+
		"ready-to-use", ginkgo.Label(p0, block, vanilla, snapshot, stable, negative), func() {

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
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
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
		Snapshot workflow for deployments
		1. Create a deployment with 1 replicas (dep-1)
		2. Write a file on each replica's pvc
		3. Create snapshots on all pvcs of the deployments
		4. Create another deployment (dep-2) such that the pvc's of this deployment
			points to the pvc whose source is the snapshot created in step.3
		5. Delete dep-1
		6. The deployment should succeed and should have the file that was created in step.2
		7. Cleanup dep-1 pv snapshots and pvs, delete dep-2
	*/
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] Snapshot workflow for deployments", ginkgo.Label(p0,
		block, vanilla, tkg, snapshot, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "", diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		ginkgo.By("Creating a Deployment using pvc1")
		var dep *appsv1.Deployment
		if windowsEnv {
			dep, err = createDeployment(ctx, client, 1, labelsMap, nil, namespace,
				[]*v1.PersistentVolumeClaim{pvclaim}, windowsExecRWXCommandPod1, false, windowsLTSC2019Image)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			dep, err = createDeployment(ctx, client, 1, labelsMap, nil, namespace,
				[]*v1.PersistentVolumeClaim{pvclaim}, execRWXCommandPod1, false, busyBoxImageOnGcr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("Delete Deployment")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for deployment pods to be up and running")
		pods, err := fdep.GetPodsForDeployment(client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Create a PVC using the snapshot created above")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)
		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		labelsMap2 := make(map[string]string)
		labelsMap2["app2"] = "test2"

		var dep2 *appsv1.Deployment
		if windowsEnv {
			dep, err = createDeployment(ctx, client, 1, labelsMap2, nil, namespace,
				[]*v1.PersistentVolumeClaim{pvclaim2}, "", false, windowsLTSC2019Image)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			dep, err = createDeployment(ctx, client, 1, labelsMap2, nil, namespace,
				[]*v1.PersistentVolumeClaim{pvclaim2}, "", false, busyBoxImageOnGcr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("Delete Deployment-2")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, dep2.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for deployment pods to be up and running")
		pods2, err := fdep.GetPodsForDeployment(client, dep2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod2 := pods2.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		var cmd []string
		if windowsEnv {
			cmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "Powershell.exe", "-Command",
				"cat C:\\mnt\\volume1\\Pod1.html"}
		} else {
			cmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat /mnt/volume1/Pod1.html "}
		}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Volume resize of a volume having snapshots
		1. Create a pvc (say size=2GB)
		2. Resize this volume (to size=4GB) - should succeed - offline resize
		3. Create a few snapshots for this volume - should succeed
		4. Resize the volume (to say size=6GB) - this operation should fail and verify the error returned
		5. Delete the snapshots
		6. Run resize and it should succeed
		7. Cleanup the pvc
	*/
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] Volume offline resize of a volume "+
		"having snapshots", ginkgo.Label(p0, block, vanilla, tkg, snapshot, stable, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if volHandle != "" {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("4Gi"))
		newDiskSize := "6Gi"
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client,
			namespace, pvclaim.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(6144)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb !=
			newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion +%v ",
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, newDiskSize, true)
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Expanding current pvc before deleting volume snapshot")
		currentPvcSize = pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize = currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("6Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		_, err = expandPVCSize(pvclaim, newSize, client)
		ginkgo.By("Snapshot webhook does not allow volume expansion on PVC")
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expanding current pvc after deleting volume snapshot")
		currentPvcSize = pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize = currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("6Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize = pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client,
			namespace, pvclaim.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb = int64(12288)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb !=
			newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion +%v ",
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Volume online resize, and snapshots
		1. Create a pvc and attach the pvc to a pod
		2. Resize the volume (to 4GB frm 2GB) - should succeed - online resize
		3. Create a few snapshots for this volume - should succeed
		4. Resize the volume (to say size=6GB) - this operation should fail and verify the error returned
		5. Delete the snapshots
		6. Run resize and it should succeed
		7. Cleanup the pvc
	*/
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] Volume online resize of a volume having "+
		"snapshots", ginkgo.Label(p0, block, vanilla, tkg, snapshot, stable, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "", diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if volHandle != "" {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating pod to attach PV to the node")
		var pod *v1.Pod
		if windowsEnv {
			pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
				false, windowsExecRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
				false, execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			// Delete POD
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

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Modify PVC spec to trigger volume expansion
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("4Gi"))
		newDiskSize := "6Gi"
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		claims, err := expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(claims).NotTo(gomega.BeNil())

		ginkgo.By("Waiting for file system resize to finish")
		claims, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := claims.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(6144)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb !=
			newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion +%v ",
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, newDiskSize, true)
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		// Modify PVC spec to trigger volume expansion
		currentPvcSize = claims.Spec.Resources.Requests[v1.ResourceStorage]
		newSize = currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("6Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		_, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for file system resize to finish")
		claims, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions = claims.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb = int64(6144)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb !=
			newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion +%v ",
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expanding current pvc")
		currentPvcSize = claims.Spec.Resources.Requests[v1.ResourceStorage]
		newSize = currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("6Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client,
			namespace, pvclaim.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb = int64(12288)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb !=
			newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion +%v ",
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Snapshot restore while the Host is Down

		1. Create a sc (vsan-default) and create a pvc using this sc
		2. Check the host on which the pvc is placed
		3. Create snapshots for this pvc
		4. verify snapshots are created successfully and bound
		5. bring the host on which the pvc was placed down
		6. use the snapshot as source to create a new volume and
			it should succeed and pvc should come to Bound state
		7. bring the host back up
		8. cleanup the snapshots, restore-pvc and source-pvc
	*/
	ginkgo.It("[block-vanilla-snapshot] Snapshot restore while the Host "+
		"is Down", ginkgo.Label(p0, block, vanilla, snapshot, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
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
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx,
				volumeSnapshotClass.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a volume snapshot")
		snapshot1, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", snapshot1.Name)

		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot1.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot is Ready to use")
		snapshot1_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = true
		gomega.Expect(snapshot1_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent1, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*snapshot1_updated.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(*snapshotContent1.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle := *snapshotContent1.Status.SnapshotHandle
		snapshotId := strings.Split(snapshothandle, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Identify the host on which the PV resides")
		framework.Logf("pvName %v", persistentvolumes[0].Name)
		vsanObjuuid := VsanObjIndentities(ctx, &e2eVSphere, persistentvolumes[0].Name)
		framework.Logf("vsanObjuuid %v", vsanObjuuid)
		gomega.Expect(vsanObjuuid).NotTo(gomega.BeNil())

		ginkgo.By("Get host info using queryVsanObj")
		hostInfo := queryVsanObj(ctx, &e2eVSphere, vsanObjuuid)
		framework.Logf("vsan object ID %v", hostInfo)
		gomega.Expect(hostInfo).NotTo(gomega.BeEmpty())
		hostIP := e2eVSphere.getHostUUID(ctx, hostInfo)
		framework.Logf("hostIP %v", hostIP)
		gomega.Expect(hostIP).NotTo(gomega.BeEmpty())

		ginkgo.By("Stop hostd service on the host on which the PV is present")
		stopHostDOnHost(ctx, hostIP)

		defer func() {
			ginkgo.By("Start hostd service on the host on which the PV is present")
			startHostDOnHost(ctx, hostIP)
		}()

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, snapshot1.Name, snapshotapigroup)

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

		ginkgo.By("Start hostd service on the host on which the PV is present")
		startHostDOnHost(ctx, hostIP)
	})

	/*
	   VC reboot with deployment pvcs having snapshot
	   1. Create a sc and create 30 pvc's using this sc
	   2. Create a deployment using 3 replicas and pvc's pointing to above
	   3. Write some files to these PVCs
	   4. Create snapshots on all the replica PVCs
	   5. Reboot the VC
	   6. Ensure the deployment comes up fine and data is available and we can write more data
	   7. Create a new deployment, by creating new volumes using the snapshots cut prior to reboot
	   8. Ensure the data written in step-4 is intanct
	   9. Delete both deployments and. the pvcs
	*/
	ginkgo.It("[block-vanilla-snapshot][tkg-snapshot] VC reboot with deployment pvcs "+
		"having snapshot", ginkgo.Label(p0, block, vanilla, tkg, snapshot, disruptive, flaky), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var restoredpvclaims []*v1.PersistentVolumeClaim
		var volumesnapshots []*snapV1.VolumeSnapshot
		var volumesnapshotsReadytoUse []*snapV1.VolumeSnapshot

		ginkgo.By("Create storage class and PVC")
		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "snapshot" + curtimestring + val
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, scName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating PVCs using the Storage Class")
		framework.Logf("VOLUME_OPS_SCALE is set to %v", 5)
		for i := 0; i < 5; i++ {
			framework.Logf("Creating pvc%v", i)
			accessMode := v1.ReadWriteOnce

			pvclaim, err = createPVC(client, namespace, nil, "", storageclass, accessMode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaims = append(pvclaims, pvclaim)
		}

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		ginkgo.By("Creating a Deployment using pvc1")

		var dep *appsv1.Deployment
		if windowsEnv {
			dep, err = createDeployment(ctx, client, 1, labelsMap, nil, namespace,
				pvclaims, windowsExecRWXCommandPod1, false, windowsLTSC2019Image)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			dep, err = createDeployment(ctx, client, 1, labelsMap, nil, namespace,
				[]*v1.PersistentVolumeClaim{pvclaim}, execRWXCommandPod1, false, busyBoxImageOnGcr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("Delete Deployment")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for deployment pods to be up and running")
		pods, err := fdep.GetPodsForDeployment(client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot")
		for _, claim := range pvclaims {
			volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, claim.Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)
			volumesnapshots = append(volumesnapshots, volumeSnapshot)
		}

		defer func() {
			ginkgo.By("Rebooting VC")
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			err = invokeVCenterReboot(vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitForHostToBeUp(e2eVSphere.Config.Global.VCenterHostname)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Done with reboot")
			essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
			checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

			// After reboot.
			bootstrap()

			framework.Logf("Deleting volume snapshot")
			for _, snapshot := range volumesnapshots {
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Rebooting VC")
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterReboot(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(e2eVSphere.Config.Global.VCenterHostname)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")
		essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		// After reboot.
		bootstrap()

		fullSyncWaitTime := 0

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Full sync interval can be 1 min at minimum so full sync wait time
			// has to be more than 120s.
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}

		ginkgo.By(fmt.Sprintf("Double Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(2*fullSyncWaitTime) * time.Second)

		ginkgo.By("Verify volume snapshot is created")
		for _, snapshot := range volumesnapshots {
			snapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(snapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
			volumesnapshotsReadytoUse = append(volumesnapshotsReadytoUse, snapshot)
		}

		ginkgo.By("Verify volume snapshot content is created")
		for _, snaps := range volumesnapshotsReadytoUse {
			snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*snaps.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(*snapshotContent.Status.ReadyToUse).To(gomega.BeTrue())
		}

		ginkgo.By("Create a PVC using the snapshot created above")
		for _, snapshot := range volumesnapshots {
			pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
				v1.ReadWriteOnce, snapshot.Name, snapshotapigroup)
			pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			restoredpvclaims = append(restoredpvclaims, pvclaim2)

			persistentvolume2, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim2},
				framework.ClaimProvisionTimeout*2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle2 := persistentvolume2[0].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		}

		defer func() {
			for _, restoredpvc := range restoredpvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, restoredpvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Rebooting VC")
		err = invokeVCenterReboot(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(e2eVSphere.Config.Global.VCenterHostname)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		// After reboot.
		bootstrap()

		labelsMap2 := make(map[string]string)
		labelsMap2["app2"] = "test2"

		var dep2 *appsv1.Deployment
		if windowsEnv {
			dep, err = createDeployment(ctx, client, 1, labelsMap2, nil, namespace,
				restoredpvclaims, "", false, windowsLTSC2019Image)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			dep, err = createDeployment(ctx, client, 1, labelsMap2, nil, namespace,
				restoredpvclaims, "", false, busyBoxImageOnGcr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("Delete Deployment-2")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, dep2.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for deployment pods to be up and running")
		pods2, err := fdep.GetPodsForDeployment(client, dep2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod2 := pods2.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		var cmd []string
		if windowsEnv {
			cmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "Powershell.exe", "-Command",
				"cat C:\\mnt\\volume1\\Pod1.html"}
		} else {
			cmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat /mnt/volume1/Pod1.html "}
		}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())
	})

	/*
	   VC password reset during snapshot creation
	   1. Create a sc and pvc using this sc
	   2. Create a volume snapshot using pvc as source
	   3. Verify snapshot created successfully
	   4. Change the VC administrator account password
	   5. Create another snapshot - creation succeeds with previous csi session
	   6. Update the vsphere.conf and the secret under vmware-system-csi ns and wait for 1-2 mins
	   7. Create snapshot should succeed
	   8. Delete snapshot
	   9. Cleanup pvc/sc
	*/
	ginkgo.It("[block-vanilla-snapshot] VC password reset during snapshot creation", ginkgo.Label(p1, block,
		vanilla, snapshot, disruptive), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotCreated = false
		var snapshot3Created = false
		nimbusGeneratedVcPwd := GetAndExpectStringEnvVar(nimbusVcPwd)

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
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, (2 * framework.ClaimProvisionTimeout))
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

		ginkgo.By("Create a volume snapshot")
		snapshot1, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", snapshot1.Name)

		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot1.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot is Ready to use")
		snapshot1_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = true
		gomega.Expect(snapshot1_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent1, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*snapshot1_updated.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(*snapshotContent1.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle := *snapshotContent1.Status.SnapshotHandle
		snapshotId := strings.Split(snapshothandle, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetching the username and password of the current vcenter session from secret")
		secret, err := c.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		originalConf := string(secret.Data[vSphereCSIConf])
		vsphereCfg, err := readConfigFromSecretString(originalConf)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Changing password on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		username := vsphereCfg.Global.User
		originalPassword := vsphereCfg.Global.Password
		newPassword := e2eTestPassword
		ginkgo.By(fmt.Sprintf("Original password %s, new password %s", originalPassword, newPassword))
		err = invokeVCenterChangePassword(username, nimbusGeneratedVcPwd, newPassword, vcAddress,
			false, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		originalVCPasswordChanged := true

		defer func() {
			if originalVCPasswordChanged {
				ginkgo.By("Reverting the password change")
				err = invokeVCenterChangePassword(username, nimbusGeneratedVcPwd, originalPassword,
					vcAddress, false, clientIndex)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot2")
		snapshot2, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshot2Created := true

		defer func() {
			if snapshot2Created {
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot2.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot 2 is creation succeeds with previous csi session")
		snapshot2, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Snapshot details is %+v", snapshot2)

		ginkgo.By("Modifying the password in the secret")
		vsphereCfg.Global.Password = newPassword
		modifiedConf, err := writeConfigToSecretString(vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Updating the secret to reflect the new password")
		secret.Data[vSphereCSIConf] = []byte(modifiedConf)
		_, err = c.CoreV1().Secrets(csiSystemNamespace).Update(ctx, secret, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Reverting the secret change back to reflect the original password")
			currentSecret, err := c.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			currentSecret.Data[vSphereCSIConf] = []byte(originalConf)
			_, err = c.CoreV1().Secrets(csiSystemNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a volume snapshot3")
		snapshot3, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", snapshot3.Name)

		defer func() {
			if snapshot3Created {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot3.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot is Ready to use")
		snapshot3_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot3.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshot3Created = true
		gomega.Expect(snapshot3_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content 3 is created")
		snapshotContent3, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*snapshot3_updated.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(*snapshotContent3.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle3 := *snapshotContent3.Status.SnapshotHandle
		snapshotId3 := strings.Split(snapshothandle3, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId3, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Reverting the password change")
		err = invokeVCenterChangePassword(username, nimbusGeneratedVcPwd, originalPassword, vcAddress, false, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		originalVCPasswordChanged = false

		ginkgo.By("Reverting the secret change back to reflect the original password")
		currentSecret, err := c.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		currentSecret.Data[vSphereCSIConf] = []byte(originalConf)
		_, err = c.CoreV1().Secrets(csiSystemNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, snapshot1.Name, snapshotapigroup)

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
	})

	/*
	   Multi-master and snapshot workflow
	   1. Create a PVC.
	   2. Create some dynamic volume snapshots.
	   3. Kill csi-snapshotter container when creation of volumesnapshot is going on.
	   4. Check if the snapshots go to Bound state.
	   5. Create a volume using each of the snapshot.
	   6. Kill csi-snapshotter container when restore operation is going on.
	   7. Verify pvcs all are in Bound state.
	   8. Cleanup all the snapshots and the pvc.
	*/
	ginkgo.It("[block-vanilla-snapshot][tkg-snapshot] Multi-master and snapshot workflow", ginkgo.Label(p1, block,
		vanilla, tkg, snapshot), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var sshClientConfig, sshWcpConfig *ssh.ClientConfig
		var csiControllerPod, k8sMasterIP, svcMasterIp, svcMasterPwd string
		var volumeSnapshotNames []string
		var volumeSnapshotContents []*snapV1.VolumeSnapshotContent
		var snapshotOpsScale = 3
		if guestCluster {
			snapshotOpsScale = 5
		}

		ginkgo.By("Create storage class and PVC")
		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
			storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if guestCluster {
			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			ginkgo.By("In defer function, deleting PVC")
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("In defer function deleting volume snapshot class")
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx,
				volumeSnapshotClass.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		if vanillaCluster {
			nimbusGeneratedK8sVmPwd := GetAndExpectStringEnvVar(nimbusK8sVmPwd)

			sshClientConfig = &ssh.ClientConfig{
				User: rootUser,
				Auth: []ssh.AuthMethod{
					ssh.Password(nimbusGeneratedK8sVmPwd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}

			/* Get current leader Csi-Controller-Pod where CSI Snapshotter is running and " +
			   find the master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current leader Csi-Controller-Pod name where csi-snapshotter is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csiControllerPod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, snapshotterContainerName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("csi-snapshotter leader is in Pod %s "+
				"which is running on master node %s", csiControllerPod, k8sMasterIP)
		} else if guestCluster {
			svcMasterIp = GetAndExpectStringEnvVar(svcMasterIP)
			svcMasterPwd = GetAndExpectStringEnvVar(svcMasterPassword)
			framework.Logf("svc master ip: %s", svcMasterIp)
			sshWcpConfig = &ssh.ClientConfig{
				User: rootUser,
				Auth: []ssh.AuthMethod{
					ssh.Password(svcMasterPwd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}
			framework.Logf("sshwcpConfig: %v", sshWcpConfig)
			csiControllerPod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				client, sshWcpConfig, snapshotterContainerName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("%s leader is running on pod %s "+
				"which is running on master node %s", snapshotterContainerName, csiControllerPod, k8sMasterIP)
		}

		for i := 0; i < snapshotOpsScale; i++ {
			ginkgo.By("Create a volume snapshot")
			framework.Logf("Creating snapshot no: %d", i+1)
			volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)
			snapshotCreated := true
			volumeSnapshotNames = append(volumeSnapshotNames, volumeSnapshot.Name)

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

			if i == 1 {
				ginkgo.By("Kill container CSI-Snapshotter on the master node where elected leader " +
					"csi controller pod is running")

				if vanillaCluster {
					/* Delete elected leader CSI-Controller-Pod where csi-snapshotter is running */
					csipods, err := client.CoreV1().Pods(csiSystemNamespace).List(ctx, metav1.ListOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Delete elected leader CSi-Controller-Pod where csi-snapshotter is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, snapshotterContainerName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = fpod.WaitForPodsRunningReady(c, csiSystemNamespace, int32(csipods.Size()),
						0, pollTimeoutShort*2, nil)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else if guestCluster {
					err = execStopContainerOnGc(sshWcpConfig, svcMasterIp,
						snapshotterContainerName, k8sMasterIP, svcNamespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		for i := 0; i < snapshotOpsScale; i++ {
			ginkgo.By("Verify volume snapshot is created")
			framework.Logf("snapshot name: %s", volumeSnapshotNames[i])
			volumeSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshotNames[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
			framework.Logf("VolumeSnapshot Name: %s", volumeSnapshot.Name)

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotContentCreated = true
			gomega.Expect(*snapshotContent.Status.ReadyToUse).To(gomega.BeTrue())
			framework.Logf("VolumeSnapshotContent Name: %s", snapshotContent.Name)
			volumeSnapshotContents = append(volumeSnapshotContents, snapshotContent)

			framework.Logf("Get volume snapshot ID from snapshot handle")
			snapshotId, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, snapshotContent, volumeSnapshotClass,
				volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("snapshot Id: %s", snapshotId)

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if vanillaCluster {
			/* Get current leader Csi-Controller-Pod where CSI Snapshotter is running and " +
			   find the master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current leader Csi-Controller-Pod name where csi-snapshotter is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csiControllerPod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				c, sshClientConfig, snapshotterContainerName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("csi-snapshotter leader is in Pod %s "+
				"which is running on master node %s", csiControllerPod, k8sMasterIP)
		} else if guestCluster {
			framework.Logf("sshwcpConfig: %v", sshWcpConfig)
			csiControllerPod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				client, sshWcpConfig, snapshotterContainerName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("%s leader is running on pod %s "+
				"which is running on master node %s", snapshotterContainerName, csiControllerPod, k8sMasterIP)
		}

		for i := 0; i < snapshotOpsScale; i++ {
			ginkgo.By("Create PVC from snapshot")
			pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
				v1.ReadWriteOnce, volumeSnapshotNames[i], snapshotapigroup)

			pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if i == 1 {
				if vanillaCluster {
					/* Delete elected leader CSI-Controller-Pod where csi-snapshotter is running */
					csipods, err := client.CoreV1().Pods(csiSystemNamespace).List(ctx, metav1.ListOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Delete elected leader CSi-Controller-Pod where csi-snapshotter is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, snapshotterContainerName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = fpod.WaitForPodsRunningReady(c, csiSystemNamespace, int32(csipods.Size()),
						0, pollTimeoutShort*2, nil)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else if guestCluster {
					err = execStopContainerOnGc(sshWcpConfig, svcMasterIp,
						snapshotterContainerName, k8sMasterIP, svcNamespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

			framework.Logf("Waiting for PVCs to come to bound state")
			persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

			ginkgo.By("Deleting PVC2")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for i := 0; i < snapshotOpsScale; i++ {
			framework.Logf("Get volume snapshot ID from snapshot handle")
			snapshotId, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, volumeSnapshotContents[i], volumeSnapshotClass,
				volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Get(ctx,
				volumeSnapshotNames[i], metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Delete volume snapshot")
			_, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
	   Max Snapshots per volume test
	   1. Check the default configuration:
	   2. Modify global-max-snapshots-per-block-volume field in vsphere-csi.conf
	   3. Ensure this can be set to different values and it honors this configuration during snap create
	   4. Check behavior when it is set to 0 and 5 as well
	   5. Validate creation of additional snapshots beyond the configured
	       max-snapshots per volume fails - check error returned
	*/
	ginkgo.It("[block-vanilla-snapshot] Max Snapshots per volume test", ginkgo.Label(p1, block,
		vanilla, snapshot), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotNames []string

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

		ginkgo.By("Fetching the default global-max-snapshots-per-block-volume value from secret")
		secret, err := c.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		originalConf := string(secret.Data[vSphereCSIConf])
		vsphereCfg, err := readConfigFromSecretString(originalConf)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		originalMaxSnapshots := vsphereCfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume
		gomega.Expect(originalMaxSnapshots).To(gomega.BeNumerically(">", 0))

		for i := 1; i <= originalMaxSnapshots; i++ {
			ginkgo.By(fmt.Sprintf("Create a volume snapshot - %d", i))
			snapshot1, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Volume snapshot name is : %s", snapshot1.Name)
			snapshotNames = append(snapshotNames, snapshot1.Name)

			ginkgo.By("Verify volume snapshot is Ready to use")
			snapshot1_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot1.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(snapshot1_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContent1, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*snapshot1_updated.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(*snapshotContent1.Status.ReadyToUse).To(gomega.BeTrue())

			framework.Logf("Get volume snapshot ID from snapshot handle")
			snapshothandle := *snapshotContent1.Status.SnapshotHandle
			snapshotId := strings.Split(snapshothandle, "+")[1]

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			for _, snapName := range snapshotNames {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapName, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Create a volume snapshot 4")
		snapshot2, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshot2Created := true

		defer func() {
			if snapshot2Created {
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot2.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot 4 is creation fails")
		snapshot2, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot2.Name)
		gomega.Expect(err).To(gomega.HaveOccurred())
		framework.Logf("Snapshot details is %+v", snapshot2)

		ginkgo.By("Delete failed snapshot")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot2.Name, pandoraSyncWaitTime)
		snapshot2Created = false

		ginkgo.By("Modifying the default max snapshots per volume in the secret to 6")
		vsphereCfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume = 6
		modifiedConf, err := writeConfigToSecretString(vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Updating the secret to reflect the new max snapshots per volume")
		secret.Data[vSphereCSIConf] = []byte(modifiedConf)
		_, err = c.CoreV1().Secrets(csiSystemNamespace).Update(ctx, secret, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Reverting the secret change back to reflect the original max snapshots per volume")
			currentSecret, err := c.CoreV1().Secrets(csiSystemNamespace).Get(ctx,
				configSecret, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			currentSecret.Data[vSphereCSIConf] = []byte(originalConf)
			_, err = c.CoreV1().Secrets(csiSystemNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Get CSI Controller's replica count from the setup
		deployment, err := client.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount := *deployment.Spec.Replicas

		ginkgo.By("Bring down csi-controller pod")
		bringDownCsiController(client)
		isCSIDown := true
		defer func() {
			if !isCSIDown {
				bringUpCsiController(client, csiReplicaCount)
			}
		}()

		bringUpCsiController(client, csiReplicaCount)
		isCSIDown = false

		for j := 4; j <= 5; j++ {
			ginkgo.By(fmt.Sprintf("Create a volume snapshot - %d", j))
			snapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Volume snapshot name is : %s", snapshot.Name)
			snapshotNames = append(snapshotNames, snapshot.Name)

			ginkgo.By("Verify volume snapshot is Ready to use")
			snapshot_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(snapshot_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*snapshot_updated.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(*snapshotContent.Status.ReadyToUse).To(gomega.BeTrue())

			framework.Logf("Get volume snapshot ID from snapshot handle")
			snapshothandle := *snapshotContent.Status.SnapshotHandle
			snapshotId := strings.Split(snapshothandle, "+")[1]

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, snapshotNames[0], snapshotapigroup)

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
	})

	/*
	   Volume snapshot creation when resize is in progress
	   1. Create a pvc and resize the pvc (say from 2GB to 4GB)
	   2. While the resize operation is in progress, create a snapshot on this volume
	   3. Expected behavior: resize operation should succeed and the
	       snapshot creation should succeed after resize completes
	*/
	ginkgo.It("[block-vanilla-snapshot][tkg-snapshot] Volume snapshot creation when "+
		"resize is in progress", ginkgo.Label(p1, block, vanilla, snapshot, tkg, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var volumeSnapshot *snapV1.VolumeSnapshot
		var snapshotContent *snapV1.VolumeSnapshotContent
		var snapshotCreated, snapshotContentCreated bool
		var snapshotId string
		var err error

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod to attach PV to the node")
		var pod *v1.Pod
		if windowsEnv {
			pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
				false, windowsExecRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
				false, execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

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

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Modify PVC spec to trigger volume expansion
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("4Gi"))
		newDiskSize := "6Gi"
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		claims, err := expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(claims).NotTo(gomega.BeNil())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		if vanillaCluster {
			volumeSnapshot, snapshotContent, snapshotCreated,
				snapshotContentCreated, snapshotId, err = createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				pvclaim, volHandle, diskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if guestCluster {
			volumeSnapshot, snapshotContent, snapshotCreated,
				snapshotContentCreated, snapshotId, err = createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				pvclaim, volHandle, newDiskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Waiting for file system resize to finish")
		claims, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := claims.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(6144)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb !=
			newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion +%v ",
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Volume provision and snapshot creation on VVOL Datastore
		1. Create a SC for VVOL datastore and provision a PVC
		2. Create Snapshot class and take a snapshot of the volume
		3. Cleanup of snapshot, pvc and sc
	*/
	ginkgo.It("[block-vanilla-snapshot][tkg-snapshot]Volume provision and "+
		"snapshot creation/restore on VVOL Datastore", ginkgo.Label(p0, block, vanilla, snapshot, tkg), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		invokeSnapshotOperationsOnSharedDatastore(client, ctx, namespace, scParameters, snapc, "VVOL",
			pandoraSyncWaitTime)

	})

	/*
		Volume provision and snapshot creation on VMFS Datastore
		1. Create a SC for VMFS datastore and provision a PVC
		2. Create Snapshot class and take a snapshot of the volume
		3. Cleanup of snapshot, pvc and sc
	*/
	ginkgo.It("[block-vanilla-snapshot][tkg-snapshot] Volume provision and "+
		"snapshot creation/restore on VMFS Datastore", ginkgo.Label(p0, block, vanilla, snapshot, tkg), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		invokeSnapshotOperationsOnSharedDatastore(client, ctx, namespace, scParameters, snapc, "VMFS", pandoraSyncWaitTime)
	})

	/*
		Volume provision and snapshot creation on NFS Datastore
		1. Create a SC for VMFS datastore and provision a PVC
		2. Create Snapshot class and take a snapshot of the volume
		3. Cleanup of snapshot, pvc and sc
	*/
	ginkgo.It("[block-vanilla-snapshot][tkg-snapshot] Volume provision and "+
		"snapshot creation/restore on NFS Datastore", ginkgo.Label(p0, block, vanilla, snapshot, tkg), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		invokeSnapshotOperationsOnSharedDatastore(client, ctx, namespace, scParameters, snapc, "NFS", pandoraSyncWaitTime)
	})

	/*
		Volume provision and snapshot creation on VSAN2 Datastore
		1. Create a SC for VMFS datastore and provision a PVC
		2. Create Snapshot class and take a snapshot of the volume
		3. Cleanup of snapshot, pvc and sc
	*/
	ginkgo.It("[tkg-snapshot] Volume provision and snapshot creation/restore on "+
		"VSAN2 Datastore", ginkgo.Label(p0, snapshot, tkg, newTest), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		invokeSnapshotOperationsOnSharedDatastore(client, ctx, namespace, scParameters, snapc, "VSAN", pandoraSyncWaitTime)
	})

	/*
	   Scale-up creation of snapshots across multiple volumes

	   1. Create a few pvcs (around 25)
	   2. Trigger parallel snapshot create calls on all pvcs
	   3. Trigger parallel snapshot delete calls on all pvcs
	   4. All calls in (2) and (3) should succeed since these are
	      triggered via k8s API (might take longer time)
	   5. Trigger create/delete calls and ensure there are no stale entries left behind
	   6. Create multiple volumes from the same snapshot
	*/

	/*
	   Scale up the total number of snapshots in a cluster, by increasing the volume counts

	   1. Create several 100 of pvcs  (say 500)
	   2. Maximize the snapshots on each volumes by reaching max_snapshots_per_volume
	      on each volume (create around 2000 snapshots)
	   3. At this scale, try a few workflow operations such as below:
	   4. Volume restore
	   5. snapshot create/delete workflow
	*/
	ginkgo.It("[block-vanilla-snapshot][tkg-snapshot] Scale-up creation of snapshots across multiple "+
		"volumes", ginkgo.Label(p0, block, vanilla, snapshot, tkg, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclass *storagev1.StorageClass
		volumesnapshots := make([]*snapV1.VolumeSnapshot, volumeOpsScale)
		snapshotContents := make([]*snapV1.VolumeSnapshotContent, volumeOpsScale)
		pvclaims := make([]*v1.PersistentVolumeClaim, volumeOpsScale)
		pvclaims2 := make([]*v1.PersistentVolumeClaim, volumeOpsScale)
		var persistentvolumes []*v1.PersistentVolume
		var err error

		ginkgo.By("Create storage class and PVC")
		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "snapshot-scale" + curtimestring + val
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, scName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating PVCs using the Storage Class")
		framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc%v", i)
			pvclaims[i], err = createPVC(client, namespace, nil, "", storageclass, v1.ReadWriteOnce)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims,
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a volume snapshot")
		framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating snapshot %v", i)
			volumesnapshots[i], err = snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaims[i].Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Volume snapshot name is : %s", volumesnapshots[i].Name)
		}

		for i := 0; i < volumeOpsScale; i++ {
			ginkgo.By("Verify volume snapshot is created")
			volumesnapshots[i], err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumesnapshots[i].Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(volumesnapshots[i].Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContents[i], err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*volumesnapshots[i].Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(*snapshotContents[i].Status.ReadyToUse).To(gomega.BeTrue())
		}

		ginkgo.By("Create Multiple PVC from one snapshot")
		for i := 0; i < volumeOpsScale; i++ {
			pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
				v1.ReadWriteOnce, volumesnapshots[0].Name, snapshotapigroup)

			pvclaims2[i], err = fpv.CreatePVC(client, namespace, pvcSpec)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Wait for the PVC to be bound")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Deleting volume snapshot")
			deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumesnapshots[i].Name, pandoraSyncWaitTime)

			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContents[i].ObjectMeta.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// TODO: Add a logic to check for the no orphan volumes
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeID := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeID))
			}
		}()
	})

	/* Create/Delete snapshot via k8s API using VolumeSnapshotContent (Pre-Provisioned Snapshots)

	   //Steps to create pre-provisioned snapshot in Guest Cluster

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

	   // TestCase Steps
	   1. Create a storage class and create a pvc using this SC
	   2. The volumesnapshotclass is set to delete
	   3. Create a dynamic volume snapshot
	   4. Create a pre-provisoned snapshot following th steps mentioned above
	   5. Perform cleanup
	*/

	ginkgo.It("[tkg-snapshot] Verify pre-provisioned static snapshot workflow", ginkgo.Label(p0, snapshot, tkg,
		newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "", diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, dynamicSnapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvclaim, volHandle, diskSize, true)
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
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		framework.Logf("Get volume snapshot handle from Supervisor Cluster")
		snapshotId, _, svcVolumeSnapshotName, err := getSnapshotHandleFromSupervisorCluster(ctx,
			volumeSnapshotClass, *snapshotContent.Status.SnapshotHandle)

		ginkgo.By("Create pre-provisioned snapshot")
		_, staticSnapshot, staticSnapshotContentCreated,
			staticSnapshotCreated, err := createPreProvisionedSnapshotInGuestCluster(ctx, volumeSnapshot, snapshotContent,
			snapc, namespace, pandoraSyncWaitTime, svcVolumeSnapshotName, diskSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if staticSnapshotCreated {
				framework.Logf("Deleting static volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, staticSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*staticSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if staticSnapshotContentCreated {
				framework.Logf("Deleting static volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*staticSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *staticSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete pre-provisioned snapshot")
		staticSnapshotCreated, staticSnapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			staticSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, dynamicSnapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[tkg-snapshot] Volume restore using dynamic and pre-provisioned snapshot on "+
		"guest cluster", ginkgo.Label(p0, snapshot, tkg, newTest, flaky), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var staticSnapshotCreated, staticSnapshotContentCreated bool

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "", diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Restore PVC using dynamic volume snapshot")
		pvclaim2, persistentVolumes2, pod := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, true)
		volHandle2 := persistentVolumes2[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
		}
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Get volume snapshot handle from Supervisor Cluster")
		_, _, svcVolumeSnapshotName, err := getSnapshotHandleFromSupervisorCluster(ctx, volumeSnapshotClass,
			*snapshotContent.Status.SnapshotHandle)

		ginkgo.By("Create pre-provisioned snapshot in Guest Cluster")
		_, staticSnapshot, staticSnapshotContentCreated,
			staticSnapshotCreated, err := createPreProvisionedSnapshotInGuestCluster(ctx, volumeSnapshot, snapshotContent,
			snapc, namespace, pandoraSyncWaitTime, svcVolumeSnapshotName, diskSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if staticSnapshotCreated {
				framework.Logf("Deleting static volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, staticSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*staticSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if staticSnapshotContentCreated {
				framework.Logf("Deleting static volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*staticSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *staticSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Restore PVC using pre-provisioned snapshot")
		pvclaim3, persistentVolumes3, pod2 := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, staticSnapshot, diskSize, true)
		volHandle3 := persistentVolumes3[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle3 = getVolumeIDFromSupervisorCluster(volHandle3)
		}
		gomega.Expect(volHandle3).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Take a snapshot of restored PVC created from dynamic snapshot")
		volumeSnapshot3, _, snapshotCreated3,
			snapshotContentCreated3, snapshotId3, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim2, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated3 {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot3.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot3.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated3 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot3.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot3.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot3, pandoraSyncWaitTime, volHandle2, snapshotId3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete pre-provisioned snapshot")
		staticSnapshotCreated, staticSnapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			staticSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Pre-provisioned snapshot using incorrect/non-existing static snapshot
	   1. Create a sc, and pvc using this sc
	   2. Create a snapshot for this pvc
	   3. Create a VolumeSnapshotContent CR using above snapshot-id, by passing the snapshotHandle
	   4. Create a VolumeSnapshot using above content as source
	   5. VolumeSnapshot and VolumeSnapshotContent should be created successfully and readToUse set to True
	   6. Delete the snapshot created in step-4
	   7. Restore: Create a volume using above pre-provisioned snapshot k8s object
	       (note the snapshotHandle its pointing to has been deleted)
	   8. Volume Create should fail with an appropriate error on k8s side
	*/
	ginkgo.It("[tkg-snapshot] Restore volume using non-existing static snapshot", ginkgo.Label(p0, snapshot, tkg,
		newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var staticSnapshotCreated, staticSnapshotContentCreated bool

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "", diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		framework.Logf("Get volume snapshot handle from Supervisor Cluster")
		staticSnapshotId, _, svcVolumeSnapshotName, err := getSnapshotHandleFromSupervisorCluster(ctx,
			volumeSnapshotClass, *snapshotContent.Status.SnapshotHandle)

		ginkgo.By("Create pre-provisioned snapshot")
		_, staticSnapshot, staticSnapshotContentCreated,
			staticSnapshotCreated, err := createPreProvisionedSnapshotInGuestCluster(ctx, volumeSnapshot, snapshotContent,
			snapc, namespace, pandoraSyncWaitTime, svcVolumeSnapshotName, diskSize)
		defer func() {
			if staticSnapshotCreated {
				framework.Logf("Deleting static volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, staticSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*staticSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if staticSnapshotContentCreated {
				framework.Logf("Deleting static volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*staticSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *staticSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete static volume snapshot")
		staticSnapshotCreated, staticSnapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			staticSnapshot, pandoraSyncWaitTime, volHandle, staticSnapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC using the snapshot deleted")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, staticSnapshot.Name, snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim2},
			framework.ClaimProvisionShortTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())

		expectedErrMsg := "error getting handle for DataSource Type VolumeSnapshot by Name"
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim2.Name)},
			expectedErrMsg)
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
	ginkgo.It("[tkg-snapshot] Restore volume from a static snapshot that is still not "+
		"ready-to-use", ginkgo.Label(p0, snapshot, tkg, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "", diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		framework.Logf("Get volume snapshot handle from Supervisor Cluster")
		snapshotId, _, svcVolumeSnapshotName, err := getSnapshotHandleFromSupervisorCluster(ctx, volumeSnapshotClass,
			*snapshotContent.Status.SnapshotHandle)

		ginkgo.By("Create Pre-provisioned snapshot in Guest Cluster")
		framework.Logf("Change the deletion policy of VolumeSnapshotContent from Delete to Retain " +
			"in Guest Cluster")
		updatedSnapshotContent, err := changeDeletionPolicyOfVolumeSnapshotContent(ctx,
			snapshotContent, snapc, snapV1.VolumeSnapshotContentRetain)

		framework.Logf("Delete dynamic volume snapshot from Guest Cluster")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

		framework.Logf("Delete VolumeSnapshotContent from Guest Cluster explicitly")
		err = deleteVolumeSnapshotContent(ctx, updatedSnapshotContent, snapc, namespace, pandoraSyncWaitTime)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Creating static VolumeSnapshotContent in Guest Cluster using "+
			"supervisor VolumeSnapshotName %s", svcVolumeSnapshotName)
		staticSnapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Create(ctx,
			getVolumeSnapshotContentSpec(snapV1.DeletionPolicy("Delete"), svcVolumeSnapshotName,
				"static-vs", namespace), metav1.CreateOptions{})

		framework.Logf("Verify VolumeSnapshotContent is created or not in Guest Cluster")
		staticSnapshotContent, err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			staticSnapshotContent.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Snapshotcontent name is  %s", staticSnapshotContent.ObjectMeta.Name)
		if !*staticSnapshotContent.Status.ReadyToUse {
			framework.Logf("VolumeSnapshotContent is not ready to use")
		}

		ginkgo.By("Create a static volume snapshot using static snapshotcontent")
		staticVolumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpecByName(namespace, "static-vs", staticSnapshotContent.ObjectMeta.Name),
			metav1.CreateOptions{})
		if err != nil {
			framework.Logf("failed to create static volume snapshot: %v", err)
		}
		framework.Logf("Volume snapshot name is : %s", staticVolumeSnapshot.Name)

		ginkgo.By("Create PVC while snapshot is still provisioning")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, "static-vs", snapshotapigroup)
		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
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

		ginkgo.By("Verify static volume snapshot is created")
		staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, staticVolumeSnapshot.Name)
		if err != nil {
			framework.Logf("failed to wait for volume snapshot: %v", err)
		}
		if staticSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize)) != 0 {
			framework.Logf("expected RestoreSize does not match")
		}
		framework.Logf("Snapshot details is %+v", staticSnapshot)

		ginkgo.By("Delete pre-provisioned snapshot")
		staticSnapshotCreated, staticSnapshotContentCreated, err := deleteVolumeSnapshot(ctx, snapc, namespace,
			staticSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if staticSnapshotCreated {
				framework.Logf("Deleting static volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, staticSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*staticSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if staticSnapshotContentCreated {
				framework.Logf("Deleting static volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*staticSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *staticSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Create snapshot on Supervisor cluster

	   Create a Storage Class, and a PVC using this SC in Supervisor cluster.
	   Create a dynamic snapshot in supervisor using above PVC as source
	   Snapshot creation should fail with appropriate error message.
	   Cleanup the snapshots, PVC and SC
	*/

	ginkgo.It("[tkg-snapshot] Verify Snapshot creation should fail on supervisor cluster", ginkgo.Label(p0,
		snapshot, tkg, newTest, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		storageclass, err := svcClient.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim, err := fpv.CreatePVC(svcClient, svcNamespace,
			getPersistentVolumeClaimSpecWithStorageClass(svcNamespace, diskSize, storageclass, nil, v1.ReadWriteOnce))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(svcClient,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(svcClient, pvclaim.Name, pvclaim.Namespace)
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

		ginkgo.By("Expected snapshot creation failure on supervisor cluster")
		_, err = snapc.SnapshotV1().VolumeSnapshots(svcNamespace).Create(ctx,
			getVolumeSnapshotSpec(svcNamespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		if err != nil {
			framework.Logf(err.Error())
		}
	})

	/*
	   Perform online resize on restored volume
	   1.  Create a Storage Class, a PVC and attach the PVC to a Pod, write a file
	   2.  Create dynamically provisioned snapshots using this PVC
	   3.  Create new volume using this snapshots as source, use the same SC and attach it to a Pod.
	   4.  Ensure the PVC gets provisioned and is Bound.
	   5.  Verify the previous snapshot data is intact and write new data to restored volume
	   6.  Perform online resize on the restored volume and make sure  resize should go fine.
	   7.  Create dynamically provisioned snapshots using the PVC created in step #4
	   8.  Verify snapshot size. It should be same as that of restored volume size.
	   9.  Run cleanup: Delete snapshots, restored-volumes, pods.
	*/
	ginkgo.It("[tkg-snapshot] Perform online resize on restored volume", ginkgo.Label(p0,
		snapshot, tkg, newTest, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var snapshotCreated = false

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "", diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pvc %s in namespace %s", pvclaim.Name, namespace))
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create/Get volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if vanillaCluster {
				err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, _, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)

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

		ginkgo.By("Create PVC from Snapshot and verify restore volume operations")
		pvclaim2, persistentVolumes2, pod := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, true)
		volHandle2 := persistentVolumes2[0].Spec.CSI.VolumeHandle
		svcPVCName2 := persistentVolumes2[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
		}
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Deleting the pvc %s in namespace %s", pvclaim2.Name, namespace))
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Perform online resize on the restored volume and make sure resize should go fine")
		verifyOnlineVolumeExpansionOnGc(client, namespace, svcPVCName2, volHandle, pvclaim2, pod, f)

		ginkgo.By("Create a volume snapshot from restored volume")
		volumeSnapshotFromRestoreVol, snapshotContentFromRestoreVol, snapshotCreated,
			snapshotContentCreated, snapshotIdFromRestoreVol, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvclaim2, volHandle2, "3Gi", true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Volume snapshot name is : %s", volumeSnapshotFromRestoreVol.Name)
		snapshotCreated = true

		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = deleteVolumeSnapshotContent(ctx, snapshotContentFromRestoreVol,
					snapc, namespace, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshotFromRestoreVol.Name, pandoraSyncWaitTime)
			}
		}()

		framework.Logf("Deleting volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshotFromRestoreVol, pandoraSyncWaitTime, volHandle2, snapshotIdFromRestoreVol)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
	   Offline relocation of FCD with snapshots
	   1.  Create a Storage Class, and a PVC.
	   2.  Ensure the Volume-snapshot and VolumeSnapshotContent is created and Bound
	   3.  Run FCD relocate on this volume using CNS side APIs
	   4.  If relocate is supported, create new snapshots after relocate is successful
	   5.  Verify snapshot status which we took before relocating FCD.
	   6.  Create new volume using this snapshot as source, use the same SC and attach it to a Pod.
	   7.  Run cleanup: Delete snapshots, restored-volumes, pods.
	*/
	ginkgo.It("[tkg-snapshot] Offline relocation of FCD with snapshots", ginkgo.Label(p0, snapshot, tkg,
		newTest, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated, snapshotCreated bool
		var datastoreUrls []string

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedVsanDatastoreURL := os.Getenv(envSharedDatastoreURL)
		if sharedVsanDatastoreURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedDatastoreURL))
		}
		datastoreUrls = append(datastoreUrls, sharedvmfsURL, sharedVsanDatastoreURL)

		storagePolicyName = os.Getenv(envStoragePolicyNameForVsanVmfsDatastores)
		if storagePolicyName == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envStoragePolicyNameForVsanVmfsDatastores))
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pvc %s in namespace %s", pvclaim.Name, namespace))
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create/Get volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if vanillaCluster {
				err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)

		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volHandle)
		framework.Logf("Volume: %s is present on %s", volHandle, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volHandle, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
				break
			}
		}

		ginkgo.By("Relocate FCD to another datastore")
		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)
		_, err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volHandle, dsRefDest, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC from snapshot")
		pvclaim2, persistentVolumes2, pod := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, true)
		volHandle2 := persistentVolumes2[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
		}
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Deleting the pvc %s in namespace %s", pvclaim2.Name, namespace))
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Deleting volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
	   Dynamic snapshot created in one guest cluster and restore it on another guest cluster
	   1.  Create a SC and PVC using this SC and attach it to Pod. Write some data on it.
	   2.  Create a volume snapshot using this PVC as source in Guest Cluster GC-1 and bound.
	   3.  Restore volume snapshot created in step #2 in another Guest Cluster GC-2
	   4.  Verify restore volume creation status in another GC fails with appropriate error.
	   5.  Run cleanup: Delete snapshots, restored-volumes, pods.
	*/
	ginkgo.It("[tkg-snapshot] Dynamic snapshot created in one guest cluster "+
		"and restore it on another guest cluster", ginkgo.Label(p0, snapshot, tkg, newTest, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated, snapshotCreated bool

		newGcKubconfigPath := os.Getenv("NEW_GUEST_CLUSTER_KUBE_CONFIG")
		if newGcKubconfigPath == "" {
			ginkgo.Skip("Env NEW_GUEST_CLUSTER_KUBE_CONFIG is missing")
		}
		clientNewGc, err = createKubernetesClientFromConfig(newGcKubconfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Error creating k8s client with %v: %v", newGcKubconfigPath, err))

		ginkgo.By("Create storage class and PVC")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pvc %s in namespace %s", pvclaim.Name, namespace))
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create/Get volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if vanillaCluster {
				err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, _, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
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

		ginkgo.By("Creating namespace on second GC")
		ns, err := framework.CreateTestingNS(f.BaseName, clientNewGc, map[string]string{
			"e2e-framework": f.BaseName,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on second GC")

		namespaceNewGC := ns.Name
		framework.Logf("Created namespace on second GC %v", namespaceNewGC)
		defer func() {
			err := clientNewGc.CoreV1().Namespaces().Delete(ctx, namespaceNewGC, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespaceNewGC, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(clientNewGc, namespaceNewGC, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pvc %s in namespace %s", pvclaim2.Name, namespace))
			err = fpv.DeletePersistentVolumeClaim(clientNewGc, pvclaim2.Name, namespaceNewGC)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		_, err = fpv.WaitForPVClaimBoundPhase(clientNewGc,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "error getting handle for DataSource Type VolumeSnapshot by Name " + volumeSnapshot.Name
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		err = waitForEvent(ctx, clientNewGc, namespaceNewGC, expectedErrMsg, pvclaim2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))

		ginkgo.By("Delete PVC created from snapshot")
		err = fpv.DeletePersistentVolumeClaim(clientNewGc, pvclaim2.Name, namespaceNewGC)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Volume mode conversion
		1.  Create a Storage Class, PVC.
		2.  Create Dynamic Provisioned snapshot on above PVC.
		3.  Verify VolumeSnapshot and VolumeSnapshotContent status.
		4.  Create new volume using snapshot created in step #4, but this time
			give access mode like ReadWriteMany or ReadOnlymany or ReadOnlyOncePod)
		5.  Restore PVC creation should fail and be stuck in Pending state with appropriate error message.
		6.  Perform Cleanup.
	*/
	ginkgo.It("[tkg-snapshot] Volume mode conversion", ginkgo.Label(p0, snapshot, tkg, newTest, stable,
		negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated, snapshotCreated bool

		ginkgo.By("Create storage class and PVC")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pvc %s in namespace %s", pvclaim.Name, namespace))
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create/Get volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if vanillaCluster {
				err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)

		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
			}
		}()

		accessModes := []v1.PersistentVolumeAccessMode{v1.ReadWriteMany, v1.ReadOnlyMany}

		for _, accessMode := range accessModes {
			ginkgo.By(fmt.Sprintf("Create PVC from snapshot with %s access mode", accessMode))
			pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
				accessMode, volumeSnapshot.Name, snapshotapigroup)

			pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			_, err = fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
			framework.Logf("Error from creating pvc with %s accessmode is : %s", accessMode, err.Error())
			gomega.Expect(err).To(gomega.HaveOccurred())

			expectedErrMsg := "no datastores found to create file volume"
			framework.Logf("Expected failure message: %+q", expectedErrMsg)
			err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim2.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))

			ginkgo.By(fmt.Sprintf("Deleting the pvc %s in namespace %s", pvclaim2.Name, namespace))
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = waitForPvcToBeDeleted(ctx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		framework.Logf("Deleting pending PVCs from SVC namespace")
		pvcList := getAllPVCFromNamespace(svcClient, svcNamespace)
		for _, pvc := range pvcList.Items {
			if pvc.Status.Phase == v1.ClaimPending {
				framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(svcClient, pvc.Name, svcNamespace),
					"Failed to delete PVC", pvc.Name)
			}
		}
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Volume snapshot creation on a file-share volume
		    Create a file-share pvc
		    Try creating a snapshot on this pvc
		    Should fail with an appropriate error
	*/

	ginkgo.It("[tkg-snapshot] Volume snapshot creation on a file-share volume on a guest "+
		"cluster", ginkgo.Label(p1, snapshot, tkg, newTest, negative, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Create storage class and PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as invalid storage policy is specified in Storage Class")
		framework.ExpectError(fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout))
		expectedErrMsg := "no datastores found to create file volume, vsan file service may be disabled"
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
	})

	/*
			PVC/Pod → Snapshot → RestoreVolume/Pod → Snapshot → Restore Vol again/Pod

		Create a Storage Class, a PVC and attach the PVC to a Pod, write a file
		Create dynamically provisioned snapshots using this PVC
		Create new volume using this snapshots as source, use the same SC
		Ensure the PVC gets provisioned and is Bound
		Attach the PVC to a Pod and ensure data from snapshot is available (file that was written in step.1
			should be available)
		And also write new data to the restored volumes and it should succeed
		Take a snapshot of restore volume created in step #3.
		Create new volume using the snapshot as source use the same SC created in step #7
		Ensure the PVC gets provisioned and is Bound
		Attach the PVC to a Pod and ensure data from snapshot is available (file that was written in
			step.1 and step 5 should be available)
		And also write new data to the restored volumes and it should succeed
		Run cleanup: Delete snapshots, restored-volumes, pods.
	*/

	ginkgo.It("[tkg-snapshot] Create restore volume snapshot in consistent order", ginkgo.Label(p0, snapshot,
		tkg, newTest, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execRWXCommandPod)
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

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod.html "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod")).NotTo(gomega.BeFalse())

		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod' > /mnt/volume1/Pod.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod")).NotTo(gomega.BeFalse())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
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

		ginkgo.By("Restore volume from snapshot created above")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)
		pvclaim1, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		persistentvolume1, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim1},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle1 := persistentvolume1[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle1).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle1 = getVolumeIDFromSupervisorCluster(volHandle1)
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod1.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod1.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle1, pod1.Spec.NodeName))
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volHandle1, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd = []string{"exec", pod1.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd = []string{"exec", pod1.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim1, volHandle1, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, namespace, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Restore volume from snapshot created above")
		pvcSpec = getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot1.Name, snapshotapigroup)
		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Create Pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false,
			execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod2.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod2.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle2, pod2.Spec.NodeName))
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volHandle2, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod2.html "}
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod2")).NotTo(gomega.BeFalse())

		wrtiecmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2' > /mnt/volume1/Pod2.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2")).NotTo(gomega.BeFalse())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated1, snapshotContentCreated1, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot1, pandoraSyncWaitTime, volHandle1, snapshotId1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Detach volume with snapshot
		1. Create a Storage Class, PVC and Pod. Write data on it.
		2. Create Dynamic Provisioned snapshot on above PVC.
		3. Verify VolumeSnapshot and VolumeSnapshotContent status.
		4. Delete the Pod. Verify the volume should be detached and in release state.
		5. Create new volumes using the snapshots created in step #2
		6. Verify new volumes created successfully. Attached it to a Pod.
		7. Verify the older data. It should be intact and write new data.
		8. Perform cleanup.
	*/
	ginkgo.It("[tkg-snapshot] Detach volume with snapshot", ginkgo.Label(p1, snapshot, tkg, newTest,
		stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var snapshotCreated = false
		var vmUUID string

		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Get storage class")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create PVC")
		pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
			diskSize, storageclass, true)
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Pod to attach to Pvc")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		nodeName := pod.Spec.NodeName
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create/Get volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if vanillaCluster {
				err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, _, snapshotCreated,
			snapshotContentCreated, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvclaim, volHandle, diskSize, true)

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

		ginkgo.By("Deleting pod and wait for it to be detached from node")
		deletePodAndWaitForVolsToDetach(ctx, client, pod)

		pvclaim2, persistentVolumes2, pod2 := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, volumeSnapshot, diskSize, true)
		volHandle2 := persistentVolumes2[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
		}
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Delete Dynamic snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	// This test verifies the static provisioning workflow in guest cluster.
	//
	// Test Steps:
	// 1. Create FCD with valid storage policy on gc-svc.
	// 2. Create Resource quota.
	// 3. Create CNS register volume with above created FCD on SVC.
	// 4. verify PV, PVC got created , check the bidirectional reference on svc.
	// 5. On GC create a PV by pointing volume handle got created by static
	//    provisioning on gc-svc (in step 4).
	// 6. On GC create a PVC pointing to above created PV.
	// 7. Wait for PV , PVC to get bound.
	// 8. Create POD, verify the status.
	// 9. Delete all the above created PV, PVC and resource quota.

	ginkgo.It("[tkg-snapshot] Provisioning of static volume on guest cluster using FCD with snapshot "+
		"creation", ginkgo.Label(p0, snapshot, tkg, newTest, stable), func() {

		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var snapshotContentCreated = false
		var snapshotCreated = false
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		svpvcName := "cns-pvc-" + curtimestring + val
		framework.Logf("pvc name :%s", svpvcName)
		namespace = getNamespaceToRunTests(f)

		// Get supvervisor cluster client.
		svcClient, svNamespace := getSvcClientAndNamespace()
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)

		// Get restConfig.
		var restConfig *restclient.Config
		if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
			restConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating FCD (CNS Volume)")
		fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
			"staticfcd"+curtimestring, profileID, diskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		//deleteFCDRequired = false
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create CNS register volume with above created FCD")
		cnsRegisterVolume := getCNSRegisterVolumeSpec(ctx, svNamespace, fcdID, "", svpvcName, v1.ReadWriteOnce)
		err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx,
			restConfig, namespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

		ginkgo.By("verify created PV, PVC and check the bidirectional reference")
		svcPVC, err := svcClient.CoreV1().PersistentVolumeClaims(svNamespace).Get(ctx, svpvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		svcPV := getPvFromClaim(svcClient, svNamespace, svpvcName)
		volumeID := svcPV.Spec.CSI.VolumeHandle
		framework.Logf("volumeID: %s", volumeID)
		verifyBidirectionalReferenceOfPVandPVC(ctx, svcClient, svcPVC, svcPV, fcdID)
		// TODO: add volume health check after PVC creation.

		volumeHandle := svcPVC.GetName()
		framework.Logf("Volume Handle :%s", volumeHandle)

		ginkgo.By("Creating PV in guest cluster")
		gcPV := getPersistentVolumeSpecWithStorageclass(volumeHandle,
			v1.PersistentVolumeReclaimRetain, storageclass.Name, nil, diskSize)
		gcPV, err = client.CoreV1().PersistentVolumes().Create(ctx, gcPV, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gcPVName := gcPV.GetName()
		time.Sleep(time.Duration(10) * time.Second)
		framework.Logf("PV name in GC : %s", gcPVName)

		ginkgo.By("Creating PVC in guest cluster")
		gcPVC := getPVCSpecWithPVandStorageClass(svpvcName, namespace, nil, gcPVName, storageclass.Name, diskSize)
		gcPVC, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, gcPVC, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for claim to be in bound phase")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			namespace, gcPVC.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("PVC name in GC : %s", gcPVC.GetName())

		volHandleFromGcPv := gcPV.Spec.CSI.VolumeHandle
		if guestCluster {
			volHandleFromGcPv = getVolumeIDFromSupervisorCluster(volHandleFromGcPv)
		}
		gomega.Expect(volHandleFromGcPv).NotTo(gomega.BeEmpty())
		framework.Logf("volHandleFromGcPv: %s", volHandleFromGcPv)

		ginkgo.By("Create/Get volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if vanillaCluster {
				err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, gcPVC.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		snapshotCreated = true
		if volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize)) != 0 {
			framework.Failf("unexpected restore size")
		}

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		snapshotContent, err = waitForVolumeSnapshotContentReadyToUse(*snapc, ctx, snapshotContent.Name)
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
			}
		}()

		framework.Logf("Get volume snapshot handle from Supervisor Cluster")
		_, _, svcVolumeSnapshotName, err := getSnapshotHandleFromSupervisorCluster(ctx, volumeSnapshotClass,
			*snapshotContent.Status.SnapshotHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, staticSnapshot, staticSnapshotContentCreated,
			staticSnapshotCreated, err := createPreProvisionedSnapshotInGuestCluster(ctx, volumeSnapshot,
			snapshotContent, snapc, namespace, pandoraSyncWaitTime, svcVolumeSnapshotName, diskSize)
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

			if staticSnapshotContentCreated {
				framework.Logf("Deleting static volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		framework.Logf("snapshot name: %s", staticSnapshot.Name)
		framework.Logf("snapshot details: %s", staticSnapshot)
		pvclaim2, persistentVolumes2, pod2 := verifyVolumeRestoreOperation(ctx, client,
			namespace, storageclass, staticSnapshot, diskSize, true)
		volHandle2 := persistentVolumes2[0].Spec.CSI.VolumeHandle
		if guestCluster {
			volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
		}
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Delete Preprovisioned snapshot")
		framework.Logf("Delete volume snapshot and verify the snapshot content is deleted")
		deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, staticSnapshot.Name, pandoraSyncWaitTime)
		staticSnapshotCreated = false

		framework.Logf("Wait until the volume snapshot content is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *staticSnapshot.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		staticSnapshotContentCreated = false

		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, gcPVC.Name, namespace),
			"Failed to delete PVC ", gcPVC.Name)

		ginkgo.By("Verify PV should be released not deleted")
		framework.Logf("Waiting for PV to move to released state")
		// TODO: replace sleep with polling mechanism.
		time.Sleep(time.Duration(100) * time.Second)
		gcPV, err = client.CoreV1().PersistentVolumes().Get(ctx, gcPVName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gcPVStatus := gcPV.Status.Phase
		if gcPVStatus != "Released" {
			framework.Logf("gcPVStatus: %s", gcPVStatus)
			gomega.Expect(gcPVStatus).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify volume is not deleted in Supervisor Cluster")
		volumeExists := verifyVolumeExistInSupervisorCluster(svcPVC.GetName())
		gomega.Expect(volumeExists).NotTo(gomega.BeFalse())

		ginkgo.By("Deleting the PV Claim in supervisor cluster")
		framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(svcClient,
			svcPVC.Name, svNamespace), "Failed to delete PVC", svcPVC.Name)

		ginkgo.By("Verify PV should be deleted automatically from SVC")
		framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(svcClient, svcPV.Name,
			poll, supervisorClusterOperationsTimeout))

	})

	/*
		Scale up the total number of snapshots in a cluster, by increasing the volume counts

		1. Create Storage Class
		2. Create 300 PVCs using above Storage Class (using vSAN Datastore)
		3. Create 3 snapshots for each volume created in step #2.
		4. At this scale, try a few workflow operations such as below:
			a) Volume restore
			b) snapshot create/delete workflow
			c) Restart services
	*/
	ginkgo.It("[tkg-snapshot] Scale up snapshot creation by increasing the volume counts and "+
		"in between restart services", ginkgo.Label(p1, snapshot, tkg, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclass *storagev1.StorageClass
		volumesnapshots := make([]*snapV1.VolumeSnapshot, volumeOpsScale)
		snapshotContents := make([]*snapV1.VolumeSnapshotContent, volumeOpsScale)
		pvclaims := make([]*v1.PersistentVolumeClaim, volumeOpsScale)
		pvclaims2 := make([]*v1.PersistentVolumeClaim, volumeOpsScale)
		var persistentvolumes []*v1.PersistentVolume
		var err error

		ginkgo.By("Create storage class and PVC")
		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "snapshot-scale" + curtimestring + val
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, scName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating PVCs using the Storage Class")
		framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc%v", i)
			pvclaims[i], err = createPVC(client, namespace, nil, "", storageclass, v1.ReadWriteOnce)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims,
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("Create a volume snapshot")
		framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating snapshot %v", i)
			volumesnapshots[i], err = snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaims[i].Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Volume snapshot name is : %s", volumesnapshots[i].Name)
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		for i := 0; i < volumeOpsScale; i++ {
			ginkgo.By("Verify volume snapshot is created")
			volumesnapshots[i], err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumesnapshots[i].Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(volumesnapshots[i].Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContents[i], err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*volumesnapshots[i].Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(*snapshotContents[i].Status.ReadyToUse).To(gomega.BeTrue())
		}

		ginkgo.By("Create Multiple PVC from one snapshot")
		for i := 0; i < volumeOpsScale; i++ {
			pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
				v1.ReadWriteOnce, volumesnapshots[0].Name, snapshotapigroup)

			pvclaims2[i], err = fpv.CreatePVC(client, namespace, pvcSpec)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Rebooting VC")
		err = invokeVCenterReboot(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(e2eVSphere.Config.Global.VCenterHostname)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")
		var essentialServices []string
		if vanillaCluster {
			essentialServices = []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		} else if guestCluster {
			essentialServices = []string{spsServiceName, vsanhealthServiceName, vpxdServiceName, wcpServiceName}
		}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		//After reboot
		bootstrap()

		fullSyncWaitTime := 0

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		}

		ginkgo.By("Wait for the PVC to be bound")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims2, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Deleting volume snapshot")
			deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumesnapshots[i].Name, pandoraSyncWaitTime)

			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContents[i].ObjectMeta.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeID := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeID))
			}
		}()
	})

	/*
		Max Snapshots per volume on GC
		1. Create a sc, a pvc and ensure the pvc gets provisioned and is Bound
		2. Create 33 dynamically provisioned snapshots using this pvc
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
	ginkgo.It("[tkg-snapshot] Max Snapshots per volume on GC", ginkgo.Label(
		p1, snapshot, tkg, newTest), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var volumeSnapshots []*snapV1.VolumeSnapshot
		var volumeSnapshotContents []*snapV1.VolumeSnapshotContent

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		}

		ginkgo.By("Get storage class and create PVC")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		pvclaim, err = createPVC(client, namespace, nil, "", storageclass, "")
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
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
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

		ginkgo.By("Create/Get volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if vanillaCluster {
				err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		for i := 0; i < 33; i++ {
			ginkgo.By("Create a volume snapshot")
			framework.Logf("Creating snapshot no: %d", i+1)
			volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)
			snapshotCreated := true

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
			gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
			framework.Logf("VolumeSnapshot Name: %s", volumeSnapshot.Name)
			volumeSnapshots = append(volumeSnapshots, volumeSnapshot)

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotContentCreated = true
			gomega.Expect(*snapshotContent.Status.ReadyToUse).To(gomega.BeTrue())
			framework.Logf("VolumeSnapshotContent Name: %s", snapshotContent.Name)
			volumeSnapshotContents = append(volumeSnapshotContents, snapshotContent)

			framework.Logf("Get volume snapshot ID from snapshot handle")
			snapshotId, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, snapshotContent, volumeSnapshotClass,
				volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("snapshot Id: %s", snapshotId)

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		for i := 0; i < 33; i++ {
			framework.Logf("Get volume snapshot ID from snapshot handle")
			snapshotId, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, volumeSnapshotContents[i], volumeSnapshotClass,
				volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Deleted volume snapshot is created above")
			framework.Logf("VolumeSnapshot Name to be deleted: %s", volumeSnapshots[i].Name)
			deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshots[i].Name, pandoraSyncWaitTime)

			framework.Logf("Wait till the volume snapshot is deleted")
			framework.Logf("VolumeSnapshotContent Name to be deleted: %s", volumeSnapshotContents[i].Name)
			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, volumeSnapshotContents[i].ObjectMeta.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify snapshot entry is deleted from CNS")
			err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})
})

// invokeSnapshotOperationsOnSharedDatastore is a wrapper method which invokes creation of volume snapshot
// and restore of volume snapshot on shared datastore
func invokeSnapshotOperationsOnSharedDatastore(client clientset.Interface, ctx context.Context, namespace string,
	scParameters map[string]string, snapc *snapclient.Clientset, sharedDatastoreType string,
	pandoraSyncWaitTime int) {
	var storageclass *storagev1.StorageClass
	var err error
	var snapshotContentCreated = false
	var snapshotCreated = false
	var sharedDatastoreURL, storagePolicyName, snapshotId string

	if sharedDatastoreType == "VMFS" {
		if vanillaCluster {
			sharedDatastoreURL = os.Getenv(envSharedVMFSDatastoreURL)
			if sharedDatastoreURL == "" {
				ginkgo.Skip("Skipping the test because SHARED_VMFS_DATASTORE_URL is not set. " +
					"This may be due to testbed is not having shared VMFS datastore.")
			}
		} else {
			storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForVmfsDatastores)
		}
	} else if sharedDatastoreType == "NFS" {
		if vanillaCluster {
			sharedDatastoreURL = os.Getenv(envSharedNFSDatastoreURL)
			if sharedDatastoreURL == "" {
				ginkgo.Skip("Skipping the test because SHARED_NFS_DATASTORE_URL is not set. " +
					"This may be due to testbed is not having shared NFS datastore.")
			}
		} else {
			storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForNfsDatastores)
		}
	} else if sharedDatastoreType == "VVOL" {
		if vanillaCluster {
			sharedDatastoreURL = os.Getenv(envSharedVVOLDatastoreURL)
			if sharedDatastoreURL == "" {
				ginkgo.Skip("Skipping the test because SHARED_VVOL_DATASTORE_URL is not set. " +
					"This may be due to testbed is not having shared VVOL datastore.")
			}
		} else {
			storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForVvolDatastores)
		}
	} else {
		if vanillaCluster {
			sharedDatastoreURL = os.Getenv(envSharedDatastoreURL)
			if sharedDatastoreURL == "" {
				ginkgo.Skip("Skipping the test because SHARED_VSPHERE_DATASTORE_URL is not set. " +
					"This may be due to testbed is not having shared VSAN datastore.")
			}
		} else {
			storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		}
	}

	if vanillaCluster {
		ginkgo.By("Create storage class")
		scParameters[scParamDatastoreURL] = sharedDatastoreURL
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	} else {
		ginkgo.By("Get storage class and create PVC")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
	ginkgo.By("Create PVC")
	pvclaim, persistentVolumes := createPVCAndQueryVolumeInCNS(client, namespace, nil, "",
		diskSize, storageclass, true)
	volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
	if guestCluster {
		volHandle = getVolumeIDFromSupervisorCluster(volHandle)
	}
	gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

	defer func() {
		err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By("Create/Get volume snapshot class")
	volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		if vanillaCluster {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	ginkgo.By("Create a dynamic volume snapshot")
	volumeSnapshot, _, snapshotCreated,
		snapshotContentCreated, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
		volumeSnapshotClass, pvclaim, volHandle, diskSize, true)
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

	pvclaim2, persistentVolumes2, pod2 := verifyVolumeRestoreOperation(ctx, client,
		namespace, storageclass, volumeSnapshot, diskSize, true)
	volHandle2 := persistentVolumes2[0].Spec.CSI.VolumeHandle
	if guestCluster {
		volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
	}
	gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

	defer func() {
		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By("Delete Dynamic snapshot")
	snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
		volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}
