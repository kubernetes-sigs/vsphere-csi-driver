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
	_ "fmt" // no more error
	"math/rand"
	"strconv"
	"strings"
	"time"

	"os"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"

	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"

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
	//	restConfig   *restclient.Config
		snapc        *snapclient.Clientset
	//	volHandle    string
		
		
		allowedTopologies          []v1.TopologySelectorLabelRequirement
		topologyAffinityDetails    map[string][]string
		topologyCategories         []string
		topologyLength             int
		
	
		sshClientConfig            *ssh.ClientConfig
	//	vcAddress                  string
	//	controller_name            string
	//	sts_count                  int
	//	statefulSetReplicaCount    int32
	//	pvcCount                   int
		originalSizeInMb           int64
		fsSize                     int64
	//	expectedErrMsg             string
	//	deploymentReplicaCount     int32
	//	deploymentList             []*appsv1.Deployment
		
	//	pandoraSyncWaitTime        int
		defaultDatacenter          *object.Datacenter
	//	defaultDatastore           *object.Datastore
		fullSyncWaitTime           int
	//	isSPSServiceStopped        bool
	  //  isVsanhealthServiceStopped bool

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


        

		    var cancel context.CancelFunc
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client = f.ClientSet
			namespace = f.Namespace.Name
			bootstrap()
			sc, err := client.StorageV1().StorageClasses().Get(ctx, "example-windows-sc", metav1.GetOptions{})
			if err == nil && sc != nil {
				gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
			nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}
			
			//bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		//	topologyLength = 5
		//	isSPSServiceStopped = false
		//	isVsanhealthServiceStopped = false

		/*	topologyMap := GetAndExpectStringEnvVar(topologyMap)
			topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap, topologyLength)
			framework.Logf("topologyAffinityDetails %s,topologyMap%s  topologyCategories%s",topologyMap,topologyAffinityDetails,topologyCategories)
			allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)*/

			sshClientConfig = &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(k8sVmPasswd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}
			
		/*	vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			if os.Getenv(envPandoraSyncWaitTime) != "" {
				pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				pandoraSyncWaitTime = defaultPandoraSyncWaitTime
			}*/
			var datacenters []string
			datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
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
			// Read full-sync value.
			if os.Getenv(envFullSyncWaitTime) != "" {
				fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
				framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}





        


		// var datacenters []string
		// var defaultDatacenter *object.Datacenter
		// var defaultDatastore *object.Datastore
		// datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		// ctx, cancel := context.WithCancel(context.Background())
		// defer cancel()
		// finder := find.NewFinder(e2eVSphere.Client.Client, false)
		// cfg, err := getConfig()
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// dcList := strings.Split(cfg.Global.Datacenters, ",")
		// for _, dc := range dcList {
		// 	dcName := strings.TrimSpace(dc)
		// 	if dcName != "" {
		// 		datacenters = append(datacenters, dcName)
		// 	}
		// }

		// for _, dc := range datacenters {
		// 	defaultDatacenter, err = finder.Datacenter(ctx, dc)
		// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// 	finder.SetDatacenter(defaultDatacenter)
		// 	defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
		// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// }

		// ginkgo.By("Deleting FCD:")
		// err = e2eVSphere.deleteFCD(ctx, "545b6008-a93d-4ccf-9f2e-96e8c05bfc90", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "f97050a5-b4ed-4cd0-a1f2-e6c1b2ee1db6", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "9720b584-dc37-4827-9a02-0b7fe0615551", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "c1485578-ea54-41ff-bdad-5c35b41bc318", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "a362ce90-6404-4c1f-8def-6337b6fa71be", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "45ab2c28-9bb2-4b19-b054-8921e5bf63de", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "798bc565-c88f-4db5-a2cc-79c656f07714", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "9218eee5-6e26-40e8-8eaf-a70d23a86915", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "965ecf02-075c-40e6-997e-9f076d53c677", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "a38b3920-88c5-4753-9334-606358799322", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "c81eb16e-34fd-494b-aa96-fe510bb6ad2f", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.deleteFCD(ctx, "fed39bee-efca-484b-8288-18898ce37317", defaultDatastore.Reference())
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// time.Sleep(time.Duration(50) * time.Second)

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
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
	//	var snapshotContentCreated = false
	//	var snapshotCreated = false

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false,"","")
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


		for i := 0; i < 4; i++ {
	 	   ginkgo.By("Create a volume snapshot")
		   volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name+strconv.Itoa(i), pvclaim.Name), metav1.CreateOptions{})
		   gomega.Expect(err).NotTo(gomega.HaveOccurred())
		   framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name+strconv.Itoa(i))
		}

	/*	defer func() {
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
		time.Sleep(30 * time.Second)
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())*/
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
			namespace, nil, scParameters, diskSize, nil, "", false, "","example-windows-sc")
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
<<<<<<< HEAD
		Create pre-provisioned snapshot using backend snapshot (create via CNS)

=======
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
>>>>>>> origin/master
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
			namespace, nil, scParameters, diskSize, nil, "", false, "","example-windows-sc")
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
			namespace, nil, scParameters, diskSize, nil, "", false, "","example-windows-sc")
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
	ginkgo.It("Verify snapshot static provisioning with deletion policy Retain - test2", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var snapshotCreated = false
		var contentName string

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "", "example-windows-sc")
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

		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, snapshot1.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					contentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		snapshot1_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = true
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
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Deleting volume snapshot 1 " + snapshot1.Name)
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, snapshot1.Name,
			metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = false

		time.Sleep(30 * time.Second)

		_, err = snapc.SnapshotV1().VolumeSnapshots(namespace).Get(ctx, snapshot1.Name,
			metav1.GetOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Delete snapshot entry from CNS")
		err = e2eVSphere.deleteVolumeSnapshotInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume snapshot content is not deleted")
		snapshotContentGetResult, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			content.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(snapshotContentGetResult.Name).Should(gomega.Equal(content.Name))
		framework.Logf("Snapshotcontent name is  %s", snapshotContentGetResult.Name)

		framework.Logf("Delete volume snapshot content")
		err = snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
			snapshotContentGetResult.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	ginkgo.It("Volume restore using snapshot a dynamic snapshot b pre-provisioned snapshot", func() {
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
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output2 := framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output2, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd2...)
		output2 = framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output2, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())



		wrtiecmd3 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cksum /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd3...)
		output3 := framework.RunKubectlOrDie(namespace, wrtiecmd3...)
		gomega.Expect(strings.Contains(output3, "checksum Hello message from test into Pod1")).NotTo(gomega.BeFalse())

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
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated3 {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		err = verifySnapshotIsCreatedInCNS(volHandle2, snapshotId3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		wrtiecmd4 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cksum /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd4...)
		output4 := framework.RunKubectlOrDie(namespace, wrtiecmd4...)
		gomega.Expect(strings.Contains(output4, "checksum Hello message from test into Pod1 post snaps creation")).NotTo(gomega.BeFalse())


		ginkgo.By("Deleted volume snapshot is created above")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot3.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated3 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContent3.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated3 = false

		ginkgo.By("Deleted volume snapshot is created above")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, staticSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated2 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContentNew.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	
	ginkgo.It("Volume restore using snapshot a dynamic snapshot b pre-provisioned snapshot", func() {
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
			namespace, nil, scParameters, diskSize, nil, "", false, "","example-windows-sc")
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
			pod , err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, windowsCommand)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pod , err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false,
				execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_ = pod
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
					pod2 , err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false, windowsCommand)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
				pod2 , err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim3}, false,
						execRWXCommandPod1)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				_ = pod2
		}
		framework.Logf("pod is %v" ,pod2)

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

		
	

		if windowsEnv{
			ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
			//_, err = framework.LookForStringInPodExec(namespace, pod.Name,
				//	[]string{"powershell.exe", "cat", "C:\\test\\data.txt"}, "", time.Minute)
			//	gomega.Expect(err).NotTo(gomega.HaveOccurred())
			cmd := []string{"exec", pod.Name, "--namespace=" + namespace,"powershell.exe", "cat", "C:\\mnt\\volume1\\data.txt"}
			output := framework.RunKubectlOrDie(namespace, cmd...)
		    gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())
		}else{
			ginkgo.By("Verify the volume is accessible and Read/write is possible")
		    cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		    output := framework.RunKubectlOrDie(namespace, cmd...)
		    gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())
		}






		var output string
	

				if windowsEnv{
					ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
					//_, err = framework.LookForStringInPodExec(namespace, pod.Name,
						//	[]string{"powershell.exe", "cat", "C:\\test\\data.txt"}, "", time.Minute)
					//	gomega.Expect(err).NotTo(gomega.HaveOccurred())
					cmd := []string{"exec", pod.Name, "--namespace=" + namespace,"powershell.exe", "cat", "C:\\mnt\\volume1\\data.txt"}
					output := framework.RunKubectlOrDie(namespace, cmd...)
					gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
				}else{
			//var output []string
			var cmd []string
		    wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
		}


		if windowsEnv{
			ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
			//_, err = framework.LookForStringInPodExec(namespace, pod.Name,
				//	[]string{"powershell.exe", "cat", "C:\\test\\data.txt"}, "", time.Minute)
			//	gomega.Expect(err).NotTo(gomega.HaveOccurred())
			cmd := []string{"exec", pod2.Name, "--namespace=" + namespace,"powershell.exe", "cat", "C:\\mnt\\volume1\\data.txt"}
			output := framework.RunKubectlOrDie(namespace, cmd...)
			gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
		}else{
		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output2 := framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output2, "Hello message from Pod1")).NotTo(gomega.BeFalse())
		}


		if windowsEnv{
			ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
			//_, err = framework.LookForStringInPodExec(namespace, pod.Name,
				//	[]string{"powershell.exe", "cat", "C:\\test\\data.txt"}, "", time.Minute)
			//	gomega.Expect(err).NotTo(gomega.HaveOccurred())
			cmd := []string{"exec", pod.Name, "--namespace=" + namespace,"powershell.exe", "cat", "C:\\mnt\\volume1\\data.txt"}
			output := framework.RunKubectlOrDie(namespace, cmd...)
			gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
		}else{
		wrtiecmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd2...)
		output2 = framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output2, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
		}

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
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated3 {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		err = verifySnapshotIsCreatedInCNS(volHandle2, snapshotId3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleted volume snapshot is created above")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot3.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated3 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContent3.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated3 = false

		ginkgo.By("Deleted volume snapshot is created above")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, staticSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated2 = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContentNew.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		Volume restore using snapshot on a different storageclass

		1. Create a sc with thin-provisioned spbm policy, create a pvc and attach the pvc to a pod
		2. Create a dynamically provisioned snapshots using this pvc
		3. create another sc pointing to a different spbm policy (say thick)
		4. Run a restore workflow by giving a different storageclass in the pvc spec
		5. the new storageclass would point to a thick provisioned spbm plocy,
		   while the source pvc was created usig thin provisioned psp-operatorlicy
		6. cleanup spbm policies, sc's, pvc's
	*/
	ginkgo.It("Volume restore using snapshot on a different storageclass", func() {
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
			namespace, nil, scParameters, diskSize, nil, "", false, "", "example-windows-sc")
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

		scParameters1 := make(map[string]string)

		scParameters1[scParamStoragePolicyName] = "Management Storage Policy - Regular"

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "snapshot" + curtimestring + val
		storageclass1, err := createStorageClass(client, scParameters1, nil, "", "", false, scName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcSpec2 := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass1, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec2)
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

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	ginkgo.It("Volume deletion with existing snapshots", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var snapshotCreated = false
		var isPVCDeleted = false
		var isPVDeleted = false

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "" ,"example-windows-sc")
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
			if !isPVCDeleted {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isPVCDeleted = true
			}
			if !isPVDeleted {
				err := client.CoreV1().PersistentVolumes().Delete(ctx, persistentvolumes[0].Name,
					*metav1.NewDeleteOptions(0))
				if !apierrors.IsNotFound(err) {
					// Skip if failure is "not found" - object may already been deleted
					// by test.
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
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
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
					volumeSnapshot.Name, metav1.DeleteOptions{})
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

		ginkgo.By("Delete PVC before deleting the snapshot")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isPVCDeleted = true

		ginkgo.By("Get PV and check the PV is still not deleted")
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, persistentvolumes[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pv.Name).Should(gomega.Equal(persistentvolumes[0].Name))

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = false

		framework.Logf("Wait till the volume snapshot is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete PV")
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isPVDeleted = true
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
	ginkgo.It("Create a pre-provisioned snapshot using VolumeSnapshotContent as source", func() {
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
			namespace, nil, scParameters, diskSize, nil, "", false, "","example-windows-sc")
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

		ginkgo.By("Create a volume snapshot2")
		volumeSnapshot2, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpecByName(namespace, "static-vs", snapshotContent.ObjectMeta.Name),
			metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated2 := true

		defer func() {
			if snapshotCreated2 {
				err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, "static-vs", metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot 2 is creation failed")
		staticSnapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot2.Name)
		gomega.Expect(err).To(gomega.HaveOccurred())
		framework.Logf("Snapshot details is %+v", staticSnapshot)

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	ginkgo.It("Pre-provisioned snapshot using incorrect/non-existing static snapshot", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "", "example-windows-sc")
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

		ginkgo.By("Create PVC using the snapshot deleted")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, "static-vs-cns", snapshotapigroup)

		pvclaim2, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim2},
			framework.ClaimProvisionShortTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())

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
	ginkgo.It("Create a volume from a snapshot that is still not ready-to-use", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "","example-windows-sc")
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
	ginkgo.It("Delete the namespace hosting the pvcs and volume-snapshots and "+
		"recover the data using snapshot-content", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error
		var snapshotContentCreated = false
		var snapshotCreated = false

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
			newNamespaceName, nil, scParameters, diskSize, nil, "", false, "","example-windows-sc")
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
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx,
				volumeSnapshotClass.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a volume snapshot")
		snapshot1, err := snapc.SnapshotV1().VolumeSnapshots(newNamespaceName).Create(ctx,
			getVolumeSnapshotSpec(newNamespaceName, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot name is : %s", snapshot1.Name)

		defer func() {
			if !isNamespaceDeleted {
				if snapshotContentCreated {
					framework.Logf("Deleting volume snapshot content")
					err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
						*snapshot1.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					err := snapc.SnapshotV1().VolumeSnapshots(newNamespaceName).Delete(ctx,
						snapshot1.Name, metav1.DeleteOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		ginkgo.By("Verify volume snapshot is Ready to use")
		snapshot1_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, newNamespaceName, snapshot1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = true
		gomega.Expect(snapshot1_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent1, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*snapshot1_updated.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true

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

		time.Sleep(1 * time.Minute)

		ginkgo.By("Verify volume snapshot is deleted")
		_, err = snapc.SnapshotV1().VolumeSnapshots(newNamespaceName).Get(ctx,
			snapshot1.Name, metav1.GetOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		content_updated, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			snapshotContent1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(content_updated.Name).Should(gomega.Equal(snapshotContent1.Name))

		ginkgo.By("Creating another new namespace for the test")
		namespace2, err := framework.CreateTestingNS(f.BaseName, client, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isNamespace2Deleted := false

		namespace2Name := namespace2.Name

		defer func() {
			if !isNamespace2Deleted {
				ginkgo.By("Delete namespace2")
				err = client.CoreV1().Namespaces().Delete(ctx, namespace2Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isNamespace2Deleted = true
			}
		}()

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
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					snapshotContent2_updated.ObjectMeta.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace2Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Deleted volume snapshot is created above")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace2Name).Delete(ctx,
			volumeSnapshot2_updated.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated2 = false

		time.Sleep(5 * time.Second)

		framework.Logf("Wait till the volume snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
			volumeSnapshot2_updated.ObjectMeta.Name, metav1.DeleteOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, volumeSnapshot2_updated.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false

		framework.Logf("Deleting volume snapshot content 1")
		err = snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
			snapshot1_updated.ObjectMeta.Name, metav1.DeleteOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshot1_updated.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete namespace")
		err = client.CoreV1().Namespaces().Delete(ctx, namespace2Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isNamespace2Deleted = true
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
	ginkgo.It("Delete a non-existent snapshot", func() {
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
			namespace, nil, scParameters, diskSize, nil, "", false, "","example-windows-sc")
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
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
					volumeSnapshot.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = true
		gomega.Expect(volumeSnapshot_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent_updated, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*volumeSnapshot_updated.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		gomega.Expect(*snapshotContent_updated.Status.ReadyToUse).To(gomega.BeTrue())

		framework.Logf("Get volume snapshot ID from snapshot handle")
		snapshothandle := *snapshotContent_updated.Status.SnapshotHandle
		snapshotId := strings.Split(snapshothandle, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete snapshot from CNS")
		err = e2eVSphere.deleteVolumeSnapshotInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		time.Sleep(1 * time.Minute)

		ginkgo.By("Delete volume snapshot")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Deleting volume snapshot content")
		err = snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
			snapshotContent_updated.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Wait till the volume snapshot content is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshotContent_updated.ObjectMeta.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false
	})

	/*
		Create snapshots using default VolumeSnapshotClass

		1. Create a VolumeSnapshotClass and set it as default
		2. Create a snapshot without providing the snapshotClass input and
			ensure the default class is picked and honored for snapshot creation
		3. Validate the fields after snapshot creation succeeds (snapshotClass, retentionPolicy)
	*/
	ginkgo.It("Create snapshots using default VolumeSnapshotClass", func() {
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
			namespace, nil, scParameters, diskSize, nil, "", false, "","example-windows-sc")
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
				err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
					volumeSnapshot.Name, metav1.DeleteOptions{})
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
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
			volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		Create Volume from snapshot with different size (high and low)

		1. Restore operation (or creation of volume from snapshot) should pass
			only if the volume size is same as that of the snapshot being used
		2. If a different size (high or low) is provided the pvc creation should fail with error
		3. Verify the error
		4. Create with exact size and ensure it succeeds
	*/
	ginkgo.It("Create Volume from snapshot with different size", func() {
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
			namespace, nil, scParameters, diskSize, nil, "", false, "","example-windows-sc")
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

		time.Sleep(30 * time.Second)

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	ginkgo.It("Snapshot workflow for statefulsets", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		var snapshotContentCreated = false
		var snapshotCreated = false

		ginkgo.By("Create storage class and PVC")

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "nginx-sc-default-" + curtimestring + val

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
			Annotations["volume.beta.kubernetes.io/storage-class"] = scName
		*statefulset.Spec.Replicas = 2
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)

		if windowsEnv{
			ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
			statefulPodList := fss.GetPodList(client, statefulset)

			for _, statefulPod := range statefulPodList.Items {
				_, err = framework.LookForStringInPodExec(statefulPod.Namespace, statefulPod.Name,
					[]string{"powershell.exe", "cat", "C:\\test\\data.txt"}, "", time.Minute)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
	
		}else {
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

		// Verify the attached volume match the one in CNS cache
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv1.Spec.CSI.VolumeHandle,
			pvc1.ClaimName, pv1.ObjectMeta.Name, pod1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		// Verify the attached volume match the one in CNS cache
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv2.Spec.CSI.VolumeHandle,
			pvc2.ClaimName, pv2.ObjectMeta.Name, pod2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		snapshothandle1 := *snapshotContent1.Status.SnapshotHandle
		snapshotId1 := strings.Split(snapshothandle1, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle1, snapshotId1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
		snapshothandle2 := *snapshotContent2.Status.SnapshotHandle
		snapshotId2 := strings.Split(snapshothandle2, "+")[1]

		ginkgo.By("Query CNS and check the volume snapshot entry")
		err = verifySnapshotIsCreatedInCNS(volHandle2, snapshotId2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
		var snapshotDeleted *snapV1.VolumeSnapshot

		//var volId string
		//Find the missing pod and check if the cnsvolumemetadata is deleted or not
		if ssPodsAfterScaleDown.Items[0].Name == pod1.Name {
			pvcToDelete = pvclaim2
			snapshotDeleted = volumeSnapshot2
			//volId = volHandle2

		} else {
			pvcToDelete = pvclaim1
			snapshotDeleted = volumeSnapshot1
			//volId = volHandle1
		}

		err = fpv.DeletePersistentVolumeClaim(client, pvcToDelete.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// err = e2eVSphere.waitForCNSVolumeToBeDeleted(volId)
		// gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Create a new PVC")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, "1Gi", sc, nil,
			v1.ReadWriteOnce, snapshotDeleted.Name, snapshotapigroup)
		pvcSpec.Name = pvcToDelete.Name
		pvclaim3, err := fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expecting the volume to bound")
		newPV, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim3},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volHandleOfNewPV := newPV[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandleOfNewPV).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandleOfNewPV)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		replicas += 1
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		time.Sleep(5 * time.Minute)
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)

		ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
			volumeSnapshot1.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated = false

		err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
			volumeSnapshot2.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotCreated2 = false

		framework.Logf("Wait till the volume snapshot content is deleted")
		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot1.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = false

		err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
			*volumeSnapshot2.Status.BoundVolumeSnapshotContentName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated2 = false

		ginkgo.By("Verify snapshot entry is deleted from CNS")
		err = verifySnapshotIsDeletedInCNS(volHandle1, snapshotId1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = verifySnapshotIsDeletedInCNS(volHandle2, snapshotId2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})





/*
			TESTCASE-4
			Verify the behaviour when CSI-resizer deleted and VSAN-Health is down during online Volume expansion
			Immediate +allowedTopologies (all 5 level)

			Steps//
			1. Identify the Pod where CSI-resizer is the leader.
			2. Create SC with allowVolumeExpansion set to true.
			3. Create multiple PVC's (around 10) using SC above created SC.
			4. Verify node affinity details on PV's.
			5. Create multiple Pod's using above created PVCs.
			6. Bring down vsan-health service (Login to VC and execute : service-control --stop vsan-health)
			7. Trigger online volume expansion on all the PVC's. At the same time delete the Pod identified in the Step 1.
			8. Expand volume should fail with error service unavailable.
			9. Bring up the VSAN-health (Login to VC and execute : service-control --start vsan-health)
			10. Expect Volume should be expanded by the newly elected csi-resizer leader, and filesystem for the
			volume on the pod should also be expanded.
			11. Describe PV and Verify the node affinity rule.
			Make sure node affinity should contain all 5 levels of topology details
			12. POD should be running in the appropriate nodes.
			13. Delete Pod, PVC and SC.
		*/

		ginkgo.It("Verify snapshot behaviour online resize expansion in progress", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			controller_name := "csi-resizer"
		//pvcCount = 5
			scParameters := make(map[string]string)
			var podList []*v1.Pod
			var pvclaim *v1.PersistentVolumeClaim
		//	var volHandle string
			var snapshotCreated = false
			
			
			
			

		    //var pvclaims []*v1.PersistentVolumeClaim

			/* Get current leader Csi-Controller-Pod where CSI Resizer is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Resizer is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			csi_controller_pod, k8sMasterIP, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
				client, sshClientConfig, controller_name)
			framework.Logf("CSI-Resizer is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Get allowed topologies for Storage Class */
			allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)

			// Create SC with Immedaite Binding mode and allowVolumeExpansion set to true
			ginkgo.By("Create SC with Immedaite Binding mode and allowVolumeExpansion set to true")
			


			
			ginkgo.By("Create SC with Immedaite Binding mode and allowVolumeExpansion set to true")
			scParameters[scParamFsType] = ext4FSType
			storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC,
				"", "", true, "example-windows-sc")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			// Creating multiple PVCs
			ginkgo.By("Trigger multiple PVCs")
			framework.Logf("Storage class is '%v'",storageclass)
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, 1)

		
		    ginkgo.By("Expecting the volume to bound")
		   newPV, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaimsList[0]},
			framework.ClaimProvisionTimeout)
		   gomega.Expect(err).NotTo(gomega.HaveOccurred())

		   volHandleOfNewPV := newPV[0].Spec.CSI.VolumeHandle
		   gomega.Expect(volHandleOfNewPV).NotTo(gomega.BeEmpty())
			

			
				ginkgo.By("Creating Pod")
				pvclaims = append(pvclaims, pvclaimsList[0])
				//pod, err := createPod(client, namespace, nil, pvclaims, false, "")
				var pod *v1.Pod
				if windowsEnv {
					pod , err = createPod(client, namespace, nil, pvclaims, false, windowsCommand)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else {
					pod, err = createPod(client, namespace, nil, pvclaims, false, "")
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				podList = append(podList, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// verify volume is attached to the node
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					newPV[0].Spec.CSI.VolumeHandle, pod.Spec.NodeName))
				vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, newPV[0].Spec.CSI.VolumeHandle, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")

				persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
				
	
			    
				ginkgo.By(fmt.Sprintf("Volume %s is no volHandle ",volHandle))
				
				
			
			//fmt.Sprintf("Volume %s is no volHandle ",volHandle)
			

			
				/*
			defer func() {
				// cleanup code for deleting PVC
				ginkgo.By("Deleting PVC's and PV's")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			defer func() {
				// cleanup code for deleting POD
				for i := 0; i < len(podList); i++ {
					ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
					err = fpod.DeletePodWithWait(client, podList[i])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				// Verify volume is detached from the node
				ginkgo.By("Verify volume is detached from the node")
				for i := 0; i < len(pvclaimsList); i++ {
					pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
					isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
						pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
						fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
				}
			}()

           

			// Expanding pvc 
			ginkgo.By("Expanding pvc when vsan-health service on vcenter host is down")
			//for i := 0; i < len(pvclaimsList); i++ {
			currentPvcSize := pvclaims[0].Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("1Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			pvclaim, err = expandPVCSize(pvclaims[0], newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvclaim).NotTo(gomega.BeNil())
			*/

			//}	
			
			/* Get current leader Csi-Controller-Pod where CSI Resizer is running" +
			find master node IP where this Csi-Controller-Pod is running */
			ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Resizer is running and " +
				"find the master node IP where this Csi-Controller-Pod is running")
			//csi_controller_pod, k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx,
			//	client, sshClientConfig, controller_name)
				k8sMasterIP ="10.186.192.8"
			framework.Logf("CSI-Resizer is running on elected Leader Pod %s "+
				"which is running on master node %s", csi_controller_pod, k8sMasterIP)
				
	


					//create snapshot


		          // Verify using CNS Query API if VolumeID retrieved from PV is present.
			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
			gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

			/*ginkgo.By("Create volume snapshot class")
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
			var snapshotContentCreated = false
					
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
					gomega.Expect(err).NotTo(gomega.HaveOccurred())*/

					




			// Expanding pvc when vsan-health service on vcenter host is started
		ginkgo.By("Expanding pvc when vsan-health service on vcenter host is started")
		for i := 0; i < len(pvclaimsList); i++ {
			
			currentPvcSize := pvclaims[0].Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("1Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			pvclaim, err = expandPVCSize(pvclaims[0], newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvclaim).NotTo(gomega.BeNil())
			}

			/*	ginkgo.By("Waiting for file system resize to finish")
				pvclaim, err = waitForFSResize(pvclaims[0], client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())*/
				

			//	pvcConditions := pvclaim.Status.Conditions
			//	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")
		//	}


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
		var snapshotContentCreated = false
				
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

			ginkgo.By("Waiting for file system resize to finish")
				pvclaim, err = waitForFSResize(pvclaims[0], client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())


			//for i := 0; i < len(podList); i++ {
				ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
				fsSize, err = getFSSizeMb(f, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
					fmt.Sprintf("error updating filesystem size."+
						"Resulting filesystem size is %d", fsSize))
				ginkgo.By("File system resize finished successfully")
		//	}

			/* Verify PV nde affinity and that the pods are running on appropriate nodes
			for each StatefulSet pod */
			ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
			for i := 0; i < len(podList); i++ {
				verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, podList[i],
					namespace, allowedTopologies)
			}
			ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
					err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		
	
	

		

		
	})
	