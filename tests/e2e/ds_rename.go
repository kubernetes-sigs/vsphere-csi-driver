package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	admissionapi "k8s.io/pod-security-admission/api"

	pbmtypes "github.com/vmware/govmomi/pbm/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

var _ bool = ginkgo.Describe("ds-rename", func() {
	f := framework.NewDefaultFramework("ds-rename")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                     clientset.Interface
		namespace                  string
		storagePolicyName          string
		scParameters               map[string]string
		pandoraSyncWaitTime        int
		snapc                      *snapclient.Clientset
		isVsanHealthServiceStopped bool
		isSPSServiceStopped        bool
		vcAddress                  string
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort

		govmomiClient := newClient(ctx, &e2eVSphere)
		pc = newPbmClient(ctx, govmomiClient)

		// reading fullsync wait time
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		// Get snapshot client using the rest config
		if vanillaCluster {
			restConfig = getRestConfigClient()
			snapc, err = snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if supervisorCluster {
			restConfig = getRestConfigClient()
			snapc, err = snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			setStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, rqLimit)
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if isVsanHealthServiceStopped {
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}

		if isSPSServiceStopped {
			startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
		}
	})

	/*
	   Create PVC and then rename datastore
	   Steps:
	   1	create a SC which points to remote vsan ds
	   2	create 5 pvcs each on remote vsan ds and wait for them to be bound
	   3	attach a pod to each of the PVCs created in step 2
	   4	wait for all pods to be running and verify that the respective pvcs are accessible
	   5	storage vmotion remote workers to local datastore
	   6	verify that volumes are accessible for all the pods
	   7	storage vmotion remote workers to back to remote datastore
	   8	verify that volumes are accessible for all the pods
	   9	cleanup all the pods, pvcs and SCs created for the test
	*/
	ginkgo.It("Create PVC and then rename datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		pvcCount := 5
		var stsReplicas int32 = 3
		var sc *storagev1.StorageClass
		var err error
		var volHandle string

		snapshotIDs, volHandles := []string{}, []string{}
		pvclaimsList := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		volumeSnapshotList := []*snapV1.VolumeSnapshot{}

		randomStr := strconv.Itoa(r1.Intn(1000))

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, err := createPVC(ctx, client, namespace, nil, diskSize1GB, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
		}
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pvs[i].Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a statefulset")
		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false,
			false, stsReplicas, "", 3, "")

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		allPvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pvclaim := range allPvcs.Items {
			pv := getPvFromClaim(client, namespace, pvclaim.Name)
			volHandle = pv.Spec.CSI.VolumeHandle
			volHandles = append(volHandles, volHandle)
			volumeSnapshot, _, snapshotCreated,
				_, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				&pvclaim, volHandle, diskSize1GB, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volumeSnapshotList = append(volumeSnapshotList, volumeSnapshot)
			snapshotIDs = append(snapshotIDs, snapshotId)

			defer func() {

				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

					framework.Logf("Wait till the volume snapshot is deleted")
					err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
						*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
		}
		datastoreName, dsRef, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Original datastore name: %s", datastoreName)
		ginkgo.By("Rename datastore to a new name")
		e2eVSphere.renameDs(ctx, datastoreName+randomStr, &dsRef)

		defer func() {
			framework.Logf("Renaming datastore back to original name")
			e2eVSphere.renameDs(ctx, datastoreName, &dsRef)
		}()

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		ginkgo.By("Verifying statefulset scale up went fine on statefulset")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		for i := 0; i < len(volumeSnapshotList); i++ {
			ginkgo.By("Verify volume restore fromsnapshots created earlier is successful")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshotList[i], diskSize1GB, false)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshotList[i], pandoraSyncWaitTime, volHandles[i], snapshotIDs[i], true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

	})

	/*
	   Create workloads and rename datastore after SPS is down
	   Steps:
	   1	create a SC which points to remote vsan ds
	   2	create 5 pvcs each on remote vsan ds and wait for them to be bound
	   3	attach a pod to each of the PVCs created in step 2
	   4	wait for all pods to be running and verify that the respective pvcs are accessible
	   5	storage vmotion remote workers to local datastore
	   6	verify that volumes are accessible for all the pods
	   7	storage vmotion remote workers to back to remote datastore
	   8	verify that volumes are accessible for all the pods
	   9	cleanup all the pods, pvcs and SCs created for the test
	*/
	ginkgo.It("Create workloads and rename datastore"+
		" after SPS is down", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		pvcCount := 5
		var stsReplicas int32 = 3
		pvclaimsList := []*v1.PersistentVolumeClaim{}
		var sc *storagev1.StorageClass
		var err error
		var volHandle string

		snapshotIDs, volHandles := []string{}, []string{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		volumeSnapshotList := []*snapV1.VolumeSnapshot{}

		randomStr := strconv.Itoa(r1.Intn(1000))

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, err := createPVC(ctx, client, namespace, nil, diskSize1GB, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
		}
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pvs[i].Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a statefulset")
		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false,
			false, stsReplicas, "", 3, "")

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		allPvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pvclaim := range allPvcs.Items {
			pv := getPvFromClaim(client, namespace, pvclaim.Name)
			volHandle = pv.Spec.CSI.VolumeHandle
			volHandles = append(volHandles, volHandle)
			volumeSnapshot, _, snapshotCreated,
				_, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				&pvclaim, volHandle, diskSize1GB, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volumeSnapshotList = append(volumeSnapshotList, volumeSnapshot)
			snapshotIDs = append(snapshotIDs, snapshotId)

			defer func() {

				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

					framework.Logf("Wait till the volume snapshot is deleted")
					err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
						*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
		}

		datastoreName, dsRef, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Stopping SPS on the vCenter host"))
		isSPSServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("Rename datastore to a new name")
		e2eVSphere.renameDs(ctx, datastoreName+randomStr, &dsRef)

		defer func() {
			framework.Logf("Renaming datastore back to original name")
			e2eVSphere.renameDs(ctx, datastoreName, &dsRef)
		}()

		ginkgo.By(fmt.Sprintln("Starting SPS on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		ginkgo.By("Verifying statefulset scale up went fine on statefulset")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		for i := 0; i < len(volumeSnapshotList); i++ {
			ginkgo.By("Verify volume restore fromsnapshots created earlier is successful")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshotList[i], diskSize1GB, false)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshotList[i], pandoraSyncWaitTime, volHandles[i], snapshotIDs[i], true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

	})

	/*
	   Create workloads and rename datastore after vsan health is down
	   Steps:
	   1	create a SC which points to remote vsan ds
	   2	create 5 pvcs each on remote vsan ds and wait for them to be bound
	   3	attach a pod to each of the PVCs created in step 2
	   4	wait for all pods to be running and verify that the respective pvcs are accessible
	   5	storage vmotion remote workers to local datastore
	   6	verify that volumes are accessible for all the pods
	   7	storage vmotion remote workers to back to remote datastore
	   8	verify that volumes are accessible for all the pods
	   9	cleanup all the pods, pvcs and SCs created for the test
	*/
	ginkgo.It("Create workloads and rename datastore"+
		" after vsan health is down", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		pvcCount := 10
		var stsReplicas int32 = 3
		var sc *storagev1.StorageClass
		var err error
		var volHandle string

		snapshotIDs, volHandles := []string{}, []string{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		volumeSnapshotList := []*snapV1.VolumeSnapshot{}
		pvclaimsList := []*v1.PersistentVolumeClaim{}
		randomStr := strconv.Itoa(r1.Intn(1000))

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, err := createPVC(ctx, client, namespace, nil, diskSize1GB, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
		}
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pvs[i].Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false,
			false, stsReplicas, "", 3, "")

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		allPvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pvclaim := range allPvcs.Items {
			pv := getPvFromClaim(client, namespace, pvclaim.Name)
			volHandle = pv.Spec.CSI.VolumeHandle
			volHandles = append(volHandles, volHandle)
			volumeSnapshot, _, snapshotCreated,
				_, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				&pvclaim, volHandle, diskSize1GB, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volumeSnapshotList = append(volumeSnapshotList, volumeSnapshot)
			snapshotIDs = append(snapshotIDs, snapshotId)

			defer func() {
				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

					framework.Logf("Wait till the volume snapshot is deleted")
					err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
						*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
		}

		datastoreName, dsRef, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("Rename datastore to a new name")
		e2eVSphere.renameDs(ctx, datastoreName+randomStr, &dsRef)

		defer func() {
			framework.Logf("Renaming datastore back to original name")
			e2eVSphere.renameDs(ctx, datastoreName, &dsRef)
		}()

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		ginkgo.By("Verifying statefulset scale up went fine on statefulset")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		for i := 0; i < len(volumeSnapshotList); i++ {
			ginkgo.By("Verify volume restore fromsnapshots created earlier is successful")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshotList[i], diskSize1GB, false)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshotList[i], pandoraSyncWaitTime, volHandles[i], snapshotIDs[i], true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

	})

	/*
		Create CSI snapshot while renaming datastore
		Steps:
		1  Create a storageclass with with storage policy set as vsan default storage policy.
		2  Create some PVCs with storageclass created in step 1 and verify they are in bound state.
		3  Verify CNS metadata for all volumes and verify it is placed on correct datastore.
		4  Rename the datastore while creating CSI snapshot from volumes created in step 2.
		5  Verify CSI snapshots are in readyToUse state.
		6  Invoke query API to check snapshots metadata in CNS
		7  Create new volumes on the renamed datastore.
		8  Restore volume from snapshot created in step 5 and verify this operation is a success.
		9  Delete all the workloads created in the test.
	*/
	ginkgo.It("Create CSI snapshot while renaming datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)

		pvcCount := 10
		var sc *storagev1.StorageClass
		var err error
		var volHandle string
		randomStr := strconv.Itoa(r1.Intn(1000))

		pvclaimsList := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		volumeSnapshotList := []*snapV1.VolumeSnapshot{}

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, err := createPVC(ctx, client, namespace, nil, diskSize1GB, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
		}
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle = pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pvs[i].Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Rename datastore to a new name while creating volume snapshot in parallel")
		datastoreName, dsRef, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var wg sync.WaitGroup
		ch := make(chan *snapV1.VolumeSnapshot)

		lock := &sync.Mutex{}
		wg.Add(2)
		go createDynamicSnapshotInParallel(ctx, namespace, snapc, pvclaimsList, volumeSnapshotClass.Name, ch, lock, &wg)
		go e2eVSphere.renameDsInParallel(ctx, datastoreName+randomStr, &dsRef, &wg)
		go func() {
			for v := range ch {
				volumeSnapshotList = append(volumeSnapshotList, v)
			}
		}()
		wg.Wait()
		close(ch)

		defer func() {
			framework.Logf("Renaming datastore back to original name")
			e2eVSphere.renameDs(ctx, datastoreName, &dsRef)
		}()

		defer func() {
			framework.Logf("Deleting snapshots created in test")
			for _, volumeSnapshot := range volumeSnapshotList {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		ginkgo.By("Verify volume snapshot is created")
		for i, volumeSnapshot := range volumeSnapshotList {
			volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize1GB)) != 0 {
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "unexpected restore size")
			}

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			snapshotContent, err = waitForVolumeSnapshotContentReadyToUse(*snapc, ctx, snapshotContent.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle = pvs[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			framework.Logf("Get volume snapshot ID from snapshot handle")
			snapshotId, _, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, snapshotContent)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = waitForCNSSnapshotToBeCreated(volHandle, snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify restore volume from snapshot is successful")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshot, diskSize1GB, false)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

	/*
		Create workloads while renaming datastore
		Steps:
		1  Create a storageclass with with storage policy set as vsan default storage policy.
		2  Rename the datastore while creating some PVCs and statefulsets on that datastore using the storageclass
			created in step 1.
		3  Verify all PVCs come to bound state.
		4  Verify all pods are up and in running state.
		5  Verify CNS metadata for all volumes and verify it is placed on newly renamed datastore.
		6  Create new volumes on the renamed datastore.
		7  Scale up statefulset replicas to 5.
		8  Delete all the workloads created in the test.
	*/
	ginkgo.It("Create workloads while renaming datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)

		pvcCount := 5
		var sc *storagev1.StorageClass
		var err error
		var volHandle string
		randomStr := strconv.Itoa(r1.Intn(1000))

		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		pvclaimsList := []*v1.PersistentVolumeClaim{}
		newPvcList := []*v1.PersistentVolumeClaim{}
		newPodList := []*v1.Pod{}

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, err := createPVC(ctx, client, namespace, nil, diskSize1GB, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
		}
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle = pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pvs[i].Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Rename datastore to a new name while creating workloads in parallel")
		datastoreName, dsRef, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var wg sync.WaitGroup
		pvcChan := make(chan *v1.PersistentVolumeClaim)
		podChan := make(chan *v1.Pod)

		pvcLock := &sync.Mutex{}
		podLock := &sync.Mutex{}
		wg.Add(3)
		go createPvcInParallel(ctx, client, namespace, "", sc, pvcChan, pvcLock, &wg, pvcCount)
		go func() {
			for v := range pvcChan {
				newPvcList = append(newPvcList, v)
			}
		}()
		go createPodsInParallel(client, namespace, pvclaimsList, ctx, podLock, podChan, &wg, pvcCount)
		go func() {
			for v := range podChan {
				podLock.Lock()
				newPodList = append(newPodList, v)
				podLock.Unlock()
			}
		}()
		go e2eVSphere.renameDsInParallel(ctx, datastoreName+randomStr, &dsRef, &wg)

		wg.Wait()
		close(pvcChan)
		close(podChan)

		defer func() {
			framework.Logf("Renaming datastore back to original name")
			e2eVSphere.renameDs(ctx, datastoreName, &dsRef)
		}()

		defer func() {
			for _, pvc := range newPvcList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		defer func() {
			ginkgo.By("Delete Pods")
			deletePodsAndWaitForVolsToDetach(ctx, client, newPodList, true)
		}()

		ginkgo.By("wait for pvcs to be bound")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, newPvcList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, newPodList, pvclaims2d)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

	})

	/*
		Expand volumes while renaming datastore
		Steps:
		1  Create a storageclass with with storage policy set as vsan default storage policy.
		2  Rename the datastore while creating some PVCs and statefulsets on that datastore using the storageclass
			created in step 1.
		3  Verify all PVCs come to bound state.
		4  Verify all pods are up and in running state.
		5  Verify CNS metadata for all volumes and verify it is placed on newly renamed datastore.
		6  Create new volumes on the renamed datastore.
		7  Scale up statefulset replicas to 5.
		8  Delete all the workloads created in the test.
	*/
	ginkgo.It("Expand volumes while renaming datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)

		pvcCount := 5
		var sc *storagev1.StorageClass
		var err error
		var volHandle string
		randomStr := strconv.Itoa(r1.Intn(1000))

		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		pvclaimsList := []*v1.PersistentVolumeClaim{}
		newPvcList := []*v1.PersistentVolumeClaim{}
		newPodList := []*v1.Pod{}

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", true)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, err := createPVC(ctx, client, namespace, nil, diskSize1GB, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
		}
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle = pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pvs[i].Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Rename datastore to a new name while expanding volumes in parallel")
		datastoreName, dsRef, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var wg sync.WaitGroup
		podChan := make(chan *v1.Pod)

		lock := &sync.Mutex{}
		wg.Add(3)
		go expandVolumeInParallel(client, pvclaimsList, &wg, "10Gi")
		go createPodsInParallel(client, namespace, pvclaimsList, ctx, lock, podChan, &wg, pvcCount)
		go e2eVSphere.renameDsInParallel(ctx, datastoreName+randomStr, &dsRef, &wg)
		go func() {
			for v := range podChan {
				lock.Lock()
				newPodList = append(newPodList, v)
				lock.Unlock()
			}
		}()
		wg.Wait()
		close(podChan)

		defer func() {
			framework.Logf("Renaming datastore back to original name")
			e2eVSphere.renameDs(ctx, datastoreName, &dsRef)
		}()

		defer func() {
			for _, pvc := range newPvcList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		defer func() {
			ginkgo.By("Delete pods created in test")
			deletePodsAndWaitForVolsToDetach(ctx, client, newPodList, true)
		}()

		ginkgo.By("wait for pvcs to be bound")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, newPvcList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, newPodList, pvclaims2d)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

	})

	/*
	   Relocate volumes after renaming of datastore
	   Steps:
	   1	create a SC which points to remote vsan ds
	   2	create 5 pvcs each on remote vsan ds and wait for them to be bound
	   3	attach a pod to each of the PVCs created in step 2
	   4	wait for all pods to be running and verify that the respective pvcs are accessible
	   5	storage vmotion remote workers to local datastore
	   6	verify that volumes are accessible for all the pods
	   7	storage vmotion remote workers to back to remote datastore
	   8	verify that volumes are accessible for all the pods
	   9	cleanup all the pods, pvcs and SCs created for the test
	*/
	ginkgo.It("Relocate volumes after renaming of datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pvcCount := 5
		var stsReplicas int32 = 3
		var sc *storagev1.StorageClass
		var err error
		var volHandle string
		var datastoreUrls []string
		pvclaimsList := []*v1.PersistentVolumeClaim{}
		snapshotIDs, volHandles := []string{}, []string{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		volumeSnapshotList := []*snapV1.VolumeSnapshot{}

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		sharedvmfs2URL := os.Getenv(envSharedVMFSDatastore2URL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastore2URL))
		}
		datastoreUrls = append(datastoreUrls, sharedvmfsURL, sharedvmfs2URL)

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		randomStr := strconv.Itoa(rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix
		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		attachTagToDS(ctx, tagID, sharedvmfsURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfsURL)
		}()

		attachTagToDS(ctx, tagID, sharedvmfs2URL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfs2URL)
		}()

		ginkgo.By("create SPBM policy with thin/ezt volume allocation")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var policyID *pbmtypes.PbmProfileId

		policyID, policyName := createVmfsStoragePolicy(
			ctx, pc, eztAllocType, map[string]string{categoryName: tagName})

		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = policyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, err := createPVC(ctx, client, namespace, nil, diskSize1GB, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
		}
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pvs[i].Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create statefulset")
		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false,
			false, stsReplicas, "", 3, "")

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		allPvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshots from PVCs created")
		for _, pvclaim := range allPvcs.Items {
			pv := getPvFromClaim(client, namespace, pvclaim.Name)
			volHandle = pv.Spec.CSI.VolumeHandle
			volHandles = append(volHandles, volHandle)
			volumeSnapshot, _, snapshotCreated,
				_, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
				volumeSnapshotClass,
				&pvclaim, volHandle, diskSize1GB, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volumeSnapshotList = append(volumeSnapshotList, volumeSnapshot)
			snapshotIDs = append(snapshotIDs, snapshotId)

			defer func() {

				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace,
						volumeSnapshot.Name, pandoraSyncWaitTime)

					framework.Logf("Wait till the volume snapshot is deleted")
					err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
						*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
		}

		datastoreName, dsRef, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Rename datastore to a new name")
		e2eVSphere.renameDs(ctx, datastoreName+randomStr, &dsRef)

		defer func() {
			framework.Logf("Renaming datastore back to original name")
			e2eVSphere.renameDs(ctx, datastoreName, &dsRef)
		}()

		ginkgo.By("Relocate all volumes to a different datastore")
		for i := range pvs {
			volumeID := pvs[i].Spec.CSI.VolumeHandle
			dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volumeID)
			framework.Logf("Volume is present on %s", dsUrlWhereVolumeIsPresent)
			e2eVSphere.verifyDatastoreMatch(volumeID, datastoreUrls)
			destDsUrl := ""
			for _, dsurl := range datastoreUrls {
				if dsurl != dsUrlWhereVolumeIsPresent {
					destDsUrl = dsurl
					break
				}
			}

			framework.Logf("dest url for volume id %s is %s", volumeID, destDsUrl)
			dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)
			_, err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volumeID, dsRefDest)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			e2eVSphere.verifyDatastoreMatch(volumeID, []string{destDsUrl})
		}

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		ginkgo.By("Verifying statefulset scale up went fine on statefulset")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		for i := 0; i < len(volumeSnapshotList); i++ {
			ginkgo.By("Verify volume restore fromsnapshots created earlier is successful")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshotList[i], diskSize1GB, false)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshotList[i], pandoraSyncWaitTime, volHandles[i], snapshotIDs[i], true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

	})

	/*
		Change volume allocation while renaming datastore
		Steps:
		1  Create a storageclass with with storage policy set as vsan default storage policy.
		2  Rename the datastore while creating some PVCs and statefulsets on that datastore using the storageclass
			created in step 1.
		3  Verify all PVCs come to bound state.
		4  Verify all pods are up and in running state.
		5  Verify CNS metadata for all volumes and verify it is placed on newly renamed datastore.
		6  Create new volumes on the renamed datastore.
		7  Scale up statefulset replicas to 5.
		8  Delete all the workloads created in the test.
	*/
	ginkgo.It("Change volume allocation while renaming datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pvcCount := 10
		var sc *storagev1.StorageClass
		var err error
		var volHandle string

		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		pvclaimsList := []*v1.PersistentVolumeClaim{}
		newPvcList := []*v1.PersistentVolumeClaim{}
		newPodList := []*v1.Pod{}

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}
		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		randomStr := strconv.Itoa(rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix
		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		attachTagToDS(ctx, tagID, sharedvmfsURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfsURL)
		}()

		ginkgo.By("create SPBM policy with thin/ezt volume allocation")
		ginkgo.By("create a storage class with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policy created from step 2")
		var policyID *pbmtypes.PbmProfileId

		policyID, policyName := createVmfsStoragePolicy(
			ctx, pc, eztAllocType, map[string]string{categoryName: tagName})

		defer func() {
			deleteStoragePolicy(ctx, pc, policyID)
		}()

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = policyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, err := createPVC(ctx, client, namespace, nil, diskSize1GB, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
		}
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle = pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pvs[i].Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Rename datastore to a new name while updating storage policy in parallel")
		datastoreName, dsRef, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Rename datastore to a new name")
		e2eVSphere.renameDs(ctx, datastoreName+randomStr, &dsRef)

		defer func() {
			framework.Logf("Renaming datastore back to original name")
			e2eVSphere.renameDs(ctx, datastoreName, &dsRef)
		}()

		ginkgo.By("Update and reconfigure policy to EZT")
		err = updateVmfsPolicyAlloctype(ctx, pc, eztAllocType, policyName, policyID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf(
			"trying to reconfigure volume %v with policy %v which is expected to fail", volHandle, policyName)
		err = e2eVSphere.reconfigPolicy(ctx, volHandle, policyID.UniqueId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for _, pvc := range newPvcList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, newPodList, true)
		}()

		ginkgo.By("wait for pvcs to be bound")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, newPvcList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, newPodList, pvclaims2d)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

	})

})
