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
	"os"
	"strconv"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

var _ = ginkgo.Describe("[block-snapshot-negative] Volume Snapshot Fault-Injection Test", func() {
	f := framework.NewDefaultFramework("file-snapshot")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                 clientset.Interface
		csiNamespace           string
		csiReplicas            int32
		svcCsiReplicas         int32
		isServiceStopped       bool
		namespace              string
		scParameters           map[string]string
		datastoreURL           string
		fullSyncWaitTime       int
		pvclaims               []*v1.PersistentVolumeClaim
		restConfig             *restclient.Config
		guestClusterRestConfig *restclient.Config
		snapc                  *snapclient.Clientset
		serviceName            string
		pandoraSyncWaitTime    int
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		isServiceStopped = false
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

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Full sync interval can be 1 min at minimum so full sync wait time has to be more than 120s
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}

		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)

			csiDeployment, err := svcClient.AppsV1().Deployments(csiNamespace).Get(
				ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			svcCsiReplicas = *csiDeployment.Spec.Replicas
		}

		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		}
		if isServiceStopped {
			if serviceName == "CSI" {
				framework.Logf("Starting CSI driver")
				ignoreLabels := make(map[string]string)
				err := updateDeploymentReplicawithWait(client, csiReplicas, vSphereCSIControllerPodNamePrefix,
					csiSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Wait for the CSI Pods to be up and Running
				list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				num_csi_pods := len(list_of_pods)
				err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0,
					pollTimeout, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else if serviceName == hostdServiceName {
				framework.Logf("In afterEach function to start the hostd service on all hosts")
				hostIPs := getAllHostsIP(ctx)
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
			} else {
				vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err := invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By(fmt.Sprintf("Resetting provisioner time interval to %s sec", defaultProvisionerTimeInSec))
		updateCSIDeploymentProvisionerTimeout(client, csiSystemNamespace, defaultProvisionerTimeInSec)
	})

	/*
		Volume snapshot creation on a file-share volume
		1. Create a file-share pvc
		2. Try creating a snapshot on this pvc
		3. Should fail with an appropriate error
	*/
	ginkgo.It("Volume snapshot creation on a file-share volume", ginkgo.Label(p1, block, vanilla, snapshot,
		negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("Create storage class and PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
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
		snapshotCreated := true

		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
			}
		}()

		ginkgo.By("Verify volume snapshot is created")
		volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})
	/*
	   Snapshot lifecycle ops with fault-injection
	   1. Create Snapshot (Pre-provisioned and dynamic)
	   2. Delete Snapshot
	   3. Create Volume from Snapshot
	   4. During 1a, 1b and 1c run the following fault events and ensure the operator
	       eventually succeeds and there is no functional impact
	   5. vSphere side service restarts: vpxd, sps, vsan-health, host-restart
	   6. k8s side: csi pod restarts with improved_idempotency enabled as well
	       as run a scenario with improved_idempotency disabled
	*/
	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] create volume snapshot when "+
		"hostd goes down", ginkgo.Label(p0, block, vanilla, tkg, snapshot, disruptive), func() {

		serviceName = hostdServiceName
		snapshotOperationWhileServiceDown(serviceName, namespace, client, snapc, datastoreURL,
			csiNamespace, fullSyncWaitTime, isServiceStopped, true, csiReplicas, pandoraSyncWaitTime)
	})

	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] create volume snapshot when CSI "+
		"restarts", ginkgo.Label(p0, block, vanilla, tkg, snapshot, disruptive), func() {

		serviceName = "CSI"
		snapshotOperationWhileServiceDown(serviceName, namespace, client, snapc, datastoreURL,
			csiNamespace, fullSyncWaitTime, isServiceStopped, true, csiReplicas, pandoraSyncWaitTime)
	})

	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] create volume snapshot when VPXD "+
		"goes down", ginkgo.Label(p0, block, vanilla, tkg, snapshot, disruptive), func() {

		serviceName = vpxdServiceName
		snapshotOperationWhileServiceDown(serviceName, namespace, client, snapc, datastoreURL,
			csiNamespace, fullSyncWaitTime, isServiceStopped, false, csiReplicas, pandoraSyncWaitTime)
	})

	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] create volume snapshot when CNS goes "+
		"down", ginkgo.Label(p0, block, vanilla, tkg, snapshot, disruptive), func() {

		serviceName = vsanhealthServiceName
		snapshotOperationWhileServiceDown(serviceName, namespace, client, snapc, datastoreURL,
			csiNamespace, fullSyncWaitTime, isServiceStopped, false, csiReplicas, pandoraSyncWaitTime)
	})

	ginkgo.It("[block-vanilla-snapshot] [tkg-snapshot] create volume snapshot when SPS "+
		"goes down", ginkgo.Label(p0, block, vanilla, tkg, snapshot, disruptive), func() {

		serviceName = spsServiceName
		snapshotOperationWhileServiceDown(serviceName, namespace, client, snapc, datastoreURL,
			csiNamespace, fullSyncWaitTime, isServiceStopped, true, csiReplicas, pandoraSyncWaitTime)
	})

	ginkgo.It("[tkg-snapshot] create volume snapshot when SVC CSI restarts", ginkgo.Label(p0,
		tkg, snapshot, disruptive, newTest), func() {

		serviceName = "WCP CSI"
		snapshotOperationWhileServiceDown(serviceName, namespace, client, snapc, datastoreURL,
			csiNamespace, fullSyncWaitTime, isServiceStopped, true, svcCsiReplicas, pandoraSyncWaitTime)
	})
})

// snapshotOperationWhileServiceDown creates the volumesnapshot while the services is down
func snapshotOperationWhileServiceDown(serviceName string, namespace string,
	client clientset.Interface, snapc *snapclient.Clientset, datastoreURL string, csiNamespace string,
	fullSyncWaitTime int, isServiceStopped bool, isSnapshotCreated bool, csiReplicas int32, pandoraSyncWaitTime int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var pvclaim2 *v1.PersistentVolumeClaim
	var err error
	var snapshotContentCreated = false
	var volumeSnapshotClass *snapV1.VolumeSnapshotClass
	scParameters := make(map[string]string)

	storagePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

	if vanillaCluster {
		ginkgo.By("Create storage class")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	} else {
		ginkgo.By("Get storage class")
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
	volumeSnapshotClass, err = createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Create a volume snapshot")
	snapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
		getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Volume snapshot name is : %s", snapshot.Name)
	snapshotCreated := true

	defer func() {
		if snapshotCreated {
			framework.Logf("Deleting volume snapshot")
			deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot.Name, pandoraSyncWaitTime)
		}
	}()

	defer func() {
		if snapshotCreated {
			framework.Logf("Deleting volume snapshot")
			deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot.Name, pandoraSyncWaitTime)

			framework.Logf("Wait till the volume snapshot is deleted")
			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshot.ObjectMeta.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotContentCreated = false
		}

		if snapshotContentCreated {
			framework.Logf("Deleting volume snapshot content")
			deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
				*snapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

			framework.Logf("Wait till the volume snapshot is deleted")
			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *snapshot.Status.BoundVolumeSnapshotContentName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	if serviceName == "CSI" {
		ginkgo.By("Stopping CSI driver")
		isServiceStopped, err = stopCSIPods(ctx, client, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				framework.Logf("Starting CSI driver")
				isServiceStopped, err = startCSIPods(ctx, client, csiReplicas, csiNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		framework.Logf("Starting CSI driver")
		isServiceStopped, err = startCSIPods(ctx, client, csiReplicas, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
	} else if serviceName == hostdServiceName {
		ginkgo.By("Fetch IPs for the all the hosts in the cluster")
		hostIPs := getAllHostsIP(ctx)
		isServiceStopped = true

		var wg sync.WaitGroup
		wg.Add(len(hostIPs))

		for _, hostIP := range hostIPs {
			go stopHostD(ctx, hostIP, &wg)
		}
		wg.Wait()

		defer func() {
			framework.Logf("In defer function to start the hostd service on all hosts")
			if isServiceStopped {
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
				isServiceStopped = false
			}
		}()

		ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
		time.Sleep(pollTimeoutSixMin)

		for _, hostIP := range hostIPs {
			startHostDOnHost(ctx, hostIP)
		}
		isServiceStopped = false

	} else if serviceName == "WCP CSI" {
		ginkgo.By("Stopping CSI driver")
		svcClient, _ := getSvcClientAndNamespace()
		isServiceStopped, err = stopCSIPods(ctx, svcClient, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				framework.Logf("Starting CSI driver")
				isServiceStopped, err = startCSIPods(ctx, svcClient, csiReplicas, csiNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		framework.Logf("Starting CSI driver")
		isServiceStopped, err = startCSIPods(ctx, svcClient, csiReplicas, csiNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

	} else {
		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = true
		err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}
		}()

		ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
		time.Sleep(pollTimeoutSixMin)

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
		err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = false
		err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Sleeping for full sync interval")
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
	}

	//After service restart
	bootstrap()

	if isSnapshotCreated {
		ginkgo.By("Verify volume snapshot is Ready to use")
		snapshot1_updated, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, snapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(snapshot1_updated.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

		ginkgo.By("Verify volume snapshot content is created")
		snapshotContent1, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
			*snapshot1_updated.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		snapshotContentCreated = true
		_, err = waitForVolumeSnapshotContentReadyToUse(*snapc, ctx, snapshotContent1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, snapshot1_updated.Name, snapshotapigroup)

		pvclaim2, err = fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if serviceName == "CSI" {
			ginkgo.By("Stopping CSI driver")
			isServiceStopped, err = stopCSIPods(ctx, client, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				if isServiceStopped {
					framework.Logf("Starting CSI driver")
					isServiceStopped, err = startCSIPods(ctx, client, csiReplicas, csiNamespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			framework.Logf("Starting CSI driver")
			isServiceStopped, err = startCSIPods(ctx, client, csiReplicas, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
		} else if serviceName == hostdServiceName {
			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs := getAllHostsIP(ctx)
			isServiceStopped = true

			var wg sync.WaitGroup
			wg.Add(len(hostIPs))

			for _, hostIP := range hostIPs {
				go stopHostD(ctx, hostIP, &wg)
			}
			wg.Wait()

			defer func() {
				framework.Logf("In defer function to start the hostd service on all hosts")
				if isServiceStopped {
					for _, hostIP := range hostIPs {
						startHostDOnHost(ctx, hostIP)
					}
					isServiceStopped = false
				}
			}()

			ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
			time.Sleep(pollTimeoutSixMin)

			for _, hostIP := range hostIPs {
				startHostDOnHost(ctx, hostIP)
			}
			isServiceStopped = false
			ginkgo.By("Sleeping for full sync interval")
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
		} else if serviceName == "WCP CSI" {
			ginkgo.By("Stopping CSI driver")
			svcClient, _ := getSvcClientAndNamespace()
			isServiceStopped, err = stopCSIPods(ctx, svcClient, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				if isServiceStopped {
					framework.Logf("Starting CSI driver")
					isServiceStopped, err = startCSIPods(ctx, svcClient, csiReplicas, csiNamespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			framework.Logf("Starting CSI driver")
			isServiceStopped, err = startCSIPods(ctx, svcClient, csiReplicas, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		} else {
			ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			err = invokeVCenterServiceControl(stopOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = true
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcStoppedMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				if isServiceStopped {
					ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
					err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isServiceStopped = false
				}
			}()

			ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
			time.Sleep(pollTimeoutSixMin)

			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
			err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = false
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Sleeping for full sync interval")
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
		}

		//After service restart
		bootstrap()
		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
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

	}
	ginkgo.By("Deleted volume snapshot is created above")
	deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, snapshot.Name, pandoraSyncWaitTime)
	snapshotCreated = false

	if isSnapshotCreated {
		if serviceName == "CSI" {
			ginkgo.By("Stopping CSI driver")
			isServiceStopped, err = stopCSIPods(ctx, client, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				if isServiceStopped {
					framework.Logf("Starting CSI driver")
					isServiceStopped, err = startCSIPods(ctx, client, csiReplicas, csiNamespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			framework.Logf("Starting CSI driver")
			isServiceStopped, err = startCSIPods(ctx, client, csiReplicas, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
		} else if serviceName == hostdServiceName {
			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs := getAllHostsIP(ctx)
			isServiceStopped = true

			var wg sync.WaitGroup
			wg.Add(len(hostIPs))

			for _, hostIP := range hostIPs {
				go stopHostD(ctx, hostIP, &wg)
			}
			wg.Wait()

			defer func() {
				framework.Logf("In defer function to start the hostd service on all hosts")
				if isServiceStopped {
					for _, hostIP := range hostIPs {
						startHostDOnHost(ctx, hostIP)
					}
					isServiceStopped = false
				}
			}()

			ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
			time.Sleep(pollTimeoutSixMin)

			for _, hostIP := range hostIPs {
				startHostDOnHost(ctx, hostIP)
			}
			isServiceStopped = false
		} else if serviceName == "WCP CSI" {
			ginkgo.By("Stopping CSI driver")
			svcClient, _ := getSvcClientAndNamespace()
			isServiceStopped, err = stopCSIPods(ctx, svcClient, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				if isServiceStopped {
					framework.Logf("Starting CSI driver")
					isServiceStopped, err = startCSIPods(ctx, svcClient, csiReplicas, csiNamespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
			framework.Logf("Starting CSI driver")
			isServiceStopped, err = startCSIPods(ctx, svcClient, csiReplicas, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		} else {
			ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			err = invokeVCenterServiceControl(stopOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = true
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcStoppedMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				if isServiceStopped {
					ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
					err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isServiceStopped = false
				}
			}()

			ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
			time.Sleep(pollTimeoutSixMin)

			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
			err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = false
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Sleeping for full sync interval")
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
		}
	}

	//After service restart
	bootstrap()

	framework.Logf("Wait till the volume snapshot is deleted")
	err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, snapshot.ObjectMeta.Name)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	snapshotContentCreated = false
}
