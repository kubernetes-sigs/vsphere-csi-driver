/*
Copyright 2021 The Kubernetes Authors.

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

package transactionsupport

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
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
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

var e2eTestConfig *config.E2eTestConfig
var vcAddress string
var pvclaims []*v1.PersistentVolumeClaim
var pvcSnapshots []*snapV1.VolumeSnapshot
var persistentvolumes []*v1.PersistentVolume
var totalQuotaUsedBefore, storagePolicyQuotaBefore, storagePolicyUsageBefore *resource.Quantity
var isTestPassed bool

func createPVC(ctx context.Context, client clientset.Interface, namespace string, ds string,
	storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode, pvclaims []*v1.PersistentVolumeClaim, index int, wgMain *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wgMain.Done()
	pvclaim, err := k8testutil.CreatePVC(ctx, client, namespace, nil, ds, storageclass, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pvclaims[index] = pvclaim
}

func createSnapshot(ctx context.Context, namespace string, pvclaims []*v1.PersistentVolumeClaim, index int, pvcSnapshots []*snapV1.VolumeSnapshot, wgMain *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wgMain.Done()
	restConfig := k8testutil.GetRestConfigClient(e2eTestConfig)
	snapc, _ := snapclient.NewForConfig(restConfig)
	ginkgo.By("Create volume snapshot class")
	volumeSnapshotClass, err := k8testutil.CreateVolumeSnapshotClass(ctx, e2eTestConfig, snapc, constants.DeletionPolicy)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pvclaim := pvclaims[index]
	ginkgo.By("Create a volume snapshot")
	framework.Logf("Volume snapshot class name is : %s", volumeSnapshotClass.Name)
	volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
		k8testutil.GetVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)
	pvcSnapshots[index] = volumeSnapshot

	// defer func() {
	// 	if snapshotCreated {
	// 		framework.Logf("Deleting volume snapshot on failure")
	// 		var pandoraSyncWaitTime int
	// 		if os.Getenv(constants.EnvPandoraSyncWaitTime) != "" {
	// 			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(constants.EnvPandoraSyncWaitTime))
	// 			gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 		} else {
	// 			pandoraSyncWaitTime = constants.DefaultPandoraSyncWaitTime
	// 		}
	// 		DeleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
	// 	}
	// }()
}

func createVolumeFromSnapshot(ctx context.Context, client clientset.Interface, storageclass *storagev1.StorageClass, namespace string, pvcSnapshots []*snapV1.VolumeSnapshot, pvcsCreatedFromSnapshot []*v1.PersistentVolumeClaim, index int, diskSize string, wgMain *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wgMain.Done()

	ginkgo.By("Create PVC from snapshot")
	pvcSpec := k8testutil.GetPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
		v1.ReadWriteOnce, pvcSnapshots[index].Name, constants.Snapshotapigroup)
	pvcFromSnapshot, err := k8testutil.CreatePvcWithSpec(ctx, client, namespace, pvcSpec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
		[]*v1.PersistentVolumeClaim{pvcFromSnapshot}, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
	gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

	pvcsCreatedFromSnapshot[index] = pvcFromSnapshot
}

func createLinkedClone(ctx context.Context, client clientset.Interface, storageclass *storagev1.StorageClass, namespace string, pvcSnapshots []*snapV1.VolumeSnapshot, pvcsCreatedWithLinkedClone []*v1.PersistentVolumeClaim, index int, diskSize string, wgMain *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wgMain.Done()
	ginkgo.By("Create PVC from snapshot")
	pvclaim, _ := k8testutil.CreateAndValidateLinkedClone(ctx, client, namespace, storageclass, pvcSnapshots[index].Name, diskSize)
	pvcsCreatedWithLinkedClone[index] = pvclaim
}

func restartService(ctx context.Context, c clientset.Interface, serviceName string, wgMain *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wgMain.Done()

	time.Sleep(time.Duration(2) * time.Second) //Waiting provisioning to start

	var fullSyncWaitTime int
	var isCsiServiceStopped, isVpxaServiceStopped, isWebHookServiceStopped, isServiceStopped bool //isHostDServiceStopped
	switch serviceName {

	case constants.CsiServiceName:
		// Get CSI Controller's replica count from the setup
		deployment, err := c.AppsV1().Deployments(constants.CsiSystemNamespace).Get(ctx,
			constants.VSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount := *deployment.Spec.Replicas

		ginkgo.By("Stopping CSI driver")
		isCsiServiceStopped, err = k8testutil.StopCSIPods(ctx, c, constants.CsiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isCsiServiceStopped {
				framework.Logf("Starting CSI driver")
				isCsiServiceStopped, err = k8testutil.StartCSIPods(ctx, c, csiReplicaCount, constants.CsiSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		framework.Logf("Starting CSI driver")
		isCsiServiceStopped, err = k8testutil.StartCSIPods(ctx, c, csiReplicaCount, constants.CsiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if os.Getenv(constants.EnvFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(constants.EnvFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Full sync interval can be 1 min at minimum so full sync wait time has to be more than 120s
			if fullSyncWaitTime < 120 || fullSyncWaitTime > constants.DefaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = constants.DefaultFullSyncWaitTime
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

	case constants.StorageQuotaWebhookServiceName:
		// Get CSI Controller's replica count from the setup
		deployment, err := c.AppsV1().Deployments(constants.KubeSystemNamespace).Get(ctx,
			constants.StorageQuotaWebhookPrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount := *deployment.Spec.Replicas

		ginkgo.By("Stopping webhook driver")
		isWebHookServiceStopped, err = k8testutil.StopStorageQuotaWebhookPodInKubeSystem(ctx, c, constants.KubeSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isWebHookServiceStopped {
				framework.Logf("Starting storage-quota-webhook driver")
				isWebHookServiceStopped, err = k8testutil.StartStorageQuotaWebhookPodInKubeSystem(ctx, c, csiReplicaCount, constants.KubeSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		framework.Logf("Starting storage-quota-webhook ")
		isWebHookServiceStopped, err = k8testutil.StartStorageQuotaWebhookPodInKubeSystem(ctx, c, csiReplicaCount, constants.KubeSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if os.Getenv(constants.EnvFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(constants.EnvFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Full sync interval can be 1 min at minimum so full sync wait time has to be more than 120s
			if fullSyncWaitTime < 120 || fullSyncWaitTime > constants.DefaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = constants.DefaultFullSyncWaitTime
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

	case constants.HostdServiceName:
		ginkgo.By("Fetch IPs for the all the hosts in the cluster")
		clusterName := os.Getenv(constants.EnvComputeClusterName)
		framework.Logf("Cluster Name : %s", clusterName)
		hostIPs := vcutil.GetAllHostsIPsInCluster(ctx, e2eTestConfig, clusterName)
		// isHostDServiceStopped = true
		framework.Logf("No of hosts in the cluster : %s = %d", clusterName, len(hostIPs))

		var wg sync.WaitGroup
		wg.Add(len(hostIPs))
		for _, hostIP := range hostIPs {
			go k8testutil.StopHostd(ctx, e2eTestConfig, hostIP, &wg)
		}
		wg.Wait()

		ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
		time.Sleep(constants.PollTimeoutSixMin)

		wg.Add(len(hostIPs))
		for _, hostIP := range hostIPs {
			go k8testutil.StartHostd(ctx, e2eTestConfig, hostIP, &wg)
		}
		wg.Wait()
		// isHostDServiceStopped = false

	case constants.VpxaServiceName:
		ginkgo.By("Fetch IPs for the all the hosts in the cluster")
		clusterName := os.Getenv(constants.EnvComputeClusterName)
		framework.Logf("Cluster Name : %s", clusterName)
		hostIPs := vcutil.GetAllHostsIPsInCluster(ctx, e2eTestConfig, clusterName)
		isVpxaServiceStopped = true
		framework.Logf("No of hosts in the cluster : %s = %d", clusterName, len(hostIPs))

		var wg sync.WaitGroup
		wg.Add(len(hostIPs))

		for _, hostIP := range hostIPs {
			go k8testutil.StopVpxa(ctx, e2eTestConfig, hostIP, &wg)
		}
		wg.Wait()

		defer func() {
			framework.Logf("In defer function to start the vpxa service on all hosts")
			if isVpxaServiceStopped {
				for _, hostIP := range hostIPs {
					k8testutil.StartVpxaOnHost(ctx, e2eTestConfig, hostIP)
				}
				isVpxaServiceStopped = false
			}
		}()

		ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
		time.Sleep(constants.PollTimeoutSixMin)

		for _, hostIP := range hostIPs {
			k8testutil.StartVpxaOnHost(ctx, e2eTestConfig, hostIP)
		}
		isVpxaServiceStopped = false

	default:
		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
		err := vcutil.InvokeVCenterServiceControl(&e2eTestConfig.TestInput.TestBedInfo, ctx, constants.StopOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = true
		err = vcutil.WaitVCenterServiceToBeInState(ctx, e2eTestConfig, serviceName, vcAddress, constants.SvcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err = vcutil.InvokeVCenterServiceControl(&e2eTestConfig.TestInput.TestBedInfo, ctx, constants.StartOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = vcutil.WaitVCenterServiceToBeInState(ctx, e2eTestConfig, serviceName, vcAddress, constants.SvcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}
		}()

		ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
		time.Sleep(constants.PollTimeoutSixMin)

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
		err = vcutil.InvokeVCenterServiceControl(&e2eTestConfig.TestInput.TestBedInfo, ctx, constants.StartOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = false
		err = vcutil.WaitVCenterServiceToBeInState(ctx, e2eTestConfig, serviceName, vcAddress, constants.SvcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Sleeping for full sync interval")
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
	}
}

func getStorageClass(ctx context.Context, scParameters map[string]string, client clientset.Interface, namespace string, storagePolicyName string) *storagev1.StorageClass {
	var err error
	var storageclass *storagev1.StorageClass
	// Decide which test setup is available to run
	if e2eTestConfig.TestInput.ClusterFlavor.VanillaCluster {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		// TODO: Create Thick Storage Policy from Pre-setup
		scParameters[constants.ScParamStoragePolicyName] = "Management Storage Policy - Regular"
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "transactionsupport" + curtimestring + val
		storageclass, err = k8testutil.CreateStorageClass(client, e2eTestConfig, scParameters, nil, "", "", false, scName)
	} else if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		thickProvPolicy := os.Getenv(constants.EnvStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(constants.EnvStoragePolicyNameWithThickProvision + " env variable not set")
		}
		profileID := vcutil.GetSpbmPolicyID(thickProvPolicy, e2eTestConfig)
		scParameters[constants.ScParamStoragePolicyID] = profileID
		// create resource quota
		//createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		restConfig := k8testutil.GetRestConfigClient(e2eTestConfig)
		k8testutil.SetStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, constants.RqLimit)
		storageclass, err = k8testutil.CreateStorageClass(client, e2eTestConfig, scParameters, nil, "", "", false, thickProvPolicy)
	} else {
		ginkgo.By("CNS_TEST: Running for GC setup")
		thickProvPolicy := os.Getenv(constants.EnvStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(constants.EnvStoragePolicyNameWithThickProvision + " env variable not set")
		}
		k8testutil.CreateResourceQuota(client, e2eTestConfig, namespace, constants.RqLimit, thickProvPolicy)
		scParameters[constants.SvStorageClassName] = thickProvPolicy
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, thickProvPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		if e2eTestConfig.TestInput.ClusterFlavor.VanillaCluster {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()
	return storageclass
}
