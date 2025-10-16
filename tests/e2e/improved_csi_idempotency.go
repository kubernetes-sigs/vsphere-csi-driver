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
	appsv1 "k8s.io/api/apps/v1"
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
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("Improved CSI Idempotency Tests", func() {
	f := framework.NewDefaultFramework("idempotency-csi")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	const defaultVolumeOpsScale = 30
	const defaultVolumeOpsScaleWCP = 29
	var (
		client            clientset.Interface
		c                 clientset.Interface
		fullSyncWaitTime  int
		namespace         string
		scParameters      map[string]string
		storagePolicyName string
		volumeOpsScale    int
		isServiceStopped  bool
		serviceName       string
		csiReplicaCount   int32
		deployment        *appsv1.Deployment
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		isServiceStopped = false
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")

		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}

		if os.Getenv("VOLUME_OPS_SCALE") != "" {
			volumeOpsScale, err = strconv.Atoi(os.Getenv(envVolumeOperationsScale))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			if vanillaCluster {
				volumeOpsScale = defaultVolumeOpsScale
			} else {
				volumeOpsScale = defaultVolumeOpsScaleWCP
			}
		}
		framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)

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

		// Get CSI Controller's replica count from the setup
		controllerClusterConfig := os.Getenv(contollerClusterKubeConfig)
		c = client
		if controllerClusterConfig != "" {
			framework.Logf("Creating client for remote kubeconfig")
			remoteC, err := createKubernetesClientFromConfig(controllerClusterConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			c = remoteC
		}
		deployment, err = c.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount = *deployment.Spec.Replicas
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if isServiceStopped {
			if serviceName == "CSI" {
				framework.Logf("Starting CSI driver")
				ignoreLabels := make(map[string]string)
				err := updateDeploymentReplicawithWait(c, csiReplicaCount, vSphereCSIControllerPodNamePrefix,
					csiSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Wait for the CSI Pods to be up and Running
				list_of_pods, err := fpod.GetPodsInNamespace(ctx, client, csiSystemNamespace, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				num_csi_pods := len(list_of_pods)
				err = fpod.WaitForPodsRunningReady(ctx, client, csiSystemNamespace, int(num_csi_pods),
					time.Duration(pollTimeout))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else if serviceName == hostdServiceName {
				framework.Logf("In afterEach function to start the hostd service on all hosts")
				hostIPs := getAllHostsIP(ctx, true)
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
			} else {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err := invokeVCenterServiceControl(ctx, startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By(fmt.Sprintf("Resetting provisioner time interval to %s sec", defaultProvisionerTimeInSec))
		updateCSIDeploymentProvisionerTimeout(c, csiSystemNamespace, defaultProvisionerTimeInSec)

		if supervisorCluster {
			deleteResourceQuota(client, namespace)
			dumpSvcNsEventsOnTestFailure(client, namespace)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
			dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}
	})

	/*
		Reduce external provisioner timeout to very small interval and create pvc
		1. Create a SC using a thick provisioned policy
		2. Reduce external-provisioner timeout to 10s and wait for csi pod rollout
		3. Create PVCs using SC
		4. Wait for PVCs created to be bound
		5. Delete PVCs and SC
		6. Restore external-provisioner timeout to default value
		7. Verify no orphan volumes are left (using cnsctl tool)
	*/

	ginkgo.It("[stable-pq-vks][csi-block-vanilla][csi-file-vanilla][csi-guest][csi-supervisor][pq-vanilla-file]"+
		"[pq-vanilla-block][pq-vks][pq-vks-n1][pq-vks-n2] Reduce external provisioner timeout and create "+
		"volumes - idempotency", ginkgo.Label(p0, disruptive, block, file, windows, wcp, tkg, vanilla, vc70), func() {
		createVolumesByReducingProvisionerTime(namespace, client, storagePolicyName, scParameters,
			volumeOpsScale, shortProvisionerTimeout, c)
	})

	/*
		Create volume when hostd(esxi) goes down
		1. Create a SC using a thick provisioned policy
		2. Create a PVCs using SC
		3. Find the esxi host on which the volume is being created
			and bring hostd service on the host down and wait for 5mins (default provisioner timeout)
		4. Bring up hostd service on the host from step 4
		5. Wait for PVCs to be bound
		6. Delete PVCs and SC
		7. Verify no orphan volumes are left

		Verify create volume when esx host is temporarily is disconnected
		1. Create a SC using a thick provisioned policy
		2. Create a PVCs using SC
		3. Find the host on which the volume is getting created and disconnect that host for 6 mins and reconnect it
		4. Wait for PVCs to be bound
		5. Delete PVCs and SC
		6. Verify no orphan volumes are left
	*/
	ginkgo.It("[stable-pq-vks][csi-block-vanilla][csi-file-vanilla][csi-guest][csi-supervisor][pq-vanilla-file]"+
		"[pq-vanilla-block][pq-vks][pq-vks-n1][pq-vks-n2] create volume when hostd service goes down - "+
		"idempotency", ginkgo.Label(p0, disruptive, block, file, windows, wcp, tkg, vanilla, vc70), func() {
		serviceName = hostdServiceName
		createVolumeWithServiceDown(serviceName, namespace, client, storagePolicyName,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

	/*
		Create volume when CNS goes down
		1. Create a SC using a thick provisioned policy
		2. Create a PVCs using SC
		3. Bring down vsan-health service and wait for 5mins (default provisioner timeout)
		4. Bring up vsan-health
		5. Wait for pvcs to be bound
		6. Delete pvcs and SC
		7. Verify no orphan volumes are left
	*/
	ginkgo.It("[stable-pq-vks][csi-block-vanilla][csi-file-vanilla][csi-guest][csi-supervisor][pq-vanilla-file]"+
		"[pq-vanilla-block][pq-vks][pq-vks-n1][pq-vks-n2] create volume when CNS goes down - idempotency", ginkgo.Label(p0,
		disruptive, block, file, windows, wcp, tkg, vanilla, vc70), func() {
		serviceName = vsanhealthServiceName
		createVolumeWithServiceDown(serviceName, namespace, client, storagePolicyName,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

	/*
		Create volume when vpxd goes down
		1. Create a SC using a thick provisioned policy
		2. Create a PVCs using SC
		3. Bring down vpxd service and wait for 5mins (default provisioner timeout)
		4. Bring up vpxd
		5. Wait for pvcs to be bound
		6. Delete pvcs and SC
		7. Verify no orphan volumes are left
	*/
	ginkgo.It("[stable-pq-vks][csi-block-vanilla][csi-file-vanilla][csi-guest][csi-supervisor][pq-vanilla-file]"+
		"[pq-vanilla-block][pq-vks][pq-vks-n1][pq-vks-n2] create volume when VPXD goes down - "+
		"idempotency", ginkgo.Label(p0, disruptive, block, file, windows, wcp, tkg, vanilla, vc70), func() {
		serviceName = vpxdServiceName
		createVolumeWithServiceDown(serviceName, namespace, client, storagePolicyName,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

	/*
		Create volume when SPS goes down
		1. Create a SC using a thick provisioned policy
		2. Create a PVCs using SC
		3. Bring down sps service and wait for 5mins (default provisioner timeout)
		4. Bring up sps
		5. Wait for pvcs to be bound
		6. Delete pvcs and SC
		7. Verify no orphan volumes are left
	*/
	ginkgo.It("[stable-pq-vks][csi-block-vanilla][csi-file-vanilla][csi-guest][csi-supervisor][pq-vanilla-file]"+
		"[pq-vanilla-block][pq-vks][pq-vks-n1][pq-vks-n2] create volume when SPS goes down - idempotency", ginkgo.Label(p0,
		block, file, windows, wcp, tkg, vanilla, vc80), func() {
		serviceName = spsServiceName
		createVolumeWithServiceDown(serviceName, namespace, client, storagePolicyName,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

	/*
		Create volume when csi restarts
		1. Create a SC using a thick provisioned policy
		2. Create a PVCs using SC
		3. Restart CSI driver when VC task has started
		4. Wait for PVCs to be bound
		5. Delete PVCs and SC
		6. Verify no orphan volumes are left
	*/
	ginkgo.It("[stable-pq-vks][csi-block-vanilla][csi-file-vanilla][csi-guest][csi-supervisor][pq-vanilla-file]"+
		"[pq-vanilla-block][pq-vks][pq-vks-n1][pq-vks-n2] create volume when CSI restarts - idempotency", ginkgo.Label(p0,
		disruptive, block, file, windows, wcp, tkg, vanilla, vc70), func() {
		serviceName = "CSI"
		createVolumeWithServiceDown(serviceName, namespace, client, storagePolicyName,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

	/*
		Extend volume when csi restarts
		1. Create a SC using a thick provisioned policy
		2. Create a PVCs using SC and wait for PVCs to be bound
		3. Extend pvcs size
		4. Restart CSI when VC task is ongoing
		5. Verify resize was successful
		6. Delete pvcs and SC
		7. Verify no orphan volumes are left
	*/
	ginkgo.It("[stable-pq-vks][csi-block-vanilla][csi-file-vanilla][csi-guest][csi-supervisor][pq-vanilla-file]"+
		"[pq-vanilla-block][pq-vks][pq-vks-n1][pq-vks-n2] extend volume when csi restarts - idempotency", ginkgo.Label(p0,
		disruptive, block, file, windows, wcp, tkg, vanilla, vc70), func() {
		serviceName = "CSI"
		extendVolumeWithServiceDown(serviceName, namespace, client, storagePolicyName, scParameters,
			volumeOpsScale, true, isServiceStopped, c)
	})

	/*
		Extend volume when CNS goes down
		1. Create a SC using a thick provisioned policy
		2. Create a PVCs using SC and wait for PVC to be bound
		3. Extend pvcs and wait for vc task to start
		4. Bring down vsan-health service for 6mins
		5. Restart vsan-health service
		6. Verify resize was successful
		7. Delete pvcs and SC
		8. Verify no orphan volumes are left
	*/
	ginkgo.It("[stable-pq-vks][csi-block-vanilla][csi-file-vanilla][csi-guest][csi-supervisor][pq-vanilla-file]"+
		"[pq-vanilla-block][pq-vks][pq-vks-n1][pq-vks-n2] extend volume when CNS goes down - idempotency", ginkgo.Label(p0,
		disruptive, block, file, windows, wcp, tkg, vanilla, vc70), func() {
		serviceName = vsanhealthServiceName
		extendVolumeWithServiceDown(serviceName, namespace, client, storagePolicyName, scParameters,
			volumeOpsScale, true, isServiceStopped, c)
	})

	/*
		Create volume when SPS goes down
		1. Create a SC using a thick provisioned policy
		2. Create a PVCs using SC
		3. Bring down storage-quota-webhook service and wait (default provisioner timeout)
		4. Bring up storage-quota-webhook
		5. Wait for pvcs to be bound
		6. Delete pvcs and SC
		7. Verify no orphan volumes are left
	*/
	ginkgo.It("[csi-supervisor] create volume when storage-quota-weebhook goes down", ginkgo.Label(p0, disruptive,
		wcp), func() {
		serviceName = storageQuotaWebhookPrefix
		createVolumeWithServiceDown(serviceName, namespace, client, storagePolicyName,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

})

// createVolumesByReducingProvisionerTime creates the volumes by reducing the provisioner timeout
func createVolumesByReducingProvisionerTime(namespace string, client clientset.Interface, storagePolicyName string,
	scParameters map[string]string, volumeOpsScale int, customProvisionerTimeout string, c clientset.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By(fmt.Sprintf("Invoking Test for create volume by reducing the provisioner timeout to %s",
		customProvisionerTimeout))
	var storageclass *storagev1.StorageClass
	var persistentvolumes []*v1.PersistentVolume
	var pvclaims []*v1.PersistentVolumeClaim
	var err error
	pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

	// Decide which test setup is available to run
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		// TODO: Create Thick Storage Policy from Pre-setup to support 6.7 Setups
		scParameters[scParamStoragePolicyName] = "Management Storage Policy - Regular"
		// Check if it is file volumes setups
		if rwxAccessMode {
			scParameters[scParamFsType] = nfs4FSType
		}
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "idempotency" + curtimestring + val
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, scName)
	} else if supervisorCluster {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		profileID := e2eVSphere.GetSpbmPolicyID(thickProvPolicy)
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, thickProvPolicy)
	} else {
		ginkgo.By("CNS_TEST: Running for GC setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		scParameters[svStorageClassName] = thickProvPolicy
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, thickProvPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		if vanillaCluster {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	// TODO: Stop printing csi logs on the console
	collectPodLogs(ctx, c, csiSystemNamespace)

	// This assumes the tkg-controller-manager's auto sync is disabled
	ginkgo.By(fmt.Sprintf("Reducing Provisioner time interval to %s Sec for the test...", customProvisionerTimeout))
	updateCSIDeploymentProvisionerTimeout(c, csiSystemNamespace, customProvisionerTimeout)

	defer func() {
		ginkgo.By(fmt.Sprintf("Resetting provisioner time interval to %s sec", defaultProvisionerTimeInSec))
		updateCSIDeploymentProvisionerTimeout(c, csiSystemNamespace, defaultProvisionerTimeInSec)
	}()

	ginkgo.By("Creating PVCs using the Storage Class")
	framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
	for i := 0; i < volumeOpsScale; i++ {
		framework.Logf("Creating pvc%v", i)
		accessMode := v1.ReadWriteOnce

		// Check if it is file volumes setups
		if rwxAccessMode {
			accessMode = v1.ReadWriteMany
		}
		pvclaims[i], err = createPVC(ctx, client, namespace, nil, "", storageclass, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
		2*framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// TODO: Add a logic to check for the no orphan volumes
	defer func() {
		for _, claim := range pvclaims {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
				framework.PodDeleteTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeID))
		}
	}()
}

// createVolumeWithServiceDown creates the volumes and immediately stops the services and wait for
// the service to be up again and validates the volumes are bound
func createVolumeWithServiceDown(serviceName string, namespace string, client clientset.Interface,
	storagePolicyName string, scParameters map[string]string, volumeOpsScale int, isServiceStopped bool,
	c clientset.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By(fmt.Sprintf("Invoking Test for create volume when %v goes down", serviceName))
	var storageclass *storagev1.StorageClass
	var persistentvolumes []*v1.PersistentVolume
	var pvclaims []*v1.PersistentVolumeClaim
	var err error
	var fullSyncWaitTime int
	pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

	// Decide which test setup is available to run
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		// TODO: Create Thick Storage Policy from Pre-setup to support 6.7 Setups
		scParameters[scParamStoragePolicyName] = "Management Storage Policy - Regular"
		// Check if it is file volumes setups
		if rwxAccessMode {
			scParameters[scParamFsType] = nfs4FSType
		}
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "idempotency" + curtimestring + val
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, scName)
	} else if supervisorCluster {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		profileID := e2eVSphere.GetSpbmPolicyID(thickProvPolicy)
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		//createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		restConfig = getRestConfigClient()
		setStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, rqLimit)
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, thickProvPolicy)
	} else {
		ginkgo.By("CNS_TEST: Running for GC setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		scParameters[svStorageClassName] = thickProvPolicy
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, thickProvPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		if vanillaCluster {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	var totalQuotaUsedBefore, storagePolicyQuotaBefore, storagePolicyUsageBefore *resource.Quantity
	if supervisorCluster {
		restConfig := getRestConfigClient()
		totalQuotaUsedBefore, _, storagePolicyQuotaBefore, _, storagePolicyUsageBefore, _ =
			getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, pvcUsage, volExtensionName, false)

	}

	ginkgo.By("Creating PVCs using the Storage Class")
	framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
	for i := 0; i < volumeOpsScale; i++ {
		framework.Logf("Creating pvc%v", i)
		accessMode := v1.ReadWriteOnce

		// Check if it is file volumes setups
		if rwxAccessMode {
			accessMode = v1.ReadWriteMany
		}
		pvclaims[i], err = createPVC(ctx, client, namespace, nil, "", storageclass, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	if serviceName == "CSI" {
		// Get CSI Controller's replica count from the setup
		deployment, err := c.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount := *deployment.Spec.Replicas

		ginkgo.By("Stopping CSI driver")
		isServiceStopped, err = stopCSIPods(ctx, c, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				framework.Logf("Starting CSI driver")
				isServiceStopped, err = startCSIPods(ctx, c, csiReplicaCount, csiSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		framework.Logf("Starting CSI driver")
		isServiceStopped, err = startCSIPods(ctx, c, csiReplicaCount, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
	} else if serviceName == hostdServiceName {
		ginkgo.By("Fetch IPs for the all the hosts in the cluster")
		hostIPs := getAllHostsIP(ctx, true)
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
	} else if serviceName == "storage-quota-webhook" {
		// Get CSI Controller's replica count from the setup
		deployment, err := c.AppsV1().Deployments(kubeSystemNamespace).Get(ctx,
			storageQuotaWebhookPrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount := *deployment.Spec.Replicas

		ginkgo.By("Stopping webhook driver")
		isServiceStopped, err = stopStorageQuotaWebhookPodInKubeSystem(ctx, c, kubeSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				framework.Logf("Starting storage-quota-webhook driver")
				isServiceStopped, err = startStorageQuotaWebhookPodInKubeSystem(ctx, c, csiReplicaCount, kubeSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		framework.Logf("Starting storage-quota-webhook ")
		isServiceStopped, err = startStorageQuotaWebhookPodInKubeSystem(ctx, c, csiReplicaCount, kubeSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
	} else {
		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
		err = invokeVCenterServiceControl(ctx, stopOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = true
		err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err = invokeVCenterServiceControl(ctx, startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}
		}()

		ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
		time.Sleep(pollTimeoutSixMin)

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
		err = invokeVCenterServiceControl(ctx, startOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = false
		err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Sleeping for full sync interval")
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
	}

	//After service restart
	bootstrap()

	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
		2*framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// TODO: Add a logic to check for the no orphan volumes
	defer func() {
		for _, claim := range pvclaims {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
				framework.PodDeleteTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeID))
		}
	}()

	newdiskSizeInMb := diskSizeInMb * int64(volumeOpsScale)
	newdiskSizeInMbstr := convertInt64ToStrMbFormat(newdiskSizeInMb)
	if supervisorCluster {
		sp_quota_pvc_status, sp_usage_pvc_status := validateQuotaUsageAfterResourceCreation(ctx, restConfig,
			storageclass.Name, namespace, pvcUsage, volExtensionName,
			[]string{newdiskSizeInMbstr}, totalQuotaUsedBefore, storagePolicyQuotaBefore,
			storagePolicyUsageBefore, false)
		gomega.Expect(sp_quota_pvc_status && sp_usage_pvc_status).NotTo(gomega.BeFalse())

	}

}

// extendVolumeWithServiceDown extends the volumes and immediately stops the service and wait for
// the service to be up again and validates the volumes are bound
func extendVolumeWithServiceDown(serviceName string, namespace string, client clientset.Interface,
	storagePolicyName string, scParameters map[string]string, volumeOpsScale int, extendVolume bool,
	isServiceStopped bool, c clientset.Interface) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By(fmt.Sprintf("Invoking Test for create volume when %v goes down", serviceName))
	var storageclass *storagev1.StorageClass
	var persistentvolumes []*v1.PersistentVolume
	var pvclaims []*v1.PersistentVolumeClaim
	var err error
	var fullSyncWaitTime int
	pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

	// Decide which test setup is available to run
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		// TODO: Create Thick Storage Policy from Pre-setup to support 6.7 Setups
		scParameters[scParamStoragePolicyName] = "Management Storage Policy - Regular"
		// Check nfs4FSType for file volumes
		if rwxAccessMode {
			ginkgo.Skip("File Volume extend is not supported, skipping the test")
		}
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "idempotency" + curtimestring + val
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", true, scName)
	} else if supervisorCluster {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		profileID := e2eVSphere.GetSpbmPolicyID(thickProvPolicy)
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		restConfig = getRestConfigClient()
		setStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, rqLimit)
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", true, thickProvPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		ginkgo.By("CNS_TEST: Running for GC setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		scParameters[svStorageClassName] = thickProvPolicy
		if windowsEnv {
			scParameters[scParamFsType] = ntfsFSType
		} else {
			scParameters[scParamFsType] = ext4FSType
		}
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, thickProvPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		var allowExpansion = true
		storageclass.AllowVolumeExpansion = &allowExpansion
		storageclass, err = client.StorageV1().StorageClasses().Update(ctx, storageclass, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		if vanillaCluster {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	var totalQuotaUsedBefore, storagePolicyQuotaBefore, storagePolicyUsageBefore *resource.Quantity

	if supervisorCluster {
		restConfig := getRestConfigClient()
		totalQuotaUsedBefore, _, storagePolicyQuotaBefore, _, storagePolicyUsageBefore, _ =
			getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, pvcUsage, volExtensionName, false)
	}

	ginkgo.By("Creating PVCs using the Storage Class")
	framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
	for i := 0; i < volumeOpsScale; i++ {
		framework.Logf("Creating pvc%v", i)
		pvclaims[i], err = fpv.CreatePVC(ctx, client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, ""))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
		2*framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// TODO: Add a logic to check for the no orphan volumes
	defer func() {
		for _, claim := range pvclaims {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
				framework.PodDeleteTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the "+
					"CNS after it is deleted from kubernetes", volumeID))
		}
	}()

	newdiskSizeInMb := diskSizeInMb * int64(volumeOpsScale)
	newdiskSizeInMbStr := convertInt64ToStrMbFormat(newdiskSizeInMb)
	if supervisorCluster {
		sp_quota_pvc_status, sp_usage_pvc_status := validateQuotaUsageAfterResourceCreation(ctx, restConfig,
			storageclass.Name, namespace, pvcUsage, volExtensionName,
			[]string{newdiskSizeInMbStr}, totalQuotaUsedBefore, storagePolicyQuotaBefore,
			storagePolicyUsageBefore, false)
		gomega.Expect(sp_quota_pvc_status && sp_usage_pvc_status).NotTo(gomega.BeFalse())

	}

	ginkgo.By("Create POD")
	pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Modify PVC spec to trigger volume expansion
	// We expand the PVC while no pod is using it to ensure offline expansion
	ginkgo.By("Expanding current pvc")
	for _, claim := range pvclaims {
		currentPvcSize := claim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("4Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		claims, err := expandPVCSize(claim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(claims).NotTo(gomega.BeNil())
	}

	if serviceName == "CSI" {
		// Get CSI Controller's replica count from the setup
		deployment, err := c.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount := *deployment.Spec.Replicas

		ginkgo.By("Stopping CSI driver")
		isServiceStopped, err = stopCSIPods(ctx, c, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				framework.Logf("Starting CSI driver")
				isServiceStopped, err = startCSIPods(ctx, c, csiReplicaCount, csiSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		framework.Logf("Starting CSI driver")
		isServiceStopped, err = startCSIPods(ctx, c, csiReplicaCount, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
	} else {
		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
		err = invokeVCenterServiceControl(ctx, stopOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = true
		err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if isServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err = invokeVCenterServiceControl(ctx, startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}
		}()

		ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
		time.Sleep(pollTimeoutSixMin)

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
		err = invokeVCenterServiceControl(ctx, startOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = false
		err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(totalResizeWaitPeriod)
	}
	//After service restart
	bootstrap()

	ginkgo.By("Waiting for file system resize to finish")
	for _, claim := range pvclaims {
		claims, err := waitForFSResize(claim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := claims.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")
	}

	if supervisorCluster {
		totalquotaAfterExpansion, _ := getTotalQuotaConsumedByStoragePolicy(ctx,
			restConfig, storageclass.Name, namespace, false)
		framework.Logf("totalquotaAfterExpansion :%v", totalquotaAfterExpansion)

		storagepolicyquotaAfterExpansion, _ := getStoragePolicyQuotaForSpecificResourceType(ctx,
			restConfig, storageclass.Name, namespace, volExtensionName, false)
		framework.Logf("storagepolicyquotaAfterExpansion :%v", storagepolicyquotaAfterExpansion)

		storagepolicyUsageAfterExpansion, _ := getStoragePolicyUsageForSpecificResourceType(ctx, restConfig,
			storageclass.Name, namespace, pvcUsage)
		framework.Logf("storagepolicy_usage_pvc_after_expansion :%v", storagepolicyUsageAfterExpansion)

		newDiskSizeinMb := diskSizeInMb * 3 * int64(volumeOpsScale)
		newDiskSizeinMbstr := convertInt64ToStrMbFormat(newDiskSizeinMb)

		//New size is 6Gi, diskSizeInMb is 2Gi so multiplying by 3 to make the expected quota consumption value
		quotavalidationStatus := validate_totalStoragequota(ctx, []string{newDiskSizeinMbstr},
			totalQuotaUsedBefore, totalquotaAfterExpansion)
		gomega.Expect(quotavalidationStatus).NotTo(gomega.BeFalse())
		quotavalidationStatus = validate_totalStoragequota(ctx, []string{newDiskSizeinMbstr},
			storagePolicyQuotaBefore, storagepolicyquotaAfterExpansion)
		gomega.Expect(quotavalidationStatus).NotTo(gomega.BeFalse())
		quotavalidationStatus = validate_totalStoragequota(ctx, []string{newDiskSizeinMbstr},
			storagePolicyUsageBefore, storagepolicyUsageAfterExpansion)
		gomega.Expect(quotavalidationStatus).NotTo(gomega.BeFalse())
	}
}
