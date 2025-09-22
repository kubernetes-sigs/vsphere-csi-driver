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
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/zapr"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	cr_log "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

var _ = ginkgo.Describe("Transaction_Support_Create_Volume", func() {
	f := framework.NewDefaultFramework("transaction-support")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	log := logger.GetLogger(context.Background())
	cr_log.SetLogger(zapr.NewLogger(log.Desugar()))

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
		csiReplicaCount   int32
		deployment        *appsv1.Deployment
	)
	ginkgo.DescribeTableSubtree("Transaction_Support_Table_Tests",
		func(serviceNames []string) {

			ginkgo.BeforeEach(func() {
				e2eTestConfig = bootstrap.Bootstrap()
				client = f.ClientSet
				vcAddress = e2eTestConfig.TestInput.TestBedInfo.VcAddress
				namespace = vcutil.GetNamespaceToRunTests(f, e2eTestConfig)
				scParameters = make(map[string]string)
				storagePolicyName = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicyNameForSharedDatastores)
				dsType = env.GetStringEnvVarOrDefault(constants.EnvDatastoreType, constants.Vmfs)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
				framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
				isTestPassed = false

				if !(len(nodeList.Items) > 0) {
					framework.Failf("Unable to find ready and schedulable Node")
				}

				if e2eTestConfig.TestInput.ClusterFlavor.GuestCluster {
					svcClient, svNamespace := k8testutil.GetSvcClientAndNamespace()
					k8testutil.SetResourceQuota(svcClient, svNamespace, constants.RqLimit)
				}

				if os.Getenv("VOLUME_OPS_SCALE") != "" {
					volumeOpsScale, err = strconv.Atoi(os.Getenv(constants.EnvVolumeOperationsScale))
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else {
					if e2eTestConfig.TestInput.ClusterFlavor.VanillaCluster {
						volumeOpsScale = defaultVolumeOpsScale
					} else {
						volumeOpsScale = defaultVolumeOpsScaleWCP
					}
				}
				framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)

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

				// Get CSI Controller's replica count from the setup
				controllerClusterConfig := os.Getenv(constants.ContollerClusterKubeConfig)
				c = client
				if controllerClusterConfig != "" {
					framework.Logf("Creating client for remote kubeconfig")
					remoteC, err := k8testutil.CreateKubernetesClientFromConfig(controllerClusterConfig)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					c = remoteC
				}
				deployment, err = c.AppsV1().Deployments(constants.CsiSystemNamespace).Get(ctx,
					constants.VSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				csiReplicaCount = *deployment.Spec.Replicas
			})

			ginkgo.AfterEach(func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				for _, serviceName := range serviceNames {
					switch serviceName {
					case constants.CsiServiceName:
						framework.Logf("Starting CSI driver")
						ignoreLabels := make(map[string]string)
						err := k8testutil.UpdateDeploymentReplicawithWait(c, csiReplicaCount, constants.VSphereCSIControllerPodNamePrefix,
							constants.CsiSystemNamespace)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						// Wait for the CSI Pods to be up and Running
						list_of_pods, err := fpod.GetPodsInNamespace(ctx, client, constants.CsiSystemNamespace, ignoreLabels)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						num_csi_pods := len(list_of_pods)
						err = fpod.WaitForPodsRunningReady(ctx, client, constants.CsiSystemNamespace, int(num_csi_pods),
							time.Duration(constants.PollTimeout))
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					case constants.HostdServiceName:
						framework.Logf("In afterEach function to start the hostd service on all hosts")
						hostIPs := vcutil.GetAllHostsIP(ctx, e2eTestConfig, true)
						for _, hostIP := range hostIPs {
							k8testutil.StartHostDOnHost(ctx, e2eTestConfig, hostIP)
						}
					case constants.VpxaServiceName:
						framework.Logf("In afterEach function to start the vpxa service on all hosts")
						hostIPs := vcutil.GetAllHostsIP(ctx, e2eTestConfig, true)
						for _, hostIP := range hostIPs {
							k8testutil.StartVpxaOnHost(ctx, e2eTestConfig, hostIP)
						}
					default:
						ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
						err := vcutil.InvokeVCenterServiceControl(&e2eTestConfig.TestInput.TestBedInfo, ctx, constants.StartOperation, serviceName, vcAddress)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						err = vcutil.WaitVCenterServiceToBeInState(ctx, e2eTestConfig, serviceName, vcAddress, constants.SvcRunningMessage)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}

				ginkgo.By(fmt.Sprintf("Resetting provisioner time interval to %s sec", constants.DefaultProvisionerTimeInSec))
				k8testutil.UpdateCSIDeploymentProvisionerTimeout(c, constants.CsiSystemNamespace, constants.DefaultProvisionerTimeInSec)

				framework.Logf("Is Test %v passed %t ", serviceNames, isTestPassed)
				if isTestPassed { //If test passed then only doing cleanup otherwise keeping the things as it is
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
						err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volumeID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred(),
							fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
								"kubernetes", volumeID))
					}

					if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
						k8testutil.DeleteResourceQuota(client, namespace)
						k8testutil.DumpSvcNsEventsOnTestFailure(client, namespace)
					} else if e2eTestConfig.TestInput.ClusterFlavor.GuestCluster {
						svcClient, svNamespace := k8testutil.GetSvcClientAndNamespace()
						k8testutil.SetResourceQuota(svcClient, svNamespace, constants.RqLimit)
						k8testutil.DumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
					}
				}
			})

			/*
				Create volume when service(s) goes down
				1. Create a SC using a thick provisioned policy
				2. Create a PVCs using SC
				3. Bring down hostd service on all the hosts and wait for 5mins (default provisioner timeout)
				4. Bring up hostd service on the host from step 4
				5. Wait for PVCs to be bound
				6. Delete PVCs and SC
				7. Verify no orphan volumes are left
			*/

			ginkgo.It("[csi-block-vanilla] [csi-guest] [csi-supervisor] "+
				"Veify Create Volume With Transaction Support During Service Down-APD-vSAN-Partitioning", ginkgo.Label(constants.P0, constants.Disruptive, constants.Block,
				constants.Windows, constants.Wcp, constants.Tkg, constants.Vanilla, constants.Vc91), func() {
				if slices.Contains(serviceNames, constants.ApdName) {
					if dsType != constants.Vmfs {
						framework.Logf("Currently APD test(s) are only covered for VMFS datastore")
						ginkgo.Skip("Currently APD test(s) are only covered for VMFS datastore")
					}
				} else if slices.Contains(serviceNames, constants.VsanPartition) {
					if dsType != constants.Vsan {
						framework.Logf("Vsan-Partition test(s) are only for VSAN datastore")
						ginkgo.Skip("Vsan-Partition test(s) are only for VSAN datastore")
					}
				}
				createVolumeWithServiceDown(serviceNames, namespace, client, storagePolicyName,
					scParameters, volumeOpsScale, c)
			})
		},

		ginkgo.Entry("CSI-Service-Down", []string{constants.CsiServiceName}),
		ginkgo.Entry("Vsanhealth-Service-Down", []string{constants.VsanhealthServiceName}),
		ginkgo.Entry("Vpxd-Service-Down", []string{constants.VpxdServiceName}),
		ginkgo.Entry("SPS-Service-Down", []string{constants.SpsServiceName}),
		ginkgo.Entry("Vpxa-Service-Down", []string{constants.VpxaServiceName}),
		ginkgo.Entry("Hostd-Service-Down", []string{constants.HostdServiceName}),
		ginkgo.Entry("Wcp-Service-Down", []string{constants.WcpServiceName}),
		ginkgo.Entry("VcDb-Service-Down", []string{constants.VcDbServiceName}),

		ginkgo.Entry("CSI-Vsanhealth-Services-Down", []string{constants.CsiServiceName, constants.VsanhealthServiceName}),
		ginkgo.Entry("CSI-Vpxd-Services-Down", []string{constants.CsiServiceName, constants.VpxdServiceName}),
		ginkgo.Entry("CSI-Sps-Services-Down", []string{constants.CsiServiceName, constants.SpsServiceName}),
		ginkgo.Entry("CSI-Wcp-Services-Down", []string{constants.CsiServiceName, constants.WcpServiceName}),
		ginkgo.Entry("CSI-VcDb-Services-Down", []string{constants.CsiServiceName, constants.VcDbServiceName}),
		ginkgo.Entry("CSI-Hostd-Services-Down", []string{constants.CsiServiceName, constants.HostdServiceName}),
		ginkgo.Entry("CSI-Vpxa-Services-Down", []string{constants.CsiServiceName, constants.VpxaServiceName}),

		ginkgo.Entry("Vsanhealth-Vpxd-Services-Down", []string{constants.VsanhealthServiceName, constants.VpxdServiceName}),
		ginkgo.Entry("Vsanhealth-Sps-Services-Down", []string{constants.VsanhealthServiceName, constants.SpsServiceName}),
		ginkgo.Entry("Vsanhealth-Wcp-Services-Down", []string{constants.VsanhealthServiceName, constants.WcpServiceName}),
		ginkgo.Entry("Vsanhealth-VcDb-Services-Down", []string{constants.VsanhealthServiceName, constants.VcDbServiceName}),
		ginkgo.Entry("Vsanhealth-Hostd-Services-Down", []string{constants.VsanhealthServiceName, constants.HostdServiceName}),
		ginkgo.Entry("Vsanhealth-Vpxa-Services-Down", []string{constants.VsanhealthServiceName, constants.VpxaServiceName}),

		ginkgo.Entry("Vpxd-Sps-Services-Down", []string{constants.VpxdServiceName, constants.SpsServiceName}),
		ginkgo.Entry("Vpxd-Wcp-Services-Down", []string{constants.VpxdServiceName, constants.WcpServiceName}),
		ginkgo.Entry("Vpxd-VcDb-Services-Down", []string{constants.VpxdServiceName, constants.VcDbServiceName}),
		ginkgo.Entry("Vpxd-Hostd-Services-Down", []string{constants.VpxdServiceName, constants.HostdServiceName}),
		ginkgo.Entry("Vpxd-Vpxa-Services-Down", []string{constants.VpxdServiceName, constants.VpxaServiceName}),

		ginkgo.Entry("CSI-Vsanhealth-Vpxd-Services-Down", []string{constants.CsiServiceName, constants.VsanhealthServiceName, constants.VpxdServiceName}),
		ginkgo.Entry("CSI-Vpxd-Hostd-Services-Down", []string{constants.CsiServiceName, constants.VpxdServiceName, constants.HostdServiceName}),
		ginkgo.Entry("CSI-Vsanhealth-Sps-Services-Down", []string{constants.CsiServiceName, constants.VsanhealthServiceName, constants.SpsServiceName}),
		ginkgo.Entry("CSI-Vpxd-Wcp-Services-Down", []string{constants.CsiServiceName, constants.VpxdServiceName, constants.WcpServiceName}),
		ginkgo.Entry("CSI-Vsanhealth-Hostd-Services-Down", []string{constants.CsiServiceName, constants.VsanhealthServiceName, constants.HostdServiceName}),
		ginkgo.Entry("CSI-Vpxd-Vpxa-Services-Down", []string{constants.CsiServiceName, constants.VpxdServiceName, constants.VpxaServiceName}),
		ginkgo.Entry("CSI-Vsanhealth-Vpxa-Services-Down", []string{constants.CsiServiceName, constants.VsanhealthServiceName, constants.VpxaServiceName}),

		ginkgo.Entry("Datastore-APD", []string{constants.ApdName}),
		ginkgo.Entry("APD-CSI-Service-Down", []string{constants.ApdName, constants.CsiServiceName}),
		ginkgo.Entry("APD-Vsanhealth-Services-Down", []string{constants.ApdName, constants.VsanhealthServiceName}),
		ginkgo.Entry("APD-Vpxd-Services-Down", []string{constants.ApdName, constants.VpxdServiceName}),
		ginkgo.Entry("APD-Sps-Services-Down", []string{constants.ApdName, constants.SpsServiceName}),
		ginkgo.Entry("APD-Wcp-Services-Down", []string{constants.ApdName, constants.WcpServiceName}),
		ginkgo.Entry("APD-VcDb-Services-Down", []string{constants.ApdName, constants.VcDbServiceName}),
		ginkgo.Entry("APD-Hostd-Services-Down", []string{constants.ApdName, constants.HostdServiceName}),
		ginkgo.Entry("APD-Vpxa-Services-Down", []string{constants.ApdName, constants.VpxaServiceName}),

		ginkgo.Entry("VSAN-Partitioning", []string{constants.VsanPartition}),
		ginkgo.Entry("VSAN-Partitioning-CSI-Service-Down", []string{constants.VsanPartition, constants.CsiServiceName}),
		ginkgo.Entry("VSAN-Partitioning-Vsanhealth-Services-Down", []string{constants.VsanPartition, constants.VsanhealthServiceName}),
		ginkgo.Entry("VSAN-Partitioning-Vpxd-Services-Down", []string{constants.VsanPartition, constants.VpxdServiceName}),
		ginkgo.Entry("VSAN-Partitioning-Sps-Services-Down", []string{constants.VsanPartition, constants.SpsServiceName}),
		ginkgo.Entry("VSAN-Partitioning-Wcp-Services-Down", []string{constants.VsanPartition, constants.WcpServiceName}),
		ginkgo.Entry("VSAN-Partitioning-VcDb-Services-Down", []string{constants.VsanPartition, constants.VcDbServiceName}),
		ginkgo.Entry("VSAN-Partitioning-Hostd-Services-Down", []string{constants.VsanPartition, constants.HostdServiceName}),
		ginkgo.Entry("VSAN-Partitioning-Vpxa-Services-Down", []string{constants.VsanPartition, constants.VpxaServiceName}),
	)
})

// createVolumeWithServiceDown creates the volumes and immediately restart the services and wait for
// the service to be up again and validates the volumes are bound
func createVolumeWithServiceDown(serviceNames []string, namespace string, client clientset.Interface,
	storagePolicyName string, scParameters map[string]string, volumeOpsScale int,
	c clientset.Interface) {
	var err error
	var accessMode v1.PersistentVolumeAccessMode

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	diskSize := constants.DiskSize5GB
	diskSizeInMb := constants.DiskSize5GBInMb //TODO modify these values as per datastore

	ginkgo.By(fmt.Sprintf("`Invoking Test for create volume when` %v goes down", serviceNames))
	pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

	storageclass := getStorageClass(ctx, scParameters, client, namespace, storagePolicyName)
	dsFcdFootprintMapBeforeProvisioning := k8testutil.GetDatastoreFcdFootprint(ctx, e2eTestConfig)

	if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
		restConfig := k8testutil.GetRestConfigClient(e2eTestConfig)
		totalQuotaUsedBefore, _, storagePolicyQuotaBefore, _, storagePolicyUsageBefore, _ =
			k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName)
	}

	ginkgo.By("Creating PVCs using the Storage Class")
	var wg sync.WaitGroup
	framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
	wg.Add(len(serviceNames) + volumeOpsScale)

	if e2eTestConfig.TestInput.TestBedInfo.RwxAccessMode {
		accessMode = v1.ReadWriteMany
	} else {
		accessMode = v1.ReadWriteOnce
	}

	for i := range volumeOpsScale {
		framework.Logf("Creating pvc%v", i)
		go createPVC(ctx, client, namespace, diskSize, storageclass, accessMode, pvclaims, i, &wg)
	}

	for _, serviceName := range serviceNames {
		go restartService(ctx, c, serviceName, &wg)
	}
	wg.Wait()

	//After service restart
	bootstrap.Bootstrap()

	ginkgo.By("Waiting for all claims to be in bound state")
	framework.Logf("Waiting for all claims : %d (volumeOpsScale : %d) to be in bound state ", len(pvclaims), volumeOpsScale)

	persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
		2*framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
			err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeID))
		}
	}()

	gomega.Expect(len(pvclaims) == volumeOpsScale).NotTo(gomega.BeFalse())
	// Wait for quota updation
	framework.Logf("Waiting for qutoa updation")
	time.Sleep(1 * time.Minute)

	newdiskSizeInMb := diskSizeInMb * int64(volumeOpsScale)
	newdiskSizeInBytes := newdiskSizeInMb * int64(1024) * int64(1024)
	if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
		restConfig := k8testutil.GetRestConfigClient(e2eTestConfig)
		total_quota_used_status, sp_quota_pvc_status, sp_usage_pvc_status := k8testutil.ValidateQuotaUsageAfterResourceCreation(ctx, restConfig,
			storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName,
			newdiskSizeInMb, totalQuotaUsedBefore, storagePolicyQuotaBefore,
			storagePolicyUsageBefore)
		gomega.Expect(total_quota_used_status && sp_quota_pvc_status && sp_usage_pvc_status).NotTo(gomega.BeFalse())
	}

	dsFcdFootprintMapAfterProvisioning := k8testutil.GetDatastoreFcdFootprint(ctx, e2eTestConfig)
	//Verify Vmdk count and fcd/volume list and used space
	usedSpaceRetVal, numberOfVmdksRetVal, numberOfFcdsRetVal, numberOfVolumesRetVal, _, deltaUsedSpace := k8testutil.ValidateSpaceUsageAfterResourceCreationUsingDatastoreFcdFootprint(dsFcdFootprintMapBeforeProvisioning, dsFcdFootprintMapAfterProvisioning, newdiskSizeInBytes, volumeOpsScale)
	framework.Logf("Is Datastore Used Space Matched : %t, Delta Used Space If any : %d", usedSpaceRetVal, deltaUsedSpace)
	framework.Logf("Is Num of Vmdks Matched : %t", numberOfVmdksRetVal)
	framework.Logf("Is Num of Fcds Matched : %t", numberOfFcdsRetVal)
	framework.Logf("Is Num of Volumes Matched : %t", numberOfVolumesRetVal)

	gomega.Expect(usedSpaceRetVal).NotTo(gomega.BeFalse(), "Used space not matched")
	gomega.Expect(numberOfVmdksRetVal).NotTo(gomega.BeFalse(), "Vmdks count not matched")
	gomega.Expect(numberOfFcdsRetVal).NotTo(gomega.BeFalse(), "Fcds count not matched")
	gomega.Expect(numberOfVolumesRetVal).NotTo(gomega.BeFalse(), "Volumes count not matched")

	// k8testutil.PvcUsability(ctx, e2eTestConfig, client, namespace, storageclass, pvclaims, diskSize)
	isTestPassed = true
}
