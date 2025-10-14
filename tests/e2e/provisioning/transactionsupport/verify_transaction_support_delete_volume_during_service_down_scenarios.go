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
	"slices"
	"sync"
	"time"

	"github.com/go-logr/zapr"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	cr_log "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

// TestCase holds the data for a single table-driven test case.
type TestCase struct {
	Description string   `json:"description"`
	Input       []string `json:"input"`
}

var _ = ginkgo.Describe("Transaction_Support_DeleteVolume", func() {

	f := framework.NewDefaultFramework("transaction-support")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	log := logger.GetLogger(context.Background())
	cr_log.SetLogger(zapr.NewLogger(log.Desugar()))

	ginkgo.Context("When one or more services are down", func() {
		// Generate entries dynamically from the JSON file at test construction time.
		var entries []ginkgo.TableEntry
		testCases := loadTestCases("transaction_support_test_cases.json")
		for _, tc := range testCases {
			entries = append(entries, ginkgo.Entry(tc.Description, tc.Input))
		}

		ginkgo.DescribeTableSubtree("Transaction_Support_DeleteVolume_Table_Tests",
			func(serviceNames []string) {

				ginkgo.BeforeEach(func() {
					testSetUp(f)
				})

				ginkgo.AfterEach(func() {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					testCleanUp(ctx, serviceNames)
				})

				ginkgo.It("[csi-block-vanilla] [csi-guest] [csi-supervisor] "+
					"Veify Delete Volume With Transaction Support During Service Down-APD-vSAN-Partitioning", ginkgo.Label(constants.P0, constants.Disruptive, constants.Block,
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
					deleteVolumeWithServiceDown(serviceNames, namespace, client, storagePolicyName,
						scParameters, volumeOpsScale, c)
				})
			},
			entries,
		)
	})
})

// createVolumeWithServiceDown creates the volumes and immediately restart the services and wait for
// the service to be up again and validates the volumes are bound
func deleteVolumeWithServiceDown(serviceNames []string, namespace string, client clientset.Interface,
	storagePolicyName string, scParameters map[string]string, volumeOpsScale int,
	c clientset.Interface) {
	var err error
	var accessMode v1.PersistentVolumeAccessMode

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	diskSize := constants.DiskSize10GB
	diskSizeInMb := constants.DiskSize10GBInMb //TODO modify these values as per datastore

	ginkgo.By(fmt.Sprintf("`Invoking Test for delete volume when` %v goes down", serviceNames))
	pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

	storageclass := getStorageClass(ctx, scParameters, client, namespace, storagePolicyName)

	ginkgo.By("Creating PVCs using the Storage Class")
	var wg sync.WaitGroup
	framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)

	wg.Add(volumeOpsScale)

	if e2eTestConfig.TestInput.TestBedInfo.RwxAccessMode {
		accessMode = v1.ReadWriteMany
	} else {
		accessMode = v1.ReadWriteOnce
	}

	for i := range volumeOpsScale {
		framework.Logf("Creating pvc %v", i)
		go createPVC(ctx, client, namespace, diskSize, storageclass, accessMode, pvclaims, i, &wg)
	}
	wg.Wait()

	ginkgo.By("Waiting for all claims to be in bound state")
	framework.Logf("Waiting for all claims : %d (volumeOpsScale : %d) to be in bound state ", len(pvclaims), volumeOpsScale)

	persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
		2*framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// defer func() {
	// 	for _, claim := range pvclaims {
	// 		err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 	}
	// 	ginkgo.By("Verify PVs, volumes are deleted from CNS")
	// 	for _, pv := range persistentvolumes {
	// 		err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
	// 			framework.PodDeleteTimeout)
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 		volumeID := pv.Spec.CSI.VolumeHandle
	// 		err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volumeID)
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
	// 			fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
	// 				"kubernetes", volumeID))
	// 	}
	// }()

	// Wait for quota updation
	framework.Logf("Waiting for qutoa updation")
	time.Sleep(1 * time.Minute)

	dsFcdFootprintMapBeforeProvisioning := k8testutil.GetDatastoreFcdFootprint(ctx, e2eTestConfig)

	if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
		restConfig := k8testutil.GetGcRestConfigClient(e2eTestConfig)
		totalQuotaUsedBefore, _, storagePolicyQuotaBefore, _, storagePolicyUsageBefore, _ =
			k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName)
	}

	wg.Add(len(serviceNames) + volumeOpsScale)
	for i := range volumeOpsScale {
		framework.Logf("Deleting pvc %v", i)
		go deletePVC(ctx, client, namespace, pvclaims, persistentvolumes, i, &wg)
	}

	for _, serviceName := range serviceNames {
		go restartService(ctx, c, serviceName, &wg)
	}
	wg.Wait()

	//After service restart
	e2eTestConfig = bootstrap.Bootstrap()

	ginkgo.By("Verify PVs, volumes are deleted from CNS")
	for _, pv := range persistentvolumes {
		err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
			framework.PodDeleteTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volumeID := pv.Spec.CSI.VolumeHandle
		err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volumeID)

		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
				"kubernetes", volumeID))
	}

	// Wait for quota updation
	framework.Logf("Waiting for qutoa updation")
	time.Sleep(2 * time.Minute)

	volumeOpsScale = volumeOpsScale * -1

	newdiskSizeInMb := diskSizeInMb * int64(volumeOpsScale)
	newdiskSizeInBytes := newdiskSizeInMb * int64(1024) * int64(1024)

	if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
		restConfig := k8testutil.GetGcRestConfigClient(e2eTestConfig)
		total_quota_used_status, sp_quota_pvc_status, sp_usage_pvc_status := k8testutil.ValidateQuotaUsageAfterResourceCreation(ctx, restConfig,
			storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName,
			newdiskSizeInMb, totalQuotaUsedBefore, storagePolicyQuotaBefore,
			storagePolicyUsageBefore)
		framework.Logf("Verification of quota usage status  %t:", total_quota_used_status && sp_quota_pvc_status && sp_usage_pvc_status)
		framework.Logf("Is totalQuotaUsedStatus Matched : %t", total_quota_used_status)
		framework.Logf("Is storagePolicyQuotaStatus : %t", sp_quota_pvc_status)
		framework.Logf("Is storagePolicyUsageStatus : %t", sp_usage_pvc_status)
		// gomega.Expect(total_quota_used_status && sp_quota_pvc_status && sp_usage_pvc_status).NotTo(gomega.BeFalse())
	}

	dsFcdFootprintMapAfterProvisioning := k8testutil.GetDatastoreFcdFootprint(ctx, e2eTestConfig)
	//Verify Vmdk count and fcd/volume list and used space
	usedSpaceRetVal, numberOfVmdksRetVal, numberOfFcdsRetVal, numberOfVolumesRetVal, _, deltaUsedSpace := k8testutil.ValidateSpaceUsageAfterResourceCreationUsingDatastoreFcdFootprint(dsFcdFootprintMapBeforeProvisioning, dsFcdFootprintMapAfterProvisioning, newdiskSizeInBytes, volumeOpsScale)
	framework.Logf("DeleteVolume-------------------------")
	framework.Logf("Is Datastore Used Space Matched : %t, Delta Used Space If any : %d", usedSpaceRetVal, deltaUsedSpace)
	framework.Logf("Is Num of Vmdks Matched : %t", numberOfVmdksRetVal)
	framework.Logf("Is Num of Fcds Matched : %t", numberOfFcdsRetVal)
	framework.Logf("Is Num of Volumes Matched : %t", numberOfVolumesRetVal)

	gomega.Expect(usedSpaceRetVal).NotTo(gomega.BeFalse(), "Used space not matched")
	// gomega.Expect(numberOfVmdksRetVal).NotTo(gomega.BeFalse(), "Vmdks count not matched")
	// gomega.Expect(numberOfFcdsRetVal).NotTo(gomega.BeFalse(), "Fcds count not matched")
	// gomega.Expect(numberOfVolumesRetVal).NotTo(gomega.BeFalse(), "Volumes count not matched")

	isTestPassed = true
}
