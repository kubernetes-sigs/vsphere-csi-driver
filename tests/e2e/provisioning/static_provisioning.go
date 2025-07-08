/*
Copyright 2025 The Kubernetes Authors.

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

package provisioning

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

var e2eTestConfig *config.E2eTestConfig

var _ = ginkgo.Describe("Basic Static Provisioning", func() {
	f := framework.NewDefaultFramework("e2e-csistaticprovision")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	framework.TestContext.DeleteNamespace = true
	var (
		client                     clientset.Interface
		namespace                  string
		fcdID                      string
		pv                         *v1.PersistentVolume
		pvc                        *v1.PersistentVolumeClaim
		defaultDatacenter          *object.Datacenter
		defaultDatastore           *object.Datastore
		deleteFCDRequired          bool
		pandoraSyncWaitTime        int
		err                        error
		datastoreURL               string
		storagePolicyName          string
		isVsanHealthServiceStopped bool
		isSPSserviceStopped        bool
		ctx                        context.Context
	)

	ginkgo.BeforeEach(func() {
		ginkgo.By(fmt.Sprintf("In before each"))
		e2eTestConfig = bootstrap.Bootstrap()
		ginkgo.By(fmt.Sprintf("In before each-2"))
		client = f.ClientSet
		namespace = vcutil.GetNamespaceToRunTests(f, e2eTestConfig)
		ginkgo.By(fmt.Sprintf("Namespace is %s", namespace))
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		storagePolicyName = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicyNameForSharedDatastores)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		if os.Getenv(constants.EnvPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(constants.EnvPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = constants.DefaultPandoraSyncWaitTime
		}
		deleteFCDRequired = false
		isVsanHealthServiceStopped = false
		isSPSserviceStopped = false
		var datacenters []string
		datastoreURL = env.GetAndExpectStringEnvVar(constants.EnvSharedDatastoreURL)
		var fullSyncWaitTime int
		finder := find.NewFinder(e2eTestConfig.VcClient.Client, false)
		cfg, err := config.GetConfig()
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
			defaultDatastore, err = k8testutil.GetDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if e2eTestConfig.TestInput.ClusterFlavor.GuestCluster {
			// Get a config to talk to the apiserver
			restConfig := vcutil.GetRestConfigClient(e2eTestConfig)
			_, svNamespace := k8testutil.GetSvcClientAndNamespace()
			k8testutil.SetStoragePolicyQuota(ctx, restConfig, storagePolicyName, svNamespace, constants.RqLimit)
		}

		if os.Getenv(constants.EnvFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(constants.EnvFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if fullSyncWaitTime <= 0 || fullSyncWaitTime > constants.DefaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = constants.DefaultFullSyncWaitTime
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Performing test cleanup")
		if deleteFCDRequired {
			ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))

			err := vcutil.DeleteFCD(ctx, fcdID, e2eTestConfig, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if pvc != nil {
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace),
				"Failed to delete PVC", pvc.Name)
		}

		if pv != nil {
			framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, constants.Poll, constants.PollTimeoutShort))
			framework.ExpectNoError(vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, pv.Spec.CSI.VolumeHandle))
		}

		if isVsanHealthServiceStopped {
			ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
			err = vcutil.InvokeVCenterServiceControl(&e2eTestConfig.TestInput.TestBedInfo, ctx, constants.StartOperation, constants.VsanhealthServiceName, e2eTestConfig.TestInput.TestBedInfo.VcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", constants.VsanHealthServiceWaitTime))
			time.Sleep(time.Duration(constants.VsanHealthServiceWaitTime) * time.Second)
		}

		if isSPSserviceStopped {
			ginkgo.By(fmt.Sprintln("Starting sps on the vCenter host"))
			err = vcutil.InvokeVCenterServiceControl(&e2eTestConfig.TestInput.TestBedInfo, ctx, constants.StartOperation, constants.SpsServiceName, e2eTestConfig.TestInput.TestBedInfo.VcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to come up again", constants.VsanHealthServiceWaitTime))
			time.Sleep(time.Duration(constants.VsanHealthServiceWaitTime) * time.Second)
		}

		if e2eTestConfig.TestInput.ClusterFlavor.GuestCluster {
			svcClient, svNamespace := k8testutil.GetSvcClientAndNamespace()
			k8testutil.SetResourceQuota(svcClient, svNamespace, constants.RqLimit)
			k8testutil.DumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			k8testutil.DumpSvcNsEventsOnTestFailure(client, namespace)
		}
	})

	// This test verifies the static provisioning workflow.
	//
	// Test Steps:
	// 1. Create FCD and wait for fcd to allow syncing with pandora.
	// 2. Create PV Spec with volumeID set to FCDID created in Step-1, and
	//    PersistentVolumeReclaimPolicy is set to Delete.
	// 3. Create PVC with the storage request set to PV's storage capacity.
	// 4. Wait for PV and PVC to bound.
	// 5. Create a POD.
	// 6. Verify volume is attached to the node and volume is accessible in the pod.
	// 7. Verify container volume metadata is present in CNS cache.
	// 8. Delete POD.
	// 9. Verify volume is detached from the node.
	// 10. Delete PVC.
	// 11. Verify PV is deleted automatically.
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] Verify basic static provisioning "+
		"workflow", func() {
		var err error

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Creating FCD Disk")
		fcdID, err := vcutil.CreateFCD(ctx, e2eTestConfig, "BasicStaticFCD", constants.DiskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		deleteFCDRequired = true

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		// Creating label for PV.
		// PVC will use this label as Selector to find PV.
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID

		ginkgo.By("Creating the PV")
		pv = k8testutil.GetPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimDelete, staticPVLabels, constants.Ext4FSType)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		if err != nil {
			return
		}
		err = vcutil.WaitForCNSVolumeToBeCreated(e2eTestConfig, pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC")
		pvc = k8testutil.GetPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv, pvc))

		// Set deleteFCDRequired to false.
		// After PV, PVC is in the bind state, Deleting PVC should delete
		// container volume. So no need to delete FCD directly using vSphere
		// API call.
		deleteFCDRequired = false

		ginkgo.By("Verifying CNS entry is present in cache")
		_, err = vcutil.QueryCNSVolumeWithResult(e2eTestConfig, pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the Pod")
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		pod, err := k8testutil.CreatePod(ctx, e2eTestConfig, client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := k8testutil.GetNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := vcutil.IsVolumeAttachedToVM(client, e2eTestConfig, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")

		ginkgo.By("Verify the volume is accessible and available to the pod by creating an empty file")
		filepath := filepath.Join("/mnt/volume1", "/emptyFile.txt")
		k8testutil.CreateEmptyFilesOnVSphereVolume(e2eTestConfig, namespace, pod.Name, []string{filepath})

		ginkgo.By("Verify container volume metadata is present in CNS cache")
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolume with VolumeID: %s", pv.Spec.CSI.VolumeHandle))
		_, err = vcutil.QueryCNSVolumeWithResult(e2eTestConfig, pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := []types.KeyValue{{Key: "fcd-id", Value: fcdID}}
		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = vcutil.VerifyVolumeMetadataInCNS(pv.Spec.CSI.VolumeHandle, e2eTestConfig,
			pvc.Name, pv.ObjectMeta.Name, pod.Name, labels...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the Pod")
		framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, pod), "Failed to delete pod", pod.Name)

		ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", pod.Spec.NodeName))
		isDiskDetached, err := vcutil.WaitForVolumeDetachedFromNode(e2eTestConfig, client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")

		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace),
			"Failed to delete PVC", pvc.Name)
		pvc = nil

		ginkgo.By("Verify PV should be deleted automatically")
		framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, constants.Poll, constants.PollTimeout))
		pv = nil
	})

})
