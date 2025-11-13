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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

var _ = ginkgo.Describe("CNS_Register_Volume_Status_Verification", func() {
	f := framework.NewDefaultFramework("e2e-cns-register-volume-status")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	framework.TestContext.DeleteNamespace = true
	var (
		client              clientset.Interface
		namespace           string
		fcdID               string
		pv                  *v1.PersistentVolume
		pvc                 *v1.PersistentVolumeClaim
		defaultDatacenter   *object.Datacenter
		defaultDatastore    *object.Datastore
		deleteFCDRequired   bool
		pandoraSyncWaitTime int
		datastoreURL        string
		storagePolicyName   string
		ctx                 context.Context
	)

	ginkgo.BeforeEach(func() {
		e2eTestConfig = bootstrap.Bootstrap()
		client = f.ClientSet
		namespace = vcutil.GetNamespaceToRunTests(f, e2eTestConfig)
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		storagePolicyName = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicyNameForSharedDatastores)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		pandoraSyncWaitTime = 120

		deleteFCDRequired = false
		var datacenters []string
		datastoreURL = env.GetAndExpectStringEnvVar(constants.EnvSharedDatastoreURL)
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
		}
		if e2eTestConfig.TestInput.ClusterFlavor.GuestCluster {
			restConfig := vcutil.GetRestConfigClient(e2eTestConfig)
			_, svNamespace := k8testutil.GetSvcClientAndNamespace()
			k8testutil.SetStoragePolicyQuota(ctx, restConfig, storagePolicyName, svNamespace, constants.RqLimit)
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
			framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx,
				client, pv.Name, constants.Poll, constants.PollTimeoutShort))
			framework.ExpectNoError(vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, pv.Spec.CSI.VolumeHandle))
		}
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			k8testutil.DumpSvcNsEventsOnTestFailure(client, namespace)
		}
	})

	/*
		Positive Test : This test verifies the cnsregistervolume status.
		Test Steps:
		1. Create FCD.
		2. Create a CnsRegisterVolume custom resource.
		3. Wait for the CnsRegisterVolume to be registered.
		4. A PVC is created automatically, so get the PVC.
		5. Wait for the PV and PVC to be bound.
		6. Cleanup PVC, PV, FCD and CnsRegisterVolume.
	*/
	ginkgo.It("[csi-supervisor]Verify CnsRegisterVolume Status", func() {
		var err error
		var isRegistered bool
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		storagePolicyName := env.GetAndExpectStringEnvVar(constants.EnvStoragePolicyNameForSharedDatastores)
		profileID := vcutil.GetSpbmPolicyID(storagePolicyName, e2eTestConfig)
		ginkgo.By("Creating FCD Disk with profile")
		fcdID, err := vcutil.CreateFCDwithValidProfileID(ctx,
			e2eTestConfig, "cns-register-volume-fcd", profileID, constants.DiskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		deleteFCDRequired = true

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		pvcName := "cns-register-pvc"
		// Create a CnsRegisterVolume custom resource.
		cnsRegisterVolume := k8testutil.GetCNSRegisterVolumeSpec(ctx, namespace, fcdID, "", pvcName, v1.ReadWriteOnce)
		restConfig := vcutil.GetRestConfigClient(e2eTestConfig)
		err = k8testutil.CreateCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for the CnsRegisterVolume to be registered.
		err, isRegistered = k8testutil.WaitForCNSRegisterVolumeToGetCreatedWithStatus(ctx, restConfig, namespace,
			cnsRegisterVolume, constants.Poll, constants.PollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "CNS Register Volume failed")
		gomega.Expect(isRegistered).To(gomega.BeTrue(), "Expected is registered true")

		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv = k8testutil.GetPvFromClaim(client, namespace, pvc.Name)
		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv, pvc))

		// Set deleteFCDRequired to false.
		// After PV, PVC is in the bind state, Deleting PVC should delete
		// container volume. So no need to delete FCD directly using vSphere
		// API call.
		deleteFCDRequired = false

		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace),
			"Failed to delete PVC", pvc.Name)
		pvc = nil

		ginkgo.By("Verify PV should be deleted automatically")
		framework.ExpectNoError(
			fpv.WaitForPersistentVolumeDeleted(ctx,
				client, pv.Name, constants.Poll, constants.PollTimeout))
		pv = nil
	})

	/*
		Negative Test : This test verifies the cnsregistervolume status.
		Test Steps:
		1. Create a CnsRegisterVolume custom resource with invalid fcdId.
		2. Verify PVC to be in pending state.
		3. Cleanup PVC and CnsRegisterVolume.
	*/
	ginkgo.It("[csi-supervisor]Verify CnsRegisterVolume Status", func() {
		var err error
		var isRegistered bool
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Invalid fcdId.")
		invalidFcdID := uuid.New()
		fcdID = invalidFcdID.String()
		fmt.Printf("Generated UUID: %s", fcdID)

		pvcName := "cns-register-pvc-invalid-fcdid"
		// Create a CnsRegisterVolume custom resource.
		cnsRegisterVolume := k8testutil.GetCNSRegisterVolumeSpec(ctx, namespace, fcdID, "", pvcName, v1.ReadWriteOnce)
		restConfig := vcutil.GetRestConfigClient(e2eTestConfig)
		err = k8testutil.CreateCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for the CnsRegisterVolume to be registered.
		err, isRegistered = k8testutil.WaitForCNSRegisterVolumeToGetCreatedWithStatus(ctx, restConfig, namespace,
			cnsRegisterVolume, constants.Poll, constants.PollTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(), "Expected CNS Register Volume to be failed")
		gomega.Expect(isRegistered).NotTo(gomega.BeTrue(), "Expected is registered false")

		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace),
			"Failed to delete PVC", pvc.Name)
		pvc = nil
		pv = nil
	})

	/*
		Negative Test : This test verifies the cnsregistervolume status.
		Test Steps:
		1. Create a CnsRegisterVolume with invalid vmdk path.
		2. Verify PVC to be in pending state.
		3. Cleanup PVC and CnsRegisterVolume.
	*/
	ginkgo.It("[csi-supervisor]Verify CnsRegisterVolume Status", func() {
		var err error
		var isRegistered bool

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		invalidVmdkpath := "https://192.1.1.1/folder/vm-1/vm-1_1.vmdk?dcPath=wcp-sanity&dsName=vsanDatastore"
		ginkgo.By("With invalid vmdk path")
		fmt.Printf("Invalid vmdk path: %s", invalidVmdkpath)

		pvcName := "cns-register-pvc-invalid-vmdk-path"
		// Create a CnsRegisterVolume custom resource.
		cnsRegisterVolume := k8testutil.GetCNSRegisterVolumeSpec(ctx, namespace, "", invalidVmdkpath, pvcName,
			v1.ReadWriteOnce)
		restConfig := vcutil.GetRestConfigClient(e2eTestConfig)
		err = k8testutil.CreateCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for the CnsRegisterVolume to be registered.
		err, isRegistered = k8testutil.WaitForCNSRegisterVolumeToGetCreatedWithStatus(ctx, restConfig, namespace,
			cnsRegisterVolume, constants.Poll, constants.PollTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(), "Expected CNS Register Volume to be failed")
		gomega.Expect(isRegistered).NotTo(gomega.BeTrue(), "Expected is registered false")

		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace),
			"Failed to delete PVC", pvc.Name)
		pvc = nil
		pv = nil
	})
})
