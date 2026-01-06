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

package linked_clone

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/csisnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
)

var e2eTestConfig *config.E2eTestConfig

var _ bool = ginkgo.Describe("[linked-clone-p0] Linked-Clone-P0", func() {

	f := framework.NewDefaultFramework("linked-clone")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true
	var (
		client          clientset.Interface
		namespace       string
		storageclass    *v1.StorageClass
		restConfig      *restclient.Config
		err             error
		volumeSnapshot  *snapV1.VolumeSnapshot
		vcRestSessionId string
		statuscode      int
		doCleanup       bool
		storagePolicy   string
	)

	ginkgo.BeforeEach(func() {
		e2eTestConfig = bootstrap.Bootstrap()
		client = f.ClientSet
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// doCleanup set to false, to avoild individual resource cleaning up.
		doCleanup = false

		// Read Env variable needed for the test suite
		storagePolicy = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicy)

		// Get the storageclass from storagepolicy
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// reading vc session id
		if vcRestSessionId == "" {
			vcRestSessionId = k8testutil.CreateVcSession4RestApis(ctx, e2eTestConfig)
		}

		storagePolicyId := vcutil.GetSpbmPolicyID(storagePolicy, e2eTestConfig)

		// creating namespace with storagePolicy
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			namespace, statuscode, err = k8testutil.CreatetWcpNsWithZonesAndPolicies(e2eTestConfig, vcRestSessionId,
				[]string{storagePolicyId}, k8testutil.GetSvcId(vcRestSessionId, e2eTestConfig),
				[]string{}, "", "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(statuscode).To(gomega.Equal(204))
		} else {
			labels_ns := map[string]string{}
			labels_ns[admissionapi.EnforceLevelLabel] = string(admissionapi.LevelPrivileged)
			labels_ns["e2e-framework"] = f.BaseName
			gcNs, err := framework.CreateTestingNS(ctx, f.BaseName, client, labels_ns)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on GC")
			namespace = gcNs.Name
		}

		//After NS creation need sometime to load usage CRs
		time.Sleep(constants.PollTimeoutShort)

		restConfig = vcutil.GetRestConfigClient(e2eTestConfig)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		framework.Logf("Collecting supervisor PVC events before performing PV/PVC cleanup")
		k8testutil.DumpSvcNsEventsOnTestFailure(client, namespace)
		eventList, err := client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, item := range eventList.Items {
			framework.Logf("%q", item.Message)
		}

		// Cleanup the resources created in the test
		if doCleanup {
			k8testutil.Cleanup(ctx, client, e2eTestConfig, namespace)
		}

		// Delete namespace
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			k8testutil.DelTestWcpNs(e2eTestConfig, vcRestSessionId, namespace)
			gomega.Expect(k8testutil.WaitForNamespaceToGetDeleted(ctx, client, namespace, constants.Poll, constants.PollTimeout)).To(gomega.Succeed())
		} else {
			err = client.CoreV1().Namespaces().Delete(ctx, namespace, *metav1.NewDeleteOptions(0))
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

	})

	/*
		Create a linked clone (PVC) from a snapshot
		Pre setup:
		1. Create a namespace
		2. Create a storage class and assign it to a namespace
		3. Create a PVC
		4. Validate PVC is Bound
		5. Create and attach a Pod to it.
		6. Create a snapshot
		Test steps:
		1. Validate the quota using PersistentVolumeClaim StoragePolicyUsage
		2. Create a linked clone using csi.vsphere.volume/linked-clone: true annotation
		3. Validate that LC-PVC is bound.
		4. Create and attach a Pod to LC-PVC.
		5. List volume
		6. The linked clone volume must be listed
		7. Validate the quota using PersistentVolumeClaim, StoragePolicyUsage has increased
		8. Delete LC
		9. Validate quota is returned to the quotapool
		10. Run volume usability verification. (i.e create Pod/VM → verify
		11. PVC & pod status → extend PVC → detach → attach)
		12. Run cleanup
	*/

	ginkgo.It("Create a linked clone (PVC) from a snapshot", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Create a linked clone (PVC) from a snapshot ")

		// Create PVC, pod and volume snapshot
		_, _, volumeSnapshot = k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, true, false)

		// Get the storage quota used before LC creation
		quota := make(map[string]*resource.Quantity)
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			quota["totalQuotaUsedBefore"], _, quota["pvc_storagePolicyQuotaBefore"], _,
				quota["pvc_storagePolicyUsageBefore"], _ =
				k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName, false)
		}

		// create linked clone PVC and verify its bound
		linkdeClonePvc, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		// Create and attach pod to linked clone PVC
		lcPod, _ := k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// TODO: write some data on LC-PVC and validate

		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			// List volume
			k8testutil.ValidateLcInListVolume(ctx, e2eTestConfig, f.ClientSet, linkdeClonePvc, namespace)

			diskSizeInMbStr := k8testutil.ConvertInt64ToStrMbFormat(constants.DiskSizeInMb)
			sp_quota_pvc_status, sp_usage_pvc_status := k8testutil.ValidateQuotaUsageAfterResourceCreation(ctx, restConfig,
				storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName,
				[]string{diskSizeInMbStr}, quota["totalQuotaUsedBefore"], quota["pvc_storagePolicyQuotaBefore"],
				quota["pvc_storagePolicyUsageBefore"], false)
			gomega.Expect(sp_quota_pvc_status && sp_usage_pvc_status).NotTo(gomega.BeFalse())
		}

		// Delete Pod attached to linked-clone
		err = fpod.DeletePodWithWait(ctx, client, lcPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Delete linked clone
		err := fpv.DeletePersistentVolumeClaim(ctx, client, linkdeClonePvc.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Post delete it takes few seconds to update the quota
		time.Sleep(constants.HealthStatusPollInterval)

		// get the quota post LC deletion
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			_, _, quota["storagePolicyQuotaAfterCleanup"], _, quota["storagePolicyUsageAfterCleanup"], _ =
				k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName, false)

			k8testutil.ExpectEqual(quota["storagePolicyQuotaAfterCleanup"], quota["pvc_storagePolicyQuotaBefore"],
				"Before and After values of storagePolicy-Quota CR should match")
			k8testutil.ExpectEqual(quota["storagePolicyUsageAfterCleanup"], quota["pvc_storagePolicyUsageBefore"],
				"Before and After values of storagePolicy-Usage CR should match")
		}

		// TODO volume usability

		// Delete the resources individually
		doCleanup = true

		framework.Logf("Ending test: Create a linked clone (PVC) from a snapshot ")

	})

	// TC-2
	ginkgo.It("Create a snapshot from a linked clone with pod attached to LC-PVC", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Create a snapshot from a linked clone with pod attached to LC-PVC ")

		// Create PVC, deployment pod and volume snapshot
		_, _, volumeSnapshot = k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, false, true)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		// Create and attach pod to linked clone PVC
		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// Get the snapshot usage before creating it
		quota := make(map[string]*resource.Quantity)
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			quota["totalQuotaUsedBefore"], _, quota["snap_storagePolicyQuotaBefore"], _,
				quota["snap_storagePolicyUsageBefore"], _ =
				k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, constants.SnapshotUsage, constants.SnapshotExtensionName, false)
		}

		// Create snapshot from LC_PVC
		framework.Logf("Create snapshot from LC_PVC ")
		_, _ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, linkdeClonePvc, lcPv, constants.DiskSize)

		// Check the quota usage for snapshot
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			quota["totalQuotaUsedAfter"], _, quota["snap_storagePolicyQuotaAfter"], _,
				quota["snap_storagePolicyUsageAfter"], _ =
				k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, constants.SnapshotUsage, constants.SnapshotExtensionName, false)

			// Get the numeric part
			snap_storagePolicyQuotaAfter, err1 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["snap_storagePolicyQuotaAfter"]))
			gomega.Expect(err1).To(gomega.BeNil())
			snap_storagePolicyUsageAfter, err2 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["snap_storagePolicyUsageAfter"]))
			gomega.Expect(err2).To(gomega.BeNil())

			snap_storagePolicyQuotaBefore, err3 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["snap_storagePolicyQuotaBefore"]))
			gomega.Expect(err3).To(gomega.BeNil())
			snap_storagePolicyUsageBefore, err4 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["snap_storagePolicyUsageBefore"]))
			gomega.Expect(err4).To(gomega.BeNil())

			// Validate storageQuota increased
			gomega.Expect(snap_storagePolicyQuotaAfter).To(gomega.BeNumerically(">", snap_storagePolicyQuotaBefore), "Expected %v to be greater than %v", quota["snap_storagePolicyQuotaAfter"], quota["snap_storagePolicyQuotaBefore"])
			gomega.Expect(snap_storagePolicyUsageAfter).To(gomega.BeNumerically(">", snap_storagePolicyUsageBefore), "Expected %v to be greater than %v", quota["snap_storagePolicyUsageAfter"], quota["snap_storagePolicyUsageBefore"])
		}

		// TODO volume usability

		framework.Logf("Ending test: reate a snapshot from a linked clone with pod attached to LC-PVC")

	})

	// TC-3
	ginkgo.It("Create a snapshot from a linked clone with no pod attached to LC-PVC", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Create a snapshot from a linked clone with no pod attached to LC-PVC ")

		// Can run this test as Devops also
		runningAsDevopsUser := env.GetBoolEnvVarOrDefault("IS_DEVOPS_USER", false)
		if runningAsDevopsUser {
			_, client = k8testutil.InitializeClusterClientsByUserRoles(client, e2eTestConfig)
		}

		// Create PVC and volume snapshot
		_, _, volumeSnapshot = k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, false, false)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		// Create and attach pod to linked clone PVC
		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// It takes few seconds to update the quota
		time.Sleep(constants.HealthStatusPollInterval)

		// Get the snapshot usage before creating it
		quota := make(map[string]*resource.Quantity)
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			quota["totalQuotaUsedBefore"], _, quota["snap_storagePolicyQuotaBefore"], _,
				quota["snap_storagePolicyUsageBefore"], _ =
				k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, constants.SnapshotUsage, constants.SnapshotExtensionName, false)
		}
		// Create snapshot from LC_PVC
		framework.Logf("Create snapshot from LC_PVC ")
		_, _ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, linkdeClonePvc, lcPv, constants.DiskSize)

		// It takes few seconds to update the quota
		time.Sleep(constants.HealthStatusPollInterval)

		// Check the quota usage for snapshot
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			quota["totalQuotaUsedAfter"], _, quota["snap_storagePolicyQuotaAfter"], _,
				quota["snap_storagePolicyUsageAfter"], _ =
				k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, constants.SnapshotUsage, constants.SnapshotExtensionName, false)
		}

		// TODO write data to get the snap quota to increase
		/*
			// Get the numeric part
			snap_storagePolicyQuotaAfter, err1 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["snap_storagePolicyQuotaAfter"]))
			gomega.Expect(err1).To(gomega.BeNil())
			snap_storagePolicyUsageAfter, err2 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["snap_storagePolicyUsageAfter"]))
			gomega.Expect(err2).To(gomega.BeNil())


				snap_storagePolicyQuotaBefore, err3 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["snap_storagePolicyQuotaBefore"]))
				gomega.Expect(err3).To(gomega.BeNil())
				snap_storagePolicyUsageBefore, err4 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["snap_storagePolicyUsageBefore"]))
				gomega.Expect(err4).To(gomega.BeNil())

				// Validate storageQuota increased
				gomega.Expect(snap_storagePolicyQuotaAfter).To(gomega.BeNumerically(">", snap_storagePolicyQuotaBefore), "Expected %v to be greater than %v", quota["snap_storagePolicyQuotaAfter"], quota["snap_storagePolicyQuotaBefore"])
				gomega.Expect(snap_storagePolicyUsageAfter).To(gomega.BeNumerically(">", snap_storagePolicyUsageBefore), "Expected %v to be greater than %v", quota["snap_storagePolicyUsageAfter"], quota["snap_storagePolicyUsageBefore"])
		*/
		// TODO volume usability

		framework.Logf("Ending test: Create a snapshot from a linked clone with no pod attached to LC-PVC")

	})

	// TC-4
	ginkgo.It("Relocate the base PVC from which the linked clone is created", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Relocate the base PVC from which the linked clone is created ")

		// Create PVC and volume snapshot
		_, pv, volumeSnapshot := k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, false, false)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		// Create and attach pod to linked clone PVC
		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// Relocate the source PVC
		ginkgo.By("Relocate source volume from one datastore to another datastore using" +
			"CnsRelocateVolume API")
		destDsUrl := os.Getenv(constants.EnvTargetDSUrlForRelocate)
		dsRefDest := vcutil.GetDsMoRefFromURL(ctx, e2eTestConfig, destDsUrl)
		volumeID := pv[0].Spec.CSI.VolumeHandle
		_, err = vcutil.CnsRelocateVolume(ctx, e2eTestConfig, volumeID, dsRefDest)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcutil.VerifyDatastoreMatch(e2eTestConfig, volumeID, []string{destDsUrl})

		// TODO Check snapshot got relocated

		// Validate the LC volume is not relocated
		volumeIDLc := lcPv[0].Spec.CSI.VolumeHandle
		actualDatastoreUrl := vcutil.FetchDsUrl4CnsVol(e2eTestConfig, volumeIDLc)
		gomega.Expect(actualDatastoreUrl).ShouldNot(gomega.BeElementOf(destDsUrl),
			"Volume is not provisioned on any of the given datastores: %s, but on: %s", destDsUrl,
			actualDatastoreUrl)

		// TODO volume usability

		framework.Logf("Ending test: Relocate the base PVC from which the linked clone is created")

	})

	// TC-5
	ginkgo.It("Relocate the linked clone volume ", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Relocate the linked clone volume ")

		// Create PVC and volume snapshot
		_, pv, volumeSnapshot := k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, false, false)

		// create linked clone PVC and verify its bound
		_, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		// Relocate the source PVC
		ginkgo.By("Relocate source volume from one datastore to another datastore using" +
			"CnsRelocateVolume API")
		destDsUrl := os.Getenv(constants.EnvTargetDSUrlForRelocate)
		dsRefDest := vcutil.GetDsMoRefFromURL(ctx, e2eTestConfig, destDsUrl)
		volumeID := lcPv[0].Spec.CSI.VolumeHandle
		_, err = vcutil.CnsRelocateVolume(ctx, e2eTestConfig, volumeID, dsRefDest)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcutil.VerifyDatastoreMatch(e2eTestConfig, volumeID, []string{destDsUrl})

		// Validate the LC volume is not relocated
		volumeIDLc := pv[0].Spec.CSI.VolumeHandle
		actualDatastoreUrl := vcutil.FetchDsUrl4CnsVol(e2eTestConfig, volumeIDLc)
		gomega.Expect(actualDatastoreUrl).ShouldNot(gomega.BeElementOf(destDsUrl),
			"Volume is not provisioned on any of the given datastores: %s, but on: %s", destDsUrl,
			actualDatastoreUrl)

		// TODO volume usability

		framework.Logf("Ending test: Relocate the linked clone volume")

	})

	// TC-6
	ginkgo.It("Attach LC PVC to parent pod ", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Attach LC PVC to parent pod ")

		// Create PVC and volume snapshot
		srcPvc, _, volumeSnapshot := k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, false, false)

		// create linked clone PVC and verify its bound
		lcPvc, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		//create pod attaching to src-pvc and lc, expect it to fail
		_, err = k8testutil.CreatePod(ctx, e2eTestConfig, client, namespace, nil, []*corev1.PersistentVolumeClaim{srcPvc, lcPvc}, false,
			constants.ExecRWXCommandPod1)
		gomega.Expect(err).To(gomega.HaveOccurred())

		framework.Logf("Ending test: Attach LC PVC to parent pod")

	})

	// TC-7
	ginkgo.It("Create multiple linked clones from a snapshot", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Create multiple linked clones from a snapshot ")

		// Create PVC and volume snapshot
		_, _, volumeSnapshot := k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, true, false)

		// create linked clone PVC and verify its bound
		lcPvc1, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		//create another linked clone PVC and verify its bound
		lcPvc2, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		k8testutil.PvcUsability(ctx, e2eTestConfig, client, namespace, storageclass, []*corev1.PersistentVolumeClaim{lcPvc1, lcPvc2}, constants.DiskSize)

		framework.Logf("Ending test: Create multiple linked clones from a snapshot")

	})

	// TC-8
	ginkgo.It("Verify creating the LC with statefulsets", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var volumeSnapshot *snapV1.VolumeSnapshot

		framework.Logf("Starting test: Verify creating the LC with statefulsets")

		// Create a statefulset with replica=3
		ginkgo.By("Creating statefulset")
		var replicas int32 = 3
		ginkgo.By("Creating service")
		_ = k8testutil.CreateService(namespace, client)
		statefulset := k8testutil.CreateCustomisedStatefulSets(ctx, client, e2eTestConfig.TestInput, namespace, true, replicas, false, nil,
			false, true, "", "", storageclass, storageclass.Name)

		// List the STS Pvcs
		stsPod, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeMap := map[*corev1.PersistentVolumeClaim]*corev1.PersistentVolume{}
		for _, pod := range stsPod.Items {
			for _, volumeSpec := range pod.Spec.Volumes {
				if volumeSpec.PersistentVolumeClaim != nil {
					svPvcName := volumeSpec.PersistentVolumeClaim.ClaimName
					pv := k8testutil.GetPvFromClaim(client, namespace, svPvcName)
					// Get SVC PVC
					svcPVC, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, svPvcName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					volumeMap[svcPVC] = pv
				}
			}
		}

		// Create a snapshot for all 3 PVCs
		for pvclaim, pv := range volumeMap {
			volumeSnapshot, _ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, []*corev1.PersistentVolume{pv}, constants.DiskSize1GB)
		}

		// Create linked clone using sts
		k8testutil.CreateAndValidateLcWithSts(ctx, e2eTestConfig, client, namespace, storageclass, volumeSnapshot.Name, constants.Snapshotapigroup)

		framework.Logf("Ending test: Verify creating the LC with statefulsets")

	})

	// TC-9
	ginkgo.It("Verify linked clone can be created on the static volume", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Verify linked clone can be created on the static volume")

		curtime := time.Now().Unix()
		curtimestring := strconv.FormatInt(curtime, 10)
		pvcName := "cns-pvc-" + curtimestring
		framework.Logf("pvc name :%s", pvcName)

		restConfig, storageclass, profileID := k8testutil.StaticProvisioningPreSetUpUtil(ctx, e2eTestConfig, f, client, storagePolicy, namespace)
		framework.Logf("Storage class : %s", storageclass.Name)

		ginkgo.By("Creating FCD (CNS Volume)")
		var datacenters []string
		var defaultDatacenter *object.Datacenter
		datastoreURL := env.GetAndExpectStringEnvVar(constants.EnvSharedDatastoreURL)
		dsRef := vcutil.GetDsMoRefFromURL(ctx, e2eTestConfig, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)
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
			_, err = vcutil.GetDatastoreByURL(ctx, e2eTestConfig, datastoreURL, defaultDatacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defaultDatastore, err := vcutil.GetDatastoreByURL(ctx, e2eTestConfig, datastoreURL, defaultDatacenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fcdID, err := vcutil.CreateFCDwithValidProfileID(ctx, e2eTestConfig,
			"staticfcd"+curtimestring, profileID, constants.DiskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			constants.DefaultPandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(constants.DefaultPandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create CNS register volume with above created FCD ")
		cnsRegisterVolume := k8testutil.GetCNSRegisterVolumeSpec(ctx, namespace, fcdID, "", pvcName, corev1.ReadWriteOnce)
		err = k8testutil.CreateCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.ExpectNoError(k8testutil.WaitForCNSRegisterVolumeToGetCreated(ctx, restConfig,
			namespace, cnsRegisterVolume, constants.Poll, constants.SupervisorClusterOperationsTimeout))
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

		ginkgo.By(" verify created PV, PVC and check the bidirectional reference")
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := k8testutil.GetPvFromClaim(client, namespace, pvcName)
		k8testutil.VerifyBidirectionalReferenceOfPVandPVC(ctx, client, pvc, pv, fcdID)

		ginkgo.By("Creating pod")
		pod, err := k8testutil.CreatePod(ctx, e2eTestConfig, client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podName := pod.GetName()
		framework.Logf("podName : %s", podName)

		// create volume snapshot
		volumeSnapshot, _ := k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvc, []*corev1.PersistentVolume{pv}, constants.DiskSize)

		// create linked clone PVC and verify its bound
		lcPvc1, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		k8testutil.PvcUsability(ctx, e2eTestConfig, client, namespace, storageclass, []*corev1.PersistentVolumeClaim{lcPvc1}, constants.DiskSize)

		framework.Logf("Ending test: Verify linked clone can be created on the static volume")
	})

	// TC-10
	ginkgo.It("Verify linked clone can be created on the static snapshot", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901, constants.Tkg), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Validate the LC creation passes on the restored volume")

		pvclaim, pv := k8testutil.CreateAndValidatePvc(ctx, client, namespace, storageclass)

		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, client, namespace, []*corev1.PersistentVolumeClaim{pvclaim}, true, false)

		// create a dynamic volume snapshot
		volumeSnapshot, snapshotContent := k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, pv, constants.DiskSize)

		// Get snapshot client using the rest config
		var guestClusterRestConfig *restclient.Config
		guestClusterRestConfig = k8testutil.GetRestConfigClientForGuestCluster(guestClusterRestConfig)
		snapc, err := snapclient.NewForConfig(guestClusterRestConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Get volume snapshot handle from Supervisor Cluster")
		_, _, svcVolumeSnapshotName, _ := csisnapshot.GetSnapshotHandleFromSupervisorCluster(ctx,
			*snapshotContent.Status.SnapshotHandle)

		// Create a static snapshot
		ginkgo.By("Create a static or pre-provisioned  volume snapshot ")
		_, staticSnapshot, _,
			_, err := k8testutil.CreatePreProvisionedSnapshotInGuestCluster(ctx, volumeSnapshot, snapshotContent,
			snapc, namespace, constants.DefaultPandoraSyncWaitTime, svcVolumeSnapshotName, constants.DiskSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create linked clone PVC and verify its bound
		_, _ = k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, staticSnapshot.Name, e2eTestConfig)

		framework.Logf("Ending test: Verify linked clone can be created on the static snapshot")

	})

	// TC-11
	ginkgo.It("Validate the LC creation passes on the restored volume", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Validate the LC creation passes on the restored volume")

		// Create PVC and volume snapshot
		_, _, volumeSnapshot = k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, true, false)

		// create linked clone PVC and verify its bound
		_, _ = k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		// Restore the volume using above snapshot
		ginkgo.By("Restore snapshot to create a new volume")
		restoredPvc, restoredPV, _ := k8testutil.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, storageclass,
			volumeSnapshot, constants.DiskSize, false)

		// create volume snapshot from restored pvc
		volumeSnapshot, _ := k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, restoredPvc, restoredPV, constants.DiskSize)

		// create linked clone PVC and verify its bound
		_, _ = k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		framework.Logf("Ending test: Validate the LC creation passes on the restored volume")

	})

	// TC-12
	ginkgo.It("Validate the restoration of LC-PVC", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Validate the restoration of LC-PVC")

		// Create PVC and volume snapshot
		_, _, volumeSnapshot = k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, true, false)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name, e2eTestConfig)

		// Create and attach pod to linked clone PVC
		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// Create snapshot from LC_PVC
		framework.Logf("Create snapshot from LC_PVC ")
		volumeSnapshot, _ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, linkdeClonePvc, lcPv, constants.DiskSize)

		// Restore the volume using above snapshot
		ginkgo.By("Restore snapshot to create a new volume")
		pvc, _, _ := k8testutil.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, storageclass,
			volumeSnapshot, constants.DiskSize, false)

		// PVC usability
		k8testutil.PvcUsability(ctx, e2eTestConfig, client, namespace, storageclass, []*corev1.PersistentVolumeClaim{pvc}, constants.DiskSize)

		framework.Logf("Endig Test: Validate the restoration of LC-PVC")

	})

	// TC-14
	ginkgo.It("Validate LC PVC-Pod can be created with WFFC binding", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Validate LC PVC-Pod can be created with WFFC binding")

		// Fetch latebinding storage class
		spWffc := storagePolicy + "-latebinding"
		storageclassWffc, err := client.StorageV1().StorageClasses().Get(ctx, spWffc, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Create PVC
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		pvc, err := k8testutil.CreatePVC(ctx, client, namespace, labelsMap, "", storageclassWffc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create pod
		ginkgo.By("Create Pod to attach to Pvc-2")
		_, err = k8testutil.CreatePod(ctx, e2eTestConfig, client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false,
			constants.ExecRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*corev1.PersistentVolumeClaim{pvc}, constants.PollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		/*
			vmUUID := k8testutil.GetNodeUUID(ctx, client, pod2.Spec.NodeName)
			isDiskAttached2, err := vcutil.IsVolumeAttachedToVM(client, e2eTestConfig, pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached2).To(gomega.BeTrue(), "Volume is not attached to the node") */

		// Create snapshot
		volumeSnapshot, _ := k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvc, []*corev1.PersistentVolume{pv}, constants.DiskSize)

		// Get the storage quota used before LC creation
		quota := make(map[string]*resource.Quantity)
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			quota["totalQuotaUsedBefore"], _, quota["pvc_storagePolicyQuotaBefore"], _,
				quota["pvc_storagePolicyUsageBefore"], _ =
				k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName, true)
		}

		// create linked clone PVC and verify its bound
		linkdeClonePvc, err := k8testutil.CreateLinkedClonePvc(ctx, client, namespace, storageclassWffc, volumeSnapshot.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create PVC: %v", err))

		// Create and attach pod to linked clone PVC
		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// Validate PVC is bound
		pvList, err := fpv.WaitForPVClaimBoundPhase(ctx,
			client, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//create snapshot from LC-PVC
		_, _ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, linkdeClonePvc, pvList, constants.DiskSize)

		// Check the PVC usage quota increased
		if e2eTestConfig.TestInput.ClusterFlavor.SupervisorCluster {
			quota["totalQuotaUsedAfter"], _, quota["pvc_storagePolicyQuotaAfter"], _,
				quota["pvc_storagePolicyUsageAfter"], _ =
				k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
					storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName, true)

			// Get the numeric part
			pvc_storagePolicyQuotaAfter, err1 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["pvc_storagePolicyQuotaAfter"]))
			gomega.Expect(err1).To(gomega.BeNil())
			pvc_storagePolicyUsageAfter, err2 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["pvc_storagePolicyUsageAfter"]))
			gomega.Expect(err2).To(gomega.BeNil())

			pvc_storagePolicyQuotaBefore, err3 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["pvc_storagePolicyQuotaBefore"]))
			gomega.Expect(err3).To(gomega.BeNil())
			pvc_storagePolicyUsageBefore, err4 := k8testutil.ParseMi(fmt.Sprintf("%v", quota["pvc_storagePolicyUsageBefore"]))
			gomega.Expect(err4).To(gomega.BeNil())

			// Validate storageQuota increased
			gomega.Expect(pvc_storagePolicyQuotaAfter).To(gomega.BeNumerically(">", pvc_storagePolicyQuotaBefore), "Expected %v to be greater than %v", quota["snap_storagePolicyQuotaAfter"], quota["snap_storagePolicyQuotaBefore"])
			gomega.Expect(pvc_storagePolicyUsageAfter).To(gomega.BeNumerically(">", pvc_storagePolicyUsageBefore), "Expected %v to be greater than %v", quota["snap_storagePolicyUsageAfter"], quota["snap_storagePolicyUsageBefore"])
		}
		framework.Logf("Ending test: Validate LC PVC-Pod can be created with WFFC binding")

	})

})
