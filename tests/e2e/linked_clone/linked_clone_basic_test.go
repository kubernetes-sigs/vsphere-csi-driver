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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
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
	)

	ginkgo.BeforeEach(func() {
		e2eTestConfig = bootstrap.Bootstrap()
		client = f.ClientSet
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// doCleanup set to false, to avoild individual resource cleaning up.
		doCleanup = false

		// Read Env variable needed for the test suite
		storagePolicy := env.GetAndExpectStringEnvVar(constants.EnvStoragePolicy)

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
		namespace, statuscode, err = k8testutil.CreatetWcpNsWithZonesAndPolicies(e2eTestConfig, vcRestSessionId,
			[]string{storagePolicyId}, k8testutil.GetSvcId(vcRestSessionId, e2eTestConfig),
			[]string{}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(204))

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
		k8testutil.DelTestWcpNs(e2eTestConfig, vcRestSessionId, namespace)
		gomega.Expect(k8testutil.WaitForNamespaceToGetDeleted(ctx, client, namespace, constants.Poll, constants.PollTimeout)).To(gomega.Succeed())

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
		quota["totalQuotaUsedBefore"], _, quota["pvc_storagePolicyQuotaBefore"], _,
			quota["pvc_storagePolicyUsageBefore"], _ =
			k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName, false)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

		// Create and attach pod to linked clone PVC
		lcPod, _ := k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// TODO: write some data on LC-PVC and validate

		// List volume
		k8testutil.ValidateLcInListVolume(ctx, e2eTestConfig, f.ClientSet, linkdeClonePvc, namespace)

		diskSizeInMbStr := k8testutil.ConvertInt64ToStrMbFormat(constants.DiskSizeInMb)
		sp_quota_pvc_status, sp_usage_pvc_status := k8testutil.ValidateQuotaUsageAfterResourceCreation(ctx, restConfig,
			storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName,
			[]string{diskSizeInMbStr}, quota["totalQuotaUsedBefore"], quota["pvc_storagePolicyQuotaBefore"],
			quota["pvc_storagePolicyUsageBefore"], false)
		gomega.Expect(sp_quota_pvc_status && sp_usage_pvc_status).NotTo(gomega.BeFalse())

		// Delete Pod attached to linked-clone
		err = fpod.DeletePodWithWait(ctx, client, lcPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Delete linked clone
		err := fpv.DeletePersistentVolumeClaim(ctx, client, linkdeClonePvc.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// get the quota post LC deletion
		_, _, quota["storagePolicyQuotaAfterCleanup"], _, quota["storagePolicyUsageAfterCleanup"], _ =
			k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName, false)

		k8testutil.ExpectEqual(quota["storagePolicyQuotaAfterCleanup"], quota["pvc_storagePolicyQuotaBefore"],
			"Before and After values of storagePolicy-Quota CR should match")
		k8testutil.ExpectEqual(quota["storagePolicyUsageAfterCleanup"], quota["pvc_storagePolicyUsageBefore"],
			"Before and After values of storagePolicy-Usage CR should match")

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
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

		// Create and attach pod to linked clone PVC
		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// Get the snapshot usage before creating it
		quota := make(map[string]*resource.Quantity)
		quota["totalQuotaUsedBefore"], _, quota["snap_storagePolicyQuotaBefore"], _,
			quota["snap_storagePolicyUsageBefore"], _ =
			k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, constants.SnapshotUsage, constants.SnapshotExtensionName, false)

		// Create snapshot from LC_PVC
		framework.Logf("Create snapshot from LC_PVC ")
		_ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, linkdeClonePvc, lcPv, constants.DiskSize)

		// Check the quota usage for snapshot
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

		// TODO volume usability

		framework.Logf("Ending test: Create a linked clone (PVC) from a snapshot ")

	})

	// TC-3
	ginkgo.It("Create a snapshot from a linked clone with no pod attached to LC-PVC", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Create a snapshot from a linked clone with no pod attached to LC-PVC ")

		// Create PVC and volume snapshot
		_, _, volumeSnapshot = k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, false, false)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

		// Create and attach pod to linked clone PVC
		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// Get the snapshot usage before creating it
		quota := make(map[string]*resource.Quantity)
		quota["totalQuotaUsedBefore"], _, quota["snap_storagePolicyQuotaBefore"], _,
			quota["snap_storagePolicyUsageBefore"], _ =
			k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, constants.SnapshotUsage, constants.SnapshotExtensionName, false)

		// Create snapshot from LC_PVC
		framework.Logf("Create snapshot from LC_PVC ")
		_ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, linkdeClonePvc, lcPv, constants.DiskSize)

		// Check the quota usage for snapshot
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

		// TODO volume usability

		framework.Logf("Ending test: Create a linked clone (PVC) from a snapshot ")

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
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

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

		framework.Logf("Ending test: Create a linked clone (PVC) from a snapshot ")

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
		_, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

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
		lcPvc, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

		//create pod attaching to src-pvc and lc
		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{srcPvc, lcPvc}, true, false)

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
		lcPvc1, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

		//create another linked clone PVC and verify its bound
		lcPvc2, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

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
			volumeSnapshot = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, []*corev1.PersistentVolume{pv}, constants.DiskSize1GB)
		}

		// Create linked clone using sts
		k8testutil.CreateAndValidateLcWithSts(ctx, e2eTestConfig, client, namespace, storageclass, volumeSnapshot.Name, constants.Snapshotapigroup)

		framework.Logf("Ending test: Verify creating the LC with statefulsets")

	})

})
