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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	v1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
)

var _ bool = ginkgo.Describe("[linked-clone-concurrent] Linked-Clone-concurrent", func() {

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
		1. Create a PVC
		2. Validate that PVC is bound
		3. Create and attach a Pod to it.
		4. Create a snapshot
		5. Trigger 5 threads to create a linked clone PVC
		6. Trigger the CSI driver restart when the LC creation is ongoing
		7. Validate LC creation shouldn't be affected
		8. All 5 PVCs are created
		9. CSI driver is up after a restart
		10.Run volume usability verification on LC-PVC. (i.e create Pod/VM → verify PVC & pod status → detach → attach)
	*/

	ginkgo.It("Verify the CSI drive restart during linked clone creation", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Create a linked clone (PVC) from a snapshot ")

		// Create PVC, pod and volume snapshot
		volumeSnapshot = k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, true, false)

		// Get the storage quota used before LC creation
		quota := make(map[string]*resource.Quantity)
		quota["totalQuotaUsedBefore"], _, quota["pvc_storagePolicyQuotaBefore"], _,
			quota["pvc_storagePolicyUsageBefore"], _ =
			k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName, false)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

		// Create and attach pod to linked clone PVC
		lcPod, _ := k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, linkdeClonePvc, true, false)

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

})
