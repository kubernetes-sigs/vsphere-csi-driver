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

package e2e

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"

	v1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
)

var e2eTestConfig *config.E2eTestConfig

var _ bool = ginkgo.Describe("[linked-clone-p0] Linked-Clone-P0", func() {

	f := framework.NewDefaultFramework("linked-clone")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // TODO tests will create their own namespaces
	var (
		client       clientset.Interface
		namespace    string
		storageclass *v1.StorageClass
		restConfig   *restclient.Config
	)

	ginkgo.BeforeEach(func() {
		e2eTestConfig = bootstrap.Bootstrap()
		client = f.ClientSet
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Read Env variable needed for the test suite
		namespace = vcutil.GetNamespaceToRunTests(f, e2eTestConfig)
		storagePolicy := env.GetAndExpectStringEnvVar(constants.EnvStoragePolicy)

		// Get the storageclass from storagepolicy
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Create PVC, pod and volume snapshot
		k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, true)

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
		k8testutil.Cleanup(ctx, client, e2eTestConfig, namespace)

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

		// Get the storage quota used before LC creation
		totalQuotaUsedBefore, _, storagePolicyQuotaBefore, _, storagePolicyUsageBefore, _ :=
			k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName, false)

		// create linked clone PVC and verify its bound
		linkdeClonePvc := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass)

		// Create and attach pod to linked clone PVC
		k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, linkdeClonePvc)

		// List volume
		k8testutil.ValidateLcInListVolume(ctx, e2eTestConfig, f.ClientSet, linkdeClonePvc, namespace)

		diskSizeInMbStr := k8testutil.ConvertInt64ToStrMbFormat(constants.DiskSizeInMb)
		sp_quota_pvc_status, sp_usage_pvc_status := k8testutil.ValidateQuotaUsageAfterResourceCreation(ctx, restConfig,
			storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName,
			[]string{diskSizeInMbStr}, totalQuotaUsedBefore, storagePolicyQuotaBefore,
			storagePolicyUsageBefore, false)
		gomega.Expect(sp_quota_pvc_status && sp_usage_pvc_status).NotTo(gomega.BeFalse())

		// Delete linked clone
		err := fpv.DeletePersistentVolumeClaim(ctx, client, linkdeClonePvc.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// TODO update the delete list

		// get the quota post LC deletion
		totalQuotaUsedAfter, _, storagePolicyQuotaAfter, _, storagePolicyUsageAfter, _ :=
			k8testutil.GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
				storageclass.Name, namespace, constants.PvcUsage, constants.VolExtensionName, false)

		k8testutil.ValidateQuotaUsageAfterCleanUp(ctx, restConfig, storageclass.Name, namespace, constants.PvcUsage,
			constants.VolExtensionName, diskSizeInMbStr, totalQuotaUsedAfter, storagePolicyQuotaAfter,
			storagePolicyUsageAfter, false)

		// TODO volume usability

	})

})
