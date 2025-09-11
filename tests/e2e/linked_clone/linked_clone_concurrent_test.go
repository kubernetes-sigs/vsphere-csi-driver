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

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
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
		err             error
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
		2. Create and attach a pod to PVC
		3. Create 3 snapshots of the PVC concurrently.
		4. Create& delete 3 linked clones of 3 snapshots concurrently.
		5. Create and attach pod to linked clone PVCs
		6. Create snapshot of any 3 linked clones
		7. Run volume usability verification on LC-PVC. (i.e create Pod/VM → verify 8. PVC & pod status → detach → attach)
		9. Run cleanup
	*/
	ginkgo.It("Verify linkedclone creation concurrently", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		NO_OF_LC_TO_CREATE := 3

		// Create and attach a pod to PVC
		volumeMap := k8testutil.CreateMultiplePvcPod(ctx, e2eTestConfig, client, namespace, storageclass, true, false, NO_OF_LC_TO_CREATE)

		// Create 3 snapshots of the PVC concurrently.
		volumeSnap := k8testutil.CreateSnapshotInParallel(ctx, e2eTestConfig, namespace, volumeMap)

		// Create linked clone
		var lcToDelete []*corev1.PersistentVolumeClaim
		valSnap := <-volumeSnap
		for range NO_OF_LC_TO_CREATE {
			linkdeClonePvc, _ := k8testutil.CreateAndValidateLinkedClone(ctx, client, namespace, storageclass, valSnap.Name)
			lcToDelete = append(lcToDelete, linkdeClonePvc)
		}

		//Create & delete 3 linked clones of 3 snapshots concurrently.
		lcPvcCreated, lcPvCreated := k8testutil.CreateDeleteLinkedClonesInParallel(ctx, client, namespace, storageclass, valSnap, lcToDelete, NO_OF_LC_TO_CREATE)

		// convert chan to list
		var lcPvcList []*corev1.PersistentVolumeClaim
		for val := range lcPvcCreated {
			lcPvcList = append(lcPvcList, val)
		}
		var lcPvList [][]*corev1.PersistentVolume
		for val := range lcPvCreated {
			lcPvList = append(lcPvList, val)
		}

		// Create and attach pod to linked clone PVCs
		for i, lc := range lcPvcList {
			_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, client, namespace, []*corev1.PersistentVolumeClaim{lc}, true, false)

			framework.Logf("Create snapshot from LC_PVC ")
			_ = k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, lc, lcPvList[i], constants.DiskSize)
		}
	})

})
