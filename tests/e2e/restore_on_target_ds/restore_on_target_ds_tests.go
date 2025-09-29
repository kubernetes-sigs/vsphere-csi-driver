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

package restore_on_target_ds

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/csisnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"

	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
)

var e2eTestConfig *config.E2eTestConfig

var _ bool = ginkgo.Describe("[restore-on-target-ds-p0] restore-on-target-ds-p0", func() {

	f := framework.NewDefaultFramework("restore-on-target-ds")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true
	var (
		client          clientset.Interface
		namespace       string
		storageclass    *v1.StorageClass
		restoreSc       *v1.StorageClass
		err             error
		vcRestSessionId string
		statuscode      int
		storagePolicy   string
		restoreSp       string
	)

	ginkgo.BeforeEach(func() {
		e2eTestConfig = bootstrap.Bootstrap()
		client = f.ClientSet
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Read Env variable needed for the test suite
		storagePolicy = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicy)
		restoreSp = env.GetAndExpectStringEnvVar(constants.EnvRestoreStoragePolicy)

		// Get the storageclass from storagepolicy
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		restoreSc, err = client.StorageV1().StorageClasses().Get(ctx, restoreSp, metav1.GetOptions{})
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

		// Delete namespace
		k8testutil.DelTestWcpNs(e2eTestConfig, vcRestSessionId, namespace)
		gomega.Expect(k8testutil.WaitForNamespaceToGetDeleted(ctx, client, namespace, constants.Poll, constants.PollTimeout)).To(gomega.Succeed())

	})

	// TC-17
	ginkgo.It("Online & offline volume expansion of restored volume", ginkgo.Label(
		constants.P0, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Online & offline volume expansion of restored volume")

		// Create 2 PVC on sc-1 and attach them to pod
		volumeMap := k8testutil.CreateMultiplePvcPod(ctx, e2eTestConfig, client, namespace, storageclass, 2, true, false)

		// Create a snapshot of both the volume.
		for pvclaim, pvList := range volumeMap {
			volumeSnapshot, _ := csisnapshot.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, pvList, constants.DiskSize)

			//  Restore it on different SC
			ginkgo.By("Restore sanpshots to create new volumes")
			_, _, _ = csisnapshot.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, restoreSc,
				volumeSnapshot, constants.DiskSize, true)
		}

		// TODO : Run volume expansion on both the restored PVCs

		framework.Logf("Ending test: Online & offline volume expansion of restored volume")

	})

	// TC-16
	ginkgo.It("Restore with different size", ginkgo.Label(
		constants.P0, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Restore with different size")

		// Create a PVC on sc-1 and attach pod to it
		volumeMap := k8testutil.CreateMultiplePvcPod(ctx, e2eTestConfig, client, namespace, storageclass, 1, true, false)

		// Create a snapshot of the volume.
		for pvclaim, pvList := range volumeMap {
			volumeSnapshot, _ := csisnapshot.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, pvList, constants.DiskSize)

			//  Restore it on different SC
			ginkgo.By("Restore sanpshots to create new volumes")
			_, _, _ = csisnapshot.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, restoreSc,
				volumeSnapshot, constants.DiskSize1GB, true)
		}

		// TODO : Run volume expansion on both the restored PVCs

		framework.Logf("Ending test: Restore with different size")
	})

	// TC -15
	ginkgo.It("Create a snapshot of the restored PVC", ginkgo.Label(
		constants.P0, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test:Create a snapshot of the restored PVC")

		// Create a PVC on sc-1 and attach pod to it
		volumeMap := k8testutil.CreateMultiplePvcPod(ctx, e2eTestConfig, client, namespace, storageclass, 1, true, false)

		// Create a snapshot of the volume.
		for pvclaim, pvList := range volumeMap {
			volumeSnapshot, _ := csisnapshot.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, pvList, constants.DiskSize)

			//  Restore it on different SC
			ginkgo.By("Restore sanpshots to create new volumes")
			restoredPvc, restoredPvList, _ := csisnapshot.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, restoreSc,
				volumeSnapshot, constants.DiskSize, true)

			// create snapshot of volume created in #4
			vsRestoredPvc, _ := csisnapshot.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, restoredPvc, restoredPvList, constants.DiskSize)

			// Restore it with different sc
			ginkgo.By("Restore sanpshots to create new volumes")
			restoredPvc, restoredPvList, _ = csisnapshot.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, storageclass,
				vsRestoredPvc, constants.DiskSize, true)
		}

		// TODO : Run volume expansion on both the restored PVCs

		framework.Logf("Ending test: Create a snapshot of the restored PVC")
	})

	// TC -14
	ginkgo.It("Restore with a different policy on Deployment ", ginkgo.Label(
		constants.P0, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Restore with a different policy on Deployment ")

		// Create a PVC on sc-1 and attach pod to it
		volumeMap := k8testutil.CreateMultiplePvcPod(ctx, e2eTestConfig, client, namespace, storageclass, 1, false, true)

		// Create a snapshot of the volume.
		for pvclaim, pvList := range volumeMap {
			volumeSnapshot, _ := csisnapshot.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, pvList, constants.DiskSize)

			//  Restore it on different SC
			ginkgo.By("Restore sanpshots to create new volumes")
			_, _, _ = csisnapshot.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, restoreSc,
				volumeSnapshot, constants.DiskSize, true)
		}

		// TODO : Run volume expansion on both the restored PVCs

		framework.Logf("Ending test: Restore with a different policy on Deployment ")
	})

	// TC - 13
	ginkgo.It("Restore with a different policy on statefulset ", ginkgo.Label(
		constants.P0, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Restore with a different policy on statefulset")

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
			volumeSnapshot, _ := csisnapshot.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvclaim, []*corev1.PersistentVolume{pv}, constants.DiskSize1GB)

			//  Restore it on different SC
			ginkgo.By("Restore sanpshots to create new volumes")
			_, _, _ = csisnapshot.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, restoreSc,
				volumeSnapshot, constants.DiskSize, true)
		}

		// TODO : Run volume expansion on both the restored PVCs

		framework.Logf("Ending test: Restore with a different policy on statefulset")
	})

	// TC - 9
	ginkgo.It("WFFC - Create a PVC from a snapshot on a different datastore", ginkgo.Label(
		constants.P0, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: WFFC - Create a PVC from a snapshot on a different datastore")

		storagePolicyWffc := storagePolicy + "-latebinding"
		restoreSpwffc := restoreSp + "-latebinding"
		// Get the storageclass from storagepolicy
		storageclassWffc, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyWffc, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		restoreScWffc, err := client.StorageV1().StorageClasses().Get(ctx, restoreSpwffc, metav1.GetOptions{})
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

		volumeSnapshot, _ := csisnapshot.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, pvc, pvs, constants.DiskSize)

		//  Restore it on different SC-wffc
		ginkgo.By("Restore sanpshots to create new volumes")
		_, _, _ = csisnapshot.VerifyVolumeRestoreOperation(ctx, e2eTestConfig, client, namespace, restoreScWffc,
			volumeSnapshot, constants.DiskSize, true)

		// TODO : Run volume expansion on both the restored PVCs

		framework.Logf("Ending test:WFFC - Create a PVC from a snapshot on a different datastore")

	})

})
