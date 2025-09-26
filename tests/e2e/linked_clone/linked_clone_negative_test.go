package linked_clone

import (
	"context"
	"fmt"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
)

var _ bool = ginkgo.Describe("[linked-clone-Negative] Linked-Clone-Negative", func() {

	f := framework.NewDefaultFramework("linked-clone")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true
	var (
		client          clientset.Interface
		namespace       string
		storageclass    *v1.StorageClass
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

	// TC-29 & 30
	ginkgo.It("Verify onlinevolume expansion on LC", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Verify onlinevolume expansion on LC")

		// Create PVC, pod and volume snapshot
		_, _, volumeSnapshot = k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, true, false)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, _ := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

		//Perform offline expansion
		currentPvcSize := linkdeClonePvc.Spec.Resources.Requests[corev1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		_, err := k8testutil.ExpandPVCSize(linkdeClonePvc, newSize, client)
		gomega.Expect(err).To(gomega.HaveOccurred())

		// Create and attach pod to linked clone PVC
		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// Edit LC-PVC and expand the storage
		sizeBeforeexpansion := linkdeClonePvc.Status.Capacity[corev1.ResourceStorage]
		pvcSizeBeforeExpansion, _ := sizeBeforeexpansion.AsInt64()
		framework.Logf("pvcsize : %d", pvcSizeBeforeExpansion)
		currentPvcSize = linkdeClonePvc.Spec.Resources.Requests[corev1.ResourceStorage]
		newSize = currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		linkdeClonePvc, err = k8testutil.ExpandPVCSize(linkdeClonePvc, newSize, client)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(linkdeClonePvc).NotTo(gomega.BeNil())

		framework.Logf("Ending test: Verify onlinevolume expansion on LC")

	})

	// TC-22
	ginkgo.It("Create a linked clone PVC from a linked clone PVC", ginkgo.Label(
		constants.P0, constants.LinkedClone, constants.Vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Starting test: Create a snapshot from a linked clone with pod attached to LC-PVC ")

		// Create PVC, pod and volume snapshot
		_, _, volumeSnapshot = k8testutil.CreatePvcPodAndSnapshot(ctx, e2eTestConfig, client, namespace, storageclass, true, false)

		// create linked clone PVC and verify its bound
		linkdeClonePvc, lcPv := k8testutil.CreateAndValidateLinkedClone(ctx, f.ClientSet, namespace, storageclass, volumeSnapshot.Name)

		// Create and attach pod to linked clone PVC
		_, _ = k8testutil.CreatePodForPvc(ctx, e2eTestConfig, f.ClientSet, namespace, []*corev1.PersistentVolumeClaim{linkdeClonePvc}, true, false)

		// Create snapshot from LC_PVC
		framework.Logf("Create snapshot from LC_PVC ")
		lcSnap, _ := k8testutil.CreateVolumeSnapshot(ctx, e2eTestConfig, namespace, linkdeClonePvc, lcPv, constants.DiskSize)

		// create linked clone PVC
		pvcspec := k8testutil.PvcSpecWithLinkedCloneAnnotation(namespace, storageclass, corev1.ReadWriteOnce, constants.Snapshotapigroup, lcSnap.Name)
		ginkgo.By(fmt.Sprintf("Creating linked-clone PVC in namespace: %s using Storage Class: %s",
			namespace, storageclass.Name))
		_, err := fpv.CreatePVC(ctx, client, namespace, pvcspec)
		gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("Failed to create PVC: %v", err))
		// TODO volume usability

		framework.Logf("Ending test: reate a snapshot from a linked clone with pod attached to LC-PVC")

	})
})
