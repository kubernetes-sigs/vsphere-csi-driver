package restore_snapshot

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/csisnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

var _ bool = ginkgo.Describe("[restore-snapshot-other-ds] restore Snapshot on different Datastore-Basic", func() {

	f := framework.NewDefaultFramework("restore-snapshot")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		preSetupData *PreSetupTest
	)
	ginkgo.Context("Snapshot restore on diferent datastores", func() {
		sharedDatastoreType := "VSAN"
		// Generate entries dynamically from the JSON file at test construction time.
		var entries []ginkgo.TableEntry
		testCases, _ := LoadRestoreMatrix(sharedDatastoreType)
		for _, tc := range testCases {
			entries = append(entries, ginkgo.Entry(fmt.Sprintf("%s → %v", tc.SourceSC, tc.TargetSCs), tc))
		}

		ginkgo.DescribeTableSubtree("Restore-Snapshot-On-Different-Datastore-Basic-Test",
			func(tc RestoreMatrixEntry) {

				ginkgo.BeforeEach(func() {
					ginkgo.By("In BeforeEach")
					preSetupData = PreSetup(f, tc.SourceSC)
				})

				ginkgo.AfterEach(func() {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					ginkgo.By("Cleaning up preSetup PVC,Snapshot,SnapshotClass")
					if preSetupData != nil && preSetupData.VolumeSnapshot != nil {
						csisnapshot.DeleteVolumeSnapshotWithPandoraWait(ctx, preSetupData.SnapC, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
					}
					if preSetupData != nil && preSetupData.PVC != nil {
						err := fpv.DeletePersistentVolumeClaim(ctx, client, preSetupData.PVC.Name, namespace)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, preSetupData.VolHandle)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}

				})

				/*
					   CreatePvcFromSnapshotOnTargetDs
					   Steps:
						1. Create a PVC on a SC having a datastore say ds1 in storage compatible list.
						2. Create a snapshot from the volume
						3. Create a PVC from a snapshot by passing a storage class such that it has a common host as of datastore where
						   the snapshot is created
						4. Verify the PVC gets created in the datastore passed in step#3
						5. Run Volume usability test (attach - detach - relocate - data integrity)
						6. Run cleanup.
				*/
				ginkgo.It("[csi-supervisor] Restore snapshot on a different datastore", ginkgo.Label(constants.P0,
					constants.VmServiceVm, constants.Block, constants.Wcp, constants.Vc901), func() {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					ginkgo.By(fmt.Sprintf("Restoring PVC from snapshot: %s → %s", tc.SourceSC, tc.TargetSCs))
					VerifyVolumeRestoreOperationOnDifferentDatastore(ctx, e2eTestConfig, client, preSetupData.Namespace,
						tc.TargetSCs, preSetupData.VolumeSnapshot, constants.DiskSize, true)

				})

			},
			entries,
		)
	})

})
