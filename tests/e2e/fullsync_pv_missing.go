/*
Copyright 2026 The Kubernetes Authors.

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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

// Tests for the pv_missing labeling contract introduced by the
// cns-health-initiative change in pkg/syncer/fullsync.go.
//
// Per the new design, full-sync no longer unregisters CNS volumes whose
// matching K8s PV is missing. Instead it:
//
//   - observes the volume across two consecutive full-sync cycles (grace
//     period to absorb transient races),
//   - applies the metadata label `pv_missing=true` on the volume's PV-type
//     CnsKubernetesEntityMetadata,
//   - leaves the volume registered so VI admins can inspect via the
//     vCenter Container Volumes view, and
//   - exposes the count via the Prometheus gauge `vsphere_cns_volume_pv_missing`.
//
// These tests verify that contract end-to-end. They do not replace the
// existing fullsync_test_for_block_volume.go suite; they complement it with
// positive coverage of the new behaviour and its idempotency.
var _ bool = ginkgo.Describe("[csi-block-vanilla][csi-supervisor] full-sync pv_missing labeling", func() {
	f := framework.NewDefaultFramework("e2e-pv-missing")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	var (
		client                     clientset.Interface
		namespace                  string
		fullSyncWaitTime           int
		storagePolicyName          string
		scParameters               map[string]string
		isVsanHealthServiceStopped bool
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if isVsanHealthServiceStopped {
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}
	})

	// pvMissingCreatePVCAndPV provisions a single PVC backed by a CSI PV using
	// the same storage-class setup as fullsync_test_for_block_volume.go and
	// returns the StorageClass, PVC, PV, and volume handle. The caller owns
	// cleanup.
	pvMissingCreatePVCAndPV := func(ctx context.Context) (*storagev1.StorageClass, *v1.PersistentVolumeClaim,
		*v1.PersistentVolume, string) {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		if vanillaCluster {
			sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil, nil, "", nil, "", false, "")
		} else if supervisorCluster {
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			restClientConfig := getRestConfigClient()
			setStoragePolicyQuota(ctx, restClientConfig, storagePolicyName, namespace, rqLimit)
			sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil,
				scParameters, "", nil, "", true, "", storagePolicyName)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		return sc, pvc, pv, pv.Spec.CSI.VolumeHandle
	}

	// Test 1: applied (CNS volume gets labeled pv_missing=true after PV
	// deletion + 2 full-sync cycles + grace period). This is the primary
	// positive assertion of the new contract.
	ginkgo.It("[pv-missing-applied] CNS volume is retained and labeled pv_missing=true "+
		"after the K8s PV is deleted", ginkgo.Label(p0, block, vanilla, wcp, core, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sc, pvc, pv, fcdID := pvMissingCreatePVCAndPV(ctx)
		defer func() {
			if !supervisorCluster {
				_ = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			}
		}()

		ginkgo.By("Stopping vsan-health on the vCenter host to batch PVC+PV deletion outside of CNS")
		isVsanHealthServiceStopped = true
		err := invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Deleting PVC %s/%s and PV %s while vsan-health is stopped", namespace, pvc.Name, pv.Name))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Starting vsan-health on the vCenter host")
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds (2x fullSyncWaitTime) to span the grace period",
			2*fullSyncWaitTime))
		time.Sleep(time.Duration(2*fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Verify pv_missing=true label is applied on CNS volume %s", fcdID))
		err = e2eVSphere.waitForPVMissingLabel(ctx, fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the CNS volume is still registered (not unregistered)")
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).To(gomega.HaveLen(1),
			"CNS volume must remain registered after missing-PV labeling")

		ginkgo.By("Cleanup: explicitly unregister + delete FCD via cnsDeleteVolume")
		err = e2eVSphere.cnsDeleteVolume(ctx, fcdID, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	// Test 2: cleared (pv_missing label is removed when a matching K8s PV
	// is re-created). The label must not be sticky — operational remediation
	// of the missing-PV condition has to clear the signal naturally.
	ginkgo.It("[pv-missing-cleared] pv_missing label is cleared when a matching K8s PV reappears",
		ginkgo.Label(p1, block, vanilla, wcp, core, vc70), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sc, pvc, pv, fcdID := pvMissingCreatePVCAndPV(ctx)
			defer func() {
				if !supervisorCluster {
					_ = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				}
			}()

			ginkgo.By("Stopping vsan-health and deleting PVC+PV to trigger the missing-PV state")
			isVsanHealthServiceStopped = true
			err := invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

			ginkgo.By("Span the grace period so pv_missing=true is applied")
			time.Sleep(time.Duration(2*fullSyncWaitTime) * time.Second)
			err = e2eVSphere.waitForPVMissingLabel(ctx, fcdID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Re-create a static PV pointing at the same VolumeHandle so K8s
			// once again has a PV for this volume. Full-sync's existing update
			// path should overwrite the PV-entity metadata and drop pv_missing.
			ginkgo.By(fmt.Sprintf("Re-creating a static PV pointing at the same VolumeHandle %s", fcdID))
			staticPVLabels := map[string]string{"fcd-id": fcdID}
			newPV := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimRetain, staticPVLabels, ext4FSType)
			newPV, err = client.CoreV1().PersistentVolumes().Create(ctx, newPV, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full-sync to overwrite labels", fullSyncWaitTime))
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

			ginkgo.By("Verify pv_missing label is no longer present on CNS volume")
			labeled, err := e2eVSphere.hasPVMissingLabel(fcdID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(labeled).To(gomega.BeFalse(),
				"pv_missing label must be cleared once a matching K8s PV exists")

			ginkgo.By("Cleanup: delete the static PV and the underlying CNS volume + FCD")
			err = client.CoreV1().PersistentVolumes().Delete(ctx, newPV.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.cnsDeleteVolume(ctx, fcdID, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

	// Test 3: grace period (after a single full-sync cycle the label must
	// NOT be present; only after the second cycle does labeling occur).
	// This protects against regressions where the grace period is removed
	// or shortened — without it, transient API races between PV deletion
	// and full-sync would flap the label.
	ginkgo.It("[pv-missing-grace] pv_missing label is applied only after the two-cycle grace period",
		ginkgo.Label(p1, block, vanilla, core, vc70), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sc, pvc, pv, fcdID := pvMissingCreatePVCAndPV(ctx)
			defer func() {
				if !supervisorCluster {
					_ = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				}
			}()

			ginkgo.By(fmt.Sprintf("Deleting PVC %s/%s and PV %s with CNS healthy (no service stop)",
				namespace, pvc.Name, pv.Name))
			err := client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Sleeping for one fullSyncWaitTime (%v sec) — first cycle only records the volume",
				fullSyncWaitTime))
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

			ginkgo.By("After the first full-sync cycle the volume must NOT yet carry pv_missing=true")
			labeled, err := e2eVSphere.hasPVMissingLabel(fcdID)
			// hasPVMissingLabel returns false when the volume has been removed
			// from CNS (a regression — old unregister flow). We assert both
			// "volume present" and "label absent" together.
			queryResult, qerr := e2eVSphere.queryCNSVolumeWithResult(fcdID)
			gomega.Expect(qerr).NotTo(gomega.HaveOccurred())
			gomega.Expect(queryResult.Volumes).To(gomega.HaveLen(1),
				"CNS volume must remain registered through the grace period")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(labeled).To(gomega.BeFalse(),
				"pv_missing=true must NOT be applied on the first full-sync cycle (grace period)")

			ginkgo.By(fmt.Sprintf("Sleeping for another fullSyncWaitTime (%v sec) — second cycle applies the label",
				fullSyncWaitTime))
			time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

			ginkgo.By("After the second full-sync cycle the volume MUST carry pv_missing=true")
			err = e2eVSphere.waitForPVMissingLabel(ctx, fcdID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Cleanup")
			err = e2eVSphere.cnsDeleteVolume(ctx, fcdID, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

	// Test 4: idempotency (running multiple full-sync cycles against an
	// already-labeled volume must not produce duplicate labels or change
	// the existing label's value). Protects against bug classes where the
	// labeling code path keeps appending KeyValue entries.
	ginkgo.It("[pv-missing-idempotent] pv_missing label is applied at most once across multiple cycles",
		ginkgo.Label(p2, block, vanilla, core, vc70), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sc, pvc, pv, fcdID := pvMissingCreatePVCAndPV(ctx)
			defer func() {
				if !supervisorCluster {
					_ = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				}
			}()

			ginkgo.By(fmt.Sprintf("Deleting PVC %s/%s and PV %s", namespace, pvc.Name, pv.Name))
			err := client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Span grace period + 2 additional full-sync cycles (4x total) to test idempotency")
			time.Sleep(time.Duration(4*fullSyncWaitTime) * time.Second)

			ginkgo.By("Verify pv_missing=true exactly once on the PV-type CnsKubernetesEntityMetadata")
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(queryResult.Volumes).To(gomega.HaveLen(1))

			pvMissingCount := 0
			for _, baseEm := range queryResult.Volumes[0].Metadata.EntityMetadata {
				em, ok := baseEm.(*cnstypes.CnsKubernetesEntityMetadata)
				if !ok || em.EntityType != string(cnstypes.CnsKubernetesEntityTypePV) {
					continue
				}
				for _, kv := range em.Labels {
					if kv.Key == "pv_missing" && kv.Value == "true" {
						pvMissingCount++
					}
				}
			}
			gomega.Expect(pvMissingCount).To(gomega.Equal(1),
				"pv_missing=true must appear exactly once across the PV-type entity's Labels even after multiple cycles")

			ginkgo.By("Cleanup")
			err = e2eVSphere.cnsDeleteVolume(ctx, fcdID, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})
})
