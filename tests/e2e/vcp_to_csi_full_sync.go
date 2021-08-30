/*
Copyright 2021 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/migration/v1alpha1"
)

var _ = ginkgo.Describe("[csi-vcp-mig] VCP to CSI migration full sync tests", func() {
	f := framework.NewDefaultFramework("vcp-2-csi-full-sync")
	var (
		client                     clientset.Interface
		namespace                  string
		nodeList                   *v1.NodeList
		vcpScs                     []*storagev1.StorageClass
		vcpPvcsPreMig              []*v1.PersistentVolumeClaim
		vcpPvsPreMig               []*v1.PersistentVolume
		vcpPvcsPostMig             []*v1.PersistentVolumeClaim
		vcpPvsPostMig              []*v1.PersistentVolume
		err                        error
		kcmMigEnabled              bool
		isSPSserviceStopped        bool
		isVsanHealthServiceStopped bool
		labelKey                   string
		labelValue                 string
		vmdks                      []string
		pvsToDelete                []*v1.PersistentVolume
		fullSyncWaitTime           int
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		generateNodeMap(ctx, testConfig, &e2eVSphere, client)
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = false

		labelKey = "label-key"
		labelValue = "label-value"
		pvsToDelete = []*v1.PersistentVolume{}

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Full sync interval can be 1 min at minimum so full sync wait time has to be more than 120s
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}
	})

	ginkgo.JustAfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvcsToDelete []*v1.PersistentVolumeClaim
		connect(ctx, &e2eVSphere)
		if kcmMigEnabled {
			pvcsToDelete = append(vcpPvcsPreMig, vcpPvcsPostMig...)
		} else {
			pvcsToDelete = append(pvcsToDelete, vcpPvcsPreMig...)
		}
		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{}
		vcpPvcsPostMig = []*v1.PersistentVolumeClaim{}

		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort

		if isVsanHealthServiceStopped {
			ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
			err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again",
				vsanHealthServiceWaitTime))
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}

		if isSPSserviceStopped {
			ginkgo.By(fmt.Sprintln("Starting sps on the vCenter host"))
			err = invokeVCenterServiceControl(startOperation, "sps", vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to come up again", vsanHealthServiceWaitTime))
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}

		crds := []*v1alpha1.CnsVSphereVolumeMigration{}
		for _, pvc := range pvcsToDelete {
			pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vPath := pv.Spec.VsphereVolume.VolumePath
			if kcmMigEnabled {
				found, crd := getCnsVSphereVolumeMigrationCrd(ctx, vPath)
				if found {
					crds = append(crds, crd)
				}
			}
			pvsToDelete = append(pvsToDelete, pv)

			framework.Logf("Deleting PVC %v", pvc.Name)
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		var defaultDatastore *object.Datastore
		esxHost := GetAndExpectStringEnvVar(envEsxHostIP)
		for _, pv := range pvsToDelete {
			if pv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimRetain {
				err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if defaultDatastore == nil {
					defaultDatastore = getDefaultDatastore(ctx)
				}
				if pv.Spec.CSI != nil {
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = e2eVSphere.deleteFCD(ctx, pv.Spec.CSI.VolumeHandle, defaultDatastore.Reference())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else {
					if kcmMigEnabled {
						found, crd := getCnsVSphereVolumeMigrationCrd(ctx, pv.Spec.VsphereVolume.VolumePath)
						gomega.Expect(found).To(gomega.BeTrue())
						err = e2eVSphere.waitForCNSVolumeToBeDeleted(crd.Spec.VolumeID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						err = e2eVSphere.deleteFCD(ctx, crd.Spec.VolumeID, defaultDatastore.Reference())
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					err = deleteVmdk(esxHost, pv.Spec.VsphereVolume.VolumePath)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			if pv.Spec.CSI != nil {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = waitForVmdkDeletion(ctx, pv.Spec.VsphereVolume.VolumePath)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		for _, crd := range crds {
			framework.Logf("Waiting for CnsVSphereVolumeMigration crd %v to be deleted", crd.Spec.VolumeID)
			err = waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx, crd)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		vcpPvsPreMig = nil
		vcpPvsPostMig = nil

		if kcmMigEnabled {
			err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		vmdksToDel := vmdks
		vmdks = nil
		for _, vmdk := range vmdksToDel {
			err = deleteVmdk(esxHost, vmdk)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		var scsToDelete []*storagev1.StorageClass
		scsToDelete = append(scsToDelete, vcpScs...)
		vcpScs = []*storagev1.StorageClass{}
		for _, vcpSc := range scsToDelete {
			err := client.StorageV1().StorageClasses().Delete(ctx, vcpSc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	// Verify volume entry is updated in CNS when PVC bound to statically created
	// PV is deleted in K8s (when CNS was down).
	// Steps:
	// 1. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 2. Create SC1 VCP SC.
	// 3. Create vmdk1.
	// 4. Create PV1 using vmdk1 and SC1.
	// 5. Create PVC1 using SC1 and wait for binding with PV1.
	// 6. Verify cnsvspherevolumemigrations crds are created for PVC1 and PV1.
	// 7. Stop vsan-health on VC.
	// 8. Delete PVC1.
	// 9. Start vsan-health on VC.
	// 10. Verify PV1 and vmdk1 are delete.
	// 11. Verify CNS entries for PVC1 and PV1 are removed.
	// 12. Verify cnsvspherevolumemigrations crds are removed for PVC1 and PV1.
	// 13. Delete the SC1.
	// 14. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// Verify volume entry is updated in CNS when PVC is bound to statically
	// created PV in K8s (when CNS was down).
	// Steps:
	// 1. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 2. Create SC1 VCP SC.
	// 3. Create vmdk1.
	// 4. Create PV1 using vmdk1 and SC1.
	// 5. Stop vsan-health on VC.
	// 6. Create PVC1 using SC1 and wait for binding with PV1.
	// 7. Start vsan-health on VC.
	// 8. Sleep double the Full Sync interval.
	// 9. Verify cnsvspherevolumemigrations crds are created for PVC1 and PV1.
	// 10. Verify CNS entries for PVC1 and PV1.
	// 11. Delete the PVC1.
	// 12. Verify PV1 and vmdk1 are deleted.
	// 13. Verify cnsvspherevolumemigrations crds are removed for PVC1 and PV1.
	// 14. Verify CNS entries are removed for PVC1 and PV1.
	// 15. Delete the SC1.
	// 16. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// Verify volume entry is updated in CNS when PVC bound to PV with reclaim
	// policy Retain is deleted in K8s (when CNS was down).
	// Steps:
	// 1. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 2. Create SC1 VCP SC with reclaim policy Retain.
	// 3. Create PVC1 using SC1 and wait for binding with PV1.
	// 4. Verify cnsvspherevolumemigrations crds are created for PVC1 and PV1.
	// 5. Verify CNS entries for PVC1 and PV1.
	// 6. Stop vsan-health on VC.
	// 7. Delete PVC1.
	// 8. Start vsan-health on VC.
	// 9. Sleep double the Full Sync interval.
	// 10. Verify PVC name is removed from CNS entry for PV1.
	// 11. Verify CNS entry for PVC1 is removed.
	// 12. Delete PV1 and vmdk1.
	// 13. Verify CNS entry for PV1 is removed.
	// 14. Verify cnsvspherevolumemigrations crds are removed for PVC1 and PV1.
	// 15. Delete the SC1.
	// 16. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// Verify volume entries are deleted in CNS when PVC and PC with reclaim
	// policy Retain are deleted in K8s (when CNS was down).
	// Steps:
	// 1. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 2. Create SC1 VCP SC with reclaim policy Retain.
	// 3. Create PVC1 using SC1 and wait for binding with PV1.
	// 4. Verify cnsvspherevolumemigrations crds are created for PVC1 and PV1.
	// 5. Verify CNS entries for PVC1 and PV1.
	// 6. Stop vsan-health on VC.
	// 7. Delete PVC1.
	// 8. Delete PV1.
	// 9. Start vsan-health on VC.
	// 10. Sleep double the Full Sync interval.
	// 11. Verify CNS entry for PVC1 and PV1 is removed.
	// 12. Delete vmdk1.
	// 13. Verify cnsvspherevolumemigrations crds are removed for PVC1 and PV1.
	// 14. Delete the SC1.
	// 15. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// Add PV and PVC labels when CNS is down.
	// Steps:
	// 1. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 2. Create SC1 VCP SC.
	// 3. Create PVC1 using SC1 and wait for binding with PV (say PV1).
	// 4. Verify cnsvspherevolumemigrations crds are created for PVC1 and PV1.
	// 5. Verify CNS entries for PVC1 and PV1.
	// 6. Stop vsan-health on VC.
	// 7. Update labels on PV1 and PVC1.
	// 8. Start vsan-health on VC.
	// 9. Sleep double the Full Sync interval.
	// 10. Verify CNS entries for PVC1 and PV1.
	// 11. Delete the PVC1.
	// 12. Verify PV1 and underlying vmdk are also deleted.
	// 13. Verify cnsvspherevolumemigrations crds are removed for PVC1 and PV1.
	// 14. Verify CNS entries are removed for PVC1, PV1.
	// 15. Delete the SC1.
	// 16. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// Remove PV and PVC labels when CNS is down.
	// Steps:
	// 1. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 2. Create SC1 VCP SC.
	// 3. Create PVC1 using SC1 and wait for binding with PV (say PV1).
	// 4. Verify cnsvspherevolumemigrations crds are created for PVC1 and PV1.
	// 5. Update labels on PV1 and PVC1.
	// 6. Verify CNS entries for PVC1 and PV1.
	// 7. Stop vsan-health on VC.
	// 8. Remove labels on PV1 and PVC1.
	// 9. Start vsan-health on VC.
	// 10. Sleep double the Full Sync interval.
	// 11. Verify CNS entries for PVC1 and PV1.
	// 12. Delete the PVC1.
	// 13. Verify PV1 and underlying vmdk are also deleted.
	// 14. Verify cnsvspherevolumemigrations crds are removed for PVC1 and PV1.
	// 15. Verify CNS entries are removed for PVC1, PV1.
	// 16. Delete the SC1.
	// 17. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// Perform multiple operations when CNS is down.
	// Steps:
	// 1. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 2. Create VCP SC SC1 with ReclaimPolicy=Delete.
	// 3. Create VCP SC SC2 with ReclaimPolicy=Retain.
	// 4. Create 4 PVCs (1..4) using SC1.
	// 5. Create 4 PVCs (7..10) using SC2.
	// 6. Wait for all PVCs to be in Bound phase..
	// 7. Add labels to PVC 4, PVC 10, PV4, PV10.
	// 8. Verify cnsvspherevolumemigrations crds are created for all PV/PVCs.
	// 9. Verify CNS entries for all PVC/PVs.
	// 10. Stop vsan-health.
	// 11. Create PVCs 5, 6 using SC1.
	// 12. Create PVCs 11, 12 using SC2.
	// 13. Update labels on 4 PVCs (1, 2, 5, 7, 8, 12) and their PVs.
	// 14. Delete 4 PVCs (2, 3, 8, 9).
	// 15. Modify labels on PVC 4, PVC 10, PV4, PV10.
	// 16. Start vsan-health.
	// 17. Sleep double the Full Sync interval.
	// 18. Verify the deleted PVCs and PVs are no longer present in CNS cache.
	// 19. Verify the CNS entries for remaining PV/PVCs are correct with lables
	//     and PVC names for PVs.
	// 20. Verify cnsvspherevolumemigrations crds are created for all PV/PVCs.
	// 21. Delete remaining PVCs.
	// 22. Delete remaining PVs.
	// 23. Delete vmdks for PVs with ReclaimPolicy=Retain.
	// 24. Verify cnsvspherevolumemigrations crds are deleted for all PV/PVCs.
	// 25. Delete both SCs.
	// 26. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	ginkgo.It("Multiple operations when CNS is down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		labels := map[string]string{labelKey: labelValue}
		labelsNew := map[string]string{labelKey + "new": labelValue + "new"}
		cnsVolsToVerifyDeletion := []string{}
		crdsToVerifyDeletion := []*v1alpha1.CnsVSphereVolumeMigration{}

		ginkgo.By("Create VCP SC SC1 with ReclaimPolicy=Delete")
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Create VCP SC SC2 with ReclaimPolicy=Retain")
		vcpScRetain, err := createVcpStorageClass(client, scParams, nil,
			v1.PersistentVolumeReclaimRetain, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpScRetain)

		ginkgo.By("Create VCP SC SC3 with ReclaimPolicy=Delete")
		vcpSc4StaticVol, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc4StaticVol)

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Create 4 PVCs (1..4) using SC1")
		for i := 1; i < 5; i++ {
			pvc, err := createPVC(client, namespace, nil, "", vcpSc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPostMig = append(vcpPvcsPostMig, pvc)
		}
		ginkgo.By("Create 4 PVCs (7..10) using SC2")
		for i := 1; i < 5; i++ {
			pvc, err := createPVC(client, namespace, nil, "", vcpScRetain, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPostMig = append(vcpPvcsPostMig, pvc)
		}

		ginkgo.By("Waiting for PVCs to bind")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating vmdk1 on the shared datastore " + scParams[vcpScParamDatastoreName])
		esxHost := GetAndExpectStringEnvVar(envEsxHostIP)
		vmdk1, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk1)

		ginkgo.By("Creating PV1 with vmdk1")
		pv1 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk1), v1.PersistentVolumeReclaimDelete, nil)
		pv1.Spec.StorageClassName = vcpSc4StaticVol.Name
		pv1, err = client.CoreV1().PersistentVolumes().Create(ctx, pv1, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		ginkgo.By("Creating PVC1 with PV1 and VCP SC")
		pvc1 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpSc4StaticVol, nil, "")
		pvc1.Spec.VolumeName = pv1.Name
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc1, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc1)

		ginkgo.By("Waiting for PVC1 and PV1 to bind")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPreMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating vmdk2 on the shared datastore " + scParams[vcpScParamDatastoreName])
		vmdk2, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk2)

		ginkgo.By("Creating PV2 with vmdk2")
		pv2 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk2), v1.PersistentVolumeReclaimDelete, nil)
		pv2.Spec.StorageClassName = vcpSc4StaticVol.Name
		pv2, err = client.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}
		vcpPvsPreMig = append(vcpPvsPreMig, pv2)

		ginkgo.By("Add labels to PVC 4, PVC 10, PV4, PV10")
		vcpPvcsPostMig[3] = updatePvcLabel(ctx, client, namespace, vcpPvcsPostMig[3], labels)
		vcpPvcsPostMig[7] = updatePvcLabel(ctx, client, namespace, vcpPvcsPostMig[7], labels)

		vcpPvsPostMig[3] = updatePvLabel(ctx, client, namespace, vcpPvsPostMig[3], labels)
		vcpPvsPostMig[7] = updatePvLabel(ctx, client, namespace, vcpPvsPostMig[7], labels)

		ginkgo.By("Verify annotations on PV/PVCs")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false)
		// Static provisioned volumes.
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on PVC1")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPreMig)

		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("Create PVCs 5, 6 using SC1")
		for i := 1; i < 3; i++ {
			pvc, err := createPVC(client, namespace, nil, "", vcpSc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPostMig = append(vcpPvcsPostMig, pvc)
		}
		ginkgo.By("Create PVCs 11, 12 using SC2")
		for i := 1; i < 3; i++ {
			pvc, err := createPVC(client, namespace, nil, "", vcpScRetain, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPostMig = append(vcpPvcsPostMig, pvc)
		}

		ginkgo.By("Delete PVC1")
		vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc1.Name)
		found, crd := getCnsVSphereVolumeMigrationCrd(ctx, vpath)
		gomega.Expect(found).To(gomega.BeTrue())
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc1.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		cnsVolsToVerifyDeletion = append(cnsVolsToVerifyDeletion, crd.Spec.VolumeID)
		crdsToVerifyDeletion = append(crdsToVerifyDeletion, crd)

		ginkgo.By("Creating PVC2 with PV2 and VCP SC")
		pvc2 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpSc4StaticVol, nil, "")
		pvc2.Spec.VolumeName = pv2.Name
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{pvc2}

		ginkgo.By("Update labels on PVCs (1, 2, 5, 7, 8, 12) and their PVs.")
		for _, i := range []int{0, 1, 4, 5, 8, 10} {
			vcpPvcsPostMig[i] = updatePvcLabel(ctx, client, namespace, vcpPvcsPostMig[i], labels)
		}

		for _, i := range []int{0, 1, 4, 5} {
			vcpPvsPostMig[i] = updatePvLabel(ctx, client, namespace, vcpPvsPostMig[i], labels)
		}

		ginkgo.By("Delete 4 PVCs (2, 3, 8, 9)")
		for _, pvc := range []*v1.PersistentVolumeClaim{vcpPvcsPostMig[1], vcpPvcsPostMig[2], vcpPvcsPostMig[6]} {
			vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			found, crd := getCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(found).To(gomega.BeTrue())
			cnsVolsToVerifyDeletion = append(cnsVolsToVerifyDeletion, crd.Spec.VolumeID)
			crdsToVerifyDeletion = append(crdsToVerifyDeletion, crd)
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx,
			vcpPvcsPostMig[5].Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete one PV with Retain policy for which PVC was also deleted above")
		err = client.CoreV1().PersistentVolumes().Delete(ctx, vcpPvsPostMig[6].Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Modify labels on PVC 4, PVC 10, PV4, PV10")
		vcpPvcsPostMig[3] = updatePvcLabel(ctx, client, namespace, vcpPvcsPostMig[3], labelsNew)
		vcpPvcsPostMig[7] = updatePvcLabel(ctx, client, namespace, vcpPvcsPostMig[7], labelsNew)

		vcpPvsPostMig[3] = updatePvLabel(ctx, client, namespace, vcpPvsPostMig[3], labelsNew)
		vcpPvsPostMig[7] = updatePvLabel(ctx, client, namespace, vcpPvsPostMig[7], labelsNew)

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to be ready", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanHealthServiceStopped = false

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Verify the deleted PVCs and PVs are no longer present in CNS cache")
		for _, crd := range crdsToVerifyDeletion {
			err = waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx, crd)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		for _, fcdID := range cnsVolsToVerifyDeletion {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		_, err = fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvsCreatedDuringCnsDown, err := fpv.WaitForPVClaimBoundPhase(client,
			vcpPvcsPostMig[8:], framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvsPostMig = append(vcpPvsPostMig, pvsCreatedDuringCnsDown...)

		remainingPvcs := []*v1.PersistentVolumeClaim{}
		remainingPvs := []*v1.PersistentVolume{}
		for _, pvc := range append(vcpPvcsPostMig, vcpPvcsPreMig...) {
			pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				continue
			}
			remainingPvcs = append(remainingPvcs, pvc)
		}
		vcpPvcsPostMig = remainingPvcs
		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{}

		for _, pv := range append(vcpPvsPostMig, vcpPvsPreMig...) {
			pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				continue
			}
			remainingPvs = append(remainingPvs, pv)
		}
		vcpPvsPostMig = remainingPvs
		vcpPvsPreMig = []*v1.PersistentVolume{}

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on remaining PV, PVCs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, remainingPvcs)
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvsWithoutPvc(ctx, client, namespace, remainingPvs)

	})

	/*
		Verify volume entry is updated in CNS when PVC is bound to statically created PV in K8s (when SPS was down)
		Steps:
		1.	Enable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)
		2.	Create SC1 VCP SC
		3.	Create vmdk1
		4.	Create PV1 using vmdk1 and SC1
		5.	Stop SPS service on VC
		6.	Create PVC1 using SC1 and wait for binding with PV1
		7.	Start SPS service on VC
		8.	Sleep double the Full Sync interval
		9.	Verify cnsvspherevolumemigrations crds are created for PVC1 and PV1
		10.	Verify CNS entries for PVC1 and PV1
		11.	Delete the PVC1
		12.	Verify PV1 and vmdk1 are deleted
		13.	Verify cnsvspherevolumemigrations crds are removed for PVC1 and PV1
		14.	Verify CNS entries are removed for PVC1 and PV1
		15.	Delete the SC1
		16.	Disable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)
	*/
	ginkgo.It("Verify volume entry is updated in CNS when PVC is bound to statically created PV in K8s "+
		"(when SPS was down)", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Create VCP SC with ReclaimPolicy=Delete")
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Creating vmdk1 on the shared datastore " + scParams[vcpScParamDatastoreName])
		esxHost := GetAndExpectStringEnvVar(envEsxHostIP)
		vmdk1, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk1)

		ginkgo.By("Creating PV1 with vmdk1")
		pv1 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk1), v1.PersistentVolumeReclaimDelete, nil)
		pv1.Spec.StorageClassName = vcpSc.Name
		pv1, err = client.CoreV1().PersistentVolumes().Create(ctx, pv1, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		ginkgo.By(fmt.Sprintln("Stopping sps on the vCenter host"))
		isSPSserviceStopped = true
		err = invokeVCenterServiceControl(stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("Creating PVC1 with PV1 and VCP SC")
		pvc1 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpSc, nil, "")
		pvc1.Spec.VolumeName = pv1.Name
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc1, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc1)

		ginkgo.By("Waiting for PVC1 and PV1 to bind")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPreMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting sps on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to start", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isSPSserviceStopped = false

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc created before migration")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPreMig)

	})
})

// verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvs verify
// CnsVolumeMetadata and CnsVSphereVolumeMigration crd for given pvcs.
func verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvsWithoutPvc(ctx context.Context,
	client clientset.Interface, namespace string, pvs []*v1.PersistentVolume) {
	for _, pv := range pvs {
		if pv.Spec.ClaimRef != nil {
			continue
		}
		framework.Logf("Processing PV: %s", pv.Name)
		crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, pv.Spec.VsphereVolume.VolumePath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitAndVerifyCnsVolumeMetadata(crd.Spec.VolumeID, nil, pv, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

//updatePvcLabel updates the labels on the given PVC
func updatePvcLabel(ctx context.Context, client clientset.Interface, namespace string,
	pvc *v1.PersistentVolumeClaim, labels map[string]string) *v1.PersistentVolumeClaim {
	pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pvc.Labels = labels
	pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return pvc
}

//updatePvLabel updates the labels on the given PV
func updatePvLabel(ctx context.Context, client clientset.Interface, namespace string,
	pv *v1.PersistentVolume, labels map[string]string) *v1.PersistentVolume {
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv.Labels = labels
	pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return pv
}
