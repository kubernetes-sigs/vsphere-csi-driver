/*
Copyright 2020 The Kubernetes Authors.

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
	"reflect"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	cns "github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator"
	migrationv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/migration/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

var _ = ginkgo.Describe("[csi-vcp-mig] VCP to CSI migration create/delete tests", func() {
	f := framework.NewDefaultFramework("csi-vcp-mig-create-del")
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
			err = invokeVCenterServiceControl("start", vsanhealthServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again",
				vsanHealthServiceWaitTime))
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}

		if isSPSserviceStopped {
			ginkgo.By(fmt.Sprintln("Starting sps on the vCenter host"))
			err = invokeVCenterServiceControl("start", "sps", vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to come up again", vsanHealthServiceWaitTime))
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}

		for _, pvc := range pvcsToDelete {
			vPath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			_, crd := getCnsVSphereVolumeMigrationCrd(ctx, vPath)
			framework.Logf("Deleting PVC %v", pvc.Name)
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if kcmMigEnabled {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(crd.Spec.VolumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			framework.Logf("Waiting for vmdk %v to be deleted", vPath)
			err = waitForVmdkDeletion(ctx, vPath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Waiting for CnsVSphereVolumeMigration crds %v to be deleted", crd.Spec.VolumeID)
			err = waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx, crd)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// TODO: add code for PV/fcd/vmdk cleanup in case of PVs with reclaim policy Retain
		vcpPvsPreMig = []*v1.PersistentVolume{}
		vcpPvsPostMig = []*v1.PersistentVolume{}

		if kcmMigEnabled {
			err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
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

	// Migrate in-tree volumes located on different types of datastores with SC
	// parameters supported by CSI.
	// Steps:
	// 1. Create a SCs compatible with VCP and with parameters supported by CSI
	//    with different types of datastores VSAN, VVOL, VMFS, NFS, ISCSI.
	//    a. SPBM policy name.
	//    b. datastore.
	//    c. fstype.
	// 2. Create 5 PVCs using SCs created in step 1.
	// 3. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 4. Verify all the PVCs and PVs provisioned in step 2 have the following
	//    annotation - "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	// 5. Verify cnsvspherevolumemigrations crd is created for the migrated volumes.
	// 6. Verify CNS entries for the PV/PVCs.
	// 7. Delete the PVCs created in step 2.
	// 8. Verify cnsvspherevolumemigrations crds are deleted.
	// 9. Verify the CNS volumes(vmdks) are also removed.
	// 10. Delete the SCs created in step 1.
	// 11. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// create PVCs from CSI using VCP SC with SC parameters supported by CSI.
	// Steps:
	// 1. Create a SCs compatible with VCP and with parameters supported by CSI
	//    with different types of datastores VSAN, VVOL, VMFS, NFS, ISCSI.
	//    a. SPBM policy name.
	//    b. datastore.
	//    c. fstype.
	// 2. Enable feature gates on kube-controller-manager (& restart).
	// 3. Create 5 PVCs using SC created in step 1.
	// 4. Verify all the 5 PVCs are bound.
	// 5. Verify that the PVCs and PVs have the following annotation -
	//    "pv.kubernetes.io/provisioned-by: csi.vsphere.vmware.com".
	// 6. Verify cnsvspherevolumemigrations crds are created.
	// 7. Verify CNS entries for the PV/PVCs.
	// 8. Delete the PVCs created in step 3.
	// 9. Verify the CNS volumes(fcds) are also removed.
	// 10. Delete the SC created in step 1.
	// 11. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	ginkgo.It("Create volumes using VCP SC with parameters supported by CSI before and after migration", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		log := logger.GetLogger(ctx)
		ginkgo.By("Creating VCP SCs")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)
		delete(scParams, vcpScParamDatastoreName)
		scParams[vcpScParamPolicyName] = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		vcpSc, err = createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)
		scParams[vcpScParamFstype] = ext3FSType
		vcpSc, err = createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Creating VCP PVCs before migration")
		for _, sc := range vcpScs {
			pvc, err := createPVC(client, namespace, nil, "", sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPreMig = append(vcpPvcsPreMig, pvc)
		}

		ginkgo.By("Waiting for all claims created before migration to be in bound state")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPreMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By("Creating VCP PVCs after migration")
		for _, sc := range vcpScs {
			pvc, err := createPVC(client, namespace, nil, "", sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPostMig = append(vcpPvcsPostMig, pvc)
		}
		ginkgo.By("Waiting for all claims created after migration to be in bound state")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify annotations on PV/PVCs created after migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for all volumes created " +
			"before and after migration")
		for _, pvc := range append(vcpPvcsPreMig, vcpPvcsPostMig...) {
			vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			pv := getPvFromClaim(client, namespace, pvc.Name)
			log.Info("Processing PVC: " + pvc.Name)
			crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitAndVerifyCnsVolumeMetadata(crd.Spec.VolumeID, pvc, pv, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	// Migrate in-tree volumes with SC parameters not supported by CSI.
	// Steps:
	// 1. Create a VCP SC with parameters not supported by CSI.
	// 2. Create 5 PVCs using SC created in step 1.
	// 3. Enable feature gates on kube-controller-manager (& restart).
	// 4. Verify all the PVCs and PVs provisioned in step 2 have the following
	//    annotation -  "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	// 5. Verify cnsvspherevolumemigrations crds are created for migrated volumes.
	// 6. Verify CNS entries for the PV/PVCs.
	// 7. Delete the PVCs created in step 2.
	// 8. Verify cnsvspherevolumemigrations crds are deleted.
	// 9. Verify the CNS volumes(vmdks) are also removed.
	// 10. Delete the SC created in step 1.
	// 11. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// create PVCs from CSI using VCP SC with SC parameters not supported by CSI.
	// Steps:
	// 1. Create a VCP SC and with parameters not supported by CSI.
	// 2. Enable feature gates on kube-controller-manager (& restart).
	// 3. Create PVC1 using SC created in step 1.
	// 4. Verify PVC1 is stuck in pending state and verify the error in the events.
	// 5. Delete the PVC1.
	// 6. Delete the SC created in step 1.
	// 7. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	ginkgo.It("Create volumes using VCP SC with parameters not supported by CSI before and after migration", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		log := logger.GetLogger(ctx)
		ginkgo.By("Creating VCP SCs")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		scParams["hostfailurestotolerate"] = "1"
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Creating VCP PVCs before migration")
		for _, sc := range vcpScs {
			pvc, err := createPVC(client, namespace, nil, "", sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPreMig = append(vcpPvcsPreMig, pvc)
		}

		ginkgo.By("Waiting for all claims created before migration to be in bound state")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPreMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By("Creating VCP PVCs after migration")
		pvc, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(30 * time.Second)
		ginkgo.By("Checking for error in events related to pvc " + pvc.Name)
		expectedErrorMsg := "InvalidArgument"
		// Error looks like this:
		//     failed to provision volume with StorageClass "vcp-unsup-sc":
		//     rpc error: code = InvalidArgument desc = Parsing storage class
		//     parameters failed with error: vSphere CSI driver does not support
		//     creating volume using in-tree vSphere volume plugin parameter
		//     key:hostfailurestotolerate-migrationparam, value:1
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvc.Name)}, expectedErrorMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue())

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for all volumes created before migration")
		for _, pvc := range vcpPvcsPreMig {
			vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			pv := getPvFromClaim(client, namespace, pvc.Name)
			log.Info("Processing PVC: " + pvc.Name)
			crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitAndVerifyCnsVolumeMetadata(crd.Spec.VolumeID, pvc, pv, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	// Create a PV from CSI using VCP SC with SC parameters supported by CSI
	// when SPS service is down.
	// 1. Create a VCP SC with parameters supported by CSI.
	// 2. Enable feature gates on kube-controller-manager (& restart).
	// 3. Bring down SPS service.
	// 4. Create 5 PVCs using SC created in step 1.
	// 5. verify PVC is in pending state for a while (say 2mins).
	// 6. Bring up SPS service.
	// 7. Wait and verify all the 5 PVCs are bound.
	// 8. Verify that PVCs and PVs have the following annotation -
	//    "pv. kubernetes. io/provisioned-by: csi. vsphere. vmware. com".
	// 9. Verify cnsvspherevolumemigrations crds are created.
	// 10. Verify CNS entries for the PV/PVCs.
	// 11. Delete the PVCs created in step 4.
	// 12. Verify cnsvspherevolumemigrations crds are removed.
	// 13. Verify the CNS volumes(fcds) are also removed.
	// 14. Delete the SC created in step 1.
	// 15. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// Verify volume entry is deleted from CNS when PV is deleted from K8s (when
	// CNS was down).
	// 1. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 2. Create SC1 VCP SC.
	// 3. Create PVC1 using SC1 and binding with PV (say PV1).
	// 4. Wait for PVC to be in Bound phase.
	// 5. Verify CNS entries for PVC1 and PV1.
	// 6. Stop vsan-health on VC.
	// 7. Delete PVC1.
	// 8. Start vsan-health on VC.
	// 9. Verify PVC1, PV1 and underlying vmdk are deleted.
	// 10. Verify CNS entries are removed for PVC1 and PV1.
	// 11. Delete SC1.
	// 12. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// Verify volume entry is created in CNS when PV is created in K8s (when CNS
	// was down).
	// 1. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 2. Create SC1 VCP SC.
	// 3. Bring down vsan health service.
	// 4. Create PVC1 using SC1.
	// 5. Bring up vsan health service.
	// 6. Check PVC1 is be bound to a PV (say PV1).
	// 7. Verify CNS entries for PVC1 and PV1.
	// 8. Verify cnsvspherevolumemigrations crds are created for PVC1 and PV1.
	// 9. Delete the PVC1.
	// 10. Verify PV1 and underlying vmdk are deleted.
	// 11. Verify cnsvspherevolumemigrations crds are removed for PVC1 and PV1.
	// 12. Delete the SC1.
	// 13. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	ginkgo.It("Create/delete volumes using VCP SC via CSI when SPS/CNS service is down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		log := logger.GetLogger(ctx)
		ginkgo.By("Creating VCP SCs")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Creating PVC2...")
		pvc2, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc2)

		ginkgo.By("Waiting for all claims to be in bound state")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv2 := vcpPvsPostMig[0]

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for PVC2")
		var vpath string
		var crd *migrationv1alpha1.CnsVSphereVolumeMigration

		vpath = getvSphereVolumePathFromClaim(ctx, client, namespace, pvc2.Name)
		log.Info("Processing PVC: " + pvc2.Name)
		var found bool
		found, crd = getCnsVSphereVolumeMigrationCrd(ctx, vpath)
		gomega.Expect(found).To(gomega.BeTrue())
		err = waitAndVerifyCnsVolumeMetadata(crd.Spec.VolumeID, pvc2, pv2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort

		ginkgo.By(fmt.Sprintln("Stopping sps on the vCenter host"))
		isSPSserviceStopped = true
		err = invokeVCenterServiceControl("stop", "sps", vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("Creating PVC1...")
		pvc1, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc1)

		ginkgo.By("Sleeping for a min and verifying PVC1 is still in pending state")
		time.Sleep(1 * time.Minute)

		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc1.Status.Phase == v1.ClaimPending).To(gomega.BeTrue())

		ginkgo.By(fmt.Sprintln("Starting sps on the vCenter host"))
		err = invokeVCenterServiceControl("start", "sps", vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isSPSserviceStopped = false

		ginkgo.By("Waiting for all claims to be in bound state")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl("stop", vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("Creating PVC3...")
		pvc3, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc3)

		ginkgo.By("Deleting PVC2...")
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc2.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Sleeping for a min and verifying PVC3 is still in pending state")
		time.Sleep(1 * time.Minute)

		pvc3, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc3.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc3.Status.Phase == v1.ClaimPending).To(gomega.BeTrue())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl("start", vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanHealthServiceStopped = false

		vcpPvcsPostMig = append([]*v1.PersistentVolumeClaim{}, pvc1, pvc3)
		ginkgo.By("Waiting for all claims to be in bound state")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = e2eVSphere.waitForCNSVolumeToBeDeleted(crd.Spec.VolumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Waiting for vmdk %v to be deleted", vpath)
		err = waitForVmdkDeletion(ctx, vpath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Waiting for CnsVSphereVolumeMigration crds %v to be deleted", crd.Spec.VolumeID)
		err = waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx, crd)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify annotations on PV/PVCs")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes and CnsVSphereVolumeMigration CRDs to get ")
		for _, pvc := range vcpPvcsPostMig {
			vpath = getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			log.Info("Processing PVC: " + pvc.Name)
			pv := getPvFromClaim(client, namespace, pvc.Name)
			var found bool
			found, crd = getCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(found).To(gomega.BeTrue())
			err = waitAndVerifyCnsVolumeMetadata(crd.Spec.VolumeID, pvc, pv, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	// Statically provision VMDK used by a PV provisioned by VCP using CSI.
	// Steps:
	// 1. Create a VCP SC SC1 and with parameters supported by CSI with reclaim
	//    policy retain.
	// 2. Create PVC1 using SC1.
	// 3. verify PVC1 is bound.
	// 4. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 5. Verify PVC1 and PV bound to PVC1(say PV1) have the following
	//    annotation - "pv.kubernetes.io/provisioned-by: csi.vsphere.vmware.com".
	// 6. Verify cnsvspherevolumemigrations crd is created.
	// 7. Verify CNS entries for the PV/PVCs.
	// 8. Note FCD ID of the CNS volume used by PV1.
	// 9. Delete PVC1 and PV1.
	// 10. Verify cnsvspherevolumemigrations crd are deleted.
	// 11. Create CSI SC SC2.
	// 12. Create PV2 statically via CSI with the vmdk leftover by PV1 using
	//     FCD ID noted in step 8 and SC2.
	// 13. Create PVC2 using PV2 and SC2.
	// 14. Verify CNS entries for PVC2 and PV2.
	// 15. Delete PVC2.
	// 16. verify PVC2, PV2 and the cns volume(vmdk) get deleted.
	// 17. Delete SC1 and SC2.
	ginkgo.It("Statically provision VMDK used by a PV provisioned by VCP using CSI", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpScRetain, err := createVcpStorageClass(client, scParams, nil, v1.PersistentVolumeReclaimRetain, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpScRetain)

		ginkgo.By("Creating VCP PVC pvc1 before migration")
		pvc1, err := createPVC(client, namespace, nil, "", vcpScRetain, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc1)

		ginkgo.By("Waiting for all claims created before migration to be in bound state")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPreMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc created before migration")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPreMig)

		vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc1.Name)
		found, crd := getCnsVSphereVolumeMigrationCrd(ctx, vpath)
		gomega.Expect(found).To(gomega.BeTrue())
		fcdID := crd.Spec.VolumeID

		ginkgo.By("Delete PVC1 and PV1")
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc1.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = client.CoreV1().PersistentVolumes().Delete(ctx, vcpPvsPreMig[0].Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{}
		vcpPvsPreMig = []*v1.PersistentVolume{}

		ginkgo.By("Verify cnsvspherevolumemigrations crd are deleted")
		err = waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx, crd)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create CSI SC SC2")
		csiSC, err := createStorageClass(client, nil, nil, v1.PersistentVolumeReclaimDelete,
			storagev1.VolumeBindingImmediate, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PV2 statically via CSI with fcd leftover by PV1 using FCD ID noted earlier and SC2")
		pv := fpv.MakePersistentVolume(fpv.PersistentVolumeConfig{
			NamePrefix: "static-pv-",
			PVSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:           e2evSphereCSIDriverName,
					VolumeHandle:     fcdID,
					FSType:           ext4FSType,
					VolumeAttributes: map[string]string{"type": csiVolAttrVolType},
				},
			},
			ReclaimPolicy: v1.PersistentVolumeReclaimDelete,
			Capacity:      diskSize,
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
		})
		pv.Spec.StorageClassName = csiSC.Name
		pv.Annotations = map[string]string{pvAnnotationProvisionedBy: e2evSphereCSIDriverName}
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isPvDeleted := false
		defer func() {
			if !isPvDeleted {
				err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create PVC2 using PV2 and SC2")
		pvc2 := getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, csiSC, nil, v1.ReadWriteOnce)
		pvc2.Spec.VolumeName = pv.Name
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isPvDeleted = true
		}()

		ginkgo.By("Verify CNS entries for PVC2 and PV2")
		err = waitAndVerifyCnsVolumeMetadata(fcdID, pvc2, pv, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

})

// waitForMigAnnotationsPvcPvLists waits for the list PVs and PVCs to have migration related annotatations
func waitForMigAnnotationsPvcPvLists(ctx context.Context, c clientset.Interface,
	pvcs []*v1.PersistentVolumeClaim, pvs []*v1.PersistentVolume, isMigratedVol bool) {
	for i := 0; i < len(pvcs); i++ {
		pvc := pvcs[i]
		framework.Logf("Checking PVC %v", pvc.Name)
		pvc, err := waitForPvcMigAnnotations(ctx, c, pvc.Name, pvc.Namespace, isMigratedVol)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcs[i] = pvc
	}
	for i := 0; i < len(pvs); i++ {
		pv := pvs[i]
		framework.Logf("Checking PV %v", pv.Name)
		pv, err := waitForPvMigAnnotations(ctx, c, pv.Name, isMigratedVol)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvs[i] = pv
	}
}

// waitForPvcMigAnnotations waits for the PVC to have migration related annotatations
func waitForPvcMigAnnotations(ctx context.Context, c clientset.Interface, pvcName string,
	namespace string, isMigratedVol bool) (*v1.PersistentVolumeClaim, error) {
	var pvc *v1.PersistentVolumeClaim
	var hasMigAnnotations bool
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		hasMigAnnotations, pvc = pvcHasMigAnnotations(ctx, c, pvcName, namespace, isMigratedVol)
		return hasMigAnnotations, nil
	})
	return pvc, waitErr
}

// waitForPvMigAnnotations waits for the PV to have migration related annotatations
func waitForPvMigAnnotations(ctx context.Context, c clientset.Interface, pvName string,
	isMigratedVol bool) (*v1.PersistentVolume, error) {
	var pv *v1.PersistentVolume
	var hasMigAnnotations bool
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		hasMigAnnotations, pv = pvHasMigAnnotations(ctx, c, pvName, isMigratedVol)
		return hasMigAnnotations, nil
	})
	return pv, waitErr
}

// pvcHasMigAnnotations checks whether specified PVC has migartion related annotatations
func pvcHasMigAnnotations(ctx context.Context, c clientset.Interface, pvcName string,
	namespace string, isMigratedVol bool) (bool, *v1.PersistentVolumeClaim) {
	pvc, err := c.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	annotations := pvc.Annotations
	isStorageProvisionerMatching := false
	isMigratedToCsi := false
	gomega.Expect(annotations).NotTo(gomega.BeNil(), "No annotations found on PVC "+pvcName)
	for k, v := range annotations {
		if isMigratedVol {
			if k == pvcAnnotationStorageProvisioner && v == vcpProvisionerName {
				isStorageProvisionerMatching = true
				continue
			}
			if k == migratedToAnnotation && v == e2evSphereCSIDriverName {
				isMigratedToCsi = true
			}
		} else {
			if k == pvcAnnotationStorageProvisioner && v == e2evSphereCSIDriverName {
				isStorageProvisionerMatching = true
				continue
			}
			if k == migratedToAnnotation {
				isMigratedToCsi = true
			}
		}
	}
	if isMigratedVol {
		if isStorageProvisionerMatching && isMigratedToCsi {
			return true, pvc
		}
	} else {
		gomega.Expect(isMigratedToCsi).NotTo(gomega.BeTrue(),
			migratedToAnnotation+" annotation was not expected on PVC "+pvcName)
		if isStorageProvisionerMatching {
			return true, pvc
		}
	}
	framework.Logf(
		"Annotations found on PVC: %v in namespace:%v are,\n%v", pvcName, pvc.Namespace, spew.Sdump(annotations))
	return false, pvc
}

// pvHasMigAnnotations checks whether specified PV has migartion related annotatations
func pvHasMigAnnotations(ctx context.Context, c clientset.Interface,
	pvName string, isMigratedVol bool) (bool, *v1.PersistentVolume) {
	pv, err := c.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	annotations := pv.Annotations
	isProvisionedByMatching := false
	isMigratedToCsi := false
	gomega.Expect(annotations).NotTo(gomega.BeNil(), "No annotations found on PV "+pvName)
	for k, v := range annotations {
		if isMigratedVol {
			if k == pvAnnotationProvisionedBy && v == vcpProvisionerName {
				isProvisionedByMatching = true
				continue
			}
			if k == migratedToAnnotation && v == e2evSphereCSIDriverName {
				isMigratedToCsi = true
			}
		} else {
			if k == pvAnnotationProvisionedBy && v == e2evSphereCSIDriverName {
				isProvisionedByMatching = true
				continue
			}
			if k == migratedToAnnotation {
				isMigratedToCsi = true
			}
		}
	}
	if isMigratedVol {
		if isProvisionedByMatching && isMigratedToCsi {
			return true, pv
		}
	} else {
		gomega.Expect(isMigratedToCsi).NotTo(gomega.BeTrue(),
			migratedToAnnotation+" annotation was not expected on PV "+pvName)
		if isProvisionedByMatching {
			return true, pv
		}
	}
	framework.Logf(
		"Annotations found on PV: %v are,\n%v", pvName, spew.Sdump(annotations))
	return false, pv
}

// getAllCnsVSphereVolumeMigrationCrds fetches all CnsVSphereVolumeMigration crds in the cluster
func getAllCnsVSphereVolumeMigrationCrds(ctx context.Context) *migrationv1alpha1.CnsVSphereVolumeMigrationList {
	var restConfig *rest.Config
	var err error
	if k8senv := GetAndExpectStringEnvVar("KUBECONFIG"); k8senv != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	crds := &migrationv1alpha1.CnsVSphereVolumeMigrationList{}
	err = cnsOperatorClient.List(ctx, crds)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return crds
}

// getCnsVSphereVolumeMigrationCrd fetches CnsVSphereVolumeMigration crd for given volume path
func getCnsVSphereVolumeMigrationCrd(ctx context.Context, volPath string) (bool,
	*migrationv1alpha1.CnsVSphereVolumeMigration) {
	found := false
	var crdP *migrationv1alpha1.CnsVSphereVolumeMigration
	crds := getAllCnsVSphereVolumeMigrationCrds(ctx)
	for _, crd := range crds.Items {
		if crd.Spec.VolumePath == volPath {
			crdP = &crd
			found = true
			break
		}
	}
	return found, crdP
}

// generateNodeMap generates a mapping of kubernetes node to vSphere VM mapping
func generateNodeMap(ctx context.Context, config *e2eTestConfig, vs *vSphere, c clientset.Interface) {
	nodeList, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		framework.Failf("Failed to get nodes: %v", err)
	}

	err = nodeMapper.GenerateNodeMap(vs, *nodeList)
	if err != nil {
		framework.Failf("Failed to genereate node map: %v", err)
	}
}

// fileExists checks whether the specified file exists on the shared datastore
func fileExistsOnSharedDatastore(ctx context.Context, volPath string) (bool, error) {
	datastore := getDefaultDatastore(ctx)
	b, err := datastore.Browser(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	spec := types.HostDatastoreBrowserSearchSpec{
		MatchPattern: []string{"*"},
	}
	task, err := b.SearchDatastore(ctx, volPath, &spec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = task.WaitForResult(ctx, nil)
	if err != nil {
		if types.IsFileNotFound(err) {
			return false, nil
		}
		if strings.Contains(err.Error(), "A specified parameter was not correct: searchSpec") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// waitForVmdkDeletion wait for specified vmdk to get deleted from the datastore
func waitForVmdkDeletion(ctx context.Context, volPath string) error {
	waitErr := wait.PollImmediate(poll, pollTimeoutShort, func() (bool, error) {
		exists, err := fileExistsOnSharedDatastore(ctx, volPath)
		return !exists, err
	})
	return waitErr
}

// waitForCnsVSphereVolumeMigrationCrdToBeDeleted waits for the given CnsVSphereVolumeMigration crd to get deleted
func waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx context.Context,
	crd *migrationv1alpha1.CnsVSphereVolumeMigration) error {
	waitErr := wait.PollImmediate(poll, pollTimeoutShort, func() (bool, error) {
		exists, _ := getCnsVSphereVolumeMigrationCrd(ctx, crd.Spec.VolumePath)
		return !exists, nil
	})
	return waitErr
}

// verifyCnsVolumeMetadata verify the pv, pvc, pod information on given cns volume
func verifyCnsVolumeMetadata(volumeID string, pvc *v1.PersistentVolumeClaim,
	pv *v1.PersistentVolume, pod *v1.Pod) bool {
	refferedEntityCheck := true
	if e2eVSphere.Client.Version == cns.ReleaseVSAN67u3 {
		refferedEntityCheck = false
	}
	cnsQueryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if cnsQueryResult.Volumes == nil || len(cnsQueryResult.Volumes) == 0 {
		framework.Logf("CNS volume query yielded no results for volume id: " + volumeID)
		return false
	}
	cnsVolume := cnsQueryResult.Volumes[0]
	pvcEntryFound := false
	pvEntryFound := false
	podEntryFound := false
	verifyPvEntry := false
	verifyPvcEntry := false
	verifyPodEntry := false
	if pvc != nil {
		verifyPvcEntry = true
	}
	if pv != nil {
		verifyPvEntry = true
	}
	if pod != nil {
		verifyPodEntry = true
	}
	framework.Logf("Found CNS volume with id %v\n"+spew.Sdump(cnsVolume), volumeID)
	gomega.Expect(cnsVolume.Metadata).NotTo(gomega.BeNil())
	for _, entity := range cnsVolume.Metadata.EntityMetadata {
		entityMetadata := entity.(*cnstypes.CnsKubernetesEntityMetadata)
		if entityMetadata.EntityType == string(cnstypes.CnsKubernetesEntityTypePVC) {
			if verifyPvcEntry {
				pvcEntryFound = true
				if entityMetadata.EntityName != pvc.Name {
					framework.Logf("PVC name '%v' does not match PVC name in metadata '%v', for volume id %v",
						pvc.Name, entityMetadata.EntityName, volumeID)
					pvcEntryFound = false
					break
				}
				if verifyPvEntry {
					if refferedEntityCheck {
						if entityMetadata.ReferredEntity == nil {
							framework.Logf("Missing ReferredEntity in PVC entry for volume id %v", volumeID)
							pvcEntryFound = false
							break
						}
						if entityMetadata.ReferredEntity[0].EntityName != pv.Name {
							framework.Logf("PV name '%v' in referred entity does not match PV name '%v', "+
								"in PVC metadata for volume id %v", entityMetadata.ReferredEntity[0].EntityName,
								pv.Name, volumeID)
							pvcEntryFound = false
							break
						}
					}
				}
				if pvc.Labels == nil {
					if entityMetadata.Labels != nil {
						framework.Logf("PVC labels '%v' does not match PVC labels in metadata '%v', for volume id %v",
							pvc.Labels, entityMetadata.Labels, volumeID)
						pvcEntryFound = false
						break
					}
				} else {
					labels := getLabelMap(entityMetadata.Labels)
					if !(reflect.DeepEqual(labels, pvc.Labels)) {
						framework.Logf("Labels on pvc '%v' are not matching with labels in metadata '%v' for volume id %v",
							pvc.Labels, entityMetadata.Labels, volumeID)
						pvcEntryFound = false
						break
					}
				}
				if entityMetadata.Namespace != pvc.Namespace {
					framework.Logf("PVC namespace '%v' does not match PVC namespace in pvc metadata '%v', for volume id %v",
						pvc.Namespace, entityMetadata.Namespace, volumeID)
					pvcEntryFound = false
					break
				}
			}
			continue
		}
		if entityMetadata.EntityType == string(cnstypes.CnsKubernetesEntityTypePV) {
			if verifyPvEntry {
				pvEntryFound = true
				if entityMetadata.EntityName != pv.Name {
					framework.Logf("PV name '%v' does not match PV name in metadata '%v', for volume id %v",
						pv.Name, entityMetadata.EntityName, volumeID)
					pvEntryFound = false
					break
				}
				if pv.Labels == nil {
					if entityMetadata.Labels != nil {
						framework.Logf("PV labels '%v' does not match PV labels in metadata '%v', for volume id %v",
							pv.Labels, entityMetadata.Labels, volumeID)
						pvEntryFound = false
						break
					}
				} else {
					labels := getLabelMap(entityMetadata.Labels)
					if !(reflect.DeepEqual(labels, pv.Labels)) {
						framework.Logf("Labels on pv '%v' are not matching with labels in pv metadata '%v' for volume id %v",
							entityMetadata.Labels, pv.Labels, volumeID)
						pvEntryFound = false
						break
					}
				}
			}
			continue
		}
		if entityMetadata.EntityType == string(cnstypes.CnsKubernetesEntityTypePOD) {
			if verifyPodEntry {
				podEntryFound = true
				if entityMetadata.EntityName != pod.Name {
					framework.Logf("POD name '%v' does not match Pod name in metadata '%v', for volume id %v",
						pod.Name, entityMetadata.EntityName, volumeID)
					podEntryFound = false
					break
				}
				if refferedEntityCheck {
					if verifyPvcEntry {
						if entityMetadata.ReferredEntity == nil {
							framework.Logf("Missing ReferredEntity in pod entry for volume id %v", volumeID)
							podEntryFound = false
							break
						}
						if entityMetadata.ReferredEntity[0].EntityName != pvc.Name {
							framework.Logf("PVC name '%v' in referred entity does not match PVC name '%v', "+
								"in PVC metadata for volume id %v", entityMetadata.ReferredEntity[0].EntityName,
								pvc.Name, volumeID)
							podEntryFound = false
							break
						}
						if entityMetadata.ReferredEntity[0].Namespace != pvc.Namespace {
							framework.Logf("PVC namespace '%v' does not match PVC namespace in Pod metadata "+
								"referered entitry, '%v', for volume id %v",
								pvc.Namespace, entityMetadata.ReferredEntity[0].Namespace, volumeID)
							podEntryFound = false
							break
						}
					}
				}
				if entityMetadata.Namespace != pod.Namespace {
					framework.Logf("Pod namespace '%v' does not match pod namespace in pvc metadata '%v', for volume id %v",
						pod.Namespace, entityMetadata.Namespace, volumeID)
					podEntryFound = false
					break
				}
			}
		}
	}
	framework.Logf("pvEntryFound:%v, verifyPvEntry:%v, pvcEntryFound:%v, verifyPvcEntry:%v, "+
		"podEntryFound:%v, verifyPodEntry:%v", pvEntryFound, verifyPvEntry, pvcEntryFound, verifyPvcEntry,
		podEntryFound, verifyPodEntry)
	return pvEntryFound == verifyPvEntry && pvcEntryFound == verifyPvcEntry && podEntryFound == verifyPodEntry
}

// waitAndVerifyCnsVolumeMetadata verify the pv, pvc, pod information on given cns volume
func waitAndVerifyCnsVolumeMetadata(volumeID string, pvc *v1.PersistentVolumeClaim,
	pv *v1.PersistentVolume, pod *v1.Pod) error {
	waitErr := wait.PollImmediate(poll*5, pollTimeout, func() (bool, error) {
		matches := verifyCnsVolumeMetadata(volumeID, pvc, pv, pod)
		return matches, nil
	})
	return waitErr
}

// getLabelMap converts labels in []types.KeyValue from CNS to map[string]string type as in k8s
func getLabelMap(keyVals []types.KeyValue) map[string]string {
	labels := make(map[string]string)
	for _, keyval := range keyVals {
		labels[keyval.Key] = keyval.Value
	}
	return labels
}
