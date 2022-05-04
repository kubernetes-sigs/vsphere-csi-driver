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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("[csi-vcp-mig] ravi-vcp-velero", func() {
	f := framework.NewDefaultFramework("csi-vcp-mig-create-del")
	var (
		client        clientset.Interface
		namespace     string
		nodeList      *v1.NodeList
		vcpScs        []*storagev1.StorageClass
		vcpPvcsPreMig []*v1.PersistentVolumeClaim
		vcpPvsPreMig  []*v1.PersistentVolume
		//vcpPvcsPostMig []*v1.PersistentVolumeClaim
		//vcpPvsPostMig  []*v1.PersistentVolume
		err           error
		kcmMigEnabled bool
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
		connect(ctx, &e2eVSphere)

		if kcmMigEnabled {
			err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
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

		// ginkgo.By("Creating VCP PVCs after migration")
		// for _, sc := range vcpScs {
		// 	pvc, err := createPVC(client, namespace, nil, "", sc, "")
		// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// 	vcpPvcsPostMig = append(vcpPvcsPostMig, pvc)
		// }
		// ginkgo.By("Waiting for all claims created after migration to be in bound state")
		// vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Verify annotations on PV/PVCs created after migration")
		// waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false)

		// ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for all volumes created " +
		// 	"before and after migration")
		// for _, pvc := range append(vcpPvcsPreMig, vcpPvcsPostMig...) {
		// 	vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
		// 	pv := getPvFromClaim(client, namespace, pvc.Name)
		// 	framework.Logf("Processing PVC: " + pvc.Name)
		// 	crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
		// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// 	err = waitAndVerifyCnsVolumeMetadata(crd.Spec.VolumeID, pvc, pv, nil)
		// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// }
	})
})
