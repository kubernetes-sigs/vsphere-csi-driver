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
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
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
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/cnsoperator"
	migrationv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/migration/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var _ = ginkgo.Describe("[csi-vcp-mig] VCP to CSI migration create/delete tests", func() {
	f := framework.NewDefaultFramework("csi-vcp-mig-create-del")
	var (
		client         clientset.Interface
		namespace      string
		nodeList       *v1.NodeList
		vcpScs         []*storagev1.StorageClass
		vcpPvcsPreMig  []*v1.PersistentVolumeClaim
		vcpPvsPreMig   []*v1.PersistentVolume
		vcpPvcsPostMig []*v1.PersistentVolumeClaim
		vcpPvsPostMig  []*v1.PersistentVolume
		err            error
		kcmMigEnabled  bool
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
			pvcsToDelete = append(vcpPvcsPreMig, nil)
		}
		vcpPvcsPreMig = nil
		vcpPvcsPostMig = nil
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

		if kcmMigEnabled {
			err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if vcpScs != nil {
			for _, vcpSc := range vcpScs {
				err := client.StorageV1().StorageClasses().Delete(ctx, vcpSc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			vcpScs = nil
		}
	})

	/*
		Migrate in-tree volumes located on different types of datastores with SC parameters supported by CSI
		Steps:
		1.	Create a SCs compatible with VCP and with parameters supported by CSI with different types of datastores VSAN, VVOL, VMFS, NFS, ISCSI
			a.	SPBM policy name
			b.	datastore
			c.	fstype
		2.	Create 5 PVCs using SCs created in step 1
		3.	Enable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)
		4.	Verify all the PVCs and PVs provisioned in step 2 have the following annotation -  "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com"
		5.	Verify cnsvspherevolumemigrations crd is created for the migrated volumes
		6.	Verify CNS entries for the PV/PVCs
		7.	Delete the PVCs created in step 2
		8.	Verify cnsvspherevolumemigrations crds are deleted
		9.	Verify the CNS volumes(vmdks) are also removed.
		10.	Delete the SCs created in step 1
		11.	Disable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)

		create PVCs from CSI using VCP SC with SC parameters supported by CSI
		Steps:
		1.	Create a SCs compatible with VCP and with parameters supported by CSI with different types of datastores VSAN, VVOL, VMFS, NFS, ISCSI
			a.	SPBM policy name
			b.	datastore
			c.	fstype
		2.	Enable feature gates on kube-controller-manager (& restart)
		3.	Create 5 PVCs using SC created in step 1
		4.	Verify all the 5 PVCs are bound
		5.	Verify that the PVCs and PVs have the following annotation -  "pv.kubernetes.io/provisioned-by: csi.vsphere.vmware.com"
		6.	Verify cnsvspherevolumemigrations crds are created
		7.	Verify CNS entries for the PV/PVCs
		8.	Delete the PVCs created in step 3
		9.	Verify the CNS volumes(fcds) are also removed.
		10.	Delete the SC created in step 1
		11.	Disable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)
	*/
	ginkgo.It("Create in-tree volumes using VCP SC with parameters supported by CSI before and after migration", func() {
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
		delete(scParams, vcpScParamPolicyName)

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
		waitForMigAnnotationsPvcPvLists(ctx, client, namespace, vcpPvcsPreMig, vcpPvsPreMig, true)

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
		waitForMigAnnotationsPvcPvLists(ctx, client, namespace, vcpPvcsPostMig, vcpPvsPostMig, false)

		time.Sleep(30 * time.Second)
		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for all volumes created before and after migration")
		for _, pvc := range append(vcpPvcsPreMig, vcpPvcsPostMig...) {
			vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			pv := getPvFromClaim(client, namespace, pvc.Name)
			log.Info("Processing PVC: " + pvc.Name)
			found, crd := getCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(found).To(gomega.BeTrue())
			verifyCnsVolumeMetadata(crd.Spec.VolumeID, pvc, pv, nil)
		}
	})
})

// waitForMigAnnotationsPvcPvLists waits for the list PVs and PVCs to have migration related annotatations
func waitForMigAnnotationsPvcPvLists(ctx context.Context, c clientset.Interface, namespace string, pvcs []*v1.PersistentVolumeClaim, pvs []*v1.PersistentVolume, isMigratedVol bool) {
	for i := 0; i < len(pvcs); i++ {
		pvc := pvcs[i]
		pvc, err := waitForPvcMigAnnotations(ctx, c, pvc.Name, namespace, isMigratedVol)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcs[i] = pvc
	}
	for i := 0; i < len(pvs); i++ {
		pv := pvs[i]
		pv, err := waitForPvMigAnnotations(ctx, c, pv.Name, isMigratedVol)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvs[i] = pv
	}
}

// waitForPvcMigAnnotations waits for the PVC to have migration related annotatations
func waitForPvcMigAnnotations(ctx context.Context, c clientset.Interface, pvcName string, namespace string, isMigratedVol bool) (*v1.PersistentVolumeClaim, error) {
	var pvc *v1.PersistentVolumeClaim
	var hasMigAnnotations bool
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		hasMigAnnotations, pvc = pvcHasMigAnnotations(ctx, c, pvcName, namespace, isMigratedVol)
		return hasMigAnnotations, nil
	})
	return pvc, waitErr
}

// waitForPvMigAnnotations waits for the PV to have migration related annotatations
func waitForPvMigAnnotations(ctx context.Context, c clientset.Interface, pvName string, isMigratedVol bool) (*v1.PersistentVolume, error) {
	var pv *v1.PersistentVolume
	var hasMigAnnotations bool
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		hasMigAnnotations, pv = pvHasMigAnnotations(ctx, c, pvName, isMigratedVol)
		return hasMigAnnotations, nil
	})
	return pv, waitErr
}

// pvcHasMigAnnotations checks whether specified PVC has migartion related annotatations
func pvcHasMigAnnotations(ctx context.Context, c clientset.Interface, pvcName string, namespace string, isMigratedVol bool) (bool, *v1.PersistentVolumeClaim) {
	pvc, err := c.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("PVC %v", spew.Sdump(pvc))
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
			if k == migratedToAnnotation && v == e2evSphereCSIBlockDriverName {
				isMigratedToCsi = true
			}
		} else {
			if k == pvcAnnotationStorageProvisioner && v == e2evSphereCSIBlockDriverName {
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
		gomega.Expect(isMigratedToCsi).NotTo(gomega.BeTrue(), migratedToAnnotation+" annotation was not expected on PVC "+pvcName)
		if isStorageProvisionerMatching {
			return true, pvc
		}
	}
	return false, pvc
}

// pvHasMigAnnotations checks whether specified PV has migartion related annotatations
func pvHasMigAnnotations(ctx context.Context, c clientset.Interface, pvName string, isMigratedVol bool) (bool, *v1.PersistentVolume) {
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
			if k == migratedToAnnotation && v == e2evSphereCSIBlockDriverName {
				isMigratedToCsi = true
			}
		} else {
			if k == pvAnnotationProvisionedBy && v == e2evSphereCSIBlockDriverName {
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
		gomega.Expect(isMigratedToCsi).NotTo(gomega.BeTrue(), migratedToAnnotation+" annotation was not expected on PV "+pvName)
		if isProvisionedByMatching {
			return true, pv
		}
	}
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
func getCnsVSphereVolumeMigrationCrd(ctx context.Context, volPath string) (bool, *migrationv1alpha1.CnsVSphereVolumeMigration) {
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
	finder := find.NewFinder(e2eVSphere.Client.Client, false)
	dcList := strings.Split(testConfig.Global.Datacenters, ",")
	var datacentersNameList []string
	var datastore *object.Datastore
	for _, dcStr := range dcList {
		dcName := strings.TrimSpace(dcStr)
		if dcName != "" {
			datacentersNameList = append(datacentersNameList, dcName)
		}
	}
	datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
	for _, dcName := range datacentersNameList {
		datacenter, err := finder.Datacenter(ctx, dcName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		finder.SetDatacenter(datacenter)
		datastore, err = getDatastoreByURL(ctx, datastoreURL, datacenter)
		if err == nil {
			break
		}
	}
	gomega.Expect(datastore).NotTo(gomega.BeNil())
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
func waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx context.Context, crd *migrationv1alpha1.CnsVSphereVolumeMigration) error {
	waitErr := wait.PollImmediate(poll, pollTimeoutShort, func() (bool, error) {
		exists, _ := getCnsVSphereVolumeMigrationCrd(ctx, crd.Spec.VolumePath)
		return !exists, nil
	})
	return waitErr
}

// verifyCnsVolumeMetadata verify the pv, pvc, pod infromation on given cns volume
func verifyCnsVolumeMetadata(volumeID string, pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume, pod *v1.Pod) {
	cnsQueryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(cnsQueryResult.Volumes).NotTo(gomega.BeEmpty(), "CNS volume query yielded no results for volume id: "+volumeID)
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
	framework.Logf("Found CNS volume for id %v\n"+spew.Sdump(cnsVolume), volumeID)
	gomega.Expect(cnsVolume.Metadata).NotTo(gomega.BeNil())
	for _, entity := range cnsVolume.Metadata.EntityMetadata {
		entityMetadata := entity.(*cnstypes.CnsKubernetesEntityMetadata)
		if entityMetadata.EntityType == string(cnstypes.CnsKubernetesEntityTypePVC) {
			if verifyPvcEntry {
				pvcEntryFound = true
				gomega.Expect(entityMetadata.EntityName == pvc.Name).To(gomega.BeTrue(), fmt.Sprintf("PVC name '%v' does not match PVC name in metadata '%v', for volume id %v", pvc.Name, entityMetadata.EntityName, volumeID))
				if verifyPvEntry {
					gomega.Expect(entityMetadata.ReferredEntity).NotTo(gomega.BeNil())
					gomega.Expect(entityMetadata.ReferredEntity[0].EntityName == pv.Name).To(gomega.BeTrue(), fmt.Sprintf("PV name '%v' in referred entity does not match PV name '%v', in PVC metadata for volume id %v", entityMetadata.ReferredEntity[0].EntityName, pv.Name, volumeID))
				}
				if pvc.Labels == nil {
					gomega.Expect(entityMetadata.Labels).To(gomega.BeNil())
				} else {
					gomega.Expect(reflect.DeepEqual(entityMetadata.Labels, pvc.Labels)).To(gomega.BeTrue(), fmt.Sprintf("Labels on pvc '%v' are not matching with labels in metadata '%v' for volume id %v", entityMetadata.Labels, pvc.Labels, volumeID))
				}
				gomega.Expect(entityMetadata.Namespace == pvc.Namespace).To(gomega.BeTrue(), fmt.Sprintf("PVC namespace '%v' does not match PVC namespace in pvc metadata '%v', for volume id %v", pvc.Namespace, entityMetadata.Namespace, volumeID))
			}
			continue
		}
		if entityMetadata.EntityType == string(cnstypes.CnsKubernetesEntityTypePV) {
			if verifyPvEntry {
				pvEntryFound = true
				gomega.Expect(entityMetadata.EntityName == pv.Name).To(gomega.BeTrue(), fmt.Sprintf("PV name '%v' does not match PV name in metadata '%v', for volume id %v", pv.Name, entityMetadata.EntityName, volumeID))
				if pv.Labels == nil {
					gomega.Expect(entityMetadata.Labels).To(gomega.BeNil())
				} else {
					gomega.Expect(reflect.DeepEqual(entityMetadata.Labels, pv.Labels)).To(gomega.BeTrue(), fmt.Sprintf("Labels on pv '%v' are not matching with labels in pv metadata '%v' for volume id %v", entityMetadata.Labels, pv.Labels, volumeID))
				}
			}
			continue
		}
		if entityMetadata.EntityType == string(cnstypes.CnsKubernetesEntityTypePOD) {
			if verifyPodEntry {
				podEntryFound = true
				gomega.Expect(entityMetadata.EntityName == pod.Name).To(gomega.BeTrue(), fmt.Sprintf("POD name '%v' does not match POD name in metadata '%v', for volume id %v", pod.Name, entityMetadata.EntityName, volumeID))
				if verifyPvcEntry {
					gomega.Expect(entityMetadata.ReferredEntity).NotTo(gomega.BeNil())
					gomega.Expect(entityMetadata.ReferredEntity[0].EntityName == pvc.Name).To(gomega.BeTrue(), fmt.Sprintf("PVC name '%v' in referred entity does not match PVC name '%v', in PVC metadata for volume id %v", entityMetadata.ReferredEntity[0].EntityName, pvc.Name, volumeID))
					gomega.Expect(entityMetadata.ReferredEntity[0].Namespace == pvc.Namespace).To(gomega.BeTrue(), fmt.Sprintf("PVC namespace '%v' does not match PVC namespace in POD metadata referered entitry, '%v', for volume id %v", pvc.Namespace, entityMetadata.ReferredEntity[0].Namespace, volumeID))
				}
				if pod.Labels == nil {
					gomega.Expect(entityMetadata.Labels).To(gomega.BeNil())
				} else {
					gomega.Expect(reflect.DeepEqual(entityMetadata.Labels, pod.Labels)).To(gomega.BeTrue(), fmt.Sprintf("Labels on pod '%v' are not matching with labels in pod metadata '%v' for volume id %v", entityMetadata.Labels, pod.Labels, volumeID))
				}
				gomega.Expect(entityMetadata.Namespace == pod.Namespace).To(gomega.BeTrue(), fmt.Sprintf("POD namespace '%v' does not match POD namespace in metadata '%v', for volume id %v", pod.Namespace, entityMetadata.Namespace, volumeID))
			}
		}
	}
	gomega.Expect(pvEntryFound == verifyPvEntry).To(gomega.BeTrue())
	gomega.Expect(pvcEntryFound == verifyPvcEntry).To(gomega.BeTrue())
	gomega.Expect(podEntryFound == verifyPodEntry).To(gomega.BeTrue())
}
