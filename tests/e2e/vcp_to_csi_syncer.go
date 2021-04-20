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
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	"sigs.k8s.io/vsphere-csi-driver/pkg/apis/migration/v1alpha1"
)

var _ = ginkgo.Describe("[csi-vcp-mig] VCP to CSI migration syncer tests", func() {
	f := framework.NewDefaultFramework("vcp-2-csi-syncer")
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
			err = invokeVCenterServiceControl("start", vsanhealthServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}

		if isSPSserviceStopped {
			ginkgo.By(fmt.Sprintln("Starting sps on the vCenter host"))
			err = invokeVCenterServiceControl("start", "sps", vcAddress)
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

	/*
		Verify label updates on dynamically provisioned PV and PVC

		Steps
		1. Create SC1 VCP SC
		2. Create PVC1 using SC1 and wait for binding with PV (say PV1)
		3. Add PVC1 and PV1 labels
		4. Enable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)
		5. Verify the PVC1 and PV1 provisioned in step 2 have the following annotation -  "pv. kubernetes. io/migrated-to": "csi. vsphere. vmware. com"
		6. Verify cnsvspherevolumemigrations crd is created for the migrated volume
		7. wait for labels to be present in CNS for PVC1 and PV1
		8. Create PVC2 using SC1 and wait for binding with PV (say PV2)
		9. Verify cnsvspherevolumemigrations crd is created for PVC2 and PV2
		10. Add PVC2 and PV2 labels
		11. wait for labels to be present in CNS for PVC2 and PV2
		12. Delete PVC1, PVC2, PV1 and PV2 labels
		13. wait for labels to get removed from CNS for PVC1, pVC2, PV1 and PV2
		14. Delete PVC1 and PVC2
		15. wait and verify PVC1, PVC2, PV1 and PV2 entries are deleted in CNS
		16. Verify underlying vmdks are also deleted for PV1 and PV2
		17. Verify cnsvspherevolumemigrations crds are removed for PVC1, PVC2, PV1 and PV2
		18. Delete SC1
		19. Disable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)

		Verify CNS PVC entry for PV with reclaim policy Retain

		Steps:
		1.	Create SC1 VCP SC with reclaim policy Retain
		2.	Create PVC1 using SC1 and wait for binding with PV (say PV1)
		3.	Add PVC1 and PV1 labels
		4.	Enable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)
		5.	Verify the PVC1 and PV1 provisioned in step 2 have the following annotation -  "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com"
		6.	Verify cnsvspherevolumemigrations crd is created for the migrated volume
		7.	wait for labels to be present in CNS for PVC1 and PV1
		8.	Create PVC2 using SC1 and wait for binding with PV (say PV2)
		9.	Add PVC2 and PV2 labels
		10.	wait for labels to be present in CNS for PVC2 and PV2
		11.	Delete PVC1, PVC2, PV1 and PV2 labels
		12.	wait for labels to get removed from CNS for PVC1, pVC2, PV1 and PV2
		13.	Delete PVC1 and PVC2
		14.	wait and verify PVC entries are deleted in CNS for PVC1 and PVC2
		15.	verify PVC name is removed from CNS entries for PV1 and PV2
		16.	note underlying vmdks for PV1 and PV2
		17.	Delete PV1 and PV2
		18.	wait and verify PV entries are deleted in CNS for PV1 and PV2
		19.	Verify cnsvspherevolumemigrations crds are removed for PVC1, PVC2, PV1 and PV2
		20.	Delete underlying vmdks as noted in step 16 for PV1 and PV2
		21.	Delete SC1
		22.	Disable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)
	*/
	ginkgo.It("Label updates on VCP volumes before and after migration", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)
		vcpScRetain, err := createVcpStorageClass(client, scParams, nil, v1.PersistentVolumeReclaimRetain, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpScRetain)

		ginkgo.By("Creating VCP PVC pvc1 before migration")
		pvc1, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc1)

		ginkgo.By("Creating VCP PVC pvcRetain1 before migration")
		pvcRetain1, err := createPVC(client, namespace, nil, "", vcpScRetain, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvcRetain1)

		ginkgo.By("Waiting for all claims created before migration to be in bound state")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPreMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels to '%v' on VCP PV/PVCs before migration", labels))
		for i := 0; i < len(vcpPvcsPreMig); i++ {
			vcpPvcsPreMig[i], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, vcpPvcsPreMig[i].Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPreMig[i].Labels = labels
			vcpPvcsPreMig[i], err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, vcpPvcsPreMig[i], metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		for i := 0; i < len(vcpPvsPreMig); i++ {
			vcpPvsPreMig[i], err = client.CoreV1().PersistentVolumes().Get(ctx, vcpPvsPreMig[i].Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvsPreMig[i].Labels = labels
			vcpPvsPreMig[i], err = client.CoreV1().PersistentVolumes().Update(ctx, vcpPvsPreMig[i], metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, namespace, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc created before migration")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, namespace, vcpPvcsPreMig)

		ginkgo.By("Creating VCP PVC pvc2 post migration")
		pvc2, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc2)

		ginkgo.By("Creating VCP PVC pvcRetain2 post migration")
		pvcRetain2, err := createPVC(client, namespace, nil, "", vcpScRetain, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvcRetain2)

		ginkgo.By("Waiting for all claims created post migration to be in bound state")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Updating labels to '%v' on VCP PV/PVCs post migration", labels))
		for i := 0; i < len(vcpPvcsPostMig); i++ {
			vcpPvcsPostMig[i], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, vcpPvcsPostMig[i].Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPostMig[i].Labels = labels
			vcpPvcsPostMig[i], err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, vcpPvcsPostMig[i], metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		for i := 0; i < len(vcpPvsPostMig); i++ {
			vcpPvsPostMig[i].Labels = labels
			vcpPvsPostMig[i], err = client.CoreV1().PersistentVolumes().Update(ctx, vcpPvsPostMig[i], metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify annotations on PV/PVCs created post migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, namespace, vcpPvcsPostMig, vcpPvsPostMig, false)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, namespace, vcpPvcsPostMig)

	})

	/*
		Verify label updates on statically provisioned PV and PVC post migration

		Steps
		1. Create SC1 VCP SC
		2. Create vmdk1 and vmdk2
		3. Create PV1 using vmdk1 and SC1
		4. Create PVC1 using SC1 and wait for binding with PV1
		5. Add PVC1 and PV1 labels
		6. Enable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)
		7. Verify the PVC1 and PV1 provisioned in step 2 have the following annotation -  "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com"
		8. Verify cnsvspherevolumemigrations crd is created for the migrated volume
		9. wait for labels to be present in CNS for PVC1 and PV1
		10. Create PV2 using vmdk2 and SC1
		11. Create PVC2 using SC1 and wait for binding with PV2
		12. Verify cnsvspherevolumemigrations crd is created for PVC2 and PV2
		13. Add PVC2 and PV2 labels
		14. wait for labels to be present in CNS for PVC2 and PV2
		15. Delete PVC1, PVC2, PV1 and PV2 labels
		16. wait for labels to get removed from CNS for PVC1, pVC2, PV1 and PV2
		17. Delete PVC1 and PVC2
		18. wait and verify PVC1, PVC2, PV1 and PV2 entries are deleted in CNS
		19. Verify cnsvspherevolumemigrations crds are removed for PVC1, PVC2, PV1 and PV2
		20. Verify vmdk1 and vmdk2 are also deleted
		21. Delete SC1
		22. Disable CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager (& restart)
	*/
	ginkgo.It("Label updates on statically provisioned VCP volumes before and after migration", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Creating two vmdk1 on the shared datastore " + scParams[vcpScParamDatastoreName])
		esxHost := GetAndExpectStringEnvVar(envEsxHostIP)
		vmdk1, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk1)

		ginkgo.By("Creating PV1 with vmdk1")
		pv1 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk1), v1.PersistentVolumeReclaimDelete, nil)
		pv1.Spec.StorageClassName = vcpSc.Name
		_, err = client.CoreV1().PersistentVolumes().Create(ctx, pv1, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		ginkgo.By("Creating PVC1 with PV1 and VCP SC")
		pvc1 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpSc, nil, "")
		pvc1.Spec.StorageClassName = &vcpSc.Name
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc1, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc1)

		ginkgo.By("Creating PVC1 with PV1 to bind")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPreMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels to '%v' on VCP PVC PVC1 before migration", labels))
		vcpPvcsPreMig[0], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, vcpPvcsPreMig[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig[0].Labels = labels
		vcpPvcsPreMig[0], err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, vcpPvcsPreMig[0], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vcpPvsPreMig[0].Labels = labels
		vcpPvsPreMig[0], err = client.CoreV1().PersistentVolumes().Update(ctx, vcpPvsPreMig[0], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, namespace, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync to finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on PVC1")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, namespace, vcpPvcsPreMig)

		ginkgo.By("Creating two vmdk2 on the shared datastore " + scParams[vcpScParamDatastoreName])
		vmdk2, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk2)

		ginkgo.By("Creating PV2 with vmdk2")
		pv2 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk2), v1.PersistentVolumeReclaimDelete, nil)
		pv2.Spec.StorageClassName = vcpSc.Name
		_, err = client.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		ginkgo.By("Creating PVC2 with PV2 and VCP SC")
		pvc2 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpSc, nil, "")
		pvc2.Spec.StorageClassName = &vcpSc.Name
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc2)

		ginkgo.By("Creating PVC2 with PV2 to bind")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Updating labels to '%v' on VCP PVC PVC2 after migration", labels))
		vcpPvcsPostMig[0], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, vcpPvcsPostMig[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig[0].Labels = labels
		vcpPvcsPostMig[0], err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, vcpPvcsPostMig[0], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vcpPvsPostMig[0].Labels = labels
		vcpPvsPostMig[0], err = client.CoreV1().PersistentVolumes().Update(ctx, vcpPvsPostMig[0], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify annotations on PV/PVCs created post migration")
		// isMigrated should be true for static vols even if created post migration
		waitForMigAnnotationsPvcPvLists(ctx, client, namespace, vcpPvcsPostMig, vcpPvsPostMig, true)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, namespace, vcpPvcsPostMig)
	})

})

//waitForCnsVSphereVolumeMigrationCrd waits for CnsVSphereVolumeMigration crd to be created for the given volume path
func waitForCnsVSphereVolumeMigrationCrd(ctx context.Context, vpath string) (*v1alpha1.CnsVSphereVolumeMigration, error) {
	var (
		found bool
		crd   *v1alpha1.CnsVSphereVolumeMigration
	)
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		found, crd = getCnsVSphereVolumeMigrationCrd(ctx, vpath)
		return found, nil
	})
	return crd, waitErr
}

//createDir create a directory on the test esx host
func createDir(path string, host string) error {
	sshCmd := fmt.Sprintf("mkdir -p %s", path)
	framework.Logf("Invoking command '%v' on ESX host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host+":22", framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: '%s' on ESX host: %v", sshCmd, err)
	}
	return nil
}

//createVmdk create a vmdk on the host with given size, object type and disk format
func createVmdk(host string, size string, objType string, diskFormat string) (string, error) {
	dsName := GetAndExpectStringEnvVar(envSharedDatastoreName)
	dir := "/vmfs/volumes/" + dsName + "/e2e"
	err := createDir(dir, host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if diskFormat == "" {
		diskFormat = "thin"
	}
	if objType == "" {
		objType = "vsan"
	}
	if size == "" {
		size = "2g"
	}
	rand.Seed(time.Now().UnixNano())
	vmdkPath := fmt.Sprintf("%s/test-%v-%v.vmdk", dir, time.Now().UnixNano(), rand.Intn(1000))
	sshCmd := fmt.Sprintf("vmkfstools -c %s -d %s -W %s %s", size, diskFormat, objType, vmdkPath)
	framework.Logf("Invoking command '%v' on ESX host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host+":22", framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return vmdkPath, fmt.Errorf("couldn't execute command: '%s' on ESX host: %v", sshCmd, err)
	}
	return vmdkPath, nil
}

//createVmdk deletes given vmdk
func deleteVmdk(host string, vmdkPath string) error {
	sshCmd := fmt.Sprintf("rm -f %s", vmdkPath)
	framework.Logf("Invoking command '%v' on ESX host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host+":22", framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: '%s' on ESX host: %v", sshCmd, err)
	}
	return nil
}

//getCanonicalPath return canonical path for the vmdk path
func getCanonicalPath(vmdkPath string) string {
	dsName := GetAndExpectStringEnvVar(envSharedDatastoreName)
	parts := strings.Split(vmdkPath, "/")
	vmDiskPath := "[" + dsName + "] " + parts[len(parts)-2] + "/" + parts[len(parts)-1]
	datastorePathObj := new(object.DatastorePath)
	isSuccess := datastorePathObj.FromString(vmDiskPath)
	gomega.Expect(isSuccess).To(gomega.BeTrue())
	newParts := strings.Split(datastorePathObj.Path, "/")
	return strings.Replace(vmDiskPath, parts[len(parts)-2], newParts[len(newParts)-2], 1)
}

//verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs verify CnsVolumeMetadata and CnsVSphereVolumeMigration crd for given pvcs
func verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx context.Context, client clientset.Interface, namespace string, pvcs []*v1.PersistentVolumeClaim) {
	for _, pvc := range pvcs {
		vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
		framework.Logf("Processing PVC: %s", pvc.Name)
		pv := getPvFromClaim(client, namespace, pvc.Name)
		crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := getPodTryingToUsePvc(ctx, client, namespace, pvc.Name)
		err = waitAndVerifyCnsVolumeMetadata(crd.Spec.VolumeID, pvc, pv, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

//getPodTryingToUsePvc returns the first pod trying to use the PVC from the list (use only for volumes with r*o access)
func getPodTryingToUsePvc(ctx context.Context, c clientset.Interface, namespace string, pvcName string) *v1.Pod {
	pods, err := c.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, pod := range pods.Items {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim.ClaimName == pvcName {
				return &pod
			}
		}
	}
	return nil
}
