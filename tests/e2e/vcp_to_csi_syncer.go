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

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/drain"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/migration/v1alpha1"
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
		kubectlMigEnabled          bool
		isSPSserviceStopped        bool
		isVsanHealthServiceStopped bool
		labelKey                   string
		labelValue                 string
		vmdks                      []string
		pvsToDelete                []*v1.PersistentVolume
		fullSyncWaitTime           int
		podsToDelete               []*v1.Pod
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

		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false)
		kubectlMigEnabled = false

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
		fss.DeleteAllStatefulSets(client, namespace)
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

		for _, pod := range podsToDelete {
			ginkgo.By(fmt.Sprintf("Deleting pod: %s", pod.Name))
			volhandles := []string{}
			for _, vol := range pod.Spec.Volumes {
				pv := getPvFromClaim(client, namespace, vol.PersistentVolumeClaim.ClaimName)
				volhandles = append(volhandles, pv.Spec.CSI.VolumeHandle)

			}
			err = client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volHandle := range volhandles {
				ginkgo.By("Verify volume is detached from the node")
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}

		if kubectlMigEnabled {
			ginkgo.By("Disable CSI migration feature gates on kublets on k8s nodes")
			toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false)
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

	// Verify label updates on dynamically provisioned PV and PVC.
	//
	// Steps
	// 1. Create SC1 VCP SC.
	// 2. Create PVC1 using SC1 and wait for binding with PV (say PV1).
	// 3. Add PVC1 and PV1 labels.
	// 4. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 5. Verify the PVC1 and PV1 provisioned in step 2 have the following
	//    annotation - "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	// 6. Verify cnsvspherevolumemigrations crd is created for the migrated volume.
	// 7. Wait for labels to be present in CNS for PVC1 and PV1.
	// 8. Create PVC2 using SC1 and wait for binding with PV (say PV2).
	// 9. Verify cnsvspherevolumemigrations crd is created for PVC2 and PV2.
	// 10. Add PVC2 and PV2 labels.
	// 11. Wait for labels to be present in CNS for PVC2 and PV2.
	// 12. Delete PVC1, PVC2, PV1 and PV2 labels.
	// 13. Wait for labels to get removed from CNS for PVC1, pVC2, PV1 and PV2.
	// 14. Delete PVC1 and PVC2.
	// 15. Wait and verify PVC1, PVC2, PV1 and PV2 entries are deleted in CNS.
	// 16. Verify underlying vmdks are also deleted for PV1 and PV2.
	// 17. Verify cnsvspherevolumemigrations crds are removed for PVC1, PVC2,
	//     PV1 and PV2.
	// 18. Delete SC1.
	// 19. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	//
	// Verify CNS PVC entry for PV with reclaim policy Retain.
	//
	// Steps:
	// 1. Create SC1 VCP SC with reclaim policy Retain.
	// 2. Create PVC1 using SC1 and wait for binding with PV (say PV1).
	// 3. Add PVC1 and PV1 labels.
	// 4. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 5. Verify the PVC1 and PV1 provisioned in step 2 have the following
	//    annotation - "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	// 6. Verify cnsvspherevolumemigrations crd is created for the migrated volume.
	// 7. Wait for labels to be present in CNS for PVC1 and PV1.
	// 8. Create PVC2 using SC1 and wait for binding with PV (say PV2).
	// 9. Add PVC2 and PV2 labels.
	// 10. Wait for labels to be present in CNS for PVC2 and PV2.
	// 11. Delete PVC1, PVC2, PV1 and PV2 labels.
	// 12. Wait for labels to get removed from CNS for PVC1, pVC2, PV1 and PV2.
	// 13. Delete PVC1 and PVC2.
	// 14. Wait and verify PVC entries are deleted in CNS for PVC1 and PVC2.
	// 15. Verify PVC name is removed from CNS entries for PV1 and PV2.
	// 16. Note underlying vmdks for PV1 and PV2.
	// 17. Delete PV1 and PV2.
	// 18. Wait and verify PV entries are deleted in CNS for PV1 and PV2.
	// 19. Verify cnsvspherevolumemigrations crds are removed for PVC1, PVC2,
	//     PV1 and PV2.
	// 20. Delete underlying vmdks as noted in step 16 for PV1 and PV2.
	// 21. Delete SC1.
	// 22. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
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
			vcpPvcsPreMig[i], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
				vcpPvcsPreMig[i].Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPreMig[i].Labels = labels
			vcpPvcsPreMig[i], err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx,
				vcpPvcsPreMig[i], metav1.UpdateOptions{})
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
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc created before migration")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPreMig)

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
			vcpPvcsPostMig[i], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
				vcpPvcsPostMig[i].Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPostMig[i].Labels = labels
			vcpPvcsPostMig[i], err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx,
				vcpPvcsPostMig[i], metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		for i := 0; i < len(vcpPvsPostMig); i++ {
			vcpPvsPostMig[i].Labels = labels
			vcpPvsPostMig[i], err = client.CoreV1().PersistentVolumes().Update(ctx,
				vcpPvsPostMig[i], metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify annotations on PV/PVCs created post migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPostMig)

	})

	// Verify label updates on statically provisioned PV and PVC post migration.
	//
	// Steps
	// 1. Create SC1 VCP SC.
	// 2. Create vmdk1 and vmdk2.
	// 3. Create PV1 using vmdk1 and SC1.
	// 4. Create PVC1 using SC1 and wait for binding with PV1.
	// 5. Add PVC1 and PV1 labels.
	// 6. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 7. Verify the PVC1 and PV1 provisioned in step 2 have the following
	//    annotation -  "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	// 8. Verify cnsvspherevolumemigrations crd is created for the migrated volume.
	// 9. wait for labels to be present in CNS for PVC1 and PV1.
	// 10. Create PV2 using vmdk2 and SC1.
	// 11. Create PVC2 using SC1 and wait for binding with PV2.
	// 12. Verify cnsvspherevolumemigrations crd is created for PVC2 and PV2.
	// 13. Add PVC2 and PV2 labels.
	// 14. wait for labels to be present in CNS for PVC2 and PV2.
	// 15. Delete PVC1, PVC2, PV1 and PV2 labels.
	// 16. wait for labels to get removed from CNS for PVC1, pVC2, PV1 and PV2.
	// 17. Delete PVC1 and PVC2.
	// 18. wait and verify PVC1, PVC2, PV1 and PV2 entries are deleted in CNS.
	// 19. Verify cnsvspherevolumemigrations crds are removed for PVC1, PVC2,
	//     PV1 and PV2.
	// 20. Verify vmdk1 and vmdk2 are also deleted.
	// 21. Delete SC1.
	// 22. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
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
		vcpPvcsPreMig[0], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
			vcpPvcsPreMig[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig[0].Labels = labels
		vcpPvcsPreMig[0], err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx,
			vcpPvcsPreMig[0], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vcpPvsPreMig[0].Labels = labels
		vcpPvsPreMig[0], err = client.CoreV1().PersistentVolumes().Update(ctx, vcpPvsPreMig[0], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync to finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on PVC1")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPreMig)

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
		vcpPvcsPostMig[0], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
			vcpPvcsPostMig[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig[0].Labels = labels
		vcpPvcsPostMig[0], err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx,
			vcpPvcsPostMig[0], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vcpPvsPostMig[0].Labels = labels
		vcpPvsPostMig[0], err = client.CoreV1().PersistentVolumes().Update(ctx, vcpPvsPostMig[0], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify annotations on PV/PVCs created post migration")
		// isMigrated should be true for static vols even if created post migration
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, true)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPostMig)
	})

	// Verify Pod Name updates on CNS.
	// Steps:
	//
	// 1. Create SC1 VCP SC.
	// 2. Create PVC1 using SC1 and wait for binding with PV (say PV1).
	// 3. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 4. Verify the PVC1 and PV1 provisioned in step 2 have the following
	//    annotation - "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	// 5. Verify cnsvspherevolumemigrations crd is created for the migrated volume.
	// 6. Create PVC2 using SC1 and wait for binding with PV (say PV2).
	// 7. Verify cnsvspherevolumemigrations crd is created for PVC2 and PV2.
	// 8. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Enable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. verify CSI node for the corresponding K8s node has the following
	//       annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 9. Create pod1 using PVC1 and PVC2.
	// 10. Verify pod name in CNS entries for PVC1 and PVC2.
	// 11. Delete pod1 and wait for PVC1 and PVC2 to detach.
	// 12. Verify pod name is removed in CNS entries for PVC1 and PVC2.
	// 13. Delete PVC1 and PVC2.
	// 14. wait and verify CNS volumes are deleted.
	// 15. Verify underlying vmdks are also deleted for PV1 and PV2.
	// 16. Verify cnsvspherevolumemigrations crds are removed for PVC1, PVC2,
	//     PV1 and PV2.
	// 17. Delete SC1.
	// 18. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Disable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. verify CSI node for the corresponding K8s node does not have the
	//       following annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 19. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	ginkgo.It("Verify Pod Name updates on CNS", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Creating VCP PVC pvc1 before migration")
		pvc1, err := createPVC(client, namespace, nil, "", vcpSc, "")
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

		ginkgo.By("Creating VCP PVC pvc2 post migration")
		pvc2, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc2)

		ginkgo.By("Waiting for all claims created post migration to be in bound state")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify annotations on PV/PVCs created post migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPostMig)

		ginkgo.By("Enable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, true)
		kubectlMigEnabled = true

		ginkgo.By("Create pod1 using PVC1 and PVC2")
		pod := createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc1, pvc2})
		podsToDelete = append(podsToDelete, pod)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc1, pvc2})

		ginkgo.By("Delete pod")
		deletePodAndWaitForVolsToDetach(ctx, client, pod)
		podsToDelete = nil

		ginkgo.By("Wait and verify CNS entries for all CNS volumes")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc1, pvc2})

		ginkgo.By("Disable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false)
		kubectlMigEnabled = false

	})

	// Statefulsets label and pod name updates.
	// Steps:
	// 1. Create SC1 VCP SC.
	// 2. Create nginx service.
	// 3. Create nginx statefulset SS1 using SC1 with 3 replicas.
	// 4. Wait for all the replicas to come up.
	// 5. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 6. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Enable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. Verify CSI node for the corresponding K8s node has the following
	//       annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 7. Verify all PV/PVCs used by SS1 and have the following annotation -
	//    "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	// 8. Verify cnsvspherevolumemigrations crd is created for all PV/PVCs used
	//    by SS1.
	// 9. Verify CNS entries are present for all PV/PVCs used by SS1 and all
	//    PVCs have correct pod names.
	// 10. Scale down SS1 to 1 replica.
	// 11. Wait for replicas to die and pvcs to get detached.
	// 12. Verify CNS entries for the detached PVCs have pod names removed.
	// 13. Scale up SS1 replicas to 4 replicas.
	// 14. Wait for all replicas to come up.
	// 15. Verify all PV/PVCs used by SS1 and have the following annotation -
	//     "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com" except for
	//     the 4th one.
	// 16. Verify "pv.kubernetes.io/provisioned-by": "csi.vsphere.vmware.com"
	//     annotation on 4th pvc created post migration will.
	// 17. Verify cnsvspherevolumemigrations crd is created for all PV/PVCs used
	//     by SS1.
	// 18. Verify CNS entries are present for all PV/PVCs used by SS1 and all
	//     PVCs have correct pod names.
	// 19. Scale down SS1 replicas to 0 replicas.
	// 20. Verify CNS entries for the detached PVCs have pod names removed.
	// 21. Delete SS1.
	// 22. Delete nginx service.
	// 23. Delete all PVCs.
	// 24. Wait for PVs and respective vmdks to get deleted.
	// 25. Verify cnsvspherevolumemigrations crds are removed for all PV/PVCs
	//     used by SS1.
	// 26. Verify CNS entries are removed for all PVC used by SS1.
	// 27. Delete SC1.
	// 28. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Disable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. Verify CSI node for the corresponding K8s node does not have the
	//       following annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 29. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	ginkgo.It("Statefulsets label and pod name updates", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		statefulset := GetStatefulSetFromManifest(namespace)
		temp := statefulset.Spec.VolumeClaimTemplates
		temp[0].Annotations[scAnnotation4Statefulset] = vcpSc.Name
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset and waiting for the replicas to be ready")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
		for _, pod := range ssPodsBeforeScaleDown.Items {
			pvs, pvcs := getPvcPvFromPod(ctx, client, namespace, &pod)
			vcpPvcsPreMig = append(vcpPvcsPreMig, pvcs...)
			vcpPvsPreMig = append(vcpPvsPreMig, pvs...)
		}

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc created before migration")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPreMig)

		ginkgo.By("Enable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodesWithWaitForSts(ctx, client, true, []*appsv1.StatefulSet{statefulset})
		kubectlMigEnabled = true

		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown = fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", 1))
		_, scaledownErr := fss.Scale(client, statefulset, 1)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, 1)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == 1).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc after statefulset scale down")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPreMig)

		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", 4))
		_, scaledUpErr := fss.Scale(client, statefulset, 4)
		gomega.Expect(scaledUpErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, 4)
		ssPodsAfterScaleUp := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == 4).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		pod := ssPodsAfterScaleUp.Items[3]
		pvs, pvcs := getPvcPvFromPod(ctx, client, namespace, &pod)
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvcs...)
		vcpPvsPostMig = append(vcpPvsPostMig, pvs...)

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc after statefulset scale down")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPostMig)

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", 0))
		_, scaledownErr2 := fss.Scale(client, statefulset, 0)
		gomega.Expect(scaledownErr2).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, 0)
		ssPodsAfterScaleDown2 := fss.GetPodList(client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown2.Items) == 0).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
		fss.DeleteAllStatefulSets(client, namespace)
		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{}
		vcpPvcsPostMig = []*v1.PersistentVolumeClaim{}

		ginkgo.By("Disable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false)
	})

	// Verify label and pod name updates with Deployment.
	// Steps:
	// 1. Create SC1 VCP SC.
	// 2. Create nginx service.
	// 3. Create PVC1 using SC1 and wait for binding with PV (say PV1).
	// 4. Create nginx deployment DEP1 using PVC1 with 1 replica.
	// 5. Wait for all the replicas to come up.
	// 6. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 7. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Enable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. Verify CSI node for the corresponding K8s node has the following
	//       annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 8. Verify all PVC1 and PV1 and have the following annotation -
	//    "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	// 9. Verify cnsvspherevolumemigrations crd is created for PVC1 and PV1.
	// 10. Verify CNS entries are present for all PVC1 and PV1 and all PVCs has
	//     correct pod names.
	// 11. Create PVC2 using SC1 and wait for binding with PV (say PV2).
	// 12. Verify cnsvspherevolumemigrations crd is created for PVC2 and PV2.
	// 13. Patch DEP1 to use PVC2 as well.
	// 14. Verify CNS entries are present for present for PV2 and PVC2.
	// 15. Verify CNS entries for PVC1 and PVC2 have correct pod names.
	// 16. Scale down DEP1 replicas to 0 replicas and wait for PVC1 and PVC2
	//     to detach.
	// 17. Verify CNS entries for PVC1 and PVC2 have pod names removed.
	// 18. Delete DEP1.
	// 19. Delete nginx service.
	// 20. Delete PVC1 and PVC2.
	// 21. Wait for PV1 and PV2 and respective vmdks to get deleted.
	// 22. Verify cnsvspherevolumemigrations crds are removed for all PV1, PV2,
	//     PVC1 and PVC2.
	// 23. Verify CNS entries are removed for PV1, PV2, PVC1 and PVC2.
	// 24. Delete SC1.
	// 25. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Disable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. Verify CSI node for the corresponding K8s node does not have the
	//       following annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 26. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	ginkgo.It("Verify label and pod name updates with Deployment", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Creating VCP PVC pvc1 before migration")
		pvc1, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc1)

		ginkgo.By("Waiting for all claims created before migration to be in bound state")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPreMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labelsMap := make(map[string]string)
		labelsMap["dep-lkey"] = "lval"
		ginkgo.By("Creating a Deployment using pvc1")
		dep1, err := createDeployment(ctx, client, 1, labelsMap, nil,
			namespace, []*v1.PersistentVolumeClaim{pvc1}, "", false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err := fdep.GetPodsForDeployment(client, dep1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc created before migration")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPreMig)

		ginkgo.By("Enable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, true)
		kubectlMigEnabled = true

		ginkgo.By("Creating VCP PVC pvc2 post migration")
		pvc2, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc2)

		ginkgo.By("Waiting for all claims created post migration to be in bound state")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify annotations on PV/PVCs created post migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPostMig)

		dep1, err = client.AppsV1().Deployments(namespace).Get(ctx, dep1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pods, err = fdep.GetPodsForDeployment(client, dep1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod = pods.Items[0]
		rep := dep1.Spec.Replicas
		*rep = 0
		dep1.Spec.Replicas = rep
		ginkgo.By("Scale down deployment to 0 replica")
		dep1, err = client.AppsV1().Deployments(namespace).Update(ctx, dep1, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodNotFoundInNamespace(client, pod.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaims := []*v1.PersistentVolumeClaim{pvc1, pvc2}
		var volumeMounts = make([]v1.VolumeMount, len(pvclaims))
		var volumes = make([]v1.Volume, len(pvclaims))
		for index, pvclaim := range pvclaims {
			volumename := fmt.Sprintf("volume%v", index+1)
			volumeMounts[index] = v1.VolumeMount{Name: volumename, MountPath: "/mnt/" + volumename}
			volumes[index] = v1.Volume{Name: volumename, VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}
		}
		dep1, err = client.AppsV1().Deployments(namespace).Get(ctx, dep1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dep1.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
		dep1.Spec.Template.Spec.Volumes = volumes
		*rep = 1
		dep1.Spec.Replicas = rep
		ginkgo.By("Update deployment to use pvc1 and pvc2")
		dep1, err = client.AppsV1().Deployments(namespace).Update(ctx, dep1, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fdep.WaitForDeploymentComplete(client, dep1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pods, err = fdep.GetPodsForDeployment(client, dep1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(pods.Items)).NotTo(gomega.BeZero())
		pod = pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc1, pvc2})

		ginkgo.By("Scale down deployment to 0 replica")
		dep1, err = client.AppsV1().Deployments(namespace).Get(ctx, dep1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		*rep = 0
		dep1.Spec.Replicas = rep
		_, err = client.AppsV1().Deployments(namespace).Update(ctx, dep1, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodNotFoundInNamespace(client, pod.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc1, pvc2})

	})

	// Verify in-line volume creation on the migrated node.
	// Steps:
	// 1. Create vmdk1.
	// 2. Create VCP SC1.
	// 3. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 4. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Enable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. Verify CSI node for the corresponding K8s node has the following
	//       annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 5. Create pod1 with inline volume wait for it to reach Running state.
	// 6. Verify vmdk1 is registered as a CNS volume and pod metadata is added
	//    for the CNS volume.
	// 7. Delete pod1.
	// 8. Verify CNS volume for vmdk1 is removed.
	// 9. Delete SC1.
	// 10. Delete vmdk1.
	// 11. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	// 12. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Disable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. Verify CSI node for the corresponding K8s node does not have the
	//       following annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	ginkgo.It("Verify in-line volume creation on the migrated node", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Creating vmdk1 on the shared datastore " + scParams[vcpScParamDatastoreName])
		esxHost := GetAndExpectStringEnvVar(envEsxHostIP)
		vmdk1, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk1)

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Enable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, true)
		kubectlMigEnabled = true

		ginkgo.By("Create pod1 with inline volume and wait for it to reach Running state")
		pod1, err := createPodWithInlineVols(ctx, client, namespace, nil, []string{vmdk1})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podsToDelete = append(podsToDelete, pod1)

		ginkgo.By("Verify vmdk1 is registered as a CNS volume and pod metadata is added for the CNS volume")
		crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, getCanonicalPath(vmdk1))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitAndVerifyCnsVolumeMetadata(crd.Spec.VolumeID, nil, nil, pod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete pod1")
		err = fpod.DeletePodWithWait(client, pod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podsToDelete = []*v1.Pod{}

		ginkgo.By("Verify CNS volume for vmdk1 is removed")
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(crd.Spec.VolumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx, crd)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	// Relocate FCD used by PV provisioned using VCP SC post migration.
	// Steps:
	// 1. Create VCP SC SC1.
	// 2. Create a PVC PVC1 using SC1 wait for it to be bound.
	// 3. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 4. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Enable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. Verify CSI node for the corresponding K8s node has the following
	//       annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 5. Wait for PV1 and PVC1 to get the following annotation -
	//    "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	// 6. Verify cnsvspherevolumemigrations crd is created for the migrated volume.
	// 7. Verify CNS entries for PV1 and PVC1.
	// 8. Relocate FCD used by PV1 to another shared datastore.
	// 9. Verify cnsvspherevolumemigrations crd is updated for the migrated volume.
	// 10. Verify CNS entry for PV1.
	// 11. Create a POD POD1 using PVC1 and wait for it to reach `Running` state.
	// 12. Delete POD1.
	// 13. Delete PVC1.
	// 14. Wait and verify CNS entries for deleted items are gone.
	// 15. Delete SC1.
	// 16. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Disable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. Verify CSI node for the corresponding K8s node does not have the
	//       following annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 17. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	ginkgo.It("Relocate FCD used by PV provisioned using VCP SC post migration", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		destDsURL := os.Getenv(destinationDatastoreURL)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var datacenters []string
		if destDsURL == "" {
			ginkgo.Skip(destinationDatastoreURL + " param missing...")
		}
		dcList := strings.Split(cfg.Global.Datacenters,
			",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		var sourceDatastore, destDatastore *object.Datastore
		for _, dc := range datacenters {
			datacenter, err := finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(datacenter)
			sourceDatastore, err = getDatastoreByURL(ctx, datastoreURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			destDatastore, err = getDatastoreByURL(ctx, destDsURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Creating VCP PVC pvc1 before migration")
		pvc1, err := createPVC(client, namespace, nil, "", vcpSc, "")
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

		ginkgo.By("Enable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, true)
		kubectlMigEnabled = true

		vcpPv, err := client.CoreV1().PersistentVolumes().Get(ctx, vcpPvsPreMig[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volHandle := getVolHandle4Pv(ctx, client, vcpPv)

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeI.D: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("error: QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk is created on the specified datastore")
		if queryResult.Volumes[0].DatastoreUrl != datastoreURL {
			err = fmt.Errorf("disk is created on the wrong datastore")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Relocate volume
		err = e2eVSphere.relocateFCD(ctx, volHandle, sourceDatastore.Reference(), destDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to finish FCD relocation:%s to sync with pandora",
			defaultPandoraSyncWaitTime, volHandle))
		time.Sleep(time.Duration(defaultPandoraSyncWaitTime) * time.Second)

		// verify disk is relocated to the specified destination datastore
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s after relocating the disk", volHandle))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("error: QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk is relocated to the specified datastore")
		if queryResult.Volumes[0].DatastoreUrl != destDsURL {
			err = fmt.Errorf("disk is relocated on the wrong datastore")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create pod1 using PVC1")
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc1})
		podsToDelete = append(podsToDelete, pod)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes")
		err = waitAndVerifyCnsVolumeMetadata(volHandle, pvc1, vcpPv, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete pod")
		deletePodAndWaitForVolsToDetach(ctx, client, pod)
		podsToDelete = nil

		ginkgo.By("Wait and verify CNS entries for all CNS volumes")
		err = waitAndVerifyCnsVolumeMetadata(volHandle, pvc1, vcpPv, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete PVC")
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc1.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{}

		ginkgo.By("Disable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false)
		kubectlMigEnabled = false
	})

	// Vcp to csi mini feature stress.
	// Steps:
	// 1. Create VCP SC SC1.
	// 2. Create a namespace NS1.
	// 3. Create 10 statefulsets with 5 PVCs each using SC1 wait for all replicas
	//    in each of the statefulset to reach 'Running` state.
	// 4. Enable CSIMigration and CSIMigrationvSphere feature gates on
	//    kube-controller-manager (& restart).
	// 5. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Enable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. Verify CSI node for the corresponding K8s node has the following
	//       annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 6. Wait for all PVs and PVCs to get the following annotation -
	//    "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	// 7. Verify cnsvspherevolumemigrations crd is created for all the migrated
	//    volumes.
	// 8. Wait for all replicas in each of the statefulset from step 3 to reach
	//    'Running` state.
	// 9. Create 10 statefulsets with 5 PVCs each using SC1 wait for all replicas
	//    in each of the statefulset to reach 'Running` state.
	// 10. Wait for all PVs and PVCs  created in step 9 to get the following
	//     annotation - "pv.kubernetes.io/provisioned-by": "csi.vsphere.vmware.com".
	// 11. Verify cnsvspherevolumemigrations crd is created for all the migrated
	//     volumes.
	// 12. verify CNS entries for all 100 PV/PVCs (and their entity references).
	// 13. Delete 5 statefulsets each which were created in step 3 and 9.
	// 14. Verify CNS entries for all 50 PVCs (and their entity references)
	//     which were affected by step 13.
	// 15. Delete PVCs associated with statefulsets deleted in step 13.
	// 16. Wait and verify CNS entries for deleted items are gone.
	// 17. Delete namespace NS1.
	// 18. Wait and verify CNS entries for deleted items are gone.
	// 19. Delete SC1.
	// 20. Repeat the following steps for all the nodes in the k8s cluster.
	//    a. Drain and Cordon off the node.
	//    b. Disable CSIMigration and CSIMigrationvSphere feature gates on the
	//       kubelet and Restart kubelet.
	//    c. Verify CSI node for the corresponding K8s node does not have the
	//       following annotation - storage.alpha.kubernetes.io/migrated-plugins.
	//    d. Enable scheduling on the node.
	// 21. Disable CSIMigration and CSIMigrationvSphere feature gates on
	//     kube-controller-manager (& restart).
	ginkgo.It("vcp to csi mini feature stress", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ns, err := framework.CreateTestingNS(f.BaseName, client, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		stss := []*appsv1.StatefulSet{}
		fcdIds := []string{}
		for i := 0; i < 10; i++ {
			statefulset := GetStatefulSetFromManifest(ns.Name)
			temp := statefulset.Spec.VolumeClaimTemplates
			temp[0].Annotations[scAnnotation4Statefulset] = vcpSc.Name
			statefulset.Name = "pre-sts" + strconv.Itoa(i)
			statefulset.Spec.Template.Labels["app"] = statefulset.Name
			statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
			statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
			var replicas int32 = 5
			statefulset.Spec.Replicas = &replicas
			ginkgo.By("Creating statefulset and waiting for the replicas to be ready")
			CreateStatefulSet(ns.Name, statefulset, client)
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
			ssPods := fss.GetPodList(client, statefulset)
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPods.Items) == int(replicas)).To(gomega.BeTrue(),
				fmt.Sprintf("Number of Pods in the statefulset(%v) should match with number of replicas(%v)",
					len(ssPods.Items), int(replicas)))
			for _, pod := range ssPods.Items {
				pvs, pvcs := getPvcPvFromPod(ctx, client, ns.Name, &pod)
				vcpPvcsPreMig = append(vcpPvcsPreMig, pvcs...)
				vcpPvsPreMig = append(vcpPvsPreMig, pvs...)
			}
			stss = append(stss, statefulset)
		}

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Enable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodesWithWaitForSts(ctx, client, true, stss)
		kubectlMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc created before migration")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPreMig)

		stssPostMig := []*appsv1.StatefulSet{}
		for i := 0; i < 10; i++ {
			statefulset := GetStatefulSetFromManifest(ns.Name)
			temp := statefulset.Spec.VolumeClaimTemplates
			temp[0].Annotations[scAnnotation4Statefulset] = vcpSc.Name
			statefulset.Name = "post-sts" + strconv.Itoa(i)
			statefulset.Spec.Template.Labels["app"] = statefulset.Name
			statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
			statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
			var replicas int32 = 5
			statefulset.Spec.Replicas = &replicas
			ginkgo.By("Creating statefulset and waiting for the replicas to be ready")
			CreateStatefulSet(ns.Name, statefulset, client)
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
			ssPods := fss.GetPodList(client, statefulset)
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPods.Items) == int(replicas)).To(gomega.BeTrue(),
				fmt.Sprintf("Number of Pods in the statefulset(%v) should match with number of replicas(%v)",
					len(ssPods.Items), int(replicas)))
			for _, pod := range ssPods.Items {
				pvs, pvcs := getPvcPvFromPod(ctx, client, ns.Name, &pod)
				vcpPvcsPostMig = append(vcpPvcsPostMig, pvcs...)
				vcpPvsPostMig = append(vcpPvsPostMig, pvs...)
			}
			stssPostMig = append(stssPostMig, statefulset)
		}
		ginkgo.By("Verify annotations on PV/PVCs created post migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPostMig)

		for _, pv := range append(vcpPvsPreMig, vcpPvsPostMig...) {
			fcdIds = append(fcdIds, getVolHandle4Pv(ctx, client, pv))
		}

		for i := 0; i < 5; i++ {
			for _, stslist := range [][]*appsv1.StatefulSet{stss, stssPostMig} {
				statefulset := stslist[i]
				ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", 0))
				_, scaledownErr2 := fss.Scale(client, statefulset, 0)
				gomega.Expect(scaledownErr2).NotTo(gomega.HaveOccurred())
				fss.WaitForStatusReadyReplicas(client, statefulset, 0)
				ssPodsAfterScaleDown2 := fss.GetPodList(client, statefulset)
				gomega.Expect(len(ssPodsAfterScaleDown2.Items) == 0).To(gomega.BeTrue(),
					"Number of Pods in the statefulset should match with number of replicas")
				err = client.AppsV1().StatefulSets(ns.Name).Delete(ctx,
					statefulset.Name, metav1.DeleteOptions{OrphanDependents: new(bool)})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client,
			append(vcpPvcsPreMig, vcpPvcsPostMig...))

		ginkgo.By("Delete namespace")
		err = client.CoreV1().Namespaces().Delete(ctx, ns.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify cns volumes for all pvc created before and after migration are removed")
		for _, fcdId := range fcdIds {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{}
		vcpPvcsPostMig = []*v1.PersistentVolumeClaim{}

		ginkgo.By("Disable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false)

	})

})

//waitForCnsVSphereVolumeMigrationCrd waits for CnsVSphereVolumeMigration crd to be created for the given volume path
func waitForCnsVSphereVolumeMigrationCrd(
	ctx context.Context, vpath string, customTimeout ...time.Duration,
) (*v1alpha1.CnsVSphereVolumeMigration, error) {
	var (
		found bool
		crd   *v1alpha1.CnsVSphereVolumeMigration
	)
	timeout := pollTimeout
	if len(customTimeout) != 0 {
		timeout = customTimeout[0]
	}
	waitErr := wait.PollImmediate(poll, timeout, func() (bool, error) {
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

// verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs verify
// CnsVolumeMetadata and CnsVSphereVolumeMigration crd for given pvcs.
func verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx context.Context,
	client clientset.Interface, pvcs []*v1.PersistentVolumeClaim) {
	for _, pvc := range pvcs {
		framework.Logf("Checking PVC %v", pvc.Name)
		vpath := getvSphereVolumePathFromClaim(ctx, client, pvc.Namespace, pvc.Name)
		framework.Logf("Processing PVC: %s", pvc.Name)
		pv := getPvFromClaim(client, pvc.Namespace, pvc.Name)
		crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := getPodTryingToUsePvc(ctx, client, pvc.Namespace, pvc.Name)
		err = waitAndVerifyCnsVolumeMetadata(crd.Spec.VolumeID, pvc, pv, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

//getPodTryingToUsePvc returns the first pod trying to use the PVC from the list (use only for volumes with r*o access)
func getPodTryingToUsePvc(ctx context.Context, c clientset.Interface, namespace string, pvcName string) *v1.Pod {
	pods, err := c.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, pod := range pods.Items {
		framework.Logf("For pod '%s', volumes:\n%s", pod.Name, spew.Sdump(pod.Spec.Volumes))
		for _, volume := range pod.Spec.Volumes {
			if strings.Contains(volume.Name, "kube-api-access") {
				continue
			}
			if volume.VolumeSource.PersistentVolumeClaim == nil &&
				volume.VolumeSource.PersistentVolumeClaim.ClaimName == pvcName {
				return &pod
			}
		}
	}
	return nil
}

//createPodWithMultipleVolsVerifyVolMounts this method creates Pod and verifies VolumeMount
func createPodWithMultipleVolsVerifyVolMounts(ctx context.Context, client clientset.Interface,
	namespace string, pvclaims []*v1.PersistentVolumeClaim) *v1.Pod {
	// Create a Pod to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV(s) to a node")
	pod, err := createPod(client, namespace, nil, pvclaims, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var exists bool
	var vmUUID string

	if vanillaCluster {
		vmUUID = getNodeUUID(client, pod.Spec.NodeName)
	} else if guestCluster {
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
	}

	for _, pvc := range pvclaims {
		volHandle := getVolHandle4VcpPvc(ctx, client, namespace, pvc)

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s, VMUUID : %s",
			volHandle, pod.Spec.NodeName, vmUUID))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
			"Volume is not attached to the node volHandle: %s, vmUUID: %s", volHandle, vmUUID)

		ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
		_, err = framework.LookForStringInPodExec(namespace, pod.Name,
			[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	return pod
}

//getVolHandle4VcpPvc return CNS volume handle for the given PVC
func getVolHandle4VcpPvc(ctx context.Context, client clientset.Interface,
	namespace string, pvc *v1.PersistentVolumeClaim) string {
	vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
	found, crd := getCnsVSphereVolumeMigrationCrd(ctx, vpath)
	gomega.Expect(found).To(gomega.BeTrue())
	return crd.Spec.VolumeID
}

//isVcpPV returns whether true for vcp volume and false for csi, fails for any other type
func isVcpPV(ctx context.Context, c clientset.Interface, pv *v1.PersistentVolume) bool {
	if pv.Spec.CSI != nil {
		return false
	}
	gomega.Expect(pv.Spec.VsphereVolume).NotTo(gomega.BeNil())
	return true
}

//getVolHandle4Pv fetches volume handle for given PV
func getVolHandle4Pv(ctx context.Context, c clientset.Interface, pv *v1.PersistentVolume) string {
	isVcpVol := isVcpPV(ctx, c, pv)
	if isVcpVol {
		found, crd := getCnsVSphereVolumeMigrationCrd(ctx, pv.Spec.VsphereVolume.VolumePath)
		gomega.Expect(found).To(gomega.BeTrue())
		return crd.Spec.VolumeID
	}
	return pv.Spec.CSI.VolumeHandle
}

//deletePodAndWaitForVolsToDetach Delete given pod and wait for its volumes to detach
func deletePodAndWaitForVolsToDetach(ctx context.Context, client clientset.Interface, pod *v1.Pod) {
	ginkgo.By(fmt.Sprintf("Deleting pod: %s", pod.Name))
	volhandles := []string{}
	pod, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		return
	}
	for _, vol := range pod.Spec.Volumes {
		if strings.Contains(vol.Name, "kube-api-access") {
			continue
		}
		pv := getPvFromClaim(client, pod.Namespace, vol.PersistentVolumeClaim.ClaimName)
		volhandles = append(volhandles, getVolHandle4Pv(ctx, client, pv))
	}
	err = fpod.DeletePodWithWait(client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, volHandle := range volhandles {
		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
	}
}

//getPvcsPvsFromPod returns pvcs and pvs inturn used by the pod
func getPvcPvFromPod(ctx context.Context, c clientset.Interface,
	namespace string, pod *v1.Pod) ([]*v1.PersistentVolume, []*v1.PersistentVolumeClaim) {
	vols := pod.Spec.Volumes
	var pvcs []*v1.PersistentVolumeClaim
	var pvs []*v1.PersistentVolume
	var pvcName string

	for _, vol := range vols {
		if vol.VolumeSource.PersistentVolumeClaim != nil {
			pvcName = vol.VolumeSource.PersistentVolumeClaim.ClaimName
		} else {
			continue
		}
		pvc, err := c.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcs = append(pvcs, pvc)
		pv := getPvFromClaim(c, namespace, pvcName)
		pvs = append(pvs, pv)
	}
	return pvs, pvcs
}

//createPodWithInlineVols create a pod with the given volumes (vmdks) inline
func createPodWithInlineVols(ctx context.Context, client clientset.Interface,
	namespace string, nodeSelector map[string]string, vmdks []string) (*v1.Pod, error) {
	pod := fpod.MakePod(namespace, nodeSelector, nil, false, "")
	pod.Spec.Containers[0].Image = busyBoxImageOnGcr
	var volumeMounts = make([]v1.VolumeMount, len(vmdks))
	var volumes = make([]v1.Volume, len(vmdks))
	for index, vmdk := range vmdks {
		volumename := fmt.Sprintf("volume%v", index+1)
		volumeMounts[index] = v1.VolumeMount{Name: volumename, MountPath: "/mnt/" + volumename}
		volumes[index] = v1.Volume{Name: volumename, VolumeSource: v1.VolumeSource{
			VsphereVolume: &v1.VsphereVirtualDiskVolumeSource{VolumePath: getCanonicalPath(vmdk), FSType: ext4FSType}}}
	}
	pod.Spec.Containers[0].VolumeMounts = volumeMounts
	pod.Spec.Volumes = volumes
	pod, err := client.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %v", err)
	}
	// Waiting for pod to be running
	err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Running: %v", pod.Name, err)
	}
	// get fresh pod info
	pod, err = client.CoreV1().Pods(namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %v", err)
	}
	return pod, nil
}

// toggleCSIMigrationFeatureGatesOnK8snodesWithWaitForSts to toggle CSI
// migration feature gates on kublets for worker nodes.
func toggleCSIMigrationFeatureGatesOnK8snodesWithWaitForSts(ctx context.Context,
	client clientset.Interface, shouldEnable bool, stss []*appsv1.StatefulSet) {
	if stss == nil {
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, shouldEnable)
		return
	}
	var err error
	var found bool
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, node := range nodes.Items {
		if strings.Contains(node.Name, "master") || strings.Contains(node.Name, "control") {
			continue
		}
		found = isCSIMigrationFeatureGatesEnabledOnKubelet(ctx, client, node.Name)
		if found == shouldEnable {
			continue
		}
		dh := drain.Helper{
			Ctx:                 ctx,
			Client:              client,
			Force:               true,
			IgnoreAllDaemonSets: true,
			Out:                 ginkgo.GinkgoWriter,
			ErrOut:              ginkgo.GinkgoWriter,
		}
		ginkgo.By("Cordoning of node: " + node.Name)
		err = drain.RunCordonOrUncordon(&dh, &node, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Draining of node: " + node.Name)
		err = drain.RunNodeDrain(&dh, node.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Modifying feature gates in kubelet config yaml of node: " + node.Name)
		nodeIP := getK8sNodeIP(&node)
		toggleCSIMigrationFeatureGatesOnkublet(ctx, client, nodeIP, shouldEnable)
		ginkgo.By("Wait for feature gates update on the k8s CSI node: " + node.Name)
		err = waitForCSIMigrationFeatureGatesToggleOnkublet(ctx, client, node.Name, shouldEnable)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Uncordoning of node: " + node.Name)
		err = drain.RunCordonOrUncordon(&dh, &node, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, ss := range stss {
			fss.WaitForRunningAndReady(client, *ss.Spec.Replicas, ss)
		}
	}
}
