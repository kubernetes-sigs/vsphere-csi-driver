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
	"strings"
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
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/migration/v1alpha1"
)

var _ = ginkgo.Describe("[csi-vcp-mig] VCP to CSI migration attach, detach tests", func() {
	f := framework.NewDefaultFramework("vcp-2-csi-attach-detach")
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
			ginkgo.By(
				fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime),
			)
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
				volhandles = append(volhandles, getVolHandle4Pv(ctx, client, pv))
			}
			err = client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volHandle := range volhandles {
				ginkgo.By("Verify volume is detached from the node")
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(
					gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName),
				)
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

	/*
		Tests:
		1. Pod Creation using dynamic provisioned PVC - when migration is  enabled
		2. Migration enabled - Re-create the POD using dynamically provisioned PVC
		3. Migration enabled - Create POD using Static provisioned PVC, and VMDK  is used
		4. Migration enabled - ReCreate POD using Static provisioned PVC, and VMDK  is used
		7. Migration enabled - Create POD using one Migrated PVC and one newly created PVC on CSI controller
		8. Migration enabled - Create POD using PVC created with in-tree PVC and POD using newly created PVC
		13. Verify the behaviour when migration is Disabled on k8s, and also on the driver.
		14. Migration Enabled - Create Multiple PODS Before migration and verify the POD state After migration and
			Delete the namespace and make sure all POD's PVC's are deleted
		15. Migration is enabled - Delete POD which is using Statically provisioned PVC (VMDK)
		17. Verify creating SC with SPBM Policy name
		18. Verify the behaviour when CSI driver gets restarted in between
		19. Disable migrationVerify POD creation after disabling the migration
		20. Disable migration - Verify POD creation with PVC that is pointing to the Storage class created by CSI
			driver
	*/
	ginkgo.It("Attach detach combined test", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		esxHost := GetAndExpectStringEnvVar(envEsxHostIP)
		spbmPolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)
		vcpScStatic, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpScStatic)

		ginkgo.By("Creating static VCP PVCs before migration")
		vmdk3, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk3)

		pv3 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk3), v1.PersistentVolumeReclaimDelete, nil)
		pv3.Spec.StorageClassName = vcpScStatic.Name
		pv3, err = client.CoreV1().PersistentVolumes().Create(ctx, pv3, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		pvc3 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpScStatic, nil, "")
		pvc3.Spec.VolumeName = pv3.Name
		pvc3, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc3, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, pv3, pvc3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc3)

		vmdk4, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk4)

		pv4 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk4), v1.PersistentVolumeReclaimDelete, nil)
		pv4.Spec.StorageClassName = vcpScStatic.Name
		pv4, err = client.CoreV1().PersistentVolumes().Create(ctx, pv4, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		pvc4 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpScStatic, nil, "")
		pvc4.Spec.VolumeName = pv4.Name
		pvc4, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc4, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, pv4, pvc4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc4)

		vmdk15, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk15)

		pv15 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk15), v1.PersistentVolumeReclaimDelete, nil)
		pv15.Spec.StorageClassName = vcpScStatic.Name
		pv15, err = client.CoreV1().PersistentVolumes().Create(ctx, pv15, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		pvc15 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpScStatic, nil, "")
		pvc15.Spec.VolumeName = pv15.Name
		pvc15, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc15, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, pv15, pvc15)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating dynamic VCP PVCs before migration")
		pvc1, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc1)

		pvc2, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc2)

		pvc7, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc7)

		pvc13, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ns, err := framework.CreateTestingNS(f.BaseName, client, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		f.AddNamespacesToDelete(ns)

		vcpPvcsPreMig2 := []*v1.PersistentVolumeClaim{}
		pvcs14 := make([]*v1.PersistentVolumeClaim, 5)
		for i := 0; i < 5; i++ {
			pvcs14[i], err = createPVC(client, ns.Name, nil, "", vcpSc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vcpPvcsPreMig2 = append(vcpPvcsPreMig2, pvcs14[i])
		}

		ginkgo.By("Creating VCP SC with SPBM policy")
		scParams2 := make(map[string]string)
		scParams2[vcpScParamPolicyName] = spbmPolicyName
		vcpSc2, err := createVcpStorageClass(client, scParams2, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc2)

		pvc17, err := createPVC(client, namespace, nil, "", vcpSc2, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc17)

		pvc18, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc18)

		pvc19, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig2 = append(vcpPvcsPreMig2, pvc19)

		pvc20, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig2 = append(vcpPvcsPreMig2, pvc20)

		ginkgo.By("Waiting for all dynamic claims created before migration to be in bound state")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(
			client, append(vcpPvcsPreMig, vcpPvcsPreMig2...), framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fpv.WaitForPVClaimBoundPhase(
			client, []*v1.PersistentVolumeClaim{pvc13}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating standalone pods using VCP PVCs before migration")
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc2})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc4})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc18})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc19})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc20})
		for i := 0; i < 5; i++ {
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvcs14[i]})
		}

		pod13, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc13}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod15, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc15}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_ = createMultiplePods(ctx, client, pvclaims2d, false)
		pvclaims2d = [][]*v1.PersistentVolumeClaim{}

		ginkgo.By("Verify CnsVSphereVolumeMigration crd is not created for pvc used in test13")
		vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc13.Name)
		_, err = waitForCnsVSphereVolumeMigrationCrd(ctx, vpath, pollTimeoutShort)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Delete pod created for test13")
		err = fpod.DeletePodWithWait(client, pod13)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete pvc created for test13")
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc13.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = waitForVmdkDeletion(ctx, vpath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, append(vcpPvcsPreMig, vcpPvcsPreMig2...), vcpPvsPreMig, true)
		waitForMigAnnotationsPvcPvLists(
			ctx, client, []*v1.PersistentVolumeClaim{pvc15}, []*v1.PersistentVolume{pv15}, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc created before migration")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(
			ctx, client, append(vcpPvcsPreMig, vcpPvcsPreMig2...))
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, []*v1.PersistentVolumeClaim{pvc15})

		ginkgo.By("Delete pod created for test15")
		err = fpod.DeletePodWithWait(client, pod15)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete pvc created for test15")
		vpath = getvSphereVolumePathFromClaim(ctx, client, namespace, pvc15.Name)
		crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc15.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = e2eVSphere.waitForCNSVolumeToBeDeleted(crd.Spec.VolumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx, crd)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = waitForVmdkDeletion(ctx, vpath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, true)
		kubectlMigEnabled = true

		ginkgo.By("Creating VCP SC post migration")
		vcpScPost, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpScPost)

		ginkgo.By("Creating VCP PVCs post migration")
		pvc7post, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc7post)

		pvc8post, err := createPVC(client, namespace, nil, "", vcpScPost, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc8post)

		ginkgo.By("Waiting for all claims created post migration to be in bound state")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify annotations on PV/PVCs created post migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration along with their " +
			"respective CnsVSphereVolumeMigration CRDs",
		)
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPostMig)

		ginkgo.By("Creating standalone pods using VCP PVCs post migration")
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc1})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc2})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc3})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc4})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc7, pvc7post})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc8post})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc17})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc19})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc20})
		for i := 0; i < 5; i++ {
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvcs14[i]})
		}

		podsToDelete = createMultiplePods(ctx, client, pvclaims2d, true)
		pvclaims2d = [][]*v1.PersistentVolumeClaim{}

		pod18 := createPodWithMultipleVolsVerifyVolMounts(
			ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc18},
		)

		ginkgo.By("Delete pod created for test18")
		deletePodAndWaitForVolsToDetach(ctx, client, pod18)

		ginkgo.By("Restart CSI driver")
		framework.Logf("Stopping CSI driver")
		err = updateDeploymentReplicawithWait(client, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Starting CSI driver")
		err = updateDeploymentReplicawithWait(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Re-create pod for test18")
		podsToDelete = append(
			podsToDelete,
			createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc18}),
		)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(
			ctx, client, append(append(vcpPvcsPreMig, vcpPvcsPreMig2...), vcpPvcsPostMig...),
		)

		ginkgo.By("Delete pods")
		deletePodsAndWaitForVolsToDetach(ctx, client, podsToDelete, true)
		podsToDelete = []*v1.Pod{}

		volIdsToWaitForDeletion := fetchCnsVolID4VcpPvcs(
			ctx, client, append(append(vcpPvcsPreMig, pvcs14...), vcpPvcsPostMig...))

		vmdkToWaitForDeletion := []string{}
		ginkgo.By("Delete namespace created for test14")
		for _, pvc := range pvcs14 {
			pv := getPvFromClaim(client, ns.Name, pvc.Name)
			vmdkToWaitForDeletion = append(vmdkToWaitForDeletion, pv.Spec.VsphereVolume.VolumePath)
		}
		err = client.CoreV1().Namespaces().Delete(ctx, ns.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete pvcs")
		for _, pvc := range append(vcpPvcsPreMig, vcpPvcsPostMig...) {
			pv := getPvFromClaim(client, namespace, pvc.Name)
			framework.Logf("Deleting PVC %v", pvc.Name)
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmdkToWaitForDeletion = append(vmdkToWaitForDeletion, pv.Spec.VsphereVolume.VolumePath)
		}

		ginkgo.By("Wait for CNS volumes for VCP PVCs to be deleted")
		for _, volId := range volIdsToWaitForDeletion {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Wait for vmdks used by VCP PVCs to be deleted")
		for _, vmdk := range vmdkToWaitForDeletion {
			err = waitForVmdkDeletion(ctx, vmdk)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		vmdkToWaitForDeletion = []string{}
		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{}
		vcpPvcsPostMig = []*v1.PersistentVolumeClaim{}

		ginkgo.By("Disable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false)
		kubectlMigEnabled = false

		ginkgo.By("Disable CSI migration feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = false

		ginkgo.By("Creating pvc post reset")
		pvc20reset, err := createPVC(client, namespace, nil, "", vcpScPost, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all claims created post reset to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(
			client, []*v1.PersistentVolumeClaim{pvc20reset}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating pods post reset")
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc19})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc20})
		pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc20reset})
		podsToDelete = createMultiplePods(ctx, client, pvclaims2d, false)

		ginkgo.By("Deleting pods created post reset")
		deletePodsAndWaitForVolsToDetach(ctx, client, podsToDelete, false)
		podsToDelete = []*v1.Pod{}

		ginkgo.By("Delete VCP PVCs post reset")
		for _, pvc := range []*v1.PersistentVolumeClaim{pvc20reset, pvc19, pvc20} {
			pv := getPvFromClaim(client, namespace, pvc.Name)
			vmdkToWaitForDeletion = append(vmdkToWaitForDeletion, pv.Spec.VsphereVolume.VolumePath)
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Wait for vmdks used by VCP PVCs to be deleted")
		for _, vmdk := range vmdkToWaitForDeletion {
			err = waitForVmdkDeletion(ctx, vmdk)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})
})

//fetchCnsVolID4VcpPvcs return the CNS volume id for the given VCP PVCs
func fetchCnsVolID4VcpPvcs(ctx context.Context, c clientset.Interface, pvcs []*v1.PersistentVolumeClaim) []string {
	volIds := []string{}
	for _, pvc := range pvcs {
		vpath := getvSphereVolumePathFromClaim(ctx, c, pvc.Namespace, pvc.Name)
		crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volIds = append(volIds, crd.Spec.VolumeID)
	}
	return volIds
}

//createMultiplePods creates multiple pods with given 2-dimensional array of pvcs and verifies volume mounts if needed
func createMultiplePods(ctx context.Context, client clientset.Interface,
	pvclaims2d [][]*v1.PersistentVolumeClaim, verifyAttachment bool,
) []*v1.Pod {
	pods := []*v1.Pod{}
	var err error
	var exists bool
	var vmUUID string
	for _, pvcs := range pvclaims2d {
		if len(pvcs) != 0 {
			pod := fpod.MakePod(pvcs[0].Namespace, nil, pvcs, false, execCommand)
			pod.Spec.Containers[0].Image = busyBoxImageOnGcr
			pod, err := client.CoreV1().Pods(pvcs[0].Namespace).Create(ctx, pod, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pods = append(pods, pod)
		}
	}

	for i, pod := range pods {
		// Waiting for pod to be running.
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, pod.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Get fresh pod info.
		pods[i], err = client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod = pods[i]
		for _, pvc := range pvclaims2d[i] {
			if verifyAttachment {
				if vanillaCluster {
					vmUUID = getNodeUUID(client, pod.Spec.NodeName)
				} else if guestCluster {
					vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else {
					annotations := pod.Annotations
					vmUUID, exists = annotations[vmUUIDLabel]
					gomega.Expect(exists).To(
						gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel),
					)
				}
				volHandle := getVolHandle4Pvc(ctx, client, pvc)
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s, VMUUID : %s",
					volHandle, pod.Spec.NodeName, vmUUID))
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
					"Volume is not attached to the node volHandle: %s, vmUUID: %s", volHandle, vmUUID)
			}
			ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
			_, err = framework.LookForStringInPodExec(pvc.Namespace, pod.Name,
				[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
	return pods
}

//getVolHandle4Pv fetches volume handle for given PVC
func getVolHandle4Pvc(ctx context.Context, client clientset.Interface, pvc *v1.PersistentVolumeClaim) string {
	pv := getPvFromClaim(client, pvc.Namespace, pvc.Name)
	return getVolHandle4Pv(ctx, client, pv)
}

//deletePodsAndWaitForVolsToDetach Delete given pod and wait for its volumes to detach
func deletePodsAndWaitForVolsToDetach(
	ctx context.Context, client clientset.Interface, pods []*v1.Pod, verifyDetachment bool,
) {
	volhandles2d := [][]string{}
	if verifyDetachment {
		for _, pod := range pods {
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
			volhandles2d = append(volhandles2d, volhandles)
		}
	}
	for _, pod := range pods {
		framework.Logf("Deleting pod: %s", pod.Name)
		fpod.DeletePodOrFail(client, pod.Namespace, pod.Name)
	}
	for _, pod := range pods {
		err := fpod.WaitForPodNotFoundInNamespace(client, pod.Name, pod.Namespace, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	if verifyDetachment {
		for i, volhandles := range volhandles2d {
			for _, volHandle := range volhandles {
				ginkgo.By("Verify volume is detached from the node")
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, volHandle, pods[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pods[i].Spec.NodeName))
			}
		}
	}
}
