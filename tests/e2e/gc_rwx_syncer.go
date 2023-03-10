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
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwm-csi-tkg] File Volume Test for label updates", func() {
	f := framework.NewDefaultFramework("rwx-tkg-sync")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                     clientset.Interface
		namespace                  string
		scParameters               map[string]string
		storagePolicyName          string
		volHealthCheck             bool
		isVsanHealthServiceStopped bool
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		// TODO: Read value from command line
		volHealthCheck = false
		isVsanHealthServiceStopped = false
		namespace = getNamespaceToRunTests(f)
		svcClient, svNamespace := getSvcClientAndNamespace()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		setResourceQuota(svcClient, svNamespace, rqLimit)
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		if isVsanHealthServiceStopped {
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}
	})

	/*
		Annotations should be accessble to all the RWX PVCs which are in bound state.
		1. Create SC
		2.Create a PVC with "ReadWriteMany" using the storage policy created above
		3. Wait for PVC to be Bound
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata CRD is created
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Update label for PVC
		9. Wait for a min or two and verify PVC label has been updated on CNS using CNSQuery API
		10. Update label for PV
		11. Wait for a min or two and verify PV label has been updated on CNS using CNSQuery API
		12. Delete label for PV
		13. Wait for a min or two and verify PV label has been deleted on CNS using CNSQuery API
		14. Delete label for PVC
		15. Wait for a min or two and verify PVC label has been deleted on CNS using CNSQuery API
		16. Delete PVC in GC
		17. Verify if PVC and PV also deleted in the SV cluster and GC
		18. Verify CnsVolumeMetadata CRD is deleted
		19. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify RWX volume labels", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var err error
		defaultDatastore = getDefaultDatastore(ctx)
		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvcNameInSV := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		defer func() {
			if pvclaim != nil {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		pv = persistentvolumes[0]
		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", fcdIDInCNS)
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdIDInCNS)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		framework.Logf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints)
		ginkgo.By("Verifying volume type specified in PVC is honored")
		gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(),
			"Volume type is not FILE")
		ginkgo.By("Verifying volume size is honored")
		newSizeInMb := int64(2048)
		gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
			CapacityInMb == newSizeInMb).To(gomega.BeTrue(), "Volume Capaticy is not matching")

		labels := make(map[string]string)
		labels["pvcLableSyncerKey"] = "pvcLableSyncerVal"

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvclaim.Name,
			pvclaim.Namespace))
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvclaim, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvclaim.Name, pvclaim.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, pv, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvLabels := make(map[string]string)
		pvLabels["pvLabelKeySyncer"] = "pv-label-Value"

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s in namespace %s", pvLabels, pv.Name, namespace))
		pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.GetName(), metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv.Labels = pvLabels

		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", pvLabels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS,
			pvLabels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvLabels = make(map[string]string)

		ginkgo.By(fmt.Sprintf("Deleting labels %+v for pv %s in namespace %s", pvLabels, pv.Name, namespace))
		pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.GetName(), metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv.Labels = pvLabels

		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be deleted for pv %s", pvLabels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS,
			pvLabels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels = make(map[string]string)

		ginkgo.By(fmt.Sprintf("Deleting labels %+v for pvc %s in namespace %s", labels, pvclaim.Name,
			pvclaim.Namespace))
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvclaim, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be deleted for pvc %s in namespace %s",
			labels, pvclaim.Name, pvclaim.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Verify Metadata is updated with pod names for volumes
		1. Create a Storage class
		2. Create a PVC with "ReadWriteMany" using the storage policy created above GC
		3. Wait for PVC to be Bound
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata crd
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Create PV2 with "ReadWriteMany" statically backed by the same volumeID (file share) from PV1
		9. Create PVC2 with "ReadWriteMany" which gets bounds to PV2
		10. Wait for PVC2 to be Bound
		11. Verify CnsVolumeMetadata crd
		12. Verify health status of PVC
		13. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		14. Update label for PVC1
		15. Wait for a min or two and verify PVC label has been updated on CNS using CNSQuery API
		16. Update label for PVC2
		17. Wait for a min or two and verify PVC label has been updated on CNS using CNSQuery API
		18. Update label for PVs
		19. Wait for a min or two and verify PVs label has been updated on CNS using CNSQuery API
		20. Create Pod1 using PVC1
		21. Create Pod2 using PVC2
		22. Wait for a min or two and verify Pod names are updated in CNS using CNSQuery API
		23. Verify CnsFileAccessConfig CRD is created
		24. Verify Pods are in the Running phase
		25. Verify ACL net permission set by calling CNSQuery for the file volume
		26. Create a new file (file1.txt) at the mount path from Pod1. Check if the creation succeeds
		27. Create a new file (file2.txt) at the mount path from Pod2. Check if the creation succeeds
		28. Delete all the Pods
		29. Verify CnsFileAccessConfig CRD is deleted
		30. Verify if all the Pods are successfully deleted
		31. Wait for a min or two and verify Pod names are updated in CNS
		32. Delete PVCs in GC
		33. Verify if PVC and PV also deleted in the SV cluster and GC
		34. Verify CnsVolumeMetadata CRD is deleted
		35. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify PVC metadata reflects pods names", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv1 *v1.PersistentVolume
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcUID := string(pvclaim.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvcNameInSV := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			//Add a check to validate CnsVolumeMetadata crd
			verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", fcdIDInCNS)
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdIDInCNS)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		framework.Logf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints)

		ginkgo.By("Verifying volume type specified in PVC is honored")
		gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(),
			"Volume type is not FILE")
		ginkgo.By("Verifying volume size is honored")
		newSizeInMb := int64(2048)
		gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
			CapacityInMb == newSizeInMb).To(gomega.BeTrue(), "Volume Capaticy is not matching")

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, persistentvolumes[0], nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Creating label for PV
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)

		ginkgo.By("Creating the PV in guest cluster")
		pv2 := getPersistentVolumeSpecForRWX(pvcNameInSV, v1.PersistentVolumeReclaimDelete,
			staticPVLabels, diskSize, "", v1.ReadWriteMany)
		pv2, err = client.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PV2")
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating the PVC in guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespace, staticPVLabels, pv2.Name, diskSize)
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PVC2")
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client,
			framework.NewTimeoutContextWithDefaults(), namespace, pv2, pvc2))

		verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvc2, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		labels := make(map[string]string)
		labels["pvcLableSyncerKey"] = "pvcLableSyncerVal"

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvclaim.Name,
			pvclaim.Namespace))
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvclaim, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvclaim.Name, pvclaim.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels2 := make(map[string]string)
		labels2["pvcLableSyncerKey2"] = "pvcLableSyncerVal2"

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc2.Name,
			pvc2.Namespace))
		pvc2, err = client.CoreV1().PersistentVolumeClaims(pvc2.Namespace).Get(ctx, pvc2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc2.Labels = labels2
		_, err = client.CoreV1().PersistentVolumeClaims(pvc2.Namespace).Update(ctx, pvc2, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels2, pvc2.Name, pvc2.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels2,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc2.Name, pvc2.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv1 = persistentvolumes[0]

		pvLabels1 := make(map[string]string)
		pvLabels1["pvLabelKeySyncer1"] = "pv-label-Value1"

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s in namespace %s", pvLabels1, pv1.Name, namespace))
		pv1, err = client.CoreV1().PersistentVolumes().Get(ctx, pv1.GetName(), metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv1.Labels = pvLabels1

		pv1, err = client.CoreV1().PersistentVolumes().Update(ctx, pv1, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", pvLabels1, pv1.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS,
			pvLabels1, string(cnstypes.CnsKubernetesEntityTypePV), pv1.Name, pv1.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvLabels2 := make(map[string]string)
		pvLabels2["pvLabelKeySyncer2"] = "pv-label-Value2"

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s in namespace %s", pvLabels2, pv2.Name, namespace))
		pv2, err = client.CoreV1().PersistentVolumes().Get(ctx, pv2.GetName(), metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv2.Labels = pvLabels2

		pv2, err = client.CoreV1().PersistentVolumes().Update(ctx, pv2, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", pvLabels2, pv2.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS,
			pvLabels2, string(cnstypes.CnsKubernetesEntityTypePV), pv2.Name, pv2.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a Pod to use this PVC
		ginkgo.By("Creating pod to attach PV1 to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+pvcNameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		// Create a Pod to use this PVC
		ginkgo.By("Creating pod2 to attach PV2 to the node")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc2}, false, execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod2 %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod2.Spec.NodeName+"-"+pvcNameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod2.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		gcClusterID := strings.Replace(pvcNameInSV, pvcUID, "", -1)
		framework.Logf("gcClusterId " + gcClusterID)

		pv1UID := string(persistentvolumes[0].UID)
		framework.Logf("PV1 uuid " + pv1UID)

		pv2UID := string(pv2.UID)
		framework.Logf("PV2 uuid " + pv2UID)

		podUID := string(pod.UID)
		framework.Logf("Pod uuid : " + podUID)

		pod2UID := string(pod2.UID)
		framework.Logf("Pod uuid : " + pod2UID)

		//Add a check to validate CnsVolumeMetadata crd
		verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)

		verifyCRDInSupervisorWithWait(ctx, f, gcClusterID+pv1UID, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)
		verifyCRDInSupervisorWithWait(ctx, f, gcClusterID+pv2UID, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pv1UID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv1.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pv2UID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv2.Spec.CSI.VolumeHandle, false, nil, false)

		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+podUID, crdCNSVolumeMetadatas,
			crdVersion, crdGroup, true, pv1.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pod2UID, crdCNSVolumeMetadatas,
			crdVersion, crdGroup, true, pv2.Spec.CSI.VolumeHandle, false, nil, false)

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvclaim.Name, pvclaim.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels2, pvc2.Name, pvc2.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels2,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc2.Name, pvc2.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for pod1 name to appear on  %s to be updated for pvc %s in namespace %s",
			pod.Name, pvclaim.Name, pvclaim.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, nil,
			string(cnstypes.CnsKubernetesEntityTypePOD), pod.Name, pod.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for pod2 name to appear on  %s to be updated for pvc %s in namespace %s",
			pod2.Name, pvc2.Name, pvc2.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, nil,
			string(cnstypes.CnsKubernetesEntityTypePOD), pod2.Name, pod2.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod2.html "}
		output2 := framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output2, "Hello message from Pod2")).NotTo(gomega.BeFalse())

		writeCmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, writeCmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		writeCmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2' > /mnt/volume1/Pod2.html"}
		framework.RunKubectlOrDie(namespace, writeCmd2...)
		output = framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2")).NotTo(gomega.BeFalse())
	})

	/*
		Create a Pod mounted with a PVC while the csi-controller in the Supervisor cluster is down.
		1. Create a SC
		2. Create a PVC with "ReadWriteMany" using the storage policy created above GC
		3. Wait for PVC to be Bound
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata crd
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Bring down csi-controller pod in the SV
		9. Create a Pod with this PVC mounted as a volume
		10. Verify CnsFileAccessConfig CRD is created
		11. Verify Pod is still in the ContainerCreating phase
		12. Bring up csi-controller pod in the SV
		13. Verify Pod is in the Running phase
		14. Verify ACL net permission set by calling CNSQuery for the file volume
		15. Create a file (file1.txt) at the mount path. Check if the creation is successful
		16. Delete Pod
		17. Verify CnsFileAccessConfig CRD is deleted
		18. Verify if all the Pods are successfully deleted
		19. Delete PVC in GC
		20. Verify if PVC and PV also deleted in the SV cluster and GC
		21. Verify CnsVolumeMetadata CRD is deleted
		22. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify Pod mounted with PVC while the csi-controller in SV is down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var isControllerUp = true
		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcUID := string(pvclaim.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvcNameInSV := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			//Add a check to validate CnsVolumeMetadata crd
			verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", fcdIDInCNS)
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdIDInCNS)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		framework.Logf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints)

		ginkgo.By("Verifying volume type specified in PVC is honored")
		gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(),
			"Volume type is not FILE")
		ginkgo.By("Verifying volume size is honored")
		newSizeInMb := int64(2048)
		gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
			CapacityInMb == newSizeInMb).To(gomega.BeTrue(), "Volume Capaticy is not matching")

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Get CSI Controller's replica count from the setup
		deployment, err := svcClient.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount := *deployment.Spec.Replicas

		ginkgo.By("Bring down csi-controller pod in SV")
		isControllerUp = false
		bringDownCsiController(svcClient)
		defer func() {
			if !isControllerUp {
				bringUpCsiController(svcClient, csiReplicaCount)
				isControllerUp = true
			}
		}()

		// Create a Pod to use this PVC
		ginkgo.By("Creating pod to attach PV to the node")
		pod := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		pod.Spec.Containers[0].Image = busyBoxImageOnGcr
		pod, err = client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Pod creation failed")

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+pvcNameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		time.Sleep(oneMinuteWaitTimeInSeconds * time.Second)

		pod, err = client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify Pod is still in ContainerCreating phase")
		gomega.Expect(podContainerCreatingState == pod.Status.ContainerStatuses[0].State.Waiting.Reason).
			To(gomega.BeTrue())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod")
		framework.Logf("Looking for CnsFileAccessConfig CRD %s", pod.Spec.NodeName+"-"+pvcNameInSV)
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Bring up csi-controller pod in SV")
		isControllerUp = true
		bringUpCsiController(svcClient, csiReplicaCount)

		ginkgo.By("Wait for pod to be up and running")
		err = fpod.WaitForPodRunningInNamespace(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, persistentvolumes[0], pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		writeCmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, writeCmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
	})

	/*
		Verify RWX label update on SV csi-controller down
		1. Create SC
		2. Create a PVC with "ReadWriteMany" using the storage policy created above GC
		3. Wait for PVC to be Bound
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata crd
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Bring down csi-controller pod in the SV
		9. Update PV/PVC labels
		10. Verify CnsVolumeMetadata CRDs are updated
		11. Bring up csi-controller pod in the SV
		12. Verify PV and PVC entry is updated in CNS
		13. Delete PVC in GC
		14. Verify if PVC and PV also deleted in the SV cluster and GC
		15. Verify CnsVolumeMetadata CRD is deleted
		16. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify RWX label update on SV csi-controller down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var err error
		var isControllerUp bool
		defaultDatastore = getDefaultDatastore(ctx)
		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvcNameInSV := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		defer func() {
			if pvclaim != nil {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		pv = persistentvolumes[0]
		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", fcdIDInCNS)
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdIDInCNS)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		framework.Logf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints)
		ginkgo.By("Verifying volume type specified in PVC is honored")
		gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(),
			"Volume type is not FILE")
		ginkgo.By("Verifying volume size is honored")
		newSizeInMb := int64(2048)
		gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
			CapacityInMb == newSizeInMb).To(gomega.BeTrue(), "Volume Capaticy is not matching")

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, pv, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Get CSI Controller's replica count from the setup
		deployment, err := svcClient.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount := *deployment.Spec.Replicas

		ginkgo.By("Bring down csi-controller pod in SV")
		isControllerUp = false
		bringDownCsiController(svcClient)
		defer func() {
			if !isControllerUp {
				bringUpCsiController(svcClient, csiReplicaCount)
				isControllerUp = true
			}
		}()

		labels := make(map[string]string)
		labels["pvcLableSyncerKey"] = "pvcLableSyncerVal"

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvclaim.Name,
			pvclaim.Namespace))
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvclaim, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvLabels := make(map[string]string)
		pvLabels["pvLabelKeySyncer"] = "pv-label-Value"

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s in namespace %s", pvLabels, pv.Name, namespace))
		pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.GetName(), metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv.Labels = pvLabels

		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring up csi-controller pod in SV")
		isControllerUp = true
		bringUpCsiController(svcClient, csiReplicaCount)
		time.Sleep(oneMinuteWaitTimeInSeconds * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvclaim.Name, pvclaim.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", pvLabels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS,
			pvLabels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
