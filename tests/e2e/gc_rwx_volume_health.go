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
)

var _ = ginkgo.Describe("File Volume Test volume health plumbing", func() {
	f := framework.NewDefaultFramework("rwx-tkg-health")
	var (
		client                     clientset.Interface
		namespace                  string
		scParameters               map[string]string
		storagePolicyName          string
		volHealthCheck             bool
		isVsanHealthServiceStopped bool
		volumeHealthAnnotation     string = "volumehealth.storage.kubernetes.io/health"
		nonVsanStoragePolicyName   string
		vcAddress                  string
		err                        error
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		// TODO: Read value from command line
		volHealthCheck = true
		isVsanHealthServiceStopped = false
		namespace = getNamespaceToRunTests(f)
		svcClient, svNamespace := getSvcClientAndNamespace()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		nonVsanStoragePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForNonSharedDatastores)
		setResourceQuota(svcClient, svNamespace, rqLimit)
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// reading vc address with port num
		if vcAddress == "" {
			vcAddress, _, err = readVcAddress()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
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
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		dumpSvcNsEventsOnTestFailure(svcClient, csiSystemNamespace)
	})

	/*
	   Annotations should be accessble to all the RWX PVCs which are in bound state.
	   1. Create SC
	   2. Create a PVC with "ReadWriteMany" access mode using the SC from above in GC
	   3. Wait for PVC to be Bound in GC
	   4. Verify if the mapping PVC is also bound in the SV cluster using the volume handler
	   5. Verify CnsVolumeMetadata CRD is created
	   6. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
	   7. Wait for the health status interval time
	   8. Verify health annotation is added on the PVC is accessible
	   9. Delete PVC in GC
	   10. Verify if PVC and PV also deleted in the SV cluster and GC
	   11. Verify CnsVolumeMetadata CRD is deleted
	   12. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify RWX volume health is accessible", func() {
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
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvcNameInSV := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		defer func() {
			if pvclaim != nil {
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
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
		err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, fcdIDInCNS, pvcNameInSV, pvclaim, pv, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Annotations should be accessble to all the RWX PVCs which are in bound state.
	   1. Create a PVC using invalid SC
	   2. PVC should be in pending state
	   3. Verify health annotation is not added on the PVC
	   4. Delete PVC
	*/
	ginkgo.It("Verify volume health annotation is not added for PVC in pending", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = nonVsanStoragePolicyName

		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as the storage policy mentioned in Storage class " +
			"has non-vSAN compliant datastores")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health status to be not added")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx,
			pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEmpty())
	})

	/*
	   Verify RWX volume health is accessible on vsan health service restart
	   1. Create SC
	   2. Create a PVC with "ReadWriteMany" access mode using the SC from above in GC
	   3. Wait for PVC to be Bound in GC
	   4. Verify if the mapping PVC is also bound in the SV cluster using the volume handler
	   5. Verify CnsVolumeMetadata CRD is created
	   6. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
	   7. Bring vsan-health down
	   8. Wait for the health status interval time
	   9. Verify no health annotation is added on the PVC
	   10. Bring vsan-health up
	   11. Wait for the health status interval time
	   12. Verify health annotation which is added on the PVC is accessible
	   13. Delete PVC in GC
	   14. Verify if PVC and PV also deleted in the SV cluster and GC
	   15. Verify CnsVolumeMetadata CRD is deleted
	   16. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify RWX volume health is accessible on vsan health service restart", func() {
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
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvcNameInSV := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		defer func() {
			if pvclaim != nil {
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
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
		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", vsanhealthServiceName))

		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = waitVCenterServiceToBeInState(ctx, vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isVsanHealthServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
			}
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health status to be not added")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx,
			pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, fcdIDInCNS, pvcNameInSV, pvclaim, pv, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
