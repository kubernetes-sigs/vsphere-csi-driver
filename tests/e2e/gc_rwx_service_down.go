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
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("File Volume Test on Service down", func() {
	f := framework.NewDefaultFramework("rwx-tkg-service-down")
	var (
		client               clientset.Interface
		namespace            string
		scParameters         map[string]string
		storagePolicyName    string
		volHealthCheck       bool
		isVsanServiceStopped bool
		fullSyncWaitTime     int
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		// TODO: Read value from command line
		volHealthCheck = false
		isVsanServiceStopped = false
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

	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)

		if isVsanServiceStopped {
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
			err := invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		Verify create and delete a PVC while CNS is down
		1. Create a Storage class
		2. Stop vsan-health
		3. Create a PVC with "ReadWriteMany" using the storage policy created above GC
		4. Verify CnsVolumeMetadata CRD is created
		5. Check PVC status is pending both in GC and SV cluster
		6. Start vsan-health
		7. Verify PVC is bound in GC and SV cluster
		8. Verify health status of PVC
		9. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		10. Stop vsan-health
		11. Delete PVC in GC
		12. Verify PV status is in GC is deleted and PV status on SV is released
		13. Verify CnsVolumeMetadata CRD is deleted
		14. Start vsan-health
		15. Verify PV is deleted from GC
		16. Verify PV and PVC are deleted from SV cluster
		17. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify create a PVC while CNS is down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pvclaim *v1.PersistentVolumeClaim
		var err error
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")

		ginkgo.By("Creating StorageClass")
		scParameters[svStorageClassName] = storagePolicyName
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		storageclass, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", vsanhealthServiceName))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		isVsanServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isVsanServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
				err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isVsanServiceStopped = false
			}
		}()

		ginkgo.By("Creating PVC")
		pvclaim, err = fpv.CreatePVC(client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, v1.ReadWriteMany))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if pvclaim != nil {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
				if !apierrors.IsNotFound(err) {
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanServiceStopped = false

		pvArray, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcNameInSV := pvArray[0].Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		svcPV := getPvFromSupervisorCluster(pvcNameInSV)

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

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

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, pvArray[0], nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", vsanhealthServiceName))
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		isVsanServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isVsanServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
				err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isVsanServiceStopped = false
			}
		}()

		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Check SV PV exists and is released")
		_, err = waitForPvToBeReleased(ctx, svcClient, svcPV.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Add a check to validate CnsVolumeMetadata crd
		verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanServiceStopped = false

		err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Verify label update a PVC while CNS is down
		1. Create a Storage class
		2. Create a PVC with "ReadWriteMany" using the storage policy created above
		3. Wait for PVC to be Bound
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata CRD is created
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Stop vsan-health service on VC
		9. Update label for PVC
		10. Verify CnsVolumeMetadata CRD is updated
		11. Start vsan-health service on VC
		12. Verify PVC label has been updated on CNS using CNSQuery API
		13. Stop vsan-health service on VC
		14. Update label for PV
		15. Verify CnsVolumeMetadata CRD is updated
		16. Start vsan-health service on VC
		17. Verify PV label has been updated on CNS using CNSQuery API
		18. Stop vsan-health service on VC
		19. Delete label for PV
		20. Start vsan-health service on VC
		21. Verify PV label has been deleted using CNSQuery API
		22. Stop vsan-health service on VC
		23. Delete label for PVC
		24. Start vsan-health service on VC
		25. Verify PVC label has been deleted using CNSQuery API
		26. Delete PVC in GC
		27. Verify if PVC and PV also deleted in the SV cluster and GC
		28. Verify CnsVolumeMetadata CRD is deleted
		29. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify label update a PVC while CNS is down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pv *v1.PersistentVolume
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		sc, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcUID := string(pvclaim.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
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

		pv = persistentvolumes[0]
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

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", vsanhealthServiceName))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		isVsanServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isVsanServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
				err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isVsanServiceStopped = false
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

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanServiceStopped = false

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvclaim.Name, pvclaim.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", vsanhealthServiceName))
		isVsanServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isVsanServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
				err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isVsanServiceStopped = false
			}
		}()

		pvLabels := make(map[string]string)
		pvLabels["pvLabelKeySyncer"] = "pv-label-Value"

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s in namespace %s", pvLabels, pv.Name, namespace))
		pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.GetName(), metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv.Labels = pvLabels

		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanServiceStopped = false

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", pvLabels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS,
			pvLabels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", vsanhealthServiceName))
		isVsanServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isVsanServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
				err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isVsanServiceStopped = false
			}
		}()

		pvLabels = make(map[string]string)

		ginkgo.By(fmt.Sprintf("Deleting labels %+v for pv %s in namespace %s", pvLabels, pv.Name, namespace))
		pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.GetName(), metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv.Labels = pvLabels

		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanServiceStopped = false

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be deleted for pv %s", pvLabels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS,
			pvLabels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", vsanhealthServiceName))
		isVsanServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isVsanServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
				err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isVsanServiceStopped = false
			}
		}()

		labels = make(map[string]string)

		ginkgo.By(fmt.Sprintf("Deleting labels %+v for pvc %s in namespace %s", labels, pvclaim.Name,
			pvclaim.Namespace))
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvclaim, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanServiceStopped = false

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be deleted for pvc %s in namespace %s",
			labels, pvclaim.Name, pvclaim.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Verify delete volumes when csi connection to sv is broken
		1. Create a Storage class
		2. Create a PVC with "ReadWriteMany" using the storage policy created above GC
		3. Wait for PVC to be Bound
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata crd
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Change reclaimPolicy of PV to Retain
		9. Break connection to SV (Bring down the csi controller in GC)
		10. Delete PVC
		11. Delete PV
		12. Re-establish connection to SV
		13. Sleep double the Full Sync interval
		14. Verify CnsVolumeMetadata CRDs are deleted
		15. Verify entry is updated in CNS
		16. Delete PVC in the SV
		17. Verify PV also deleted in the SV
		18. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify delete volumes when csi connection to sv is broken", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pv *v1.PersistentVolume
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var isControllerUp bool
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		sc, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcUID := string(pvclaim.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
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

				//Add a check to validate CnsVolumeMetadata crd
				verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
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
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, persistentvolumes[0], nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Changing the reclaim policy of the pv to retain.
		ginkgo.By("Changing the volume reclaim policy")
		persistentvolumes[0].Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, persistentvolumes[0],
			metav1.UpdateOptions{})
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
			}
		}()

		ginkgo.By("Deleting the gc PVC")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil
		ginkgo.By("Deleting the gc PV")
		err = fpv.DeletePersistentVolume(client, pv.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring up csi-controller pod in SV")
		bringUpCsiController(svcClient, csiReplicaCount)
		isControllerUp = true

		ginkgo.By(fmt.Sprintf("Sleeping for %v * 2seconds to allow double full sync to finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime*2) * time.Second)

		//Add a check to validate CnsVolumeMetadata crd
		verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)

		var svClient clientset.Interface
		if k8senvsv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senvsv != "" {
			svClient, err = createKubernetesClientFromConfig(k8senvsv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		svcNamespace := os.Getenv("SVC_NAMESPACE")

		// Delete PVC in SVC.
		svcPVC, err := svClient.CoreV1().PersistentVolumeClaims(svcNamespace).Get(ctx, pvcNameInSV,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete the PVC in SVC")
		err = fpv.DeletePersistentVolumeClaim(svClient, svcPVC.Name, svcPVC.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Verify static provision when csi connection to sv is broken
		1. Create a Storage class
		2. Create a PVC with "ReadWriteMany" using the storage policy created above GC
		3. Wait for PVC to be Bound
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata crd
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Change reclaimPolicy of PV to Retain.
		9. Delete PVC
		10. Delete PV
		11. Break connection to SV (Bring down the csi controller in GC)
		12. Create PV in GC bound to the same PVC in the SV
		13. Create PVC bound to PV
		14. Reestablish connection to SV
		15. Sleep double the Full Sync interval
		16. Verify CnsVolumeMetadata CRD is created
		17. Verify health status of PVC
		18. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		19. Verify entry is updated on CNS
		20. Delete PVC in GC
		21. Verify if PVC and PV also deleted in the SV cluster and GC
		22. Verify CnsVolumeMetadata CRD is deleted
		23. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify static provision when csi connection to sv is broken", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pv *v1.PersistentVolume
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var isControllerUp bool
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		sc, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
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

				//Add a check to validate CnsVolumeMetadata crd
				verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
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
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, persistentvolumes[0], nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Changing the reclaim policy of the pv to retain.
		ginkgo.By("Changing the volume reclaim policy")
		persistentvolumes[0].Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, persistentvolumes[0],
			metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the gc PVC")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil

		ginkgo.By("Deleting the gc PV")
		err = fpv.DeletePersistentVolume(client, pv.Name)
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
			}
		}()

		// Creating label for PV
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)

		ginkgo.By("Creating the PV in guest cluster")
		pv2 := getPersistentVolumeSpecForRWX(pvcNameInSV, v1.PersistentVolumeReclaimDelete,
			staticPVLabels, diskSize, "", v1.ReadWriteMany)
		pv2, err = client.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if pv2 != nil {
				ginkgo.By("Deleting the PV2")
				err = client.CoreV1().PersistentVolumes().Delete(ctx, pv2.Name, *metav1.NewDeleteOptions(0))
				if !apierrors.IsNotFound(err) {
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		ginkgo.By("Creating the PVC in guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespace, staticPVLabels, pv2.Name, diskSize)
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if pvc2 != nil {
				ginkgo.By("Deleting the PVC2")
				err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pv2 = nil

				//Add a check to validate CnsVolumeMetadata crd
				verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
			}
		}()

		ginkgo.By("Bring up csi-controller pod in SV")
		bringUpCsiController(svcClient, csiReplicaCount)
		isControllerUp = true

		ginkgo.By(fmt.Sprintf("Sleeping for %v * 2seconds to allow double full sync to finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime*2) * time.Second)

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client,
			framework.NewTimeoutContextWithDefaults(), namespace, pv2, pvc2))

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvc2, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		//Add a check to validate CnsVolumeMetadata crd
		verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", fcdIDInCNS)
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(fcdIDInCNS)
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
		gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
			CapacityInMb == newSizeInMb).To(gomega.BeTrue(), "Volume Capaticy is not matching")

		ginkgo.By("Deleting the PVC2")
		err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc2 = nil

		err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Add a check to validate CnsVolumeMetadata crd
		verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)

	})

	/*
		Verify label updated when csi-controller is down
		1. Create SC
		2. Create a PVC with "ReadWriteMany" using the storage policy created above GC
		3. Wait for PVC to be Bound
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata crd
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Break connection to SV (Bring down the csi controller in GC)
		9. Update PVC labels
		10. Verify CnsVolumeMetadata CRD is not updated.
		11. Reestablish connection to SV
		12. Sleep double the Full Sync interval
		13. Verify CnsVolumeMetadata CRD is updated
		14. Verify entry is updated in CNS
		15. Delete PVC in GC
		16. Verify if PVC and PV also deleted in the SV cluster and GC
		17. Verify CnsVolumeMetadata CRD is deleted
		18. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify label updated when csi-controller is down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var isControllerUp bool
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

				verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
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

		ginkgo.By(fmt.Sprintf("Expecting error for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvclaim.Name, pvclaim.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Bring up csi-controller pod in SV")
		bringUpCsiController(svcClient, csiReplicaCount)
		isControllerUp = true

		ginkgo.By(fmt.Sprintf("Sleeping for %v * 2seconds to allow double full sync to finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime*2) * time.Second)

		ginkgo.By(fmt.Sprintf("Expecting error for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvclaim.Name, pvclaim.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(fcdIDInCNS, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
