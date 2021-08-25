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
	"os"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

var _ = ginkgo.Describe("Volume health check", func() {

	f := framework.NewDefaultFramework("volume-healthcheck")
	var (
		client                     clientset.Interface
		namespace                  string
		scParameters               map[string]string
		storagePolicyName          string
		raid0StoragePolicyName     string
		volumeHealthAnnotation     string = "volumehealth.storage.kubernetes.io/health"
		datastoreURL               string
		hostIP                     string
		pvc                        *v1.PersistentVolumeClaim
		pvclaim                    *v1.PersistentVolumeClaim
		isVsanhealthServiceStopped bool
		isSPSserviceStopped        bool
	)
	ginkgo.BeforeEach(func() {
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		isVsanhealthServiceStopped = false
		waitForAllHostsToBeUp(ctx, &e2eVSphere)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}
		if pvc != nil {
			if hostIP != "" {
				ginkgo.By("checking host status")
				err := waitForHostToBeUp(hostIP)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if pvclaim != nil {
			if hostIP != "" {
				ginkgo.By("checking host status")
				err := waitForHostToBeUp(hostIP)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if isVsanhealthServiceStopped {
			ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
			err := invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again",
				vsanHealthServiceWaitTime))
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}
		if isSPSserviceStopped {
			ginkgo.By(fmt.Sprintln("Starting sps on the vCenter host"))
			err := invokeVCenterServiceControl(startOperation, spsServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to come up again", vsanHealthServiceWaitTime))
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		}
		waitForAllHostsToBeUp(ctx, &e2eVSphere)
	})

	// Test to verify health annotation status is accessible on the pvc.
	// (Combined test for TC1 and TC2)
	// Steps:
	//    1.	Create a Storage Class.
	//    2.	Create a PVC using above SC.
	//    3.	Wait for PVC to be in Bound phase.
	//    4.	Verify health annotation is added on the PVC is accessible.
	//    5.	Wait for the CNS health api to be called again (No changes made to
	//       PV/PVC, expecting it to be accessible).
	//    6.	Delete PVC.
	//    7.	Verify PV entry is deleted from CNS.
	//    8.	Delete the SC.

	ginkgo.It("[csi-supervisor] [csi-guest] Verify health annotation added on the pvc is accessible", func() {

		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()

		ginkgo.By("Invoking Test for validating health status")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for counter := 0; counter < 2; counter++ {
			ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
				healthStatusWaitTime))
			time.Sleep(healthStatusWaitTime)

			ginkgo.By("Expect health status of the pvc to be accessible")
			pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx,
				pvclaim.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

			if guestCluster {
				// Verifying svc pvc health status.
				ginkgo.By("Expect health annotation is added on the SV pvc")
				svPVC := getPVCFromSupervisorCluster(svPVCName)
				gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(
					gomega.BeEquivalentTo(healthStatusAccessible))
			}

		}

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)

		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}

	})

	// Test to verify health annotation status is not added on the pvc which
	// is on pending state.
	//
	// Steps
	// 1.	Create a Storage Class with non shared datastore.
	// 2.	Create a PVC using above SC.
	// 3.	PVC will be created and it will be in pending state.
	// 4.	Verify health annotation is not added on the PVC.
	// 5.	Delete PVC.
	// 7.	Delete the SC.

	ginkgo.It("[csi-supervisor] [csi-guest] "+
		"Verify health annotation is not added on the pvc which is on pending state", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nonShareadstoragePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameForNonSharedDatastores)
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		ginkgo.By("Invoking Test for validating health annotation is not added to PVC on Pending state")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(nonShareadstoragePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, nonShareadstoragePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", nonShareadstoragePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = nonShareadstoragePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Verify health status annotation is not added to the pvc in pending state")

		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for annotation := range pvc.Annotations {
			gomega.Expect(annotation).ShouldNot(gomega.BeEquivalentTo(pvcHealthAnnotation))
		}

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()
	})

	// Validate the health status is updated from "unknown" status to accessible.
	//
	// Steps
	// 1. Create a Storage Class.
	// 2. Create a PVC using above SC.
	// 3. Wait for PVC to be in Bound phase.
	// 4. Bring vSAN-health down.
	// 5. Verify no health annotation is added on the PVC.
	// 6. Bring vSAN-health up.
	// 7. Verify health annotation which is added on the PVC is accessible.
	// 8. Delete PVC.
	// 9. Verify PV entry is deleted from CNS.
	// 10. Delete the SC.

	ginkgo.It("[csi-supervisor] [csi-guest] "+
		"Verify health annotation is updated from unknown status to accessible", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client,
				namespace, nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionShortTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		isVsanhealthServiceStopped = true
		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is not added on the pvc")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for describe := range pvc.Annotations {
			gomega.Expect(pvc.Annotations[describe]).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
		}

		if guestCluster {
			ginkgo.By("Expect health annotation is not added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			for describe := range svPVC.Annotations {
				gomega.Expect(svPVC.Annotations[describe]).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
			}
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanhealthServiceStopped = false

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is added on the pvc and its accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)

		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}

		ginkgo.By("Verifying disk size specified in PVC is honored")
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != diskSizeInMb {
			err = fmt.Errorf("Wrong disk size provisioned ")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	// Validate the health status is not updated to "unknown" status from
	// accessible.
	//
	// Steps
	// 1. Create a Storage Class.
	// 2. Create a PVC using above SC.
	// 3. Wait for PVC to be in Bound phase.
	// 4. Verify health annotation which is added on the PVC is accessible.
	// 5. Bring VSAN down.
	// 6. Verify the health annotation of the PVC remains accessible.
	// 7. Bring VSAN up.
	// 8. Verify health annotation which is added on the PVC is accessible.
	// 9. Delete PVC.
	// 10. Verify PV entry is deleted from CNS.
	// 11. Delete the SC.

	ginkgo.It("[csi-supervisor] [csi-guest] "+
		"Verify health annotation is not updated to unknown status from accessible", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is added on the pvc and its accessible")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		isVsanhealthServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is added on the pvc and its accessible after the vsan health is down")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanhealthServiceStopped = false

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is added on the pvc and its accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		gomega.Expect(len(queryResult.Volumes) > 0)
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))
		}
	})

	// Validate the health status is not updated when SPS is down.
	//
	// Steps
	// 1. Create a Storage Class.
	// 2. Create a PVC using above SC.
	// 3. Wait for PVC to be in Bound phase.
	// 4. Bring SPS down.
	// 5. Verify no annotation is added on the PVC.
	// 6. Bring SPS up.
	// 7. Verify annotation is added on the PVC is accessible.
	// 8. Delete PVC.
	// 9. Verify PV entry is deleted from CNS.
	// 10. Delete the SC.

	ginkgo.It("[csi-supervisor] [csi-guest] Verify health annotation is not updated when SPS is down", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle

		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintln("Stopping sps on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		isSPSserviceStopped = true
		err = invokeVCenterServiceControl(stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is not added on the pvc")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for annotation := range pvc.Annotations {
			gomega.Expect(pvc.Annotations[annotation]).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
		}

		if guestCluster {
			ginkgo.By("Expect health annotation is not added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			for describe := range svPVC.Annotations {
				gomega.Expect(svPVC.Annotations[describe]).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
			}
		}

		ginkgo.By(fmt.Sprintln("Starting sps on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isSPSserviceStopped = false

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is added on the pvc and its accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)

		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}

	})

	// Verify changing the annotated values on the PVC to random value.
	//
	// Steps
	// 1. Create a Storage Class.
	// 2. Create a PVC using above SC.
	// 3. Wait for PVC to be in Bound phase.
	// 4. Verify annotation added on the PVC in accessible.
	// 5. Kubectl edit on the annotation of the PVC and change the annotation
	//    to inaccessible state.
	// 6. Wait for the default time interval.
	// 7. Verify health annotation is added on the PVC is accessible.
	// 8. Delete PVC.
	// 9. Verify PV entry is deleted from CNS.
	// 10. Delete the SC.

	ginkgo.It("[csi-supervisor] Verify changing the annotated values on the PVC to random value", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Changing health status of the pvc to be inaccessible")
		setAnnotation := make(map[string]string)
		setAnnotation[volumeHealthAnnotation] = healthStatusInAccessible
		pvc.Annotations = setAnnotation

		_, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusInAccessible))

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By("Changing health status of the pvc to be random value")
		setAnnotation[volumeHealthAnnotation] = "vmware"
		pvc.Annotations = setAnnotation

		_, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo("vmware"))

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		ginkgo.By("Verify if health status of the pvc is changed to accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))
		}
	})

	// Verify health annotation is added on the volume created by statefulset.
	//
	// Steps
	// 1. Create a storage class.
	// 2. Create nginx service.
	// 3. Create nginx statefulset.
	// 4. Wait until all Pods are ready and PVCs are bounded with PV.
	// 5. Verify health annotation added on the PVC is accessible.
	// 6. Delete the pod(make the replicas to 0).
	// 7. Delete PVC from the tests namespace.
	// 8. Delete the storage class.

	ginkgo.It("[csi-supervisor] Verify Volume health on Statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, defaultNginxStorageClassName)
		}

		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Verify the health status is accessible on the volume created.
		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		// Get the list of Volumes attached to Pods before scale down.
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			// _, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			// gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache.
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Expect health status of the pvc to be accessible")
					pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
						volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(
						gomega.BeEquivalentTo(healthStatusAccessible))
				}
			}
		}

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()
	})

	// Verify health annotaiton is not added on the PV.
	//
	// Steps
	// 1. Create a Storage Class.
	// 2. Create a PVC using above SC.
	// 3. Wait for PVC to be in Bound phase.
	// 4. Verify health annotation is not added on the PV.
	// 5. Delete PVC.
	// 6. Verify PV entry is deleted from CNS.
	// 7. Delete the SC.

	ginkgo.It("[csi-supervisor] Verify health annotaiton is not added on the PV ", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect volume to be provisioned successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name,
			framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle

		ginkgo.By("Verify health annotation is not added on PV")
		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, persistentvolumes[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for describe := range pv.Annotations {
			gomega.Expect(describe).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
		}

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}

	})

	// Verify removing the health annotation on the PVC.
	//
	// Steps
	// 1. Create a Storage Class.
	// 2. Create a PVC using above SC.
	// 3. Wait for PVC to be in Bound phase.
	// 4. Verify annotation added on the PVC in accessible.
	// 5. Kubectl edit on the annotation of the PVC and remove the entire health
	//    annotation.
	// 6. Wait for the default time interval.
	// 7. Verify health annotation is added on the PVC is accessible.
	// 8. Delete PVC.
	// 9. Verify PV entry is deleted from CNS.
	// 10. Delete the SC.
	ginkgo.It("[csi-supervisor] Verify removing the health annotation on the PVC", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")

		ginkgo.By("CNS_TEST: Running for WCP setup")
		profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
		scParameters[scParamStoragePolicyID] = profileID
		// Create resource quota.
		createResourceQuota(client, namespace, rqLimit, storagePolicyName)
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
			nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect volume to be provisioned successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name,
			framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Removing health status from the pvc")
		setAnnotation := make(map[string]string)
		checkVolumeHealthAnnotation := "test-key"
		setAnnotation[checkVolumeHealthAnnotation] = " "
		pvc.Annotations = setAnnotation

		_, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for describe := range pvc.Annotations {
			gomega.Expect(describe).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))
		}

	})

	// Verify health annotation is added on the volume created by statefulset.
	//
	// 1. Create a storage class.
	// 2. Create nginx service.
	// 3. Create nginx statefulset.
	// 4. Wait until all Pods are ready and PVCs are bounded with PV.
	// 5. Verify health annotation added on the PVC is accessible.
	// 6. Delete the pod.
	// 7. Delete PVC from the tests namespace.
	// 8. Delete the storage class.

	ginkgo.It("[csi-guest] In Guest Cluster Verify Volume health on Statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		scParameters[svStorageClassName] = storagePolicyName
		sc, err := createStorageClass(client, scParameters, nil, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		statefulset := GetStatefulSetFromManifest(namespace)

		ginkgo.By("Create a statefulset with 3 replicas")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Waiting for pods status to be Ready.
		ginkgo.By("Wait for all Pods are Running state")
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		// Get the list of Volumes attached to Pods before scale down.
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By("Verify CnsNodeVmAttachment CRD is created")
					volumeID := pv.Spec.CSI.VolumeHandle
					// svcPVCName refers to PVC Name in the supervisor cluster.
					svcPVCName := volumeID
					volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
					verifyCRDInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+svcPVCName,
						crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					vmUUID, err = getVMUUIDFromNodeName(sspod.Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
						fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

					// Verify the health status is accessible on the volume created.
					ginkgo.By("Expect health status of the pvc to be accessible")
					pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
						volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(
						gomega.BeEquivalentTo(healthStatusAccessible))
				}
			}
		}
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()

	})

	// Verify Volume health when GC CSI is down.
	//
	// 1. Create a Storage Class.
	// 2. Create a GC PVC using above SC.
	// 3. Wait for PVC to be in Bound phase.
	// 4. Verify annotation in the GC PVC is accessible.
	// 5. Bring GC CSI controller down.
	// 6. Verify SV PVC and GC PVC have same health annotation.
	// 7. Bring GC CSI controller up.
	// 8. Verify the existing annotation in the GC PVC is not affected,
	//    remaining accessible.
	// 9. Verify SV PVC and GC PVC have same health annotation.
	// 10. Delete GC PVC.
	// 11. Verify PV entry is deleted from CNS.
	// 12. Delete the SC.

	ginkgo.It("[csi-guest] Verify Volume health when GC CSI is down", func() {

		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		var isControllerUP = true
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()

		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		// svPVCName refers to PVC Name in the supervisor cluster.
		svPVCName := volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		ginkgo.By("Expect health status of the pvc to be accessible")
		pvclaim, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		var gcClient clientset.Interface
		if k8senv := GetAndExpectStringEnvVar("KUBECONFIG"); k8senv != "" {
			gcClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Get svcClient and svNamespace")
		svClient, _ := getSvcClientAndNamespace()

		ginkgo.By("Bring down csi-controller pod in GC")
		bringDownTKGController(svClient)
		bringDownCsiController(gcClient)
		isControllerUP = false
		defer func() {
			if !isControllerUP {
				bringUpTKGController(svClient)
				bringUpCsiController(gcClient)
			}
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		ginkgo.By("Expect health status of the pvc to be accessible")
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By("Expect health annotation is added on the SV pvc")
		svPVC := getPVCFromSupervisorCluster(svPVCName)
		gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By("Bring up csi-controller pod in GC")
		bringUpTKGController(svClient)
		bringUpCsiController(gcClient)
		isControllerUP = true

		ginkgo.By("Verify health status of GC PVC after GC csi is up")
		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By("Expect health annotation is added on the SV pvc")
		svPVC = getPVCFromSupervisorCluster(svPVCName)
		gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}

	})

	// Verify Volume health after password rotation.
	//
	// 1. Create StorageClass and PVC.
	// 2. Wait for PVC to be Bound.
	// 3. verify health annotation on PVC.
	// 4. Modify the password rotation time in storage user file to 0.
	// 5. verify health annotation on PVC.
	// 6. Delete PVC.
	// 7. Delete Storage class.

	ginkgo.It("[csi-supervisor] [csi-guest] Verify Volume health after password rotation", func() {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()

		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		svPVCName := volumeID
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svPVCName := volumeID
			volumeID = getVolumeIDFromSupervisorCluster(svPVCName)
		}

		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvc = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		ginkgo.By("Expect health status of the pvc to be accessible")
		pvclaim, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		ginkgo.By("Invoking password rotation")
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = replacePasswordRotationTime(passorwdFilePath, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify health annotation on the PVC after password rotation")
		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}

	})

	// Verify Volume health when SV CSI is down.
	//
	// Create a Storage Class.
	// Create a PVC using above SC.
	// Wait for PVC to be in Bound phase.
	// Verify health annotation which is added on the PVC is accessible.
	// Bring CSI controller down.
	// Bring down link between all the hosts and datastore.
	// Existing PVC annotation should remain same.
	// Bring CSI controller up.
	// Verify health annotation which is added on the PVC is inaccessible.
	// Restore link between all the hosts and datastore.
	// Verify health annotation which is added on the PVC is accessible.
	// Delete PVC.
	// Verify PV entry is deleted from CNS.
	// Delete the SC.

	ginkgo.It("[csi-supervisor] Verify Volume health when SVC CSI is down", func() {
		var sc *storagev1.StorageClass
		var err error
		var isControllerUP = true
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()

		raid0StoragePolicyName = os.Getenv("RAID_0_STORAGE_POLICY")
		if raid0StoragePolicyName == "" {
			ginkgo.Skip("Env RAID_0_STORAGE_POLICY is missing")
		}

		ginkgo.By("CNS_TEST: Running for WCP setup")
		profileID := e2eVSphere.GetSpbmPolicyID(raid0StoragePolicyName)
		scParameters[scParamStoragePolicyID] = profileID
		// Create resource quota.
		createResourceQuota(client, namespace, rqLimit, raid0StoragePolicyName)
		sc, pvc, err = createPVCAndStorageClass(client, namespace,
			nil, scParameters, diskSize, nil, "", false, "", raid0StoragePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		pv := getPvFromClaim(client, namespace, pvc.Name)
		framework.Logf("volume name %v", pv.Name)

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvc, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvclaim, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By("Get svcClient")
		svClient, _ := getSvcClientAndNamespace()

		ginkgo.By("Bring down csi-controller pod in SVC")
		bringDownCsiController(svClient)
		isControllerUP = false
		defer func() {
			if !isControllerUP {
				bringUpCsiController(svClient)
			}
		}()

		ginkgo.By("PSOD the host")
		hostIP = psodHostWithPv(ctx, &e2eVSphere, pv.Name)

		defer func() {
			if hostIP != "" {
				ginkgo.By("checking host status")
				err := waitForHostToBeUp(hostIP)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvc = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvc, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By("Bring up csi-controller pod in SVC")
		bringUpCsiController(svClient)
		isControllerUP = true
		ginkgo.By("Verify health status of SVC PVC after csi is up(inaccessible)")

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusInAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusInAccessible))

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvc, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify health status of SVC PVC should be accessible")
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}
	})

	// Verify health annotation added on the pvc is changed from accessible to
	// inaccessible.
	//
	// Steps
	// 1. Create a Storage Class.
	// 2. Create a PVC using above SC.
	// 3. Wait for PVC to be in Bound phase.
	// 4. Verify health annotation is added on the PVC is accessible.
	// 5. Bring down link between all the hosts and datastore. (Health status
	//    should return in-accessible status).
	// 6. Verify health annotation on the PVC is updated to in-accessible.
	// 7. Restore link between all the hosts and datastore.
	// 8. Verify health annotation on the PVC is updated to accessible.
	// 9. Delete PVC.
	// 10.Verify PV entry is deleted from CNS.
	// 11.Delete the SC.

	ginkgo.It("[csi-supervisor] [csi-guest] "+
		"Verify health annotation added on the pvc is changed from accessible to inaccessible", func() {
		var storageclass *storagev1.StorageClass
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()

		raid0StoragePolicyName = os.Getenv("RAID_0_STORAGE_POLICY")
		if raid0StoragePolicyName == "" {
			ginkgo.Skip("Env RAID_0_STORAGE_POLICY is missing")
		}

		ginkgo.By("Invoking Test for validating health status")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(raid0StoragePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, raid0StoragePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", raid0StoragePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = raid0StoragePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle
		if supervisorCluster {
			pv = getPvFromClaim(client, namespace, pvclaim.Name)
			framework.Logf("volume name %v", pv.Name)
		}
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
			ginkgo.By("Get svcClient and svNamespace")
			svcClient, svNamespace := getSvcClientAndNamespace()
			pv = getPvFromClaim(svcClient, svNamespace, svPVCName)
			framework.Logf("volume name %v", pv.Name)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			// Verifying svc pvc health status also to be inaccessible.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		// PSOD the host.
		ginkgo.By("PSOD the host")
		framework.Logf("pvName %v", pv.Name)
		hostIP = psodHostWithPv(ctx, &e2eVSphere, pv.Name)
		defer func() {
			ginkgo.By("checking host status")
			err := waitForHostToBeUp(hostIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusInAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if guestCluster {
			// Verifying svc pvc health status also to be inaccessible.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(
				gomega.BeEquivalentTo(healthStatusInAccessible))
		}

		ginkgo.By("Expect health status of the pvc to be inaccessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusInAccessible))

		// CNS should return the health status as red when its inaccessible.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0)

		// It checks the colour code returned by cns for pv.
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red)")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthRed))
		}

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if guestCluster {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes)).NotTo(gomega.BeZero())
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))
		}
	})

	// Verify health status of pvc after bringing SV API server down.
	//
	// Steps
	// 1. Create a Storage Class.
	// 2. Create a PVC using above SC.
	// 3. Wait for PVC to be in Bound phase.
	// 4. Verify that the volume health is accessible.
	// 5. Bring down the SV API server completely.
	// 6. Bring down link between the host and the datastore to make volume
	//    health inaccessible.
	// 7. CNS should return the health status when API server is down (health
	//    status should be inaccessible).
	// 8. Bring up SV API server.
	// 9. validate that volume health on PVC changes from inaccessible to
	//    accessible after default time interval.

	ginkgo.It("[csi-supervisor] Verify health status of pvc after bringing SV API server down", func() {
		var storageclass *storagev1.StorageClass
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		var isSvcUp bool
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()

		raid0StoragePolicyName = os.Getenv("RAID_0_STORAGE_POLICY")
		if raid0StoragePolicyName == "" {
			ginkgo.Skip("Env RAID_0_STORAGE_POLICY is missing")
		}

		ginkgo.By("Invoking Test for validating health status")

		ginkgo.By("CNS_TEST: Running for WCP setup")
		profileID := e2eVSphere.GetSpbmPolicyID(raid0StoragePolicyName)
		scParameters[scParamStoragePolicyID] = profileID
		// Create resource quota.
		createResourceQuota(client, namespace, rqLimit, raid0StoragePolicyName)
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
			nil, scParameters, diskSize, nil, "", false, "", raid0StoragePolicyName)

		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		pv := getPvFromClaim(client, namespace, pvclaim.Name)
		framework.Logf("volume name %v", pv.Name)

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		defer func() {
			ginkgo.By("checking host status")
			if hostIP != "" {
				err := waitForHostToBeUp(hostIP)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Bringing SV API server down")
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		log.Infof("VC ip address: %v", vcAddress)

		err = bringSvcK8sAPIServerDown(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isSvcUp = false
		defer func() {
			if !isSvcUp {
				ginkgo.By("Bringing SV API server UP")
				err = bringSvcK8sAPIServerUp(ctx, client, pvclaim, vcAddress, healthStatusAccessible)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// PSOD the host.
		ginkgo.By("PSOD the host")
		hostIP = psodHostWithPv(ctx, &e2eVSphere, pv.Name)

		ginkgo.By("Query CNS volume health status")
		err = queryCNSVolumeWithWait(ctx, client, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringing SV API server UP")
		err = bringSvcK8sAPIServerUp(ctx, client, pvclaim, vcAddress, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isSvcUp = true

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

	})

	// Validate the health status is updated from "unknown" status to inaccessible.
	//
	// Steps:
	// Create a Storage Class.
	// Create a PVC using above SC.
	// Wait for PVC to be in Bound phase.
	// Bring VSAN down.
	// Verify no health annotation is added on the PVC.
	// Bring VSAN up.
	// Bring down link between all the hosts and datastore.
	// Verify health annotation which is added on the PVC is inaccessible.
	// Restore link between all the hosts and datastore.
	// Delete PVCs.
	// Verify PV entry is deleted from CNS.
	// Delete the SC.

	ginkgo.It("[csi-supervisor] [csi-guest] "+
		"Verify health annotation is updated from unknown status to inaccessible", func() {
		var storageclass *storagev1.StorageClass
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		raid0StoragePolicyName = os.Getenv("RAID_0_STORAGE_POLICY")
		if raid0StoragePolicyName == "" {
			ginkgo.Skip("Env RAID_0_STORAGE_POLICY is missing")
		}
		ginkgo.By("Invoking Test for validating health status")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(raid0StoragePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, raid0StoragePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", raid0StoragePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = raid0StoragePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle

		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		isVsanhealthServiceStopped = true
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered",
			healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is not added on the pvc")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for annotation := range pvc.Annotations {
			gomega.Expect(pvc.Annotations[annotation]).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
		}
		if supervisorCluster {
			pv = getPvFromClaim(client, namespace, pvclaim.Name)
			framework.Logf("volume name %v", pv.Name)
		}

		if guestCluster {
			ginkgo.By("Expect health annotation is not added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			for describe := range svPVC.Annotations {
				gomega.Expect(svPVC.Annotations[describe]).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
			}
			svcClient, svNamespace := getSvcClientAndNamespace()
			pv = getPvFromClaim(svcClient, svNamespace, svPVCName)
			framework.Logf("PV name in SVC for PVC in GC %v", pv.Name)
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanhealthServiceStopped = false

		// PSOD the host.
		ginkgo.By("PSOD the host")
		hostIP = psodHostWithPv(ctx, &e2eVSphere, pv.Name)

		defer func() {
			ginkgo.By("checking host status")
			err := waitForHostToBeUp(hostIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusInAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health annotation is added on the pvc and its inaccessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusInAccessible))

		if guestCluster {
			// Verifying svc pvc health status also to be inaccessible.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(
				gomega.BeEquivalentTo(healthStatusInAccessible))
		}

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if guestCluster {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		ginkgo.By("Expect health annotation is added on the pvc and its accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))
		}
	})

	// Bring the CSI controller down when there is change in health status.
	//
	// 1. Create a Storage Class.
	// 2. Create a GC PVC using above SC.
	// 3. Wait for PVC to be in Bound phase.
	// 4. Verify health annotation format which is added on the SV PVC and GC
	//    PVC is accessible.
	// 5. Bring down link between all the hosts and datastore.
	// 6. Bring GC CSI down.
	// 7. Verify health annotation which is added on the GC PVC is not changed
	//    to inaccessible.
	// 8. Bring GC CSI controller Up.
	// 9. wait for healthStatusWaitTime to make sure the GC PVC is updated with
	//    the health annotation.
	// 10. Verify health annotation which is added on the SV PVC and GC PVC is
	//     changed to inaccessible.
	// 11. Restore link between all the hosts and datastore.
	// 12. Delete GC PVC.
	// 13. Verify PV entry is deleted from CNS.
	// 14. Delete the SC.
	ginkgo.It("[csi-guest] Verify Inaccesssible Volume health when GC CSI is down", func() {
		var sc *storagev1.StorageClass
		var err error
		var isControllerUP = true
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()

		raid0StoragePolicyName = os.Getenv("RAID_0_STORAGE_POLICY")
		if raid0StoragePolicyName == "" {
			ginkgo.Skip("Env RAID_0_STORAGE_POLICY is missing")
		}

		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = raid0StoragePolicyName
		sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		// svPVCName refers to PVC Name in the supervisor cluster.
		svPVCName := volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svPVCName)
		framework.Logf("volume ID from SVC %v", volumeID)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		svcClient, svNamespace := getSvcClientAndNamespace()
		svcPV := getPvFromClaim(svcClient, svNamespace, svPVCName)
		framework.Logf("PV name in SVC for PVC in GC %v", svcPV.Name)

		var gcClient clientset.Interface
		if k8senv := GetAndExpectStringEnvVar("KUBECONFIG"); k8senv != "" {
			gcClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			ginkgo.By("checking host status")
			err := waitForHostToBeUp(hostIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if !isControllerUP {
				bringUpTKGController(svcClient)
				bringUpCsiController(gcClient)
			}
			err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvc = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvc, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvclaim, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By("Bring down csi-controller pod in GC")
		bringDownTKGController(svcClient)
		bringDownCsiController(gcClient)
		isControllerUP = false

		// Get SV PVC before PSOD.
		svPVC := getPVCFromSupervisorCluster(svPVCName)

		// PSOD the host.
		ginkgo.By("PSOD the host")
		hostIP = psodHostWithPv(ctx, &e2eVSphere, svcPV.Name)

		// Health status in gc pvc should be still accessible.
		ginkgo.By("Expect health status of the GC PVC to be accessible")
		err = pvcHealthAnnotationWatcher(ctx, client, pvc, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health annotation added on the SVC PVC is inaccessible")
		err = pvcHealthAnnotationWatcher(ctx, svcClient, svPVC, healthStatusInAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring up csi-controller pod in GC")
		bringUpTKGController(svcClient)
		bringUpCsiController(gcClient)
		isControllerUP = true

		ginkgo.By("Verify health status of GC PVC after GC csi is up")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusInAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health annotation added on the GC PVC is accessible")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health annotation added on the SVC PVC is accessible")
		err = pvcHealthAnnotationWatcher(ctx, svcClient, svPVC, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))
		}
	})

	// Verify health annotation is added on the volume created by statefulset.
	//
	// Steps
	// Create a storage class.
	// Create nginx service.
	// Create nginx statefulset.
	// Wait until all Pods are ready and PVCs are bounded with PV.
	// Verify health annotation added on the PVC is accessible.
	// Bring down link between all the hosts and datastore.
	// Verify health annotation on the PVC is updated to inaccessible.
	// Restore the link between all the hosts and datastore.
	// Delete the pod(make the replicas to 0).
	// Delete PVC from the tests namespace.
	// Delete the storage class.

	ginkgo.It("[csi-supervisor] [csi-guest] Verify Volume health Inaccessible on Statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var statusFlag bool = false
		var pvSVC *v1.PersistentVolume
		raid0StoragePolicyName = os.Getenv("RAID_0_STORAGE_POLICY")
		if raid0StoragePolicyName == "" {
			ginkgo.Skip("Env RAID_0_STORAGE_POLICY is missing")
		}

		ginkgo.By("Creating StorageClass for Statefulset")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(raid0StoragePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, defaultNginxStorageClassName)
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}

		if guestCluster {
			scParameters[svStorageClassName] = raid0StoragePolicyName
			sc, err := createStorageClass(client, scParameters, nil, "", "", false, "nginx-sc")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		if supervisorCluster {
			// Get the list of Volumes attached to Pods.
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						// Verify the attached volume match the one in CNS cache.
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						ginkgo.By("Expect health status of the pvc to be accessible")
						pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
							volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						ginkgo.By("poll for health status annotation")
						err = pvcHealthAnnotationWatcher(ctx, client, pvc, healthStatusAccessible)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						pvSVC = getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						framework.Logf("PV name in SVC for PVC in GC %v", pvSVC.Name)
					}
				}
			}
		}

		if guestCluster {
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						ginkgo.By("Verify CnsNodeVmAttachment CRD is created")
						volumeID := pv.Spec.CSI.VolumeHandle
						// svcPVCName refers to PVC Name in the supervisor cluster.
						svcPVCName := volumeID
						volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
						gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
						verifyCRDInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+svcPVCName,
							crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

						ginkgo.By("Expect health status of the pvc to be accessible")
						pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
							volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						ginkgo.By("poll for health status annotation")
						err = pvcHealthAnnotationWatcher(ctx, client, pvc, healthStatusAccessible)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						ginkgo.By("Expect health annotation is added on the SV pvc")
						svPVC := getPVCFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
						framework.Logf("svPVC %v", svPVC)

						ginkgo.By("Get svcClient and svNamespace")
						svcClient, svNamespace := getSvcClientAndNamespace()

						ginkgo.By("poll for health status annotation")
						err = pvcHealthAnnotationWatcher(ctx, svcClient, svPVC, healthStatusAccessible)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						pvSVC = getPvFromClaim(svcClient, svNamespace, pv.Spec.CSI.VolumeHandle)
						framework.Logf("PV name in SVC for PVC in GC %v", pvSVC.Name)

					}
				}
			}
		}

		// PSOD the host.
		ginkgo.By("PSOD the host")
		framework.Logf("pv.Name %v", pvSVC.Name)
		hostIP = psodHostWithPv(ctx, &e2eVSphere, pvSVC.Name)

		defer func() {
			ginkgo.By("checking host status")
			err := waitForHostToBeUp(hostIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v to allow volume health check to be triggered", svOperationTimeout))
		time.Sleep(svOperationTimeout)

		ginkgo.By("Expect health status of a pvc to be inaccessible")
		// Get the list of Volumes attached to Pods.
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					if supervisorCluster {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						// Verify the attached volume match the one in CNS cache.
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
						volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					if pvc.Annotations[volumeHealthAnnotation] == healthStatusInAccessible {
						statusFlag = true
						break
					}
				}
			}
		}
		ginkgo.By("Expect health annotation is added on the SV pvc is inaccessible")
		if guestCluster {
			ginkgo.By("Get svcClient and svNamespace")
			svcClient, _ := getSvcClientAndNamespace()
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(svcClient, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

						svPVC := getPVCFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
						if svPVC.Annotations[volumeHealthAnnotation] == healthStatusInAccessible {
							statusFlag = true
							break
						} else {
							statusFlag = false
						}
					}
				}
			}
		}

		// Expecting the status Flag to be true.
		gomega.Expect(statusFlag).NotTo(gomega.BeFalse(), "Volume health status is not as expected")

		if supervisorCluster {
			// Get the list of Volumes attached to Pods.
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						// Verify the attached volume match the one in CNS cache.
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						ginkgo.By("Expect health status of the pvc to be accessible")
						pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
							volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						ginkgo.By("poll for health status annotation")
						err = pvcHealthAnnotationWatcher(ctx, client, pvc, healthStatusAccessible)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
		}

	})

	// Validate the health status is not updated to "unknown" status from
	// inaccessible.
	//
	// Steps
	// Create a Storage Class.
	// Create a PVC using above SC.
	// Wait for PVC to be in Bound phase.
	// Bring down link between all the hosts and datastore.
	// Verify health annotation which is added on the PVC is inaccessible.
	// Bring VSAN down.
	// Verify the health annotation of the PVC remains inaccessible.
	// Bring VSAN up.
	// Restore link between all the hosts and datastore.
	// Delete PVC.
	// Verify PV entry is deleted from CNS.
	// Delete the SC.

	ginkgo.It("[csi-supervisor] [csi-guest] "+
		"Verify health annotation is not updated to unknown status from inaccessible", func() {
		var storageclass *storagev1.StorageClass
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		raid0StoragePolicyName = os.Getenv("RAID_0_STORAGE_POLICY")
		if raid0StoragePolicyName == "" {
			ginkgo.Skip("Env RAID_0_STORAGE_POLICY is missing")
		}
		ginkgo.By("Invoking Test for validating health status")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(raid0StoragePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, raid0StoragePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", raid0StoragePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = raid0StoragePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle
		pv := getPvFromClaim(client, namespace, pvclaim.Name)
		framework.Logf("volume name %v", pv.Name)
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
			svcClient, svNamespace := getSvcClientAndNamespace()
			pv = getPvFromClaim(svcClient, svNamespace, svPVCName)
			framework.Logf("PV name in SVC for PVC in GC %v", pv.Name)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		// PSOD the host.
		ginkgo.By("PSOD the host")
		hostIP = psodHostWithPv(ctx, &e2eVSphere, pv.Name)

		defer func() {
			ginkgo.By("checking host status")
			err := waitForHostToBeUp(hostIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusInAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health annotation is added on the pvc and its inaccessible")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusInAccessible))

		if guestCluster {
			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(
				gomega.BeEquivalentTo(healthStatusInAccessible))
		}

		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthRed))
		}

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanhealthServiceStopped = true
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusInAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect health annotation is added on the pvc and its inaccessible after the vsan health is down")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusInAccessible))

		if guestCluster {
			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(
				gomega.BeEquivalentTo(healthStatusInAccessible))
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanhealthServiceStopped = false

		ginkgo.By("poll for health status annotation")
		err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if guestCluster {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verifying svc pvc health status.
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		ginkgo.By("Expect health annotation is added on the pvc and its accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(len(queryResult.Volumes) > 0)

		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))
		}
	})

	// Verify pvc is not annotated with health status in block vanilla setup.
	// Steps
	// Create a Storage Class.
	// Create a PVC using above SC.
	// Wait for PVC to be in Bound phase.
	// Verify health annotation is not added on the PVC.
	// Delete PVC.
	// Verify PV entry is deleted from CNS.
	// Delete the SC.

	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] Verify pvc is not annotated "+
		"with health status in vanilla setup", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Invoking Test volume health status")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
			nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", pollTimeout))
		time.Sleep(pollTimeout)

		ginkgo.By("Expect health annotation is not added on the pvc")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for describe := range pvc.Annotations {
			gomega.Expect(pvc.Annotations[describe]).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
		}

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	// Verify pvc is not annotated with health status in file vanilla setup.
	// Steps
	// Create a Storage Class.
	// Create a PVC using above SC.
	// Wait for PVC to be in Bound phase.
	// Verify health annotation is not added on the PVC.
	// Delete PVC.
	// Verify PV entry is deleted from CNS.
	// Delete the SC.

	ginkgo.It("[csi-file-vanilla] File Vanilla Verify pvc is not annotated with health status", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType
		accessMode := v1.ReadWriteMany
		// Create Storage class and PVC.
		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessMode, nfs4FSType))
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
			nil, scParameters, "", nil, "", false, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Waiting for PVC to be bound.
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", pollTimeout))
		time.Sleep(pollTimeout)

		ginkgo.By("Expect health annotation is not added on the pvc")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for describe := range pvc.Annotations {
			gomega.Expect(pvc.Annotations[describe]).ShouldNot(gomega.BeEquivalentTo(pvcHealthAnnotation))
		}

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s", queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus))

		ginkgo.By("Verifying volume type specified in PVC is honored")
		if queryResult.Volumes[0].VolumeType != testVolumeType {
			err = fmt.Errorf("volume type is not %q", testVolumeType)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

	// Test to verify health annotation timestamp is added on the pvc.
	// Steps
	//    1. Create a Storage Class.
	//    2. Create a PVC using above SC.
	//    3. Wait for PVC to be in Bound phase.
	//    4. Verify health annotation is added on the PVC.
	//    5. Verify volume health timestamp is added on the PVC.
	//    6. Delete PVC.
	//    7. Verify PV entry is deleted from CNS.
	//    8. Delete the SC.

	ginkgo.It("[csi-supervisor] [csi-guest] Verify health timestamp annotation is added on the pvc", func() {

		var storageclass *storagev1.StorageClass
		var err error
		var svcPVCName string
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Invoking Test for validating health status")
		// Decide which test setup is available to run.
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// Create resource quota.
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace,
				nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svcPVCName = volHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) == 0)
		}()

		ginkgo.By("Expect volume health timestamp is added on the pvc")
		err = expectedAnnotation(ctx, client, pvclaim, pvcHealthAnnotation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = expectedAnnotation(ctx, client, pvclaim, pvcHealthTimestampAnnotation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if guestCluster {
			ginkgo.By("Get svcClient and svNamespace")
			svClient, _ := getSvcClientAndNamespace()

			ginkgo.By("Expect volume health timestamp is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svcPVCName)

			err = expectedAnnotation(ctx, svClient, svPVC, pvcHealthAnnotation)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = expectedAnnotation(ctx, svClient, svPVC, pvcHealthTimestampAnnotation)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

})
