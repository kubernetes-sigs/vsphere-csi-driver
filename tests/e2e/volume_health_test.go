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
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var _ = ginkgo.Describe("Volume health check", func() {
	f := framework.NewDefaultFramework("volume-healthcheck")
	var (
		client                 clientset.Interface
		namespace              string
		scParameters           map[string]string
		storagePolicyName      string
		volumeHealthAnnotation string = "volumehealth.storage.kubernetes.io/health"
	)
	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	ginkgo.AfterEach(func() {
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}
	})

	/*
		Test to verify health annotation status is accessible on the pvc .

		Steps
		1.	Create a Storage Class
		2.	Create a PVC using above SC
		3.	Wait for PVC to be in Bound phase
		4.	Verify health annotation is added on the PVC is accessible
		5.	Delete PVC
		6.	Verify PV entry is deleted from CNS
		7.	Delete the SC
	*/

	ginkgo.It("[csi-supervisor] [csi-guest] Verify health annotation added on the pvc is accessible", func() {

		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()

		ginkgo.By("Invoking Test for validating health status")
		// decide which test setup is available to run
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health status of the pvc to be accessible")

		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			//verifying svc pvc health status
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
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

		gomega.Expect(len(queryResult.Volumes) > 0)

		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}

	})

	/*
		Test to verify health annotation status is not added on the pvc which is on pending state .

		Steps
		1.	Create a Storage Class with non shared datastore
		2.	Create a PVC using above SC
		3.	PVC will be created and it will be in pending state
		4.	Verify health annotation is not added on the PVC
		5.	Delete PVC
		7.	Delete the SC
	*/

	ginkgo.It("[csi-supervisor] [csi-guest] Verify health annotation is not added on the pvc which is on pending state", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nonShareadstoragePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameForNonSharedDatastores)
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		ginkgo.By("Invoking Test for validating health annotation is not added to PVC on Pending state")
		// decide which test setup is available to run
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(nonShareadstoragePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, nonShareadstoragePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "", nonShareadstoragePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = nonShareadstoragePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Verify health status annotation is not added to the pvc in pending state")

		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for describe := range pvc.Annotations {
			gomega.Expect(pvc.Annotations[describe]).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
		}

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		Validate the health status is updated from "unknown" status to accessible .

		Steps
		1. Create a Storage Class
		2. Create a PVC using above SC
		3. Wait for PVC to be in Bound phase
		4. Bring vSAN-health down
		5. Verify no health annotation is added on the PVC
		6. Bring vSAN-health up
		7. Verify health annotation which is added on the PVC is accessible
		8. Delete PVC
		9. Verify PV entry is deleted from CNS
		10. Delete the SC
	*/

	ginkgo.It("[csi-supervisor] [csi-guest] Verify health annotation is updated from unknown status to accessible", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")
		// decide which test setup is available to run
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
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

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is added on the pvc and its accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			//verifying svc pvc health status
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
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

	/*
		Validate the health status is not updated to "unknown" status from accessible

		Steps
		1. Create a Storage Class
		2. Create a PVC using above SC
		3. Wait for PVC to be in Bound phase
		4. Verify health annotation which is added on the PVC is accessible
		5. Bring VSAN down
		6. Verify the health annotation of the PVC remains accessible
		7. Bring VSAN up
		8. Verify health annotation which is added on the PVC is accessible
		9. Delete PVC
		10. Verify PV entry is deleted from CNS
		11. Delete the SC
	*/

	ginkgo.It("[csi-supervisor] [csi-guest] Verify health annotation is not updated to unknown status from accessible", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")
		// decide which test setup is available to run
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is added on the pvc and its accessible")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			//verifying svc pvc health status
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
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is added on the pvc and its accessible after the vsan health is down")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			//verifying svc pvc health status
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is added on the pvc and its accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	/*
		Validate the health status is not updated when SPS is down .

		Steps
		1. Create a Storage Class
		2. Create a PVC using above SC
		3. Wait for PVC to be in Bound phase
		4. Bring SPS down
		5. Verify no annotation is added on the PVC
		6. Bring SPS up
		7. Verify annotation is added on the PVC is accessible
		8. Delete PVC
		9. Verify PV entry is deleted from CNS
		10. Delete the SC
	*/

	ginkgo.It("[csi-supervisor] [csi-guest] Verify health annotation is not updated when SPS is down", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")
		// decide which test setup is available to run
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		svPVCName := volHandle

		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintln("Stopping sps on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
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

		ginkgo.By(fmt.Sprintln("Starting sps on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health annotation is added on the pvc and its accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			//verifying svc pvc health status
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
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

		gomega.Expect(len(queryResult.Volumes) > 0)

		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}

	})

	/*
		Verify changing the annotated values on the PVC to random value

		Steps
		1. Create a Storage Class
		2.	Create a PVC using above SC
		3.	Wait for PVC to be in Bound phase
		4.	Verify annotation added on the PVC in accessible
		5.	Kubectl edit on the annotation of the PVC and change the annotation to inaccessible state
		6.	Wait for the default time interval
		7.	Verify health annotation is added on the PVC is accessible
		8.	Delete PVC
		9.	Verify PV entry is deleted from CNS
		10.	Delete the SC
	*/

	ginkgo.It("[csi-supervisor] Verify changing the annotated values on the PVC to random value", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")
		// decide which test setup is available to run
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster
			svcPVCName := volHandle
			volHandle = getVolumeIDFromSupervisorCluster(svcPVCName)
		}
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
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

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
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

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		ginkgo.By("Verify if health status of the pvc is changed to accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	/*
		Verify health annotation is added on the volume created by statefulset

		steps
		1. Create a storage class.
		2. Create nginx service.
		3. Create nginx statefulset
		4. Wait until all Pods are ready and PVCs are bounded with PV.
		5. Verify health annotation added on the PVC is accessible
		6. Delete the pod(make the replicas to 0)
		7. Delete PVC from the tests namespace
		8. Delete the storage class.

	*/

	ginkgo.It("[csi-supervisor] Verify Volume health on Statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storageclassname)
		}

		scSpec := getVSphereStorageClassSpec(storageclassname, scParameters, nil, "", "", false)
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
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		// Verify the health status is accessible on the volume created
		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		// Get the list of Volumes attached to Pods before scale dow
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			// _, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			// gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Expect health status of the pvc to be accessible")
					pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
				}
			}
		}

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()
	})

	/*
		Verify health annotaiton is not added on the PV

		Steps
		1. Create a Storage Class
		2. Create a PVC using above SC
		3. Wait for PVC to be in Bound phase
		4. Verify health annotation is not added on the PV
		5. Delete PVC
		6. Verify PV entry is deleted from CNS
		7. Delete the SC
	*/

	ginkgo.It("[csi-supervisor] Verify health annotaiton is not added on the PV ", func() {
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var pvclaims []*v1.PersistentVolumeClaim
		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()
		ginkgo.By("Invoking Test for validating health status")
		// decide which test setup is available to run
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect volume to be provisioned successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle

		ginkgo.By("Verify health annotation is not added on PV")
		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, persistentvolumes[0].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for describe := range pv.Annotations {
			gomega.Expect(describe).ShouldNot(gomega.BeEquivalentTo(volumeHealthAnnotation))
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

		gomega.Expect(len(queryResult.Volumes) > 0)
		ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
		for _, vol := range queryResult.Volumes {
			log.Infof("Volume health status: %s", vol.HealthStatus)
			gomega.Expect(vol.HealthStatus).Should(gomega.BeEquivalentTo(healthGreen))

		}

	})

	/*
		Verify removing the health annotation on the PVC

		Steps
		1. Create a Storage Class
		2. Create a PVC using above SC
		3. Wait for PVC to be in Bound phase
		4. Verify annotation added on the PVC in accessible
		5. Kubectl edit on the annotation of the PVC and remove the entire health annotation
		6. Wait for the default time interval
		7. Verify health annotation is added on the PVC is accessible
		8. Delete PVC
		9. Verify PV entry is deleted from CNS
		10. Delete the SC
	*/
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
		// create resource quota
		createResourceQuota(client, namespace, rqLimit, storagePolicyName)
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect volume to be provisioned successfully")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
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

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		ginkgo.By("Expect health status of the pvc to be accessible")
		pvc, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	/*
		Verify health annotation is added on the volume created by statefulset

		1. Create a storage class.
		2. Create nginx service.
		3. Create nginx statefulset
		4. Wait until all Pods are ready and PVCs are bounded with PV.
		5. Verify health annotation added on the PVC is accessible
		6. Delete the pod
		7. Delete PVC from the tests namespace
		8. Delete the storage class.

	*/

	ginkgo.It("[csi-guest] Verify Volume health on Statefulset in GC", func() {
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

		// Waiting for pods status to be Ready
		ginkgo.By("Wait for all Pods are Running state")
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)

		// Get the list of Volumes attached to Pods before scale down
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By("Verify CnsNodeVmAttachment CRD is created")
					volumeID := pv.Spec.CSI.VolumeHandle
					// svcPVCName refers to PVC Name in the supervisor cluster
					svcPVCName := volumeID
					volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
					verifyCRDInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					vmUUID, err = getVMUUIDFromNodeName(sspod.Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

					// Verify the health status is accessible on the volume created
					ginkgo.By("Expect health status of the pvc to be accessible")
					pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(pvc.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
				}
			}
		}
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()

	})

	/*
		Verify Volume health when GC CSI is down

		1. Create a Storage Class
		2. Create a GC PVC using above SC
		3. Wait for PVC to be in Bound phase
		4. Verify annotation in the GC PVC is accessible
		5. Bring GC CSI controller down
		6. Verify SV PVC and GC PVC have same health annotation
		7. Bring GC CSI controller up
		8. Verify the existing annotation in the GC PVC is not affected(remains accessible)
		9. Verify SV PVC and GC PVC have same health annotation
		10. Delete GC PVC
		11. Verify PV entry is deleted from CNS
		12. Delete the SC
	*/

	ginkgo.It("[csi-guest] Verify Volume health when GC CSI is down", func() {

		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		var isControlerUP = true
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
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		//svPVCName refers to PVC Name in the supervisor cluster
		svPVCName := volumeID
		volumeID = getVolumeIDFromSupervisorCluster(svPVCName)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
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

		var svClient clientset.Interface
		if k8senvsv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senvsv != "" {
			svClient, err = k8s.CreateKubernetesClientFromConfig(k8senvsv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring down csi-controller pod in GC")
		bringDownTKGController(svClient)
		bringDownCsiContorller(gcClient)
		isControlerUP = false
		defer func() {
			if !isControlerUP {
				bringUpTKGController(svClient)
				bringUpCsiContorller(gcClient)
			}
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
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
		bringUpCsiContorller(gcClient)
		isControlerUP = true

		ginkgo.By("Verify health status of GC PVC after GC csi is up")
		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
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

	/*
		Verify Volume health after password rotation

		1. Create StorageClass and PVC
		2. Wait for PVC to be Bound
		3. verify health annotation on PVC
		4. Modify the password rotation time in storage user file to 0.
		5. verify health annotation on PVC
		6. Delete PVC
		7. Delete Storage class
	*/

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
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "", storagePolicyName)
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
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		svPVCName := volumeID
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster
			svPVCName := volumeID
			volumeID = getVolumeIDFromSupervisorCluster(svPVCName)
		}

		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		ginkgo.By("Expect health status of the pvc to be accessible")
		pvclaim, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			//verifying svc pvc health status
			ginkgo.By("Expect health annotation is added on the SV pvc")
			svPVC := getPVCFromSupervisorCluster(svPVCName)
			gomega.Expect(svPVC.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))
		}

		ginkgo.By("Invoking password rotation")
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = replacePasswordRotationTime(passorwdFilePath, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify health annotation on the PVC after password rotation")
		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes to allow volume health check to be triggered", healthStatusWaitTime))
		time.Sleep(healthStatusWaitTime)
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim.Annotations[volumeHealthAnnotation]).Should(gomega.BeEquivalentTo(healthStatusAccessible))

		if guestCluster {
			//verifying svc pvc health status
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

})
