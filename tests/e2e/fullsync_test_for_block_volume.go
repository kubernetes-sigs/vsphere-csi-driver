/*
Copyright 2019 The Kubernetes Authors.

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

	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

/*
   Tests to verify Full Sync .

   Test 1) Verify CNS volume is created after full sync when pv entry is present.
   Test 2) Verify labels are created in CNS after updating pvc and/or pv with new labels.
   Test 3) Verify CNS volume is deleted after full sync when pv entry is delete

   Cleanup
   - Delete PVC and StorageClass and verify volume is deleted from CNS.
*/

var _ bool = ginkgo.Describe("[csi-block-vanilla] full-sync-test", func() {
	f := framework.NewDefaultFramework("e2e-full-sync-test")
	var (
		client              clientset.Interface
		namespace           string
		labelKey            string
		labelValue          string
		pandoraSyncWaitTime int
		fullSyncWaitTime    int
		datacenter          *object.Datacenter
		datastore           *object.Datastore
		err                 error
		datastoreURL        string
		fcdID               string
		datacenters         []string
		storagePolicyName   string
		scParameters        map[string]string
	)

	const (
		numberOfPVC = 5
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if fullSyncWaitTime <= 0 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}

		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters,
			",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}

		labelKey = "app"
		labelValue = "e2e-fullsync"

	})

	ginkgo.AfterEach(func() {
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}
	})

	ginkgo.It("Verify CNS volume is created after full sync when pv entry is present", func() {
		var err error

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)

		for _, dc := range datacenters {
			datacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(datacenter)
			datastore, err = getDatastoreByURL(ctx, datastoreURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating FCD Disk")
		fcdID, err = e2eVSphere.createFCD(ctx, fcdName, diskSizeInMb, datastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora", pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Creating the PV with the fcdID %s", fcdID))
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID
		pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimRetain, staticPVLabels)
		pv, err = client.CoreV1().PersistentVolumes().Create(pv)
		if err != nil {
			return
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be created", fcdID))
		err = e2eVSphere.waitForCNSVolumeToBeCreated(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting the PV %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(pv.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
		err = e2eVSphere.deleteFCD(ctx, fcdID, datastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.It("[csi-supervisor] Verify labels are created in CNS after updating pvc and/or pv with new labels", func() {
		ginkgo.By("Invoking test to verify labels creation")
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error

		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "", storagePolicyName)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(sc.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := framework.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		defer func() {
			err := framework.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels
		_, err = client.CoreV1().PersistentVolumes().Update(pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.It("[csi-supervisor] Verify CNS volume is deleted after full sync when pv entry is delete", func() {
		ginkgo.By("Invoking test to verify CNS volume creation")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "", storagePolicyName)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(sc.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := framework.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		fcdID = pv.Spec.CSI.VolumeHandle

		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0)

		if len(queryResult.Volumes) > 0 {
			// Find datastore from the retrieved datastoreURL
			finder := find.NewFinder(e2eVSphere.Client.Client, false)

			for _, dc := range datacenters {
				datacenter, err = finder.Datacenter(ctx, dc)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				finder.SetDatacenter(datacenter)
				datastore, err = getDatastoreByURL(ctx, queryResult.Volumes[0].DatastoreUrl, datacenter)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if datastore != nil {
					break
				}
			}
		}
		gomega.Expect(datastore).NotTo(gomega.BeNil())
		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Deleting PVC %s in namespace %s", pvc.Name, namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(pvc.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting the PV %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(pv.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be deleted", fcdID))
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
		err = e2eVSphere.deleteFCD(ctx, fcdID, datastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
	   Fullsync test with multiple PVCs
	   1. create a storage class with reclaim policy as "Retain"
	   2. create 5 pvcs with this storage class, and wait until all pvclaims are bound to corresponding pvs
	   3. stop vsan-health
	   4. delete pvclaim[0] and pvclaim[1]
	   5. update pvc labels for pvclaim[2]
	   6. update  pv labels for pvs[3] which is bounded to pvclaim[3]
	   7. start vsan-health and wait for full sync to finish
	   8. verify that pvc metadata for pvs[0] and pvs[1] has been deleted
	   9. verify that pvc labels for pvclaim[2] has been updated
	   10. verify that pv labels for pvs[3] has been updated
	   11. cleanup to remove pvs and pvcliams
	*/
	ginkgo.It("Verify Multiple PVCs are deleted/updated after full sync", func() {
		sc, err := createStorageClass(client, nil, nil, v1.PersistentVolumeReclaimRetain, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(sc.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		var pvclaims []*v1.PersistentVolumeClaim
		var pvs []*v1.PersistentVolume
		for i := 0; i < numberOfPVC; i++ {
			pvc, err := createPVC(client, namespace, nil, "", sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
			pvList, err := framework.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvList).NotTo(gomega.BeEmpty())
			pvclaims = append(pvclaims, pvc)
			pvs = append(pvs, pvList[0])
		}
		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		// delete two pvc, pvclaims[0] and pvclaims[1]
		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvclaims[0].Name, pvclaims[0].Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(pvclaims[0].Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvclaims[1].Name, pvclaims[1].Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(pvclaims[1].Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		// update pvc label for pvclaims[2]
		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvclaims[2].Name, pvclaims[2].Namespace))
		pvclaims[2], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(pvclaims[2].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims[2].Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(pvclaims[2])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// update pv label for pv which is bounded to pvclaims[3]
		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pvs[3].Name))
		pvs[3].Labels = labels
		_, err = client.CoreV1().PersistentVolumes().Update(pvs[3])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for pvc metadata to be deleted for pvc %s in namespace %s", pvclaims[0].Name, pvclaims[0].Namespace))
		err = e2eVSphere.waitForMetadataToBeDeleted(pvs[0].Spec.CSI.VolumeHandle, string(cnstypes.CnsKubernetesEntityTypePVC), pvclaims[0].Name, pvclaims[0].Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for pvc metadata to be deleted for pvc %s in namespace %s", pvclaims[1].Name, pvclaims[1].Namespace))
		err = e2eVSphere.waitForMetadataToBeDeleted(pvs[1].Spec.CSI.VolumeHandle, string(cnstypes.CnsKubernetesEntityTypePVC), pvclaims[1].Name, pvclaims[1].Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s", labels, pvclaims[2].Name, pvclaims[2].Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pvs[2].Spec.CSI.VolumeHandle, labels, string(cnstypes.CnsKubernetesEntityTypePVC), pvclaims[2].Name, pvclaims[2].Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pvs[3].Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pvs[3].Spec.CSI.VolumeHandle, labels, string(cnstypes.CnsKubernetesEntityTypePV), pvs[3].Name, pvs[3].Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// cleanup
		for _, pvc := range pvclaims {
			ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvc.Name, pvc.Namespace))
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(pvc.Name, nil)
			if !apierrors.IsNotFound(err) {
				// skip if failure is "not found" - object may already been deleted by test
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		for _, pv := range pvs {
			ginkgo.By(fmt.Sprintf("Deleting the PV %s", pv.Name))
			err = client.CoreV1().PersistentVolumes().Delete(pv.Name, nil)
			if !apierrors.IsNotFound(err) {
				// skip if failure is "not found" - object may already been deleted by test
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	ginkgo.It("Verify PVC metadata is created in CNS after PVC is created in k8s", func() {
		var err error

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)

		for _, dc := range datacenters {
			datacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(datacenter)
			datastore, err = getDatastoreByURL(ctx, datastoreURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating FCD Disk")
		fcdID, err = e2eVSphere.createFCD(ctx, fcdName, diskSizeInMb, datastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora", pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Creating the PV with the fcdID %s", fcdID))
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID
		pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimDelete, staticPVLabels)
		pv, err = client.CoreV1().PersistentVolumes().Create(pv)
		if err != nil {
			return
		}

		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("Creating the PVC")
		pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(framework.WaitOnPVandPVC(client, namespace, pv, pvc))

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, pvc.Name, pv.ObjectMeta.Name, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(pvc.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify PVC metadata is deleted in CNS after PVC is deleted in k8s", func() {
		var err error

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)

		for _, dc := range datacenters {
			datacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(datacenter)
			datastore, err = getDatastoreByURL(ctx, datastoreURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating FCD Disk")
		fcdID, err = e2eVSphere.createFCD(ctx, fcdName, diskSizeInMb, datastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora", pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Creating the PV with the fcdID %s", fcdID))
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID
		pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimRetain, staticPVLabels)
		pv, err = client.CoreV1().PersistentVolumes().Create(pv)
		if err != nil {
			return
		}

		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC")
		pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(framework.WaitOnPVandPVC(client, namespace, pv, pvc))

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(pvc.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		err = invokeVCenterServiceControl(startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for pvc metadata to be deleted for pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForMetadataToBeDeleted(pv.Spec.CSI.VolumeHandle, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting pv %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(pv.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
		err = e2eVSphere.deleteFCD(ctx, fcdID, datastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.It("Scale down driver deployment to zero replica and verify PV metadata is created in CNS", func() {
		var err error

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)

		for _, dc := range datacenters {
			datacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(datacenter)
			datastore, err = getDatastoreByURL(ctx, datastoreURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating FCD Disk")
		fcdID, err = e2eVSphere.createFCD(ctx, fcdName, diskSizeInMb, datastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora", pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Scaling down the csi driver to zero replica")
		deployment := updateDeploymentReplica(client, 0, vSphereCSIControllerPodNamePrefix, kubeSystemNamespace)
		ginkgo.By(fmt.Sprintf("Successfully scaled down the csi driver deployment:%s to zero replicas", deployment.Name))

		ginkgo.By(fmt.Sprintf("Creating the PV with the fcdID %s", fcdID))
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID
		pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimRetain, staticPVLabels)
		pv, err = client.CoreV1().PersistentVolumes().Create(pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scaling up the csi driver to one replica")
		deployment = updateDeploymentReplica(client, 1, vSphereCSIControllerPodNamePrefix, kubeSystemNamespace)
		ginkgo.By(fmt.Sprintf("Successfully scaled up the csi driver deployment:%s to one replica", deployment.Name))

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be created", fcdID))
		err = e2eVSphere.waitForCNSVolumeToBeCreated(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting the PV %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(pv.Name, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
		err = e2eVSphere.deleteFCD(ctx, fcdID, datastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

})
