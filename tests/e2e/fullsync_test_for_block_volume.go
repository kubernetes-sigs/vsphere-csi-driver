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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

// Tests to verify Full Sync.
//
// Test 1) Verify CNS volume is created after full sync when pv entry is
//         present.
// Test 2) Verify labels are created in CNS after updating pvc and/or pv with
//         new labels.
// Test 3) Verify CNS volume is deleted after full sync when pv entry is delete.
//
// Cleanup: Delete PVC and StorageClass and verify volume is deleted from CNS.

var _ bool = ginkgo.Describe("full-sync-test", func() {
	f := framework.NewDefaultFramework("e2e-full-sync-test")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                     clientset.Interface
		namespace                  string
		csiControllerNamespace     string
		labelKey                   string
		labelValue                 string
		pandoraSyncWaitTime        int
		fullSyncWaitTime           int
		datacenter                 *object.Datacenter
		datastore                  *object.Datastore
		datastoreURL               string
		fcdID                      string
		datacenters                []string
		storagePolicyName          string
		scParameters               map[string]string
		isVsanHealthServiceStopped bool
	)

	const (
		numberOfPVC = 5
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		if vanillaCluster {
			csiControllerNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Full sync interval can be 1 min at minimum so full sync wait time
			// has to be more than 120s.
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}

		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if isVsanHealthServiceStopped {
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}
		if supervisorCluster {
			dumpSvcNsEventsOnTestFailure(client, namespace)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}
	})

	ginkgo.It("[ef-vanilla-block][csi-block-vanilla] [csi-block-vanilla-serialized] Verify CNS volume is created "+
		"after full sync when pv entry is present", ginkgo.Label(p0, block, vanilla, core, vc70), func() {
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

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Creating the PV with the fcdID %s", fcdID))
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID
		pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimRetain, staticPVLabels, ext4FSType)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		if err != nil {
			return
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be created", fcdID))
		err = e2eVSphere.waitForCNSVolumeToBeCreated(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting the PV %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be deleted", fcdID))
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
		err = deleteFcdWithRetriesForSpecificErr(ctx, fcdID, datastore.Reference(),
			[]string{disklibUnlinkErr}, []string{objOrItemNotFoundErr})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.It("[ef-vanilla-block][csi-supervisor] [csi-block-vanilla] [csi-block-vanilla-serialized] Verify labels "+
		"are created in CNS after updating pvc and/or pv with new labels", ginkgo.Label(p0, block, vanilla, wcp, core,
		vc70), func() {
		ginkgo.By("Invoking test to verify labels creation")
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Decide which test setup is available to run.
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			restClientConfig := getRestConfigClient()
			setStoragePolicyQuota(ctx, restClientConfig, storagePolicyName, namespace, rqLimit)
			sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil,
				scParameters, "", nil, "", true, "", storagePolicyName)

		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
		pv.Labels = labels
		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.It("[ef-vanilla-block][ef-wcp][csi-supervisor] [csi-block-vanilla] [csi-block-vanilla-serialized] Verify CNS "+
		"volume is deleted after full sync when pv entry is delete", ginkgo.Label(p0, block, vanilla, wcp, core,
		vc70), func() {
		ginkgo.By("Invoking test to verify CNS volume creation")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		// Decide which test setup is available to run.
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil, nil, "", nil, "", false, "")
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			restClientConfig := getRestConfigClient()
			setStoragePolicyQuota(ctx, restClientConfig, storagePolicyName, namespace, rqLimit)
			sc, pvc, err = createPVCAndStorageClass(ctx, client, namespace, nil,
				scParameters, "", nil, "", true, "", storagePolicyName)
		}

		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		fcdID = pv.Spec.CSI.VolumeHandle

		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0)

		if len(queryResult.Volumes) > 0 {
			// Find datastore from the retrieved datastoreURL.
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
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Deleting PVC %s in namespace %s", pvc.Name, namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting the PV %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be deleted", fcdID))
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
		err = deleteFcdWithRetriesForSpecificErr(ctx, fcdID, datastore.Reference(),
			[]string{disklibUnlinkErr}, []string{objOrItemNotFoundErr})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	// Fullsync test with multiple PVCs.
	// 1. create a storage class with reclaim policy as "Retain".
	// 2. create 5 pvcs with this storage class, and wait until all pvclaims
	//    are bound to corresponding pvs.
	// 3. stop vsan-health.
	// 4. delete pvclaim[0] and pvclaim[1].
	// 5. update pvc labels for pvclaim[2].
	// 6. update  pv labels for pvs[3] which is bounded to pvclaim[3].
	// 7. start vsan-health and wait for full sync to finish.
	// 8. verify that pvc metadata for pvs[0] and pvs[1] has been deleted.
	// 9. verify that pvc labels for pvclaim[2] has been updated.
	// 10. verify that pv labels for pvs[3] has been updated.
	// 11. cleanup to remove pvs and pvcliams.
	ginkgo.It("[ef-vanilla-block][csi-block-vanilla] [csi-block-vanilla-serialized] Verify Multiple PVCs are "+
		"deleted/updated after full sync", ginkgo.Label(p0, block, vanilla, core, vc70), func() {
		sc, err := createStorageClass(client, nil, nil, v1.PersistentVolumeReclaimRetain, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		var pvclaims []*v1.PersistentVolumeClaim
		var pvs []*v1.PersistentVolume

		for i := 0; i < numberOfPVC; i++ {
			pvc, err := createPVC(ctx, client, namespace, nil, "", sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
			pvList, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
				[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvList).NotTo(gomega.BeEmpty())
			pvclaims = append(pvclaims, pvc)
			pvs = append(pvs, pvList[0])
		}
		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		// Delete two pvc, pvclaims[0] and pvclaims[1].
		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvclaims[0].Name, pvclaims[0].Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvclaims[0].Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvclaims[1].Name, pvclaims[1].Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvclaims[1].Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		// Update pvc label for pvclaims[2].
		ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s",
			labels, pvclaims[2].Name, pvclaims[2].Namespace))
		pvclaims[2], err = client.CoreV1().PersistentVolumeClaims(namespace).Get(
			ctx, pvclaims[2].Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims[2].Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvclaims[2], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Update pv label for pv which is bounded to pvclaims[3].
		ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pvs[3].Name))
		pvs[3].Labels = labels
		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pvs[3], metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for pvc metadata to be deleted for pvc %s in namespace %s",
			pvclaims[0].Name, pvclaims[0].Namespace))
		err = e2eVSphere.waitForMetadataToBeDeleted(pvs[0].Spec.CSI.VolumeHandle,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaims[0].Name, pvclaims[0].Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for pvc metadata to be deleted for pvc %s in namespace %s",
			pvclaims[1].Name, pvclaims[1].Namespace))
		err = e2eVSphere.waitForMetadataToBeDeleted(pvs[1].Spec.CSI.VolumeHandle,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaims[1].Name, pvclaims[1].Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
			labels, pvclaims[2].Name, pvclaims[2].Namespace))
		err = e2eVSphere.waitForLabelsToBeUpdated(pvs[2].Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvclaims[2].Name, pvclaims[2].Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pvs[3].Name))
		err = e2eVSphere.waitForLabelsToBeUpdated(pvs[3].Spec.CSI.VolumeHandle, labels,
			string(cnstypes.CnsKubernetesEntityTypePV), pvs[3].Name, pvs[3].Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Cleanup.
		for _, pvc := range pvclaims {
			ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvc.Name, pvc.Namespace))
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			if !apierrors.IsNotFound(err) {
				// Skip if failure is "not found" - object may already been deleted
				// by test.
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		for _, pv := range pvs {
			ginkgo.By(fmt.Sprintf("Deleting the PV %s", pv.Name))
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
			if !apierrors.IsNotFound(err) {
				// Skip if failure is "not found" - object may already been deleted
				// by test.
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	ginkgo.It("[ef-vanilla-block][csi-block-vanilla] [csi-block-vanilla-serialized] Verify PVC metadata is created "+
		"in CNS after PVC is created in k8s", ginkgo.Label(p0, block, vanilla, core, vc70), func() {
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

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Creating the PV with the fcdID %s", fcdID))
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID
		pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimDelete, staticPVLabels, ext4FSType)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		if err != nil {
			return
		}

		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("Creating the PVC")
		pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv, pvc))

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, pvc.Name, pv.ObjectMeta.Name, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("[ef-vanilla-block][csi-block-vanilla] [csi-block-vanilla-serialized] Verify PVC metadata is deleted in"+
		" CNS after PVC is deleted in k8s", ginkgo.Label(p0, block, vanilla, core, vc70), func() {
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

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Creating the PV with the fcdID %s", fcdID))
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID
		pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimRetain, staticPVLabels, ext4FSType)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		if err != nil {
			return
		}

		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC")
		pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv, pvc))

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for pvc metadata to be deleted for pvc %s in namespace %s",
			pvc.Name, pvc.Namespace))
		err = e2eVSphere.waitForMetadataToBeDeleted(pv.Spec.CSI.VolumeHandle,
			string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting pv %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be deleted", fcdID))
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
		err = deleteFcdWithRetriesForSpecificErr(ctx, fcdID, datastore.Reference(),
			[]string{disklibUnlinkErr}, []string{objOrItemNotFoundErr})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.It("[csi-block-vanilla-destructive][pq-vanilla-block] Scale down driver deployment to zero replica and "+
		"verify PV metadata is created in CNS", ginkgo.Label(p1, negative, block, vanilla, disruptive, core,
		vc70), func() {
		var err error

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		c := client
		controllerClusterConfig := os.Getenv(contollerClusterKubeConfig)
		if controllerClusterConfig != "" {
			framework.Logf("Creating client for remote kubeconfig")
			remoteC, err := createKubernetesClientFromConfig(controllerClusterConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			c = remoteC
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

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Get controller replica count before scaling down")
		deployment, err := c.AppsV1().Deployments(csiControllerNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		replica_before_scale_down := *deployment.Spec.Replicas
		framework.Logf("Replica before scale down %d", replica_before_scale_down)

		ginkgo.By("Scaling down the csi driver to zero replica")
		deployment = updateDeploymentReplica(c, 0, vSphereCSIControllerPodNamePrefix, csiControllerNamespace)
		ginkgo.By(fmt.Sprintf("Successfully scaled down the csi driver deployment:%s to zero replicas", deployment.Name))

		ginkgo.By(fmt.Sprintf("Creating the PV with the fcdID %s", fcdID))
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID
		pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimRetain, staticPVLabels, ext4FSType)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scaling up the csi driver")
		deployment = updateDeploymentReplica(c, replica_before_scale_down,
			vSphereCSIControllerPodNamePrefix, csiControllerNamespace)
		ginkgo.By(fmt.Sprintf("Successfully scaled up the csi driver deployment:%s", deployment.Name))

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be created", fcdID))
		err = e2eVSphere.waitForCNSVolumeToBeCreated(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting the PV %s", pv.Name))
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for volume %s to be deleted", fcdID))
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
		err = deleteFcdWithRetriesForSpecificErr(ctx, fcdID, datastore.Reference(),
			[]string{disklibUnlinkErr}, []string{objOrItemNotFoundErr})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Attach volume to a new pod when CNS is down and verify volume metadata in CNS post full sync
		Steps:
			1.	create a pvc pvc1, wait for pvc bound to pv
			2.	create a pod pod1, using pvc1
			3.	stop vsan-health on VC
			4.	when vsan-health is stopped, delete pod1
			5.	create a new pod pod2, using pvc1
			6.	start vsan-health on VC
			7.	after fullsync is triggered verify the CNS metadata for the volume backing pvc1
			8.	delete pod2
			9.	delete pvc1
	*/
	ginkgo.It("[ef-wcp][csi-block-vanilla][csi-supervisor][csi-guest][csi-block-vanilla-serialized][pq-vanilla-block]"+
		"Attach volume to a new pod when CNS is down and verify volume metadata in CNS post full "+
		"sync", ginkgo.Label(p1, negative, block, vanilla, wcp, tkg, core, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		var svcPVCName string

		ginkgo.By("create a pvc pvc1, wait for pvc bound to pv")
		volHandle, pvc, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, "", storagePolicyName, namespace, ext4FSType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if pvc != nil {
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("create a pod pod1, using pvc1")
		pod, _ := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvc, volHandle, "")
		defer func() {
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume metadata is matching the one in CNS")
		if guestCluster {
			svcPVCName = pv.Spec.CSI.VolumeHandle
			err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, volHandle, svcPVCName, pvc, pv, pod)
		} else {
			err = waitAndVerifyCnsVolumeMetadata(ctx, volHandle, pvc, pv, pod)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Stopping vsan-health on the vCenter host")
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// TODO: Replace static wait with polling
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			oneMinuteWaitTimeInSeconds))
		time.Sleep(time.Duration(oneMinuteWaitTimeInSeconds) * time.Second)

		ginkgo.By("when vsan-health is stopped, delete pod1")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("create a new pod pod2, using pvc1")
		pod2, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, execCommand)
		//Since VSAN is down , POD will not reach running state. It is expected to throw error
		gomega.Expect(err).To(gomega.HaveOccurred())
		defer func() {
			err := fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Starting vsan-health on the vCenter host")
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Waiting for pod pod2, to be running")
		err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume metadata is matching the one in CNS")
		if guestCluster {
			err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, volHandle, svcPVCName, pvc, pv, pod2)
		} else {
			err = waitAndVerifyCnsVolumeMetadata(ctx, volHandle, pvc, pv, pod2)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var vmUUID string
		var exists bool
		pod2, err = client.CoreV1().Pods(namespace).Get(ctx, pod2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod2.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod2.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			annotations := pod2.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod2 doesn't have %s annotation", vmUUIDLabel))
		}

		ginkgo.By("delete pod2")
		err = fpod.DeletePodWithWait(ctx, client, pod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		var isDiskDetached bool
		if supervisorCluster {
			isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, vmUUID)
		} else {
			isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod2.Spec.NodeName)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, vmUUID))

		ginkgo.By(fmt.Sprintf("Deleting pvc %s in namespace %s", pvc.Name, pvc.Namespace))
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for CNS volume %s to be deleted", volHandle))
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

})
