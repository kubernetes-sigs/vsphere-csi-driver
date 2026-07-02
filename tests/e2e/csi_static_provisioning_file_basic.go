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
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/vmware/govmomi/cns"
	cnsmethods "github.com/vmware/govmomi/cns/methods"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
)

var _ = ginkgo.Describe("[csi-file-vanilla] Basic File Volume Static Provisioning", func() {
	f := framework.NewDefaultFramework("e2e-csifilestaticprovision")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	var (
		client            clientset.Interface
		datastoreURL      string
		defaultDatacenter *object.Datacenter
		defaultDatastore  *object.Datastore
		namespace         string
		pv                *v1.PersistentVolume
		pvSpec            *v1.PersistentVolume
		pvc               *v1.PersistentVolumeClaim
		fullSyncWaitTime  int
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = f.Namespace.Name
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}

		for _, dc := range datacenters {
			defaultDatacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			if err == nil {
				break
			}
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Datastore is not found in the datacenter list")

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}
	})

	// This test verifies the static provisioning workflow.
	//
	// Test Steps:
	// 1. Create File share and get the file share id.
	// 2. Create PV Spec with volumeID set to file share ID created in Step-1,
	//    and PersistentVolumeReclaimPolicy is set to Delete.
	// 3. Create PVC with the storage request set to PV's storage capacity.
	// 4. Wait for PV and PVC to bound.
	// 5. Create a POD.
	// 6. Verify volume is attached to the node and volume is accessible in the
	//    pod by creating a file inside volume.
	// 7. Verify container volume metadata is present in CNS cache.
	// 8. Delete POD.
	// 9. Delete PVC.
	// 10. Verify PV is deleted automatically.
	ginkgo.It("[ef-f-file-vanilla] Verify basic static provisioning workflow for file volume", ginkgo.Label(p1,
		file, vanilla, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := connectCns(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating file share")
		cnsCreateReq := cnstypes.CnsCreateVolume{
			This:        cnsVolumeManagerInstance,
			CreateSpecs: []cnstypes.CnsVolumeCreateSpec{*getFileShareCreateSpec(defaultDatastore.Reference())},
		}
		cnsCreateRes, err := cnsmethods.CnsCreateVolume(ctx, e2eVSphere.CnsClient.Client, &cnsCreateReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		task, err := object.NewTask(e2eVSphere.Client.Client, cnsCreateRes.Returnval), nil
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		taskInfo, err := cns.GetTaskInfo(ctx, task)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fileShareVolumeID := taskResult.GetCnsVolumeOperationResult().VolumeId.Id

		// Deleting the volume with deleteDisk set to false.
		ginkgo.By("Deleting the fileshare with deleteDisk set to false")
		cnsDeleteReq := cnstypes.CnsDeleteVolume{
			This:       cnsVolumeManagerInstance,
			VolumeIds:  []cnstypes.CnsVolumeId{{Id: fileShareVolumeID}},
			DeleteDisk: false,
		}
		cnsDeleteRes, err := cnsmethods.CnsDeleteVolume(ctx, e2eVSphere.CnsClient.Client, &cnsDeleteReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		task, err = object.NewTask(e2eVSphere.Client.Client, cnsDeleteRes.Returnval), nil
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		taskInfo, err = cns.GetTaskInfo(ctx, task)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = cns.GetTaskResult(ctx, taskInfo)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		staticPVLabels := make(map[string]string)
		staticPVLabels["fileshare-id"] = strings.TrimPrefix(fileShareVolumeID, "file:")

		ginkgo.By("Creating the PV")
		pv = getPersistentVolumeSpecForFileShare(fileShareVolumeID,
			v1.PersistentVolumeReclaimDelete, staticPVLabels, v1.ReadOnlyMany)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC")
		pvc = getPersistentVolumeClaimSpecForFileShare(namespace, staticPVLabels, pv.Name, v1.ReadOnlyMany)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv, pvc))

		defer func() {
			ginkgo.By("Deleting the PV Claim")
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace),
				"Failed to delete PVC", pvc.Name)
			ginkgo.By("Verify PV should be deleted automatically")
			framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeoutShort))

			ginkgo.By("Verify fileshare volume got deleted")
			framework.ExpectNoError(e2eVSphere.waitForCNSVolumeToBeDeleted(fileShareVolumeID))
		}()

		ginkgo.By("Creating the Pod")
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, pod), "Failed to delete pod", pod.Name)
		}()

		ginkgo.By("Verify the volume is accessible and available to the pod by creating an empty file")
		filepath := filepath.Join("/mnt/volume1", "/emptyFile.txt")
		_, err = e2eoutput.LookForStringInPodExec(namespace, pod.Name, []string{"/bin/touch", filepath}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, pvc.Name, pv.Name, pod.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	// This test verifies the static provisioning workflow by creating the PV by
	// same name twice.
	//
	// Test Steps:
	// 1. Create File share and get the file share id.
	// 2. Create PV1 Spec with volumeID set to FCDID created in Step-1, and
	//    PersistentVolumeReclaimPolicy is set to Retain.
	// 3. Wait for the volume entry to be created in CNS.
	// 4. Delete PV1.
	// 5. Wait for PV1 to be deleted; per the cns-health-initiative design the
	//    CNS volume entry is NOT deleted, it is instead labeled
	//    pv_missing=true once full-sync observes the PV is gone.
	// 6. Create a PV2 by the same name as PV1, pointing at the same fileshare.
	// 7. Wait for full-sync to overwrite the PV-entity metadata from the
	//    reappeared PV, which clears the pv_missing label.
	// 8. Delete PV2.
	// 9. Explicitly unregister and delete the CNS volume (full-sync no longer
	//    does this as part of cleanup).
	ginkgo.It("[ef-file-vanilla][pq-n1-vanilla-file][pq-n2-vanilla-file]Verify static provisioning for file volume"+
		" workflow using same PV name twice", ginkgo.Label(p1, file, vanilla, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := connectCns(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating file share")
		cnsCreateReq := cnstypes.CnsCreateVolume{
			This:        cnsVolumeManagerInstance,
			CreateSpecs: []cnstypes.CnsVolumeCreateSpec{*getFileShareCreateSpec(defaultDatastore.Reference())},
		}
		cnsCreateRes, err := cnsmethods.CnsCreateVolume(ctx, e2eVSphere.CnsClient.Client, &cnsCreateReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		task, err := object.NewTask(e2eVSphere.Client.Client, cnsCreateRes.Returnval), nil
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		taskInfo, err := cns.GetTaskInfo(ctx, task)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fileShareVolumeID := taskResult.GetCnsVolumeOperationResult().VolumeId.Id

		// Deleting the volume with deleteDisk set to false.
		ginkgo.By("Deleting the fileshare with deleteDisk set to false")
		cnsDeleteReq := cnstypes.CnsDeleteVolume{
			This:       cnsVolumeManagerInstance,
			VolumeIds:  []cnstypes.CnsVolumeId{{Id: fileShareVolumeID}},
			DeleteDisk: false,
		}
		cnsDeleteRes, err := cnsmethods.CnsDeleteVolume(ctx, e2eVSphere.CnsClient.Client, &cnsDeleteReq)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		task, err = object.NewTask(e2eVSphere.Client.Client, cnsDeleteRes.Returnval), nil
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		taskInfo, err = cns.GetTaskInfo(ctx, task)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = cns.GetTaskResult(ctx, taskInfo)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Final cleanup: full-sync no longer unregisters CNS volumes for
		// missing PVs, so explicitly unregister and delete the underlying
		// file share once the test is done with it.
		defer func() {
			ginkgo.By("Explicitly deleting CNS volume for cleanup (cns-health-initiative)")
			err := e2eVSphere.cnsDeleteVolume(ctx, fileShareVolumeID, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fileShareVolumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		staticPVLabels := make(map[string]string)
		staticPVLabels["fileshare-id"] = strings.TrimPrefix(fileShareVolumeID, "file:")

		ginkgo.By("Creating the PV")
		pvSpec = getPersistentVolumeSpecForFileShare(fileShareVolumeID,
			v1.PersistentVolumeReclaimRetain, staticPVLabels, v1.ReadWriteMany)

		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		pvSpec.Name = "static-pv-" + curtimestring + val

		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pvSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting PV-1")
		framework.ExpectNoError(fpv.DeletePersistentVolume(ctx, client, pvSpec.Name))
		framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pvSpec.Name, poll, pollTimeout))

		// Static PV deletion (ReclaimPolicy=Retain) no longer drives CNS
		// unregister via full-sync (cns-health-initiative). Instead, once
		// full-sync observes the PV is missing (after its two-cycle grace
		// period), it labels the still-registered CNS volume pv_missing=true.
		ginkgo.By(fmt.Sprintf("Waiting for pv_missing=true label to appear on CNS volume %s", fileShareVolumeID))
		err = e2eVSphere.waitForPVMissingLabel(ctx, fileShareVolumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the CNS volume is still registered (not unregistered/deleted)")
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fileShareVolumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).To(gomega.HaveLen(1),
			"CNS volume must remain registered after missing-PV labeling")

		ginkgo.By("Creating PV-2 pointing at the same fileshare/CNS volume as PV-1")
		pv2, err := client.CoreV1().PersistentVolumes().Create(ctx, pvSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full-sync to overwrite labels from the "+
			"reappeared PV", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Verify pv_missing label is cleared now that PV-2 matches the CNS volume")
		labeled, err := e2eVSphere.hasPVMissingLabel(pv2.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(labeled).To(gomega.BeFalse(),
			"pv_missing label must be cleared once a matching K8s PV (PV-2) exists")

		ginkgo.By("Deleting PV-2")
		err = client.CoreV1().PersistentVolumes().Delete(ctx, pvSpec.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pvSpec.Name, poll, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
