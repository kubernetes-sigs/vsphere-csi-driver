/*
Copyright 2022 The Kubernetes Authors.

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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("Orphan volumes detection and cleanup", func() {

	f := framework.NewDefaultFramework("orphan-volume-test")

	var (
		client              clientset.Interface
		namespace           string
		defaultDatacenter   *object.Datacenter
		defaultDatastore    *object.Datastore
		pandoraSyncWaitTime int
		datastoreURL        string
		ctx                 context.Context
		scale               int
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
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
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}
		scale = 100
	})

	/*
		Introduce orphan volumes into the setup and perform cleanup through FCD creation

		1. Create 100 FCDs
		2. Create PVs with RetentionPolicy set to Retain
		3. Create PVCs with the PVs created above
		4. Create Pods with the PVCs created above
		5. Wait and Verify CNS contains metadata for all volumes in it
		6. Cleanup Pods, PVCs, PVs
		7. Make API call /GET OrphanVolumes
		8. Verify all the 100 FCDs are listed as orphan volumes and are also detached
		9. Make API call /DELETE OrphanVolumes
		10. Verify all of the orphan volumes are cleaned-up successfully
	*/
	ginkgo.It("[calatrava] Orphan volumes detection and cleanup", func() {
		var err error

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var fcdIds []string
		var pvs []*v1.PersistentVolume
		var pvcs []*v1.PersistentVolumeClaim
		var pods []*v1.Pod

		for i := 0; i < scale; i++ {
			ginkgo.By("Creating FCD Disk")
			fcdID, err := e2eVSphere.createFCD(ctx, "BasicStaticFCD", diskSizeInMb, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			fcdIds = append(fcdIds, fcdID)
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCDs to sync with pandora",
			pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		// Creating label for PV.
		// PVC will use this label as Selector to find PV.

		for i := 0; i < scale; i++ {
			var pv *v1.PersistentVolume

			staticPVLabels := make(map[string]string)
			staticPVLabels["fcd-id"] = fcdIds[i]

			ginkgo.By(fmt.Sprintf("Creating the PV %d", i))
			pv = getPersistentVolumeSpec(fcdIds[i], v1.PersistentVolumeReclaimDelete, staticPVLabels)
			pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
			if err != nil {
				return
			}
			err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvs = append(pvs, pv)
		}

		for i := 0; i < scale; i++ {
			ginkgo.By(fmt.Sprintf("Creating the PVC %d", i))
			staticPVLabels := make(map[string]string)
			staticPVLabels["fcd-id"] = fcdIds[i]

			var pvc *v1.PersistentVolumeClaim
			pvc = getPersistentVolumeClaimSpec(namespace, staticPVLabels, pvs[i].Name)
			pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Wait for PV and PVC to Bind.
			framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(),
				namespace, pvs[i], pvc))

			pvcs = append(pvcs, pvc)
		}

		for i := 0; i < scale; i++ {
			ginkgo.By("Verifying CNS entry is present in cache")
			_, err = e2eVSphere.queryCNSVolumeWithResult(pvs[i].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for i := 0; i < scale; i++ {
			ginkgo.By(fmt.Sprintf("Creating the Pod %d", i))
			var pvclaims []*v1.PersistentVolumeClaim
			var pod *v1.Pod
			pvclaims = append(pvclaims, pvcs[i])
			pod, err := createPod(client, namespace, nil, pvclaims, false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pods = append(pods, pod)
		}

		for i := 0; i < scale; i++ {
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pvs[i].Spec.CSI.VolumeHandle, pods[i].Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pods[i].Spec.NodeName)
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pvs[i].Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}

		for i := 0; i < scale; i++ {
			ginkgo.By("Verify the volume is accessible and available to the pod by creating an empty file")
			filepath := filepath.Join("/mnt/volume1", "/emptyFile.txt")
			_, err = framework.LookForStringInPodExec(namespace, pods[i].Name,
				[]string{"/bin/touch", filepath}, "", time.Minute)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify container volume metadata is present in CNS cache")
			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolume with VolumeID: %s", pvs[i].Spec.CSI.VolumeHandle))
			_, err = e2eVSphere.queryCNSVolumeWithResult(pvs[i].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			labels := []types.KeyValue{{Key: "fcd-id", Value: fcdIds[i]}}
			ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
			err = verifyVolumeMetadataInCNS(&e2eVSphere, pvs[i].Spec.CSI.VolumeHandle,
				pvcs[i].Name, pvs[i].ObjectMeta.Name, pods[i].Name, labels...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for i := 0; i < scale; i++ {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(client, pods[i]), "Failed to delete pod", pods[i].Name)
		}

		for i := 0; i < scale; i++ {
			ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", pods[i].Spec.NodeName))
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pvs[i].Spec.CSI.VolumeHandle, pods[i].Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")
		}

		for i := 0; i < scale; i++ {
			ginkgo.By("Deleting the PV Claim")
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, pvcs[i].Name, namespace),
				"Failed to delete PVC", pvcs[i].Name)
		}

		for i := 0; i < scale; i++ {
			ginkgo.By("Verify PV should be deleted automatically")
			framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pvs[i].Name, poll, pollTimeout))
		}

	})

})
