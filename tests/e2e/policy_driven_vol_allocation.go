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
	"math/rand"
	"os"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("[vol-allocation] Policy driven volume space allocation tests", func() {

	f := framework.NewDefaultFramework("e2e-spbm-policy")
	var (
		client    clientset.Interface
		namespace string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	/*
		Verify Thin, EZT, LZT volume creation via SPBM policies
		Steps:
			1	create 3 SPBM policies with thin, LZT, EZT volume allocation respectively
			2	create 3 storage classes, each with a SPBM policy created from step 1
			3	create a PVC each using the storage policies created from step 2
			4	Verify the PVCs created in step 3 are bound
			5	Create pods with using the PVCs created in step 3 and wait for them to be ready
			6	verify we can read and write on the PVCs
			7	Delete pods created in step 5
			8	Delete the PVCs created in step 3
			9	Delete the SCs created in step 2
			10	Deleted the SPBM polices created in step 1
	*/
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] Verify Thin, EZT, LZT volume creation via "+
		"SPBM policies", func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedvmfsURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedvmfsURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}

		govmomiClient := newClient(ctx, &e2eVSphere)
		pc := newPbmClient(ctx, govmomiClient)
		scParameters := make(map[string]string)
		policyNames := []string{}
		policyIDs := []*pbmtypes.PbmProfileId{}
		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		scs := []*storagev1.StorageClass{}

		rand.New(rand.NewSource(time.Now().UnixNano()))
		suffix := fmt.Sprintf("-%v-%v", time.Now().UnixNano(), rand.Intn(10000))
		categoryName := "category" + suffix
		tagName := "tag" + suffix

		catID, tagID := createCategoryNTag(ctx, categoryName, tagName)
		defer func() {
			deleteCategoryNTag(ctx, catID, tagID)
		}()

		attachTagToDS(ctx, tagID, sharedvmfsURL)
		defer func() {
			detachTagFromDS(ctx, tagID, sharedvmfsURL)
		}()

		allocationTypes := []string{
			"Conserve space when possible",
			"Fully initialized",
			"Reserve space",
		}

		ginkgo.By("create 3 SPBM policies with thin, LZT, EZT volume allocation respectively")
		ginkgo.By("create 3 storage classes, each with a SPBM policy created from step 1")
		ginkgo.By("create a PVC each using the storage policies created from step 2")
		for _, at := range allocationTypes {
			policyID, policyName := createVmfsStoragePolicy(
				ctx, pc, at, map[string]string{categoryName: tagName})
			defer func() {
				deleteStoragePolicy(ctx, pc, policyID)
			}()
			scParameters[scParamStoragePolicyName] = policyName
			policyNames = append(policyNames, policyName)
			policyIDs = append(policyIDs, policyID)
			storageclass, pvclaim, err := createPVCAndStorageClass(client,
				namespace, nil, scParameters, "", nil, "", false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvclaim)
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvclaim})
			scs = append(scs, storageclass)
		}

		defer func() {
			ginkgo.By("Delete the SCs created in step 2")
			for _, sc := range scs {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Deleted the SPBM polices created in step 1")
			for _, policyID := range policyIDs {
				_, err := pc.DeleteProfile(ctx, []pbmtypes.PbmProfileId{*policyID})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify the PVCs created in step 3 are bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the created CNS volumes are compliant and have correct policy id")
		for i, pv := range pvs {
			volumeID := pv.Spec.CSI.VolumeHandle
			storagePolicyExists, err := e2eVSphere.VerifySpbmPolicyOfVolume(volumeID, policyNames[i])
			e2eVSphere.verifyVolumeCompliance(volumeID, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(storagePolicyExists).To(gomega.BeTrue(), "storage policy verification failed")
		}

		defer func() {
			ginkgo.By("Delete the PVCs created in step 3")
			for i, pvc := range pvcs {
				err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[i].Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create pods with using the PVCs created in step 3 and wait for them to be ready")
		ginkgo.By("verify we can read and write on the PVCs")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		ginkgo.By("Delete pods created in step 5")
		deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
	})

})
