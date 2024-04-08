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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("Alpha feature check", func() {

	f := framework.NewDefaultFramework("alpha-features")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		scParameters                map[string]string
		pvtoBackingdiskidAnnotation string = "cns.vmware.com/pv-to-backingdiskobjectid-mapping"
		datastoreURL                string
	)
	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	// Verify pvc is annotated in block vanilla setup.
	// Steps
	// Create a Storage Class.
	// Create a PVC using above SC.
	// Wait for PVC to be in Bound phase.
	// Verify annotation is not added on the PVC.
	// Delete PVC.
	// Verify PV entry is deleted from CNS.
	// Delete the SC.

	ginkgo.It("[csi-block-vanilla-alpha-feature][pv-to-backingdiskobjectid-mapping] Verify pvc is annotated", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		featureEnabled := isCsiFssEnabled(ctx, client, GetAndExpectStringEnvVar(envCSINamespace),
			"pv-to-backingdiskobjectid-mapping")
		gomega.Expect(featureEnabled).To(gomega.BeTrue())
		ginkgo.By("Invoking Test volume status")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pvclaims []*v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace,
			nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
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
		ginkgo.By("Expect annotation is added on the pvc")
		waitErr := wait.PollUntilContextTimeout(ctx, pollTimeoutShort, pollTimeoutSixMin*3, true,
			func(ctx context.Context) (bool, error) {
				var err error
				ginkgo.By(fmt.Sprintf("Sleeping for %v minutes", pollTimeoutShort))
				pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
				if err != nil {
					return false, fmt.Errorf("error fetching pvc %q for checking pvtobackingdisk annotation: %v", pvc.Name, err)
				}
				pvbackingAnnotation := pvc.Annotations[pvtoBackingdiskidAnnotation]
				if pvbackingAnnotation != "" {
					return true, nil
				}
				return false, nil
			})

		gomega.Expect(waitErr).NotTo(gomega.HaveOccurred())
	})

	// Verify pvc is not annotated in file vanilla setup.
	// Steps
	// Create a Storage Class.
	// Create a PVC using above SC.
	// Wait for PVC to be in Bound phase.
	// Verify annotation is not added on the PVC.
	// Delete PVC.
	// Verify PV entry is deleted from CNS.
	// Delete the SC.

	ginkgo.It("[csi-file-vanilla-alpha-feature][pv-to-backingdiskobjectid-mapping] "+
		"File Vanilla Verify pvc is not annotated", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		featureEnabled := isCsiFssEnabled(ctx, client, GetAndExpectStringEnvVar(envCSINamespace),
			"pv-to-backingdiskobjectid-mapping")
		gomega.Expect(featureEnabled).To(gomega.BeTrue())
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType
		accessMode := v1.ReadWriteMany
		// Create Storage class and PVC.
		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessMode, nfs4FSType))
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace,
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
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s", queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType))

		ginkgo.By("Verifying volume type specified in PVC is honored")
		if queryResult.Volumes[0].VolumeType != testVolumeType {
			err = fmt.Errorf("volume type is not %q", testVolumeType)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v minutes", pollTimeoutSixMin))
		time.Sleep(pollTimeoutSixMin)
		ginkgo.By("Expect annotation is not added on the pvc")
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for describe := range pvc.Annotations {
			gomega.Expect(pvc.Annotations[describe]).ShouldNot(gomega.BeEquivalentTo(pvtoBackingdiskidAnnotation))
		}
	})
})
