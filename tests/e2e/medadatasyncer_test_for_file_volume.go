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

var _ bool = ginkgo.Describe("[csi-file-vanilla] label-updates for file volumes", func() {
	f := framework.NewDefaultFramework("e2e-file-volume-label-updates")
	var (
		client       clientset.Interface
		namespace    string
		labelKey     string
		labelValue   string
		storageclass *storagev1.StorageClass
		pvc          *v1.PersistentVolumeClaim
		fileshareID  string
		err          error
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		labelKey = "app"
		labelValue = "e2e-labels"
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if storageclass != nil {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if pvc != nil {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if fileshareID != "" {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fileshareID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		Verify label update for dynamic pv and pvc works
			1. Create StorageClass with fsType as "nfs4"
			2. Create a PVC with "ReadWriteMany" using the SC from above
			3. Wait for PVC to be Bound
			4. Update label for pvc and verify pvc label has been updated
			5. Update label for pv and verify pv label has been updated
			6. Delete PVC
			7. Delete Storage class
	*/
	ginkgo.It("verify labels are created in CNS after updating pvc and/or pv with new labels for file volume",
		ginkgo.Label(p0, file, vanilla, vc70), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ginkgo.By("Invoking test to verify labels creation for file volume")
			scParameters := make(map[string]string)
			scParameters[scParamFsType] = nfs4FSType
			// Create Storage class and PVC
			ginkgo.By(fmt.Sprintf("Creating Storage Class with %q", nfs4FSType))
			storageclass, pvc, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, "", nil, "", false, v1.ReadWriteMany)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
			pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
				[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvs).NotTo(gomega.BeEmpty())
			pv := pvs[0]
			fileshareID = pv.Spec.CSI.VolumeHandle

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

			ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
				labels, pvc.Name, pvc.Namespace))
			err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle,
				labels, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
			err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle,
				labels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})
})
