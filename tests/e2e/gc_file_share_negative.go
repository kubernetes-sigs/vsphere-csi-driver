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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("[csi-guest][ef-vks] File Share on Non File Service enabled setups", func() {
	f := framework.NewDefaultFramework("e2e-file-volumes")

	var (
		client            clientset.Interface
		namespace         string
		scParameters      map[string]string
		storagePolicyName string
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
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, rqLimit)
	})

	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
	})

	/*
		Create and check if File share creation fails on non file service enabled setup
		1. Create a SC
		2. Create a PVC with "ReadWriteMany" access mode using the SC from above in GC
		3. Expect the PVC creation fails
		4. Verify the error message returned on PVC failure is correct
		5. Delete PVC in GC
	*/

	/*
		Create and check if File share creation fails on non file service enabled setup
		1. Create a SC
		2. Create a PVC with "ReadOnlyMany" access mode using the SC from above in GC
		3. Expect the PVC creation fails
		4. Verify the error message returned on PVC failure is correct
		5. Delete PVC in GC
	*/
	ginkgo.It("Verify File share creation fails on non-file service enabled setup for TKG", ginkgo.Label(p2,
		file, tkg, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim

		var err error
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("Creating Storage Class and PVC")
		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Creating a Storage class and PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		//Waiting for the bound timeout as the events messages are dynamic
		ginkgo.By("Expect claim to fail")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.SingleCallTimeout/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Creating ReadOnly PVC")
		pvclaim, err = fpv.CreatePVC(ctx, client, namespace, getPersistentVolumeClaimSpecWithStorageClass(namespace,
			diskSize, storageclasspvc, nil, v1.ReadOnlyMany))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if pvclaim != nil {
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
				if !apierrors.IsNotFound(err) {
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		//Waiting for the bound timeout as the events messages are dynamic
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.SingleCallTimeout/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

})
