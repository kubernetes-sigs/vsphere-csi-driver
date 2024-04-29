/*
Copyright 2021 The Kubernetes Authors.

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
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("[rwm-csi-tkg] File Volume Provision with Non-VSAN datastore", func() {
	f := framework.NewDefaultFramework("rwx-tkg-non-vsan")
	var (
		client                   clientset.Interface
		namespace                string
		scParameters             map[string]string
		nonVsanStoragePolicyName string
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		nonVsanStoragePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForNonSharedDatastores)
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
	})

	/*
		Test to verify file volume provision on non-vsan datastore - ReadWriteMany

		Steps
		1. Create StorageClass with Non-VSAN datastore
		2. Create PVC with RWM mode which uses the StorageClass created in step 1
		3. Check for the PVC provision fail
		4. Delete PVC
	*/
	ginkgo.It("Verify RWX ReadWriteMany volume provision on non-vsan datastore fails", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = nonVsanStoragePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as the storage policy mentioned in Storage class " +
			"has non-vSAN compliant datastores")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	/*
		Test to verify file volume provision on non-vsan datastore - ReadOnlyMany

		Steps
		1. Create StorageClass with Non-VSAN datastore
		2. Create PVC with RWM mode which uses the StorageClass created in step 1
		3. Check for the PVC provision fail
		4. Delete PVC
	*/
	ginkgo.It("Verify RWX ReadOnlyMany volume provision on non-vsan datastore", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = nonVsanStoragePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadOnlyMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as the storage policy mentioned in Storage class " +
			"has non-vSAN compliant datastores")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

})
