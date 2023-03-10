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
	"strconv"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("[rwm-csi-tkg] PVCs claiming the available resource in parallel", func() {
	f := framework.NewDefaultFramework("rwx-tkg-parallel")
	var (
		client            clientset.Interface
		namespace         string
		storagePolicyName string
		svcClient         clientset.Interface
		svcNamespace      string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		storagePolicyName =
			GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		if storagePolicyName == "" {
			ginkgo.Skip(envStoragePolicyNameForSharedDatastores + " env variable not set")
		}

		// Set resource quota.
		ginkgo.By("Set Resource quota for GC")
		svcClient, svcNamespace = getSvcClientAndNamespace()
		setResourceQuota(svcClient, svcNamespace, defaultrqLimit)
	})

	ginkgo.AfterEach(func() {
		svcClient, svcNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svcNamespace, rqLimit)
	})

	/*
		Concurrency Testing / Race condition to claim the available resource quota
		1. Create a rule-based storage policy and assign it to the
			supervisor cluster's namespace where GC resides of quota 100Mi
		2. Create two PVCs concurrently with "ReadWriteMany" using the SC
		from above in GC of size 100Mi
		3. Wait for either of the PVC bound to fail due to no resource left
		and one of the PVC to be bound
	*/
	ginkgo.It("Test PVCs claiming the available resource in parallel", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		var pvclaims = make([]*v1.PersistentVolumeClaim, 2)

		ginkgo.By("CNS_TEST: Running for GC setup")
		createResourceQuota(svcClient, svcNamespace, defaultrqLimit, storagePolicyName)
		defer func() {
			setResourceQuota(svcClient, svcNamespace, rqLimit)
		}()

		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if err == nil && storageclass != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))).
				NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating PVCs using the Storage Class")
		for count := 0; count < 2; count++ {
			framework.Logf("Creating PVC index %s", strconv.Itoa(count))
			pvclaims[count], err = fpv.CreatePVC(client, namespace,
				getPersistentVolumeClaimSpecWithStorageClass(namespace, defaultrqLimit, storageclass, nil, v1.ReadWriteMany))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			for count := 0; count < 2; count++ {
				if pvclaims[count].Name != "" {
					framework.Logf("Inside Defer function now for PVC index %s", strconv.Itoa(count))
					err = fpv.DeletePersistentVolumeClaim(client, pvclaims[count].Name, pvclaims[count].Namespace)
					if !apierrors.IsNotFound(err) {
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
		}()

		framework.Logf("Waiting for claims %s to be in bound state", pvclaims[0].Name)
		_, err = fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaims[0]},
			healthStatusWaitTime)
		gomega.Expect(err).To(gomega.HaveOccurred())

		framework.Logf("Waiting for claims %s to be in bound state", pvclaims[1].Name)
		_, err = fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaims[1]},
			healthStatusWaitTime)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})
})
