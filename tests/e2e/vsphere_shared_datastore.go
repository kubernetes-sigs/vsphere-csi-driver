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
	"fmt"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

/*
	Test to verify provisioning is dependant on type of datastore (shared/non-shared), when no storage policy is offered

	Steps
	1. Create StorageClass with shared/non-shared datastore.
	2. Create PVC which uses the StorageClass created in step 1.
	3. Expect:
		3a. Volume provisioning to fail if non-shared datastore
		3b. Volume provisioning to pass if shared datastore

	This test reads env
	1. SHARED_VSPHERE_DATASTORE_URL (set to shared datastore URL)
	2. NONSHARED_VSPHERE_DATASTORE_URL (set to non-shared datastor URL)
*/

var _ = ginkgo.Describe("[csi-block-vanilla] Datastore Based Volume Provisioning With No Storage Policy", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-volume-provisioning-no-storage-policy")
	var (
		client                clientset.Interface
		namespace             string
		scParameters          map[string]string
		sharedDatastoreURL    string
		nonSharedDatastoreURL string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		scParameters = make(map[string]string)
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	// Shared datastore should be provisioned successfully
	ginkgo.It("Verify dynamic provisioning of PV passes with user specified shared datastore and no storage policy specified in the storage class", func() {
		ginkgo.By("Invoking Test for user specified Shared Datastore in Storage class for volume provisioning")
		sharedDatastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		scParameters[scParamDatastoreURL] = sharedDatastoreURL
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Expect claim to pass provisioning volume as shared datastore")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to provision volume on shared datastore with err: %v", err))
	})

	// Setting non-shared datastore in the storage class should fail dynamic volume provisioning
	ginkgo.It("Verify dynamic provisioning of PV fails with user specified non-shared datastore and no storage policy specified in the storage class", func() {
		ginkgo.By("Invoking Test for user specified non-shared Datastore in storage class for volume provisioning")
		nonSharedDatastoreURL = GetAndExpectStringEnvVar(envNonSharedStorageClassDatastoreURL)
		scParameters[scParamDatastoreURL] = nonSharedDatastoreURL
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Expect claim to fail provisioning volume on non shared datastore")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		// eventList contains the events related to pvc
		expectedErrMsg := "failed to provision volume with StorageClass \"" + storageclass.Name + "\""
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		errorOccurred := checkEventsforError(client, pvclaim.Namespace, metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(errorOccurred).To(gomega.BeTrue())

	})
})
