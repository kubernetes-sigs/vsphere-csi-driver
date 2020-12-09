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
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

/*
	Test to verify disk size specified in PVC is being honored during volume creation.

	Steps
	1. Create StorageClass.
	2. Create PVC with valid disk size.
	3. Expect PVC to pass
	4. Verify disk size specified is being honored
*/

var _ = ginkgo.Describe("[csi-block-e2e] Volume Disk Size ", func() {
	f := framework.NewDefaultFramework("volume-disksize")
	var (
		client       clientset.Interface
		namespace    string
		scParameters map[string]string
		datastoreURL string
		pvclaims     []*v1.PersistentVolumeClaim
	)
	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = f.Namespace.Name
		scParameters = make(map[string]string)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	// Test for valid disk size of 2Gi
	ginkgo.It("Verify dynamic provisioning of pv using storageclass with a valid disk size passes", func() {
		ginkgo.By("Invoking Test for valid disk size")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
		defer framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)

		ginkgo.By("Expect claim to provision volume successfully")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := framework.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", persistentvolumes[0].Spec.CSI.VolumeHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(persistentvolumes[0].Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("Error: QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size specified in PVC in honored")
		if queryResult.Volumes[0].BackingObjectDetails.CapacityInMb != diskSizeInMb {
			err = fmt.Errorf("Wrong disk size provisioned ")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
