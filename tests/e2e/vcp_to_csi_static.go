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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("[csi-vcp-mig] ravi-vcp-static", func() {
	f := framework.NewDefaultFramework("csi-vcp-mig-static")
	var (
		client         clientset.Interface
		nodeList       *v1.NodeList
		err            error
		vcpScs         []*storagev1.StorageClass
		vmdks          []string
		vcpPvcsPreMig  []*v1.PersistentVolumeClaim
		vcpPvcsPostMig []*v1.PersistentVolumeClaim
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		bootstrap()
		vcpScs = []*storagev1.StorageClass{}
		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{}
		vcpPvcsPostMig = []*v1.PersistentVolumeClaim{}
		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{}
		nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	ginkgo.It("statically provisioned VCP volumes", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating new namespace for the test")
		namespace1, err := framework.CreateTestingNS(f.BaseName, client, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Creating two vmdk1 on the shared datastore " + scParams[vcpScParamDatastoreName])
		esxHost := GetAndExpectStringEnvVar(envEsxHostIP)
		vmdk1, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk1)

		ginkgo.By("Creating PV1 with vmdk1")
		pv1 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk1), v1.PersistentVolumeReclaimDelete, nil)
		pv1.Spec.StorageClassName = vcpSc.Name
		_, err = client.CoreV1().PersistentVolumes().Create(ctx, pv1, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		ginkgo.By("Creating PVC1 with PV1 and VCP SC")
		pvc1 := getVcpPersistentVolumeClaimSpec(namespace1.Name, "", vcpSc, nil, "")
		pvc1.Spec.StorageClassName = &vcpSc.Name
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace1.Name).Create(ctx, pvc1, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc1)

		ginkgo.By("Creating PVC1 with PV1 to bind")
		_, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPreMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating two vmdk2 on the shared datastore " + scParams[vcpScParamDatastoreName])
		vmdk2, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk2)

		ginkgo.By("Creating PV2 with vmdk2")
		pv2 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk2), v1.PersistentVolumeReclaimDelete, nil)
		pv2.Spec.StorageClassName = vcpSc.Name
		_, err = client.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		ginkgo.By("Creating PVC2 with PV2 and VCP SC")
		pvc2 := getVcpPersistentVolumeClaimSpec(namespace1.Name, "", vcpSc, nil, "")
		pvc2.Spec.StorageClassName = &vcpSc.Name
		_, err = client.CoreV1().PersistentVolumeClaims(namespace1.Name).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating PVC2 with PV2 to bind")
		_, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
