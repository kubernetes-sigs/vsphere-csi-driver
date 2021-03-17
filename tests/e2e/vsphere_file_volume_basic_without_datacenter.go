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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
)

var _ = ginkgo.Describe("[csi-file-vanilla] Basic Testing without datacenter", func() {
	f := framework.NewDefaultFramework("file-volume-basic")
	var (
		client                 clientset.Interface
		namespace              string
		csiControllerNamespace string
		originalConf           string
		ctx                    context.Context
		cancel                 context.CancelFunc
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		if vanillaCluster {
			csiControllerNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		}
		bootstrap(true)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	ginkgo.AfterEach(func() {
		ginkgo.By("Reverting the secret change back to normal")
		currentSecret, err := client.CoreV1().Secrets(csiControllerNamespace).Get(ctx, configSecret, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		currentSecret.Data[vsphereCloudProviderConfiguration] = []byte(originalConf)
		_, err = client.CoreV1().Secrets(csiControllerNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restarting the controller by toggling the replica count")
		ginkgo.By("Bringing the csi-controller down")
		bringDownCsiController(client, csiControllerNamespace)
		ginkgo.By("Bringing the csi-controller up")
		bringUpCsiController(client, csiControllerNamespace)

		cancel()
	})

	/*
		Test to verify dynamic provisioning with ReadWriteMany access mode, when no storage policy and datacenter is offered
		1. Remove the datacenters in vsphere.conf secret
		2. Bootstrap with config containing no datacenter.
		3. Create StorageClass with fsType as "nfs4"
		4. Create a PVC with "ReadWriteMany" using the SC from above
		5. Wait for PVC to be Bound
		6. Get the VolumeID from PV
		7. Verify using CNS Query API if VolumeID retrieved from PV is present. Also verify Name, Capacity, VolumeType, Health matches
		8. Verify if VolumeID is created on one of the VSAN datastores from list of datacenters provided in vsphere.conf
		9. Delete PVC
		10. Delete Storage class
		11. Change back the datacenters back to normal in vsphere.conf secret.
	*/

	ginkgo.It("verify dynamic provisioning with ReadWriteMany access mode with datastoreURL is set in storage class, when no storage policy and datacenter is offered", func() {
		datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)

		ctx, cancel = context.WithCancel(context.Background())

		secret, err := client.CoreV1().Secrets(csiControllerNamespace).Get(ctx, configSecret, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		originalConf := string(secret.Data[vsphereCloudProviderConfiguration])
		vsphereCfg, err := readConfigFromSecretString(originalConf)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Changing datacenter in vsphere.conf to empty string")
		vsphereCfg.Global.Datacenters = ""

		modifiedConf, err := writeConfigToSecretString(vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Updating the secret to reflect the change")
		secret.Data[vsphereCloudProviderConfiguration] = []byte(modifiedConf)
		_, err = client.CoreV1().Secrets(csiControllerNamespace).Update(ctx, secret, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restarting the controller by toggling the replica count")
		ginkgo.By("Bringing the csi-controller down")
		bringDownCsiController(client, csiControllerNamespace)
		ginkgo.By("Bringing the csi-controller up")
		bringUpCsiController(client, csiControllerNamespace)

		testHelperForCreateFileVolumeWithDatastoreURLInSC(f, client, namespace, v1.ReadWriteMany, datastoreURL, true)

		//The vsphereCfg.Global.Datacenters will be restored in Aftereach block
	})
})
