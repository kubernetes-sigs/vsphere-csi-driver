/*
	Copyright 2023 The Kubernetes Authors.

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

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-multi-vc-topology] Multi-VC", func() {
	f := framework.NewDefaultFramework("csi-multi-vc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		topologyLength              int
		bindingMode                 storagev1.VolumeBindingMode
		allowedTopologyLen          int
		nodeAffinityToSet           bool
		parallelStatefulSetCreation bool
		stsReplicas                 int32
		startIndex                  int
		endIndex                    int
		parallelPodPolicy           bool
		scParameters                map[string]string
		storagePolicyName           string
		storagePolicyName2          string
		multipleStatefulSet         bool
		stsCount                    int
	)
	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()

		topologyLength = 5
		nodeAffinityToSet = false
		parallelStatefulSetCreation = false
		parallelPodPolicy = false
		stsReplicas = 3
		multipleStatefulSet = false
		stsCount = 1

		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		storagePolicyName2 = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores2)

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		TESTCASE-1
		Stateful set with SC contains default parameters and WFC binding mode and
		specific nodeaffinity details in statefullset

		Steps:
		1. Create SC default values, so all the AZ's should be consided for provisioning.
		2. create statefull set with parallel pod management policy
		3. Wait for PVC to reach bound state and POD to reach Running state
		4. Volumes should get distributed among the nodes which are mentioned in node affinity of
		Statefullset yaml
		5. Make sure common validation points are met
		a) Verify the PV node affinity details should have appropriate Node details
		b) The POD's should be running on the appropriate nodes
		c) CNS metadata
		6. Scale up / scale down the statefulset , and verify the common validation points on newly
		created statefullset
		7. Clean up the data
	*/

	ginkgo.It("TC-1", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		parallelPodPolicy = true

		ginkgo.By("Create StorageClass with no allowed topolgies specified")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create StatefulSet with parallel pod management policy")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, multipleStatefulSet, stsCount)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation)

		// stsReplicas = 1
		// ginkgo.By("Scale down statefulset replica count to 1")
		// scaleDownStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, isParallelStatefulSetCreation)

		// stsReplicas = 8
		// ginkgo.By("Scale up statefulset replica count to 8")
		// scaleUpStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, isParallelStatefulSetCreation)

		// ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		// verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
		// 	namespace, allowedTopologies, isParallelStatefulSetCreation)
	})

	/*
		TESTCASE-2
		Deploy workload  With  allowed topology details in SC , Which should contain all the AZs,
		so that workload will get distributed among all the VC's

		Steps:
		1. Create SC with allowedTopology details contains more than one Availability zone details
		2. Create statefull set with replica-5
		3. Wait for PVC to reach bound state and POD to reach Running state
		4. Volumes should get distributed among all the Availability zones
		5. Make sure common validation points are met
		a) Verify the PV node affinity details should have appropriate Node details
		b) The POD's should be running on the appropriate nodes
		c) CNS metadata
		6. Scale up / scale down the statefulset and verify the common validation points on newly
		created statefullset
		7. Clean up the data
	*/

	ginkgo.It("TC-2", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		startIndex = 0
		endIndex = 3
		stsReplicas = 5

		ginkgo.By("Set specific allowed topology required for Storage Class creation")
		allowedTopologies := setSpecificAllowedTopology(allowedTopologies, startIndex, endIndex)

		ginkgo.By("Create StorageClass with specific allowed topolgies details")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create StatefulSet with parallel pod management policy")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, multipleStatefulSet,
			stsCount)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation)
	})

	/*
		TESTCASE-3
		Deploy workload with Specific storage policy name available in Single VC

		Steps:
		1. Create a SC with storage policy name available in single VC
		2. Create statefull set with replica-5
		3. Wait for PVC to reach bound state and POD to reach Running state
		4. Volumes should get created under appropriate nodes which is accessible to  the storage Policy
		5. Make sure common verification Points met in PVC, PV ad POD
		a) Verify the PV node affinity details should have appropriate Node details
		b) The POD's should be running on the appropriate nodes
		c) CNS metadata
		6. Scale up / scale down the statefulset and verify the common validation points on newly created statefullset
		7. Clean up the data
	*/

	ginkgo.It("TC-3", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 5
		scParameters[scParamStoragePolicyName] = storagePolicyName
		startIndex = 0
		endIndex = 2

		ginkgo.By("Create StorageClass with storage policy specified")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create StatefulSet with default pod management policy")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, multipleStatefulSet, stsCount)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, startIndex, endIndex)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation)
	})

	/*
		TESTCASE-4
		Same Policy  is available in two VC's

		Steps:
		1. Create a SC with Storage policy name available in VC1 and VC2
		2. Create  two statefull set with replica-5
		3. Wait for PVC to reach bound state and POD to reach Running state
		4. Since both the VC's have the same storage policy , volume should get distributed among all the availability zones
		5. Make sure common verification Points met in PVC, PV ad POD
		a) Verify the PV node affinity details should have appropriate Node details
		b) The POD's should be running on the appropriate nodes
		c) CNS metadata
		6. Scale up / scale down the statefulset and verify the common validation points on newly created statefullset
		7. Clean up the data
	*/

	ginkgo.It("TC-4", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 5
		scParameters[scParamStoragePolicyName] = storagePolicyName2
		startIndex = 0
		endIndex = 2

		ginkgo.By("Create StorageClass with storage policy specified")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create StatefulSet with default pod management policy")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, multipleStatefulSet, stsCount)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, startIndex, endIndex)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation)
	})

})
