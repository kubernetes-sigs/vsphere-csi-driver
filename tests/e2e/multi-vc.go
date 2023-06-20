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
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
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
		parallelPodPolicy           bool
		scParameters                map[string]string
		storagePolicyName           string
		storagePolicyName2          string
		topValStartIndex            int
		topValEndIndex              int
		topkeyStartIndex            int
		datastoreURL                string
		otherdatastoreURL           string
		podAntiAffinityToSet        bool
	)
	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name

		multiVCbootstrap()

		topologyLength = 5
		nodeAffinityToSet = false
		parallelStatefulSetCreation = false
		parallelPodPolicy = false
		stsReplicas = 3

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
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		otherdatastoreURL = GetAndExpectStringEnvVar(envNonSharedStorageClassDatastoreURL)

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

	/* TESTCASE-1

	Stateful set with SC contains default parameters and WFC binding mode and
	specific nodeaffinity details in statefullset

	Steps:
	1. Create SC default values so all the AZ's should be consided for provisioning.
	2. Create Statefulset with parallel pod management policy
	3. Wait for PVC to reach bound state and POD to reach Running state
	4. Volumes should get distributed among the nodes which are mentioned in node affinity of
	Statefullset yaml
	5. Make sure common validation points are met
	a) Verify the PV node affinity details should have appropriate node details
	b) The Pods should be running on the appropriate nodes
	c) CNS metadata
	6. Scale-up /Scale-down the statefulset and verify the common validation points on newly
	created statefullset
	7. Clean up the data
	*/

	ginkgo.It("Verify statefulset creation on a multivc with sts having specific node affinity "+
		"with no given allowed topology", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		parallelPodPolicy = true
		nodeAffinityToSet = true
		allowedTopologyLen = 3
		topValStartIndex = 0
		topValEndIndex = 3

		ginkgo.By("Create StorageClass with no allowed topolgies specified and with WFC binding mode")
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

		ginkgo.By("Create StatefulSet with parallel pod management policy and with node affinity set")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		allowedTopologies := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)

		stsReplicas = 1
		ginkgo.By("Scale down statefulset replica count to 1")
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas,
			parallelStatefulSetCreation, true)

		stsReplicas = 4
		ginkgo.By("Scale up statefulset replica count to 6")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas,
			parallelStatefulSetCreation, true)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})

	/*
		TESTCASE-2
		Deploy workload with allowed topology details in SC which should contain all the AZ's
		so that workload will get distributed among all the VC's

		Steps:
		1. Create SC with allowedTopology details contains more than one Availability zone details
		2. Create statefull set with replica-5
		3. Wait for PVC to reach bound state and POD to reach Running state
		4. Volumes should get distributed among all the Availability zones
		5. Make sure common validation points are met
		a) Verify the PV node affinity details should have appropriate Node details
		b) The Pods should be running on the appropriate nodes
		c) CNS metadata
		6. Scale up / scale down the statefulset and verify the common validation points on newly
		created statefullset
		7. Clean up the data
	*/

	ginkgo.It("TC-2", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topValStartIndex = 2
		topValEndIndex = 5
		topkeyStartIndex = 0
		stsReplicas = 5

		// Setting specific allowed topology required for Storage Class creation
		allowedTopologies := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex, topValEndIndex)

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

		ginkgo.By("Create StatefulSet with 5 replicas")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)

		stsReplicas = 1
		ginkgo.By("Scale down statefulset replica count to 1")
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		stsReplicas = 6
		ginkgo.By("Scale up statefulset replica count to 6")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
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
		b) The Pods should be running on the appropriate nodes
		c) CNS metadata
		6. Scale-up/Scale-down the statefulset and verify the common validation points on newly created statefullset
		7. Clean up the data
	*/

	ginkgo.It("TC-3", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 5
		scParameters[scParamStoragePolicyName] = storagePolicyName
		topValStartIndex = 0
		topValEndIndex = 2

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
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex, topValEndIndex)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)

		stsReplicas = 3
		ginkgo.By("Scale down statefulset replica count to 1")
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		stsReplicas = 9
		ginkgo.By("Scale up statefulset replica count to 6")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})

	/*
		TESTCASE-4
		Same Policy  is available in two VC's

		Steps:
		1. Create a SC with Storage policy name available in VC1 and VC2
		2. Create  two statefull set with replica-5
		3. Wait for PVC to reach bound state and POD to reach Running state
		4. Since both the VC's have the same storage policy , volume should get distributed among all the
		availability zones
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
		topValStartIndex = 0
		topValEndIndex = 3
		sts_count := 2
		parallelStatefulSetCreation = true

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

		ginkgo.By("Create 2 StatefulSet with replica count 5")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, stsReplicas)
		var wg sync.WaitGroup
		wg.Add(2)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				stsReplicas, &wg)

		}
		wg.Wait()

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		time.Sleep(60 * time.Second)

		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], stsReplicas)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			// Get list of Pods in each StatefulSet and verify the replica count
			ssPods := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
			gomega.Expect(len(ssPods.Items) == int(stsReplicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
		defer func() {
			deleteAllStatefulSetAndPVs(client, namespace)
		}()

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, parallelStatefulSetCreation, true)
		}

		stsReplicas = 3
		ginkgo.By("Scale down statefulset replica count to 1")
		scaleDownStatefulSetPod(ctx, client, statefulSets[0], namespace, stsReplicas,
			parallelStatefulSetCreation, true)

		stsReplicas = 7
		ginkgo.By("Scale up statefulset replica count to 6")
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, stsReplicas,
			parallelStatefulSetCreation, true)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[0],
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})

	/*
		TESTCASE-5
		Pod affiity tests

		Steps:
		1. Create SC default values so all the AZ's should be considered for provisioning.
		2. Create statefullset with Pod affinity rules such a way that each AZ should get atleast 1 statefulset
		3. Wait for PVC to bound and POD to reach running state
		4. Verify the stateful set distribution
		5. Make sure common verification Points met in PVC, PV ad POD
		a) Verify the PV node affinity details should have appropriate Node details
		b) The Pods should be running on the appropriate nodes
		c) CNS metadata
		6. Scale-up/Scale-down the statefulset and verify the common validation points on newly created
		statefullset
		7. Clean up data
	*/

	ginkgo.It("TC-5", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 5
		nodeAffinityToSet = true
		allowedTopologyLen = 5
		podAntiAffinityToSet = true

		ginkgo.By("Create StorageClass with storage policy specified")
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

		ginkgo.By("Create StatefulSet with default pod management policy")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)

		stsReplicas = 2
		ginkgo.By("Scale down statefulset replica count to 1")
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		stsReplicas = 8
		ginkgo.By("Scale up statefulset replica count to 6")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})

	/*
		TESTCASE-6
		Deploy workload with specific policy with allowed topology and Datastore URL

		Steps:
		1. Create a SC with allowed topology and appropriate datastore url
		2. Create Statefulset with replica-5
		3. Wait for PVC to reach bound state and POD to reach Running state
		4. Volumes should get created under appropriate availability zone and on the specified datastore
		5. Make sure common validation points are met
		6. Verify the PV node affinity details should have appropriate node details
		7. The Pods should be running on the appropriate nodes
		8. Scale-up/Scale-down the statefulset
		9. Verify the node affinity details also verify the Pod details
		10. Clean up the data
	*/

	ginkgo.It("TC-6", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 5
		scParameters[scParamDatastoreURL] = datastoreURL
		topValStartIndex = 0
		topValEndIndex = 1

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with allowed topology, storage-policy and with datastore url")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, allowedTopologies, "",
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

		ginkgo.By("Create StatefulSet with replica set 5")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)

		stsReplicas = 1
		ginkgo.By("Scale down statefulset replica count to 1")
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		stsReplicas = 6
		ginkgo.By("Scale up statefulset replica count to 6")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})

	/*
		TESTCASE-7
		Deploy workload with allowed topology details in SC specific to VC1  with Immediate Binding

		Steps:
		1. Create SC with allowedTopology details set to VC1 availability zone
		2. Create statefull set with replica-3
		3. Wait for PVC to reach bound state and POD to reach Running state
		4. Make sure common validation points are met
		a) Verify the PV node affinity details should have appropriate Node details
		b) The POD's should be running on the appropriate nodes which are present in VC1
		5. Scale up / scale down the statefulset
		6. Verify the node affinity details. Verify the POD details. All the pods should come up on the
		nodes of VC1
		7. Clean up the data
	*/

	ginkgo.It("TC-7", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 3
		topValStartIndex = 0
		topValEndIndex = 1

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with allowed topology details of VC1")
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

		ginkgo.By("Create StatefulSet with replica set 3")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)

		// stsReplicas = 1
		// ginkgo.By("Scale down statefulset replica count to 1")
		// scaleDownStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		// stsReplicas = 6
		// ginkgo.By("Scale up statefulset replica count to 6")
		// scaleUpStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		// ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		// verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
		// 	namespace, allowedTopologies, parallelStatefulSetCreation)
	})

	/*
		TESTCASE-8
		Deploy workload with allowed topology details in SC specific to VC2  + WFC binding mode +
		default pod management policy

		Steps:
		1. Create SC with allowedTopology details set to VC2's Availability Zone
		2. Create statefull set with replica-3
		3. Wait for PVC to reach bound state and Pod to reach running state
		4. Make sure common validation points are met
		a) Verify the PV node affinity details should have appropriate node details
		b) The Pods should be running on the appropriate nodes which are present in VC2
		5. Scale-up/scale-down the statefulset
		6. Verify the node affinity details. Verify the pod details. All the pods should come up on the nodes of
		VC2
		7. Validate CNS metadata on appropriate VC
		8. Clean up the data
	*/

	ginkgo.It("TC-8", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 3
		topValStartIndex = 4
		topValEndIndex = 5

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with allowed topology details of VC2")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
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

		ginkgo.By("Create StatefulSet with replica set 3")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)

		// stsReplicas = 1
		// ginkgo.By("Scale down statefulset replica count to 1")
		// scaleDownStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		// stsReplicas = 6
		// ginkgo.By("Scale up statefulset replica count to 6")
		// scaleUpStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		// ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		// verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
		// 	namespace, allowedTopologies, parallelStatefulSetCreation)
	})

	/*
		TESTCASE-9
		Deploy workload with  allowed topology details in SC specific to VC3 + parallel pod management policy

		Steps:
		1. Create SC with allowedTopology details set to VC3 availability zone
		2. Create statefull set with replica-3
		3. Wait for PVC to reach bound state and POD to reach Running state
		4. Make sure common validation points are met
		a) Verify the PV node affinity details should have appropriate Node details
		b) The Pod should be running on the appropriate nodes which are present in VC3
		5. Scale up / scale down the statefulset
		6. Verify the node affinity details and also verify the pod details. All the pods should come up on the nodes of
		VC3
		7. Clean up the data
	*/

	ginkgo.It("TC-9 -------> to be tested on type-2 setup", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 3
		allowedTopologyLen = 1
		topValStartIndex = 4
		topValEndIndex = 5

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with allowed topology details of VC2")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
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

		ginkgo.By("Create StatefulSet with replica set 3")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})

	/*
		TESTCASE-10
		Deploy workload with default SC parameters with WaitForFirstConsumer

		Steps:
		1. Create a storage class with default parameters
		a) SC1 with WFC
		b) SC2 with Immediate
		2. Create statefulset with replica-5 using SC1
		3. Cretate few dynamic PVCs using SC2 and create Pods using the same PVCs
		4. Wait for PVC to reach bound state and POD to reach Running state
		5. Make sure common validation points are met
		a) Volumes should get distributed among all the availability zones
		b) Verify the PV node affinity details should have appropriate Node details
		c) The Pods should be running on the appropriate nodes
		6. Scale up / scale down the statefulset
		7. Clean up the data
	*/

	ginkgo.It("TC-10", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 2
		pvcCount := 1
		var podList []*v1.Pod

		ginkgo.By("Create StorageClass with default parameters using WFC binding mode")
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

		ginkgo.By("Create StatefulSet with replica set 5")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			deleteAllStatefulSetAndPVs(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)

		ginkgo.By("Create StorageClass with default parameters using Immediate binding mode")
		storageclass, err := createStorageClass(client, nil, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount)

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())

			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(client,
				pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			// cleanup code for deleting PVC
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify volume is detached from the node")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
					pv.Spec.CSI.VolumeHandle, podList[i].Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))
			}
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(podList); i++ {
			verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, podList[i],
				namespace, allowedTopologies, true)
		}

		stsReplicas = 1
		ginkgo.By("Scale down statefulset replica count to 1")
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		stsReplicas = 3
		ginkgo.By("Scale up statefulset replica count to 6")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, stsReplicas, parallelStatefulSetCreation, true)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})

	/*
		TESTCASE-11
		Create SC with single AllowedTopologyLabel

		Steps:
		1. Create a SC with specific topology details which is available in any one VC
		2. Create statefulset with replica-5
		3. Wait for PVC to reach Bound state and Pod to reach Running state
		4. Make sure common validation points are met
		a) Volumes should get created under appropriate zone
		b) Verify the PV node affinity details should have appropriate node details
		c) The Pods should be running on the appropriate nodes
		5. Scale-up/scale-down the statefulset
		6. Verify the node affinity details and also verify the pod details
		7. Clean up the data
	*/

	ginkgo.It("TC-11", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 5
		allowedTopologyLen = 1
		topValStartIndex = 1
		topValEndIndex = 2

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with allowed topology details of VC1")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
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

		ginkgo.By("Create StatefulSet with replica set 5")
		statefulset := createCustomisedStatefulSets(client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})

	/*
		TESTCASE-12
		Create PVC using the wrong StoragePolicy name. Consider partially matching storage policy

		Steps:
		1. Use the partially matching storahe policy and create PVC
		2. PVC should not go to bound, appropriate error should be shown
		3. Perform cleanup
	*/

	ginkgo.It("TC-12", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		scParameters[scParamStoragePolicyName] = storagePolicyName

		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Expect claim to fail provisioning volume within the topology")
		framework.ExpectError(fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, framework.PollShortTimeout, pollTimeoutShort))
		eventList, _ := client.CoreV1().Events(pvclaim.Namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(eventList.Items).NotTo(gomega.BeEmpty())
		expectedErrMsg := "failed to create volume."
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))

	})

	/*
		TESTCASE-13
		Deploy workload With allowed topology of VC1 and datastore url which is in VC2

		Steps:
		1. Create SC with allowed topology which matches VC1 details and datastore url which is in VC2
		2. PVC should not go to bound, appropriate error should be shown
	*/

	ginkgo.It("TC-13", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		allowedTopologyLen = 2
		topValStartIndex = 0
		topValEndIndex = 1
		scParameters[scParamDatastoreURL] = otherdatastoreURL

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Expect claim to fail provisioning volume within the topology")
		framework.ExpectError(fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, framework.PollShortTimeout, pollTimeoutShort))
		eventList, _ := client.CoreV1().Events(pvclaim.Namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(eventList.Items).NotTo(gomega.BeEmpty())
		expectedErrMsg := "failed to create volume."
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
	})

	/*
		TESTCASE-14
		Create storage policy in VC1 and VC2 and create storage class with the same and delete Storage policy
		in VC1, expected to go to VC2

		Steps:
		1. Create Storage policy in VC1 and VC2
		2. Create Storage class with above policy
		3. Delete storage policy from VC1
		4. Create statefulSet
		5. Expected to provision volume on VC2
		6. Make sure common validation points are met
		7. Clear data
	*/

	ginkgo.It("TC-14", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		parallelPodPolicy = true
		nodeAffinityToSet = true
		allowedTopologyLen = 3
		topValStartIndex = 0
		topValEndIndex = 3

		ginkgo.By("Create StorageClass with no allowed topolgies specified and with WFC binding mode")
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
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		allowedTopologies := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex, topValEndIndex)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})

	/*
		TESTCASE-15
		Statically Create PVC and Verify NodeAffinity details on PV
		Make sure node affinity details are mentioned while creating PV

		Steps:
		1. Create SC which point to ay one VC's allowed topology Availability zone
		2. Create PV using the above created SC and reclaim policy retain , PV should have appropriate node affinity details mentioned
		3. Create PVC using above created SC and PV
		4. Wait for PV ,PVC to be in bound
		5. Make sure common validation points are met on PV,PVC and POD
			a) Once PV and PVC are bound describe on the PV and verify node affinity details on PV
			b) Create POD using the above PVC
			c) POD should come up on the node which is present in the same zone as mentioned in storage class
		6. Clean up data
	*/

	ginkgo.It("TC-15", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topValStartIndex = 0
		topValEndIndex = 2

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with allowed topology details of VC1")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
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
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		allowedTopologies := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex, topValEndIndex)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})

	/*
		TESTCASE-16
		Create Deployment pod using SC with allowed topology set to Specific VC

		Steps:
		1. Create SC with allowedTopology details set to any one VC with Availability Zone
		2. Create PVC using above SC
		3. Wait for PVC to reach bound state
		4. Create deployment and wait for  POD to reach Running state
		5. Make sure common validation points are met on PV,PVC and POD
			a) Verify the PV node affinity details should have appropriate Node details
			b) The POD's should be running on the appropriate nodes which are present in VC1
			c) Verify the node affinity details also verify the POD details.
			 All the POd's should come up on the nodes of VC
		6. Clean up the data
	*/

	ginkgo.It("TC-16", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topValStartIndex = 2
		topValEndIndex = 3
		var lables = make(map[string]string)
		lables["app"] = "nginx"
		replica := 1

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass and PVC for Deployment")
		sc, pvclaim, err := createPVCAndStorageClass(client, namespace, nil,
			nil, diskSize, allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PVC to be in Bound phase
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims,
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()

		ginkgo.By("Create Deployments")
		deployment, err := createDeployment(ctx, client, int32(replica), lables,
			nil, namespace, pvclaims, "", false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running " +
			"on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx, client, deployment,
			namespace, allowedTopologies, false, true)
	})

	/*
		TESTCASE-17
		Create SC with invalid allowed topology details details - NEGETIVE

		Steps:
		1. Create SC with invalid label details
		2. Create PVC, Pvc should not reach bound state. It should throuw error
	*/

	ginkgo.It("TC-17", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topValStartIndex = 1
		topValEndIndex = 1

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		allowedTopologies[0].Values = []string{"new-zone"}

		/* Create SC with Immediate BindingMode and allowed topology set to
		invalid topology label in any one level */
		storageclass, err := createStorageClass(client, nil, allowedTopologies, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create PVC using above SC
		pvc, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if pvc != nil {
				ginkgo.By("Delete the PVC")
				err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Expect PVC claim to fail as invalid topology label is given in Storage Class
		ginkgo.By("Expect claim to fail as invalid topology label is specified in Storage Class")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvc.Namespace, pvc.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		framework.Logf("Volume Provisioning Failed for PVC %s due to invalid topology "+
			"label given in Storage Class", pvc.Name)
	})

	/*
		TESTCASE-18
		Verify online and offline Volume expansion

			Steps:
			1. Create SC default values, so all the AZ's should be considered for provisioning.
			2. Create PVC  and wait for it to bound
			3. Edit PVC and trigger offline volume expansion
			4. Create POD , Wait for POD to reach running state
			5. Describe PVC and verify that new size should be updated on PVC
			6. Edit the same PVC again to test Online volume expansion
			7. Wait for resize to complete
			8. Verify the newly updated size on PV and PVC
			9. Clean up the data
	*/

	ginkgo.It("TC-18", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create StorageClass")
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, nil, "",
			nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		pv, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pod := performOfflineAndOnlineVolumeExpansionOnPVC(f, client, pvclaim, pv, volHandle, namespace)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
				pv[0].Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node", pv[0].Spec.CSI.VolumeHandle))
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running " +
			"on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod,
			namespace, allowedTopologies, true)
	})

	/*
		TESTCASE-19
		Create a workload and try to reboot one of the VC - NEGETIVE

			Steps:
			1. Create SC default values, so all the AZ's should be considered for provisioning.
			2. Reboot  any one VC1
			3. Create 3 statefulset each with 5 replica
			4. Since one VC1 is rebooting, volume should get provisioned on another VC till the VC1 Comes up
			5. Wait for the statefulset, PVC's and POD's  should be in up and running state
			6. After the VC came to running state scale up/down the statefull sets
			7. Newly created statefull set should get distributed among both the VC's
			8. Make sure common validation points are met on PV,PVC and POD
			9. Perform Cleanup
	*/

	ginkgo.It("TC-19", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 5
		sts_count := 3
		parallelStatefulSetCreation = true

		ginkgo.By("Create StorageClass")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
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

		ginkgo.By("Rebooting VC")
		vcAddress := multiVCe2eVSphere.multivcConfig.Global.VCenterHostname[1] + ":" + sshdPort
		framework.Logf("vcAddress - ", vcAddress)
		err = invokeVCenterReboot(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")

		ginkgo.By("Create 3 StatefulSet with replica count 5")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, stsReplicas)
		var wg sync.WaitGroup
		wg.Add(3)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				stsReplicas, &wg)

		}
		wg.Wait()

		essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		//After reboot
		bootstrap()

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		time.Sleep(60 * time.Second)

		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			// verify that the StatefulSets pods are in ready state
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], stsReplicas)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			// Get list of Pods in each StatefulSet and verify the replica count
			ssPods := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
			gomega.Expect(len(ssPods.Items) == int(stsReplicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
		defer func() {
			deleteAllStatefulSetAndPVs(client, namespace)
		}()

		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, parallelStatefulSetCreation, true)
		}

		stsReplicas = 3
		ginkgo.By("Scale down statefulset replica count to 1")
		scaleDownStatefulSetPod(ctx, client, statefulSets[0], namespace, stsReplicas,
			parallelStatefulSetCreation, true)

		stsReplicas = 7
		ginkgo.By("Scale up statefulset replica count to 6")
		scaleUpStatefulSetPod(ctx, client, statefulSets[0], namespace, stsReplicas,
			parallelStatefulSetCreation, true)

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[0],
			namespace, allowedTopologies, parallelStatefulSetCreation, true)
	})
})
