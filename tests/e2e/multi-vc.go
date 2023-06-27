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
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	"golang.org/x/crypto/ssh"
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
		storagePolicyName3          string
		topValStartIndex            int
		topValEndIndex              int
		topkeyStartIndex            int
		datastoreURLVC1             string
		datastoreURLVC2             string
		podAntiAffinityToSet        bool
		sshClientConfig             *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd     string
		allMasterIps                []string
		masterIp                    string
		defaultDatacenter           *object.Datacenter
		defaultDatastore            *object.Datastore
		pandoraSyncWaitTime         int
		scaleUpReplicaCount         int32
		scaleDownReplicaCount       int32
		multiVCSetupType            string
	)
	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name

		multiVCbootstrap()

		topologyLength = 5
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
		storagePolicyName3 = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores3)
		datastoreURLVC1 = GetAndExpectStringEnvVar(envSharedDatastoreURLVC1)
		datastoreURLVC2 = GetAndExpectStringEnvVar(envSharedDatastoreURLVC2)
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)
		multiVCSetupType = GetAndExpectStringEnvVar(envMultiVCSetupType)

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		// fetching k8s master ip
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]

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

		framework.Logf("Perform cleanup of any left over stale PVs")
		allPvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pv := range allPvs.Items {
			err := client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{})
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

	ginkgo.It("TC1Workload creation on a multivc environment with sts specified with node affinity "+
		"and SC with no allowed topology", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		parallelPodPolicy = true
		nodeAffinityToSet = true
		stsReplicas = 3
		scaleUpReplicaCount = 5
		scaleDownReplicaCount = 2

		if multiVCSetupType == "type-1" {
			/* here in 2-VC setup statefulset node affinity is taken as k8s-zone -> zone-1,zone-2,zone-3
			i.e VC1 allowed topology and VC2 partial allowed topology
			*/
			allowedTopologyLen = 3
			topValStartIndex = 0
			topValEndIndex = 3
			allowedTopologyLen = 3
		} else if multiVCSetupType == "type-2" {
			/* here in 3-VC setup, statefulset node affinity is taken as  k8s-zone -> zone-3
			i.e only VC3 allowed topology
			*/
			allowedTopologyLen = 1
			topValStartIndex = 2
			topValEndIndex = 3
		}

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with no allowed topolgies specified and with WFC binding mode")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen,
			podAntiAffinityToSet, parallelStatefulSetCreation)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/* TESTCASE-2

	Deploy workload with allowed topology details in SC which should contain all the AZ's
	so that workload will get distributed among all the VC's

	Steps:
	1. Create SC with allowedTopology details contains more than one Availability zone details
	2. Create statefulset with replica-5
	3. Wait for PVC to reach bound state and POD to reach Running state
	4. Volumes should get distributed among all the Availability zones
	5. Make sure common validation points are met
	a) Verify the PV node affinity details should have appropriate Node details
	b) The Pods should be running on the appropriate nodes
	c) CNS metadata
	6. Scale-up/Scale-down the statefulset and verify the common validation points on newly
	created statefullset
	7. Clean up the data
	*/

	ginkgo.It("TC2Workload creation when all allowed topology specified in SC on a "+
		"multivc environment", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* here we are considering all the allowed topologies of VC1 and VC2 in case of 2-VC setup.
		i.e. k8s-zone -> zone-1,zone-2,zone-3,zone-4,zone-5

		And, all the allowed topologies of VC1, VC2 and VC3 are considered in case of 3-VC setup
		i.e k8s-zone -> zone-1,zone-2,zone-3
		*/

		stsReplicas = 5
		scaleUpReplicaCount = 7
		scaleDownReplicaCount = 3

		ginkgo.By("Create StorageClass with specific allowed topolgies details")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet,
			parallelStatefulSetCreation)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/*
		TESTCASE-3
		Deploy workload with Specific storage policy name available in Single VC

		Steps:
		1. Create a SC with storage policy name available in single VC
		2. Create statefulset with replica-5
		3. Wait for PVC to reach bound state and POD to reach Running state
		4. Volumes should get created under appropriate nodes which is accessible to  the storage Policy
		5. Make sure common verification Points met in PVC, PV ad POD
		a) Verify the PV node affinity details should have appropriate Node details
		b) The Pods should be running on the appropriate nodes
		c) CNS metadata
		6. Scale-up/Scale-down the statefulset and verify the common validation points on newly created statefullset
		7. Clean up the data
	*/

	ginkgo.It("TC3Workload creation when specific storage policy of any single VC is given in SC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* here we are considering storage policy of VC1 and the allowed topology is k8s-zone -> zone-1
		in case of 2-VC setup and 3-VC setup
		*/

		stsReplicas = 5
		scParameters[scParamStoragePolicyName] = storagePolicyName
		topValStartIndex = 0
		topValEndIndex = 1
		scaleUpReplicaCount = 9
		scaleDownReplicaCount = 2

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with storage policy specified")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet,
			parallelStatefulSetCreation)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/* TESTCASE-4
	Same Policy  is available in two VC's

	Steps:
	1. Create a SC with Storage policy name available in VC1 and VC2
	2. Create  two Statefulset with replica-3
	3. Wait for PVC to reach bound state and POD to reach Running state
	4. Since both the VCs have the same storage policy, volume should get distributed among all the
	availability zones
	5. Make sure common verification Points met in PVC, PV ad POD
	a) Verify the PV node affinity details should have appropriate Node details
	b) The POD's should be running on the appropriate nodes
	c) CNS metadata
	6. Scale-up/scale-down the statefulset and verify the common validation points on newly
	created statefullset
	7. Clean up the data
	*/

	ginkgo.It("TC4Workload creation when specific storage policy available in multivc setup "+
		"is given in SC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* In case of 2-VC setup, we are considering storage policy of VC1 and VC2 and the allowed
				topology is k8s-zone -> zone-1, zone-2 i.e VC1 allowed topology and partial allowed topology
				of VC2

		In case of 3-VC setup, we are considering storage policy of VC1 and VC2 and the allowed
				topology is k8s-zone -> zone-1, zone-2 i.e VC1 and VC2 allowed topologies.
		*/

		stsReplicas = 3
		scParameters[scParamStoragePolicyName] = storagePolicyName2
		topValStartIndex = 0
		topValEndIndex = 2
		sts_count := 2
		parallelStatefulSetCreation = true
		scaleUpReplicaCount = 7
		scaleDownReplicaCount = 2

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

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
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				stsReplicas, &wg)

		}
		wg.Wait()

		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], stsReplicas)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			ssPods := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
			gomega.Expect(len(ssPods.Items) == int(stsReplicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}
		defer func() {
			deleteAllStatefulSetAndPVs(client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, parallelStatefulSetCreation, true)
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and " +
			"verify pv and pod affinity details")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulSets[0], parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/* TESTCASE-5
	Pod affiity tests

	Steps:
	1. Create SC default values so all the AZ's should be considered for provisioning.
	2. Create statefulset with Pod affinity rules such a way that each AZ should get atleast 1 statefulset
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

	ginkgo.It("TC5Workload creation on a multivc environment with sts specified with pod affinity "+
		"and SC with no allowed topology", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 5
		podAntiAffinityToSet = true
		scaleUpReplicaCount = 8
		scaleDownReplicaCount = 1

		ginkgo.By("Create StorageClass with storage policy specified")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet,
			parallelStatefulSetCreation)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/* TESTCASE-6
	Deploy workload with allowed topology and Datastore URL

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

	ginkgo.It("TC6Deploy workload with allowed topology and datastore url on a multivc environment", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* here, we are considering datastore url of VC1 in case of both 2-VC and 3-VC multi setup
		so the allowed topology will be considered as - k8s-zone -> zone-1
		*/

		stsReplicas = 5
		scParameters[scParamDatastoreURL] = datastoreURLVC1
		topValStartIndex = 0
		topValEndIndex = 1
		scaleDownReplicaCount = 3
		scaleUpReplicaCount = 6

		ginkgo.By("Set specific allowed topology")
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

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet,
			parallelStatefulSetCreation)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/* TESTCASE-7
	Deploy workload with allowed topology details in SC specific to VC1  with Immediate Binding

	Steps:
	1. Create SC with allowedTopology details set to VC1 availability zone
	2. Create statefulset with replica-3
	3. Wait for PVC to reach bound state and POD to reach Running state
	4. Make sure common validation points are met
	a) Verify the PV node affinity details should have appropriate Node details
	b) The POD's should be running on the appropriate nodes which are present in VC1
	5. Scale-up/scale-down the statefulset
	6. Verify the node affinity details. Verify the POD details. All the pods should come up on the
	nodes of VC1
	7. Clean up the data
	*/

	ginkgo.It("TC7Deploy workload with allowed topology details in SC specific "+
		"to VC1 with Immediate Binding", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* here, we are considering allowed topology of VC1 in case of both 2-VC and 3-VC multi setup
		so the allowed topology will be considered as - k8s-zone -> zone-1
		*/

		stsReplicas = 3
		topValStartIndex = 0
		topValEndIndex = 1
		scaleDownReplicaCount = 0
		scaleUpReplicaCount = 6

		ginkgo.By("Set specific allowed topology")
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

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet,
			parallelStatefulSetCreation)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/* TESTCASE-8
	Deploy workload with allowed topology details in SC specific to VC2  + WFC binding mode +
	default pod management policy

	Steps:
	1. Create SC with allowedTopology details set to VC2's Availability Zone
	2. Create statefulset with replica-3
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

	ginkgo.It("TC8Deploy workload with allowed topology details in SC specific to VC2 with WFC "+
		"binding mode and with default pod management policy", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 3
		scaleDownReplicaCount = 2
		scaleUpReplicaCount = 5

		if multiVCSetupType == "type-1" {
			// here, we are considering partial allowed topology of VC2 i.e k8s-zone -> zone-3,zone-4, zone-5
			topValStartIndex = 2
			topValEndIndex = 5
		} else if multiVCSetupType == "type-2" {
			// here, we are considering  allowed topology of VC2 i.e k8s-zone -> zone-2
			topValStartIndex = 1
			topValEndIndex = 2
		}

		ginkgo.By("Set specific allowed topology")
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

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet,
			parallelStatefulSetCreation)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/* TESTCASE-9
	Deploy workload with allowed topology details in SC specific to VC3 + parallel pod management policy

	Steps:
	1. Create SC with allowedTopology details set to VC3 availability zone
	2. Create statefulset with replica-3
	3. Wait for PVC to reach bound state and POD to reach Running state
	4. Make sure common validation points are met
	a) Verify the PV node affinity details should have appropriate Node details
	b) The Pod should be running on the appropriate nodes which are present in VC3
	5. Scale-up /scale-down the statefulset
	6. Verify the node affinity details and also verify the pod details. All the pods should come up on the nodes of
	VC3
	7. Clean up the data
	*/

	ginkgo.It("TC9Deploy workload with allowed topology details in SC specific to VC3 with "+
		"parallel pod management policy", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 3
		parallelPodPolicy = true
		scaleDownReplicaCount = 1
		scaleUpReplicaCount = 6

		if multiVCSetupType == "type-1" {
			/* here, For 2-VC setup we will consider all allowed topology of VC1 and VC2
			i.e. k8s-zone -> zone-1,zone-2,zone-3,zone-4,zone-5 */
			topValStartIndex = 0
			topValEndIndex = 5
		} else if multiVCSetupType == "type-2" {
			/* here, For 3-VC setup we will consider allowed topology of VC3
			i.e. k8s-zone ->zone-3 */
			topValStartIndex = 2
			topValEndIndex = 3
		}

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with allowed topology details of VC3")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
			bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet,
			parallelStatefulSetCreation)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies)
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
		6. Scale-up/scale-down the statefulset
		7. Clean up the data
	*/

	ginkgo.It("TC10Deploy workload with default SC parameters with WaitForFirstConsumer", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 5
		pvcCount := 5
		var podList []*v1.Pod
		scaleDownReplicaCount = 3
		scaleUpReplicaCount = 7
		parallelPodPolicy = true
		parallelStatefulSetCreation = true

		/* here, For 2-VC setup, we are considering all the allowed topologies of VC1 and VC2
		i.e. k8s-zone -> zone-1,zone-2,zone-3,zone-4,zone-5

		For 3-VC setup, we are considering all the allowed topologies of VC1, VC2 and VC3
		i.e. k8s-zone -> zone-1,zone-2,zone-3
		*/

		ginkgo.By("Create StorageClass with default parameters using WFC binding mode")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen,
			podAntiAffinityToSet, parallelStatefulSetCreation)
		defer func() {
			deleteAllStatefulSetAndPVs(client, namespace)
			deleteService(namespace, client, service)
		}()

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

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/* TESTCASE-11
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

	ginkgo.It("TC11Create SC with single allowed topology label on a multivc environment", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 5
		/* Considering partial allowed topology of VC2 in case of type-1 setup i.e. k8s-zone -> zone-2
		And, allowed topology of VC2 in case of type-2 setup i.e. k8s-zone -> zone-2
		*/
		topValStartIndex = 1
		topValEndIndex = 2
		parallelPodPolicy = true
		scaleDownReplicaCount = 2
		scaleUpReplicaCount = 5

		ginkgo.By("Set specific allowed topology")
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

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet,
			parallelStatefulSetCreation)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/* TESTCASE-12
	Create PVC using the wrong StoragePolicy name. Consider partially matching storage policy

	Steps:
	1. Use the partially matching storage policy and create PVC
	2. PVC should not go to bound, appropriate error should be shown
	3. Perform cleanup
	*/

	ginkgo.It("TC12PVC creation failure when wrong storage policy name is specified in SC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		storagePolicyName = "shared-ds-polic"
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

		ginkgo.By("Expect claim to fail as invalid storage policy is specified in Storage Class")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		framework.Logf("Volume Provisioning Failed for PVC %s due to invalid storage "+
			"policy given in the Storage Class", pvclaim.Name)
	})

	/*
		TESTCASE-13
		Deploy workload With allowed topology of VC1 and datastore url which is in VC2

		Steps:
		1. Create SC with allowed topology which matches VC1 details and datastore url which is in VC2
		2. PVC should not go to bound, appropriate error should be shown
	*/

	ginkgo.It("TC13Deploy workload with allowed topology of VC1 and datastore url of VC2", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topValStartIndex = 0
		topValEndIndex = 1
		scParameters[scParamDatastoreURL] = datastoreURLVC2

		ginkgo.By("Set specific allowed topology")
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

		ginkgo.By("Expect claim to fail as non-compatible datastore url is specified in Storage Class")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		framework.Logf("Volume Provisioning Failed for PVC %s due to non-compatible datastore "+
			"url given in the Storage Class", pvclaim.Name)
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

	ginkgo.It("TC14Create storage policy in multivc and later delete storage policy from one of the VC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topValStartIndex = 1
		topValEndIndex = 2
		stsReplicas = 3

		scParameters[scParamStoragePolicyName] = storagePolicyName3

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex,
			topValStartIndex, topValEndIndex)

		ginkgo.By("Create StorageClass with no allowed topolgies specified and with WFC binding mode")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Delete Storage Policy created in VC1")
		err = deleteStorageProfile(masterIp, sshClientConfig, storagePolicyName3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, _ := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, allowedTopologyLen, podAntiAffinityToSet,
			parallelStatefulSetCreation)
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

	})

	/*
		TESTCASE-15
		Statically Create PVC and Verify NodeAffinity details on PV
		Make sure node affinity details are mentioned while creating PV

		Steps:
		1. Create SC which point to ay one VC's allowed topology Availability zone
		2. Create PV using the above created SC and reclaim policy delete , PV should
		have appropriate node affinity details mentioned
		3. Create PVC using above created SC and PV
		4. Wait for PV ,PVC to be in bound
		5. Make sure common validation points are met on PV,PVC and POD
			a) Once PV and PVC are bound describe on the PV and verify node affinity details on PV
			b) Create POD using the above PVC
			c) POD should come up on the node which is present in the same zone as mentioned in storage class
		6. Clean up data
	*/

	ginkgo.It("TC15Static provisioning and verify node affinity details on PV on a multivc setup", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topValStartIndex = 0
		topValEndIndex = 1
		clientIndex := 0

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex,
			topValStartIndex, topValEndIndex)

		var datacenters []string
		soapClient := multiVCe2eVSphere.multiVcClient[0].Client.Client
		if soapClient == nil {
			framework.Logf("soapClient is nil")
		}
		vimClient, err := convertToVimClient(ctx, soapClient)
		if err != nil {
			framework.Logf("Error: ", err)
		}
		finder := find.NewFinder(vimClient, false)

		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		defaultDatacenter, err = finder.Datacenter(ctx, datacenters[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		finder.SetDatacenter(defaultDatacenter)
		defaultDatastore, err = getDatastoreByURL(ctx, datastoreURLVC1, defaultDatacenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create StorageClass with allowed topology details of VC1")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		ginkgo.By("Creating FCD Disk")
		fcdID, err := multiVCe2eVSphere.createFCDForMultiVC(ctx, "BasicStaticFCD", diskSizeInMb,
			defaultDatastore.Reference(), clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
			err := multiVCe2eVSphere.deleteFCDFromMultiVC(ctx, fcdID, defaultDatastore.Reference(), clientIndex)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync "+
			"with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		// Creating label for PV.
		// PVC will use this label as Selector to find PV.
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID

		ginkgo.By("Creating the PV")
		pv := getPersistentVolumeSpecWithStorageClassFCDNodeSelector(fcdID,
			v1.PersistentVolumeReclaimDelete, sc.Name, staticPVLabels,
			diskSize, allowedTopologies)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		if err != nil {
			return
		}
		err = multiVCe2eVSphere.waitForCNSVolumeToBeCreatedInMultiVC(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC")
		pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc.Spec.StorageClassName = &sc.Name
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc,
			metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(),
			namespace, pv, pvc))

		ginkgo.By("Verifying CNS entry is present in cache")
		_, err = multiVCe2eVSphere.queryCNSVolumeWithResultInMultiVC(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the PVC")
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace),
				"Failed to delete PVC", pvc.Name)

			ginkgo.By("Deleting the PVv")
			framework.ExpectNoError(fpv.DeletePersistentVolume(client, pv.Name),
				"Failed to delete PV", pv.Name)
		}()

		ginkgo.By("Creating the Pod")
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		pod, err := createPod(client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle,
			pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")

		ginkgo.By("Verify container volume metadata is present in CNS cache")
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolume with VolumeID: %s", pv.Spec.CSI.VolumeHandle))
		_, err = multiVCe2eVSphere.queryCNSVolumeWithResultInMultiVC(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := []types.KeyValue{{Key: "fcd-id", Value: fcdID}}
		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
			pvc.Name, pv.ObjectMeta.Name, pod.Name, labels...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(client, pod), "Failed to delete pod",
				pod.Name)

			ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", pod.Spec.NodeName))
			isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies, true)
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

	ginkgo.It("TC16Create Deployment pod using SC with allowed topology set to specific VC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topValStartIndex = 2
		topValEndIndex = 3
		var lables = make(map[string]string)
		lables["app"] = "nginx"
		replica := 1

		/* here considering partial allowed topology of VC2 in case of 2VC setup i.e k8s-zone -> zone-3
		here considering allowed topology of VC2 in case of 3VC setup i.e k8s-zone -> zone-3
		*/

		ginkgo.By("Set specific allowed topology")
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
		Create SC with invalid allowed topology details - NEGETIVE

		Steps:
		1. Create SC with invalid label details
		2. Create PVC, Pvc should not reach bound state. It should throuw error
	*/

	ginkgo.It("Create SC with invalid allowed topology details", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topValStartIndex = 1
		topValEndIndex = 1

		ginkgo.By("Set invalid allowed topology for SC")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		allowedTopologies[0].Values = []string{"new-zone"}

		storageclass, err := createStorageClass(client, nil, allowedTopologies, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvc, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if pvc != nil {
				ginkgo.By("Delete the PVC")
				err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

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

	ginkgo.It("Offline and online volume expansion on a multivc setup", func() {
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

		ginkgo.By("Perform offline and online volume expansion on PVC")
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

	ginkgo.It("Create workload and reboot one of the VC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 5
		sts_count := 3
		parallelStatefulSetCreation = true
		scaleUpReplicaCount = 9
		scaleDownReplicaCount = 1

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
		vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
		vcAddress := vCenterHostname[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterReboot(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vCenterHostname[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")

		ginkgo.By("Create 3 StatefulSet with replica count 5")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, stsReplicas)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				stsReplicas, &wg)

		}
		wg.Wait()

		essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		//After reboot
		multiVCbootstrap()

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

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, parallelStatefulSetCreation, true)
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulSets[0], parallelStatefulSetCreation, namespace,
			allowedTopologies)
	})

	/* TESTCASE-20
		Storage policy is present in VC1 and VC1 is under reboot

		Steps:
	    1. Create SC with specific storage policy , Which is present in VC1
	    2. Create Statefulsets using the above SC and reboot VC1 at the same time
	    3. Since VC1 is under reboot , volume creation should be in pendig state till the VC1 is up
	    4. Once the VC1 is up , all the volumes should come up on the worker nodes which is in VC1
	    5. Make sure common validation points are met on PV,PVC and POD
	    6. Clean up the data
	*/

	ginkgo.It("Storage policy is present in VC1 and VC1 is under reboot", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas = 5
		scParameters[scParamStoragePolicyName] = storagePolicyName
		parallelStatefulSetCreation = true
		scaleDownReplicaCount = 2
		scaleUpReplicaCount = 7
		topValStartIndex = 0
		topValEndIndex = 1
		sts_count := 2

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

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

		ginkgo.By("Rebooting VC")
		vCenterHostname := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")
		vcAddress := vCenterHostname[0] + ":" + sshdPort
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterReboot(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vCenterHostname[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")

		ginkgo.By("Create 3 StatefulSet with replica count 5")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, stsReplicas)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				stsReplicas, &wg)

		}
		wg.Wait()

		essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		//After reboot
		multiVCbootstrap()

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

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, parallelStatefulSetCreation, true)
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		for i := 0; i < len(statefulSets); i++ {
			performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
				scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
				allowedTopologies)
		}
	})
})
