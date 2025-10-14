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
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
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

var _ = ginkgo.Describe("[multivc-positive] MultiVc-Topology-Positive", func() {
	f := framework.NewDefaultFramework("multivc-positive")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		bindingMode                 storagev1.VolumeBindingMode
		nodeAffinityToSet           bool
		parallelStatefulSetCreation bool
		stsReplicas                 int32
		parallelPodPolicy           bool
		scParameters                map[string]string
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
		scaleUpReplicaCount         int32
		scaleDownReplicaCount       int32
		multiVCSetupType            string
		isVsanHealthServiceStopped  bool
		stsScaleUp                  bool
		stsScaleDown                bool
		verifyTopologyAffinity      bool
		storagePolicyInVc1          string
		storagePolicyInVc1Vc2       string
		storagePolicyToDelete       string
		isSPSServiceStopped         bool
		isStorageProfileDeleted     bool
		pandoraSyncWaitTime         int
	)
	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name

		multiVCbootstrap()

		stsScaleUp = true
		stsScaleDown = true
		verifyTopologyAffinity = true

		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		scParameters = make(map[string]string)
		storagePolicyInVc1 = GetAndExpectStringEnvVar(envStoragePolicyNameVC1)
		storagePolicyInVc1Vc2 = GetAndExpectStringEnvVar(envStoragePolicyNameInVC1VC2)
		storagePolicyToDelete = GetAndExpectStringEnvVar(envStoragePolicyNameToDeleteLater)
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

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(ctx, client, namespace)
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

		if isVsanHealthServiceStopped {
			framework.Logf("Bringing vsanhealth up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}

		if isSPSServiceStopped {
			framework.Logf("Bringing sps up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
			isSPSServiceStopped = false
		}

		if isStorageProfileDeleted {
			clientIndex := 0
			err = createStorageProfile(masterIp, sshClientConfig, storagePolicyToDelete, clientIndex)
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

	ginkgo.It("[pq-multivc] Workload creation on a multivc environment with sts specified with node affinity "+
		"and SC with no allowed topology", ginkgo.Label(p0, block, vanilla, multiVc,
		vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		parallelPodPolicy = true
		nodeAffinityToSet = true
		stsReplicas = 3
		scaleUpReplicaCount = 5
		scaleDownReplicaCount = 2

		if multiVCSetupType == "multi-2vc-setup" {
			/* here in 2-VC setup statefulset node affinity is taken as k8s-zone -> zone-1,zone-2,zone-3
			i.e VC1 allowed topology and VC2 partial allowed topology
			*/
			topValStartIndex = 0
			topValEndIndex = 3
		} else if multiVCSetupType == "multi-3vc-setup" {
			/* here in 3-VC setup, statefulset node affinity is taken as  k8s-zone -> zone-3
			i.e only VC3 allowed topology
			*/
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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, stsReplicas, nodeAffinityToSet, allowedTopologies,
			podAntiAffinityToSet, parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

	ginkgo.It("[pq-multivc] Workload creation when all allowed topology specified in SC on a "+
		"multivc environment", ginkgo.Label(p0, block, vanilla, multiVc, vc70), func() {

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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet,
			parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc] Workload creation when specific storage policy of any single VC is "+
		"given in SC", ginkgo.Label(p0, block, vanilla, multiVc, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* here we are considering storage policy of VC1 and the allowed topology is k8s-zone -> zone-1
		in case of 2-VC setup and 3-VC setup
		*/

		stsReplicas = 5
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1
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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet,
			parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc] Workload creation when storage policy available in multivc setup"+
		"is given in SC", ginkgo.Label(p0, block, vanilla, multiVc, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* In case of 2-VC setup, we are considering storage policy of VC1 and VC2 and the allowed
				topology is k8s-zone -> zone-1, zone-2 i.e VC1 allowed topology and partial allowed topology
				of VC2

		In case of 3-VC setup, we are considering storage policy of VC1 and VC2 and the allowed
				topology is k8s-zone -> zone-1, zone-2 i.e VC1 and VC2 allowed topologies.
		*/

		stsReplicas = 3
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2
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

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, parallelStatefulSetCreation)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and " +
			"verify pv and pod affinity details")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulSets[0], parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc]Workload creation on a multivc environment with sts specified with pod affinity "+
		"and SC with no allowed topology", ginkgo.Label(p0, block, vanilla, multiVc, vc70), func() {

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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet,
			parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc]Deploy workload with allowed topology and datastore url on a multivc "+
		"environment", ginkgo.Label(p0, block, vanilla, multiVc, vc70), func() {

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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet,
			parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc] Deploy workload with allowed topology details in SC specific "+
		"to VC1 with Immediate Binding", ginkgo.Label(p0, block, vanilla, multiVc, vc70), func() {

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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet,
			parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc] Deploy workload with allowed topology details in SC specific to VC2 with WFC "+
		"binding mode and with default pod management policy", ginkgo.Label(p1, block, vanilla, multiVc,
		vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 3
		scaleDownReplicaCount = 2
		scaleUpReplicaCount = 5

		if multiVCSetupType == "multi-2vc-setup" {
			// here, we are considering partial allowed topology of VC2 i.e k8s-zone -> zone-3,zone-4
			topValStartIndex = 2
			topValEndIndex = 4
		} else if multiVCSetupType == "multi-3vc-setup" {
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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet,
			parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc] Deploy workload with allowed topology details in SC specific to VC3 with "+
		"parallel pod management policy", ginkgo.Label(p0, block, vanilla, multiVc, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 3
		parallelPodPolicy = true
		scaleDownReplicaCount = 1
		scaleUpReplicaCount = 6

		if multiVCSetupType == "multi-2vc-setup" {
			/* here, For 2-VC setup we will consider all allowed topology of VC1 and VC2
			i.e. k8s-zone -> zone-1,zone-2,zone-3,zone-4 */
			topValStartIndex = 0
			topValEndIndex = 4
		} else if multiVCSetupType == "multi-3vc-setup" {
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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet,
			parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc] Deploy workload with default SC parameters with WaitForFirstConsumer", ginkgo.Label(p0,
		block, vanilla, multiVc, vc70), func() {

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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, stsReplicas, nodeAffinityToSet, allowedTopologies,
			podAntiAffinityToSet, parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
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
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, pvcCount, nil)

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			var pvclaims []*v1.PersistentVolumeClaim
			pvc, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())

			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			ginkgo.By("Creating Pod")
			pvclaims = append(pvclaims, pvclaimsList[i])
			pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
			podList = append(podList, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
			isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(
				pv.Spec.CSI.VolumeHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		}
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeoutShort))
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
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
			err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, podList[i],
				allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc] Create SC with single allowed topology label on a multivc environment", ginkgo.Label(p1,
		block, vanilla, multiVc, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* Considering partial allowed topology of VC2 in case of 2-VC setup i.e. k8s-zone -> zone-2
		And, allowed topology of VC2 in case of 3-VC setup i.e. k8s-zone -> zone-2
		*/

		stsReplicas = 5
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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet,
			parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-12
	Create PVC using the wrong StoragePolicy name. Consider partially matching storage policy

	Steps:
	1. Use the partially matching storage policy and create PVC
	2. PVC should not go to bound, appropriate error should be shown
	3. Perform cleanup
	*/

	ginkgo.It("[pq-multivc] PVC creation failure when wrong storage policy name is specified in SC", ginkgo.Label(p2,
		block, vanilla, multiVc, vc70, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		storagePolicyName := "shared-ds-polic"
		scParameters[scParamStoragePolicyName] = storagePolicyName

		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as invalid storage policy is specified in Storage Class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "failed to create volume"
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

	ginkgo.It("[pq-multivc] Deploy workload with allowed topology of VC1 and datastore url of VC2", ginkgo.Label(p2,
		block, vanilla, multiVc, vc70, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topValStartIndex = 0
		topValEndIndex = 1
		scParameters[scParamDatastoreURL] = datastoreURLVC2

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail as non-compatible datastore url is specified in Storage Class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound,
			client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "failed to create volume"
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

	ginkgo.It("[pq-multivc] Create storage policy in multivc and later delete storage policy from "+
		"one of the VC", ginkgo.Label(p2, block, vanilla, multiVc, vc70, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topValStartIndex = 1
		topValEndIndex = 2
		stsReplicas = 3
		clientIndex := 0

		scParameters[scParamStoragePolicyName] = storagePolicyToDelete

		/* Considering partial allowed topology of VC2 in case of 2-VC setup i.e. k8s-zone -> zone-2
		And, allowed topology of VC2 in case of 3-VC setup i.e. k8s-zone -> zone-2
		*/

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex,
			topValStartIndex, topValEndIndex)

		ginkgo.By("Create StorageClass")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Delete Storage Policy created in VC1")
		err = deleteStorageProfile(masterIp, sshClientConfig, storagePolicyToDelete, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isStorageProfileDeleted = true
		defer func() {
			if isStorageProfileDeleted {
				err = createStorageProfile(masterIp, sshClientConfig, storagePolicyToDelete, clientIndex)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isStorageProfileDeleted = false
			}
		}()

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, _, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet,
			parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

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

	ginkgo.It("[pq-multivc] Create Deployment pod using SC with allowed topology set to specific VC", ginkgo.Label(p1,
		block, vanilla, multiVc, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topValStartIndex = 2
		topValEndIndex = 3
		var lables = make(map[string]string)
		lables["app"] = "nginx"
		replica := 1

		/* here considering partial allowed topology of VC2 in case of 2VC setup i.e k8s-zone -> zone-3
		here considering allowed topology of VC3 in case of 3VC setup i.e k8s-zone -> zone-3
		*/

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass and PVC for Deployment")
		sc, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, nil,
			nil, diskSize, allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PVC to be in Bound phase
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
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
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
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
		err = verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx, client, deployment,
			namespace, allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-17
		Create SC with invalid allowed topology details - NEGETIVE

		Steps:
		1. Create SC with invalid label details
		2. Create PVC, Pvc should not reach bound state. It should throuw error
	*/

	ginkgo.It("[pq-multivc] Create SC with invalid allowed topology details", ginkgo.Label(p2, block,
		vanilla, multiVc, vc70, negative), func() {

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

		pvc, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if pvc != nil {
				ginkgo.By("Delete the PVC")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Expect claim to fail as invalid topology label is specified in Storage Class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound,
			client, pvc.Namespace, pvc.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "failed to fetch vCenter associated with topology segments"
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvc.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
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

	ginkgo.It("[pq-multivc] Offline and online volume expansion on a multivc setup", ginkgo.Label(p1, block,
		vanilla, multiVc, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvclaims []*v1.PersistentVolumeClaim

		ginkgo.By("Create StorageClass")
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, nil, nil, "",
			nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		pv, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Perform offline volume expansion on PVC")
		err = performOfflineVolumeExpansin(client, pvclaim, volHandle, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var vmUUID string
		nodeName := pod.Spec.NodeName

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, nodeName))
		isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
				pv[0].Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node", pv[0].Spec.CSI.VolumeHandle))
		}()

		ginkgo.By("Perform online volume expansion on PVC")
		err = performOnlineVolumeExpansion(f, client, pvclaim, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running " +
			"on appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc] Create workload and reboot one of the VC", ginkgo.Label(p2, block,
		vanilla, multiVc, vc70, negative), func() {

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

		ginkgo.By("Rebooting VC1")
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterReboot(ctx, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vcAddress)
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

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, parallelStatefulSetCreation)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulSets[0], parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	ginkgo.It("[pq-multivc] Storage policy is present in VC1 and VC1 is under reboot", ginkgo.Label(p1, block,
		vanilla, multiVc, vc70, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 3
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1
		parallelStatefulSetCreation = true
		scaleDownReplicaCount = 2
		scaleUpReplicaCount = 7
		topValStartIndex = 0
		topValEndIndex = 1
		sts_count := 2

		// here, we are considering the allowed topology of VC1 i.e. k8s-zone -> zone-1

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

		ginkgo.By("Rebooting VC1")
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterReboot(ctx, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")

		ginkgo.By("Create 3 StatefulSet with replica count 3")
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

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, parallelStatefulSetCreation)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		for i := 0; i < len(statefulSets); i++ {
			err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
				scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
				allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/* TESTCASE-21
			VSAN-health down on VC1

		Steps:
	    1. Create SC default values, so all the AZ's should be considered for provisioning.
	    2. Create dynamic PVC's and Add labels to PVC's and PV's
	    3. Bring down the VSAN-health on VC1
	    4. Create two statefulset each with replica 5
	    5. Since VSAN-health on VC1 is down all the volumes should get created under VC2 availability zones
	    6. Verify the PV node affinity and the nodes on which Pods have come  should be appropriate
	    7. Update the label on PV and PVC
	    8. Bring up the VSAN-health
	    9. Scale up both the statefull set to 10
	    10. Wait for 2 Full sync cycle
	    11. Verify CNS-metadata for the volumes that are created
	    12. Verify that the volume provisioning should resume on VC1 as well
	    13. Make sure common validation points are met on PV,PVC and POD
	    14. Scale down the statefull sets to 1
	    15. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Create workloads when VSAN-health is down on VC1", ginkgo.Label(p1, block,
		vanilla, multiVc, vc70, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas = 5
		pvcCount := 3
		scaleDownReplicaCount = 1
		scaleUpReplicaCount = 10
		parallelPodPolicy = true
		parallelStatefulSetCreation = true
		labelKey := "volId"
		labelValue := "pvcVolume"
		labels := make(map[string]string)
		labels[labelKey] = labelValue
		sts_count := 2
		var fullSyncWaitTime int
		var err error

		// Read full-sync value.
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		/* here, For 2-VC setup, we are considering all the allowed topologies of VC1 and VC2
		i.e. k8s-zone -> zone-1,zone-2,zone-3,zone-4,zone-5

		For 3-VC setup, we are considering all the allowed topologies of VC1, VC2 and VC3
		i.e. k8s-zone -> zone-1,zone-2,zone-3
		*/

		ginkgo.By("Create StorageClass with default parameters")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Trigger multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, labels)

		ginkgo.By("Verify PVC claim to be in bound phase and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			pvc, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
				[]*v1.PersistentVolumeClaim{pvclaimsList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())

		}
		defer func() {
			ginkgo.By("Deleting PVC's and PV's")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimsList[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeoutShort))
				err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		ginkgo.By("Bring down Vsan-health service on VC1")
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanHealthServiceStopped = true
		defer func() {
			if isVsanHealthServiceStopped {
				framework.Logf("Bringing vsanhealth up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
			}
		}()

		ginkgo.By("Create 2 StatefulSet with replica count 5 when vsan-health is down")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, stsReplicas)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				stsReplicas, &wg)
		}
		wg.Wait()

		labelKey = "volIdNew"
		labelValue = "pvcVolumeNew"
		labels = make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By("Updating labels for PVC and PV")
		for i := 0; i < len(pvclaimsList); i++ {
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)
			framework.Logf("Updating labels %+v for pvc %s in namespace %s", labels, pvclaimsList[i].Name,
				pvclaimsList[i].Namespace)
			pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaimsList[i].Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvc.Labels = labels
			_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Updating labels %+v for pv %s", labels, pv.Name)
			pv.Labels = labels
			_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Waiting for labels to be updated for PVC and PV")
		for i := 0; i < len(pvclaimsList); i++ {
			pv := getPvFromClaim(client, pvclaimsList[i].Namespace, pvclaimsList[i].Name)

			framework.Logf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
				labels, pvclaimsList[i].Name, pvclaimsList[i].Namespace)
			err = multiVCe2eVSphere.waitForLabelsToBeUpdatedInMultiVC(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePVC), pvclaimsList[i].Name, pvclaimsList[i].Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name)
			err = multiVCe2eVSphere.waitForLabelsToBeUpdatedInMultiVC(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, parallelStatefulSetCreation)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulSets[0], parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-22
	SPS down on VC2 + use storage policy while creating statefulset

	Steps:
	1. Create SC with storage policy which is in both VCs i.e. VC1 and VC2
	2. Create Statefulset using  the same storage policy
	3. Bring down the SPS  on VC1
	4. create two statefulset each with replica 5
	5. Since SPS on VC1 is down all the volumes should get created under VC2 availability zones
	[Note: if any VC or its service is down, volume provisioning will no go through on those
	Pods which are provisioned on the VC where service is down]
	till all VCs came to up and running state]
	6. Verify the PV node affinity and the nodes on which Pods have come should be appropriate
	7. Bring up the SPS service
	8. Scale up both the Statefulset to 10
	9. Verify that the volume provisioning should resume on VC1 as well
	10. Make sure common validation points are met on PV,PVC and POD
	11. Scale down the statefull sets to 1
	12. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Create workloads with storage policy given in SC and when sps service is "+
		"down", ginkgo.Label(p1, block, vanilla, multiVc, vc70, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* here we are considering storage policy of VC1 and VC2 so
		the allowed topology to be considered as for 2-VC and 3-VC setup is k8s-zone -> zone-1,zone-2

		*/

		stsReplicas = 5
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2
		topValStartIndex = 0
		topValEndIndex = 2
		scaleUpReplicaCount = 10
		scaleDownReplicaCount = 1
		sts_count := 2
		parallelStatefulSetCreation = true
		parallelPodPolicy = true

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
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, parallelPodPolicy,
			stsReplicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet,
			parallelStatefulSetCreation, false, "", nil, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down SPS service")
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterServiceControl(ctx, stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isSPSServiceStopped = true
		err = waitVCenterServiceToBeInState(ctx, spsServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if isSPSServiceStopped {
				framework.Logf("Bringing sps up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
				isSPSServiceStopped = false
			}
		}()

		ginkgo.By("Create 2 StatefulSet with replica count 5 when sps-service is down")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, stsReplicas)
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				stsReplicas, &wg)
		}
		wg.Wait()

		ginkgo.By("Bringup SPS service")
		startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)

		framework.Logf("Waiting for %v seconds for testbed to be in the normal state", pollTimeoutShort)
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		err = waitForStsPodsToBeInReadyRunningState(ctx, client, namespace, statefulSets)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulSets[i],
				namespace, allowedTopologies, parallelStatefulSetCreation)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulSets[0], parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-15
		Statically Create PVC and Verify NodeAffinity details on PV
		Make sure node affinity details are mentioned while creating PV

		Steps:
		1. Create SC which point to ay one VC's allowed topology Availability zone
		2. Create PV using the above created SC and reclaim policy delete, PV should
		have appropriate node affinity details mentioned
		3. Create PVC using above created SC and PV
		4. Wait for PV,PVC to be in bound
		5. Make sure common validation points are met on PV,PVC and POD
			a) Once PV and PVC are bound describe on the PV and verify node affinity details on PV
			b) Create POD using the above PVC
			c) POD should come up on the node which is present in the same zone as mentioned in storage class
		6. Clean up data
	*/

	ginkgo.It("[pq-multivc] Verify static provisioning and node affinity details on PV in a multivc "+
		"setup", ginkgo.Label(p2, block, vanilla, multiVc, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/*
			Considering allowed topology of VC1 i.e. k8s-zone -> zone-1
			clientIndex 0 means VC1 here
		*/
		topValStartIndex = 0
		topValEndIndex = 1
		clientIndex := 0
		scParameters["datastoreurl"] = datastoreURLVC1

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex,
			topValStartIndex, topValEndIndex)

		ginkgo.By("Create Storage Class")
		sc, err := createStorageClass(client, scParameters, allowedTopologies, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		/* will be creating fcd on VC1 of a multivc setup, hence passing clientIndex 0th value*/
		var datacenters []string
		soapClient := multiVCe2eVSphere.multiVcClient[0].Client.Client
		if soapClient == nil {
			framework.Logf("soapClient is nil")
		}
		vimClient, err := convertToVimClient(ctx, soapClient)
		if err != nil {
			framework.Logf("Error: %v", err)
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
		defaultDatacenter, err := finder.Datacenter(ctx, datacenters[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		finder.SetDatacenter(defaultDatacenter)
		defaultDatastore, err = getDatastoreByURL(ctx, datastoreURLVC1, defaultDatacenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating Static PV and PVC")
		fcdID, pvc, pv := createStaticFCDPvAndPvc(ctx, f, client, namespace, defaultDatastore,
			pandoraSyncWaitTime, allowedTopologies, clientIndex, sc)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
			err := multiVCe2eVSphere.deleteFCDInMultiVc(ctx, fcdID, defaultDatastore.Reference(), clientIndex)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Pod")
		pvclaims = append(pvclaims, pvc)
		pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(
			pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			pv := getPvFromClaim(client, pvc.Namespace, pvc.Name)
			isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node", pv.Spec.CSI.VolumeHandle))

			ginkgo.By("Deleting PVC's and PV's")
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeoutShort))
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify pv and pod node affinity")
		err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* TESTCASE-24
	Validate Listvolume response , when volume is deleted from CNS

	Steps:
	1. Create SC with default values so that it is allowed to provision volume on any AZs
	2. Create statefulset with 10 replica
	3. For any one POD note the node on which volume is attached
	4. Verify list volume response and validate the pagination , Next_token value
	5. From the VC UI , go the the node identified in step 2 → edit settings → delete the few  disks
	6. Since there are more than 1 volume mismatch, ListResponse fails, instead "Kubernetes and CNS volumes
	completely out of sync and exceeds the threshold"  error should show up
	7. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Validate Listvolume response when volume is deleted from CNS "+
		"in a multivc", ginkgo.Label(p1, block, vanilla, multiVc, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "nginx-sc-default-" + curtimestring + val

		var volumesBeforeScaleUp []string
		containerName := "vsphere-csi-controller"

		ginkgo.By("scale down CSI driver POD to 1 , so that it will" +
			"be easy to validate all Listvolume response on one driver POD")
		collectPodLogs(ctx, client, csiSystemNamespace)
		scaledownCSIDriver, err := scaleCSIDriver(ctx, client, namespace, 1)
		gomega.Expect(scaledownCSIDriver).To(gomega.BeTrue(), "csi driver scaledown is not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Scale up the csi-driver replica to 3")
			success, err := scaleCSIDriver(ctx, client, namespace, 3)
			gomega.Expect(success).To(gomega.BeTrue(), "csi driver scale up to 3 replica not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(scName, nil, allowedTopologies, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		ginkgo.By("Creating statefulset with replica 3 and a deployment")
		statefulset, deployment, volumesBeforeScaleUp := createStsDeployment(ctx, client, namespace, sc, true,
			false, 3, "", 1, "")

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx, client, deployment, namespace,
			allowedTopologies, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//List volume responses will show up in the interval of every 1 minute.
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate ListVolume Response for all the volumes")
		logMessage := "List volume response: entries:"
		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig,
			containerName, logMessage, volumesBeforeScaleUp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleUpReplicaCount = 5
		parallelStatefulSetCreation = true
		stsScaleDown = false
		ginkgo.By("Perform scaleup operation on statefulset and verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//List volume responses will show up in the interval of every 1 minute.
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate pagination")
		logMessage = "token for next set: 3"
		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas := 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr := fss.Scale(ctx, client, statefulset, int32(replicas))
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, int32(replicas))
		ssPodsAfterScaleDown, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		pvcList := getAllPVCFromNamespace(client, namespace)
		for _, pvc := range pvcList.Items {
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace),
				"Failed to delete PVC", pvc.Name)
		}
		//List volume responses will show up in the interval of every 1 minute.
		//To see the empty response, It is required to wait for 1 min after deleteting all the PVC's
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate ListVolume Response when no volumes are present")
		logMessage = "ListVolumes served 0 results"

		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
