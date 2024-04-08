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
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	apps "k8s.io/api/apps/v1"
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

var _ = ginkgo.Describe("[csi-topology-for-level5] Topology-Provisioning-For-Statefulset-Level5", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                    clientset.Interface
		namespace                 string
		bindingMode               storagev1.VolumeBindingMode
		allowedTopologies         []v1.TopologySelectorLabelRequirement
		storagePolicyName         string
		topologyAffinityDetails   map[string][]string
		topologyCategories        []string
		pandoraSyncWaitTime       int
		defaultDatacenter         *object.Datacenter
		defaultDatastore          *object.Datastore
		DSSharedToSpecificCluster *object.Datastore
		datastoreURL              string
		topologyLength            int
		leafNode                  int
		leafNodeTag0              int
		leafNodeTag1              int
		leafNodeTag2              int
	)
	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
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
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		topologyLength, leafNode, leafNodeTag0, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2

		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap,
			topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		SharedDSURLToSpecificCluster := GetAndExpectStringEnvVar(datastoreUrlSpecificToCluster)

		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		for _, dc := range datacenters {
			defaultDatacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			DSSharedToSpecificCluster, err = getDatastoreByURL(ctx, SharedDSURLToSpecificCluster,
				defaultDatacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
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
		Steps:
		1. Create SC without specifying any topology details using volumeBindingMode
		as WaitForFirstConsumer.
		2. Create StatefulSet with default pod management policy with replica count 3 using above SC.
		3. Wait for StatefulSet pods to be in up and running state.
		4. Since there is no Topology describe on SC, volume provisioning should happen on
		any availability zone.
		5. Describe on the PV's and verify node affinity details.
		5a. Verify, PV node affinity of all 5 levels should be displayed.
		5b. Verify, If a volume is provisioned on shared datastore, pv node affinity details
			contain all the availability zone details.
		5c. Verify, If a volume provisioned on a datastore that is shared within
			the specific zone then node affinity will have details of specific zone and its node labels.
		6. Verify StatefuSet pods are created on appropriate nodes.
		7. Bring down statefulset replica to 0.
		8. Delete statefulset, PVC and SC.
	*/

	ginkgo.It("Provisioning volume when no topology details specified in storage class "+
		"and using default pod management policy for statefulset", ginkgo.Label(p0, block,
		vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Creating StorageClass when no topology details are specified using WFC Binding mode
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Creating StatefulSet with replica count 3 using default pod management policy
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Wait for StatefulSet pods to be in up and running state
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Verify PV node affinity and that the PODS are running on appropriate nodes
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset to 0 replicas
		replicas -= 3
		ginkgo.By("Scale down statefulset replica count to 0")
		err = scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-2
		Steps:
		1. Create SC without specifying any topology details using volumeBindingMode as
		 WaitForFirstConsumer.
		2. Create StatefulSet with parallel pod management policy with replica count 3 using above SC.
		3. Wait for StatefulSet pods to be in up and running state.
		4. Since there is no Topology describe on SC, volume provisioning should happen on
		any availability zone.
		5. Describe on the PV's and verify node affinity details.
		5a. Verify, PV node affinity of all 5 levels should be displayed.
		5b. Verify, If a volume is provisioned on shared datastore, pv node affinity details
			contain all the availability zone details.
		5c. Verify, If a volume provisioned on a datastore that is shared within the specific
			zone then node affinity will have details of specific zone and its node labels.
		6. Verify StatefuSet pods are created on appropriate nodes.
		7. Scale up the StatefulSet replica count to 5.
		8. Scale down the StatefulSet replica count to 1.
		9. Verify scale up and scale down of StaefulSet pods are successful.
		10. Verify newly created StatefuSet pods are created on appropriate nodes.
		11. Describe PV and verify the node affinity details should display all 5 topology levels.
		12. Bring down all statefulset replica to 0.
		13. Delete statefulset, PVC and SC.
	*/

	ginkgo.It("Provisioning volume when no topology details specified in storage class "+
		"and using parallel pod management policy for statefulset", ginkgo.Label(p0, block,
		vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Creating StorageClass when no topology details are specified using WFC Binding mode
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Creating StatefulSet with replica count 3 using parallel pod management policy
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating StatefulSet with replica count 3 using parallel pod management policy")
		*(statefulset.Spec.Replicas) = 3
		statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &sc.Name
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Wait for StatefulSet pods to be in up and running state
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Verify PV node affinity and that the PODS are running on appropriate node
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate nodes")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
			statefulset, namespace, allowedTopologies, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale up statefulset replica count to 5
		replicas += 5
		ginkgo.By("Scale up statefulset replica count to 5")
		err = scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset replica count to 1
		replicas -= 1
		ginkgo.By("Scale down statefulset replica count to 1")
		err = scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify newly created PV node affinity details  and that the new PODS are running on appropriate nodes
		ginkgo.By("Verify newly created PV node affinity details  and that the new PODS are running on appropriate nodes")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset replicas to 0
		replicas = 0
		ginkgo.By("Scale down statefulset replica count to 0")
		err = scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-3
		Steps:
		1. Create SC with volumeBindingMode as WaitForFirstConsumer with allowed topology details.
		(here in this case Allowed Topology specified:
			region1 > zone1 > building1 > level1 > rack > rack3)
		2. Create StatefulSet with parallel pod management policy with replica count 3 using above SC.
		3. Wait for StatefulSet pods to be in up and running state.
		4. Describe PV's and verify node affinity details of all 5 levels should be displayed.
		5. Verify pods are created on nodes as mentioned in the storage class.
		6. Scale up StatefulSet replica count to 5.
		7. Verify StatefulSet scale	 up is successful.
		8. Verify newly created StatefuSet pods are created on nodes as mentioned in the storage class.
		9. Describe new PV and verify node affinity details of all the 5 levels should be displayed.
		10. Bring down statefulset replica to 0.
		11. Delete statefulset, PVC and SC.
	*/

	ginkgo.It("Provisioning volume when storage class specified with WFC Binding mode "+
		"with allowed topologies and using parallel pod management policy for statefulset", ginkgo.Label(p1,
		block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* Get allowed topologies for Storage Class
		(region1 > zone1 > building1 > level1 > rack > rack3) */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)

		// Create StorageClass with allowed Topologies
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologyForSC,
			"", bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Create StatefulSet using parallel pod management policy
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Updating replicas to 3 and podManagement Policy as Parallel")
		*(statefulset.Spec.Replicas) = 3
		statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &sc.Name
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Wait for StatefulSet pods to be in up and running state
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		/* Verify PV node affinity and that the PODS are running on
		appropriate node as specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running " +
			"on appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale up statefulset replicas to 5
		replicas += 5
		ginkgo.By("Scale up statefulset replica count to 5")
		err = scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* Verify newly created PV node affinity and that the news PODS are running
		on appropriate node as specified in the allowed topologies of SC */
		ginkgo.By("Verify newly created PV node affinity and that the news PODS " +
			"are running on appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset replicas to 0
		replicas = 0
		ginkgo.By("Scale down statefulset replica count to 0")
		err = scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-4
		Steps:
		1. Create SC with Immediate BindingMode with higher level allowed topologies and use
		shared datastore across cluster.
		(here in this case allowed topology - region1 > zone1 > building1).
		2. Create StatefulSet with default pod management policy with replica count 3 using above SC.
		3. Wait for StatefulSet pods to be in up and running state.
		4. Describe PV's and verify node affinity details of all 5 levels should be displayed.
		4a. Verify volume provisioning can be on any rack. PV should have node affinity of all 5.
		5. Verify pods are created on nodes as mentioned in the storage class.
		6. Scale up StatefulSet replica count to 5.
		7. Scale up StatefulSet replica count to 1.
		8. Verify StatefulSet scale up and scale down is successful.
		9. Verify newly created StatefuSet pods are created on nodes as mentioned in the storage class.
		10. Describe newly created PV's and verify node affinity details of all the 5 levels
		should be displayed.
		11. Bring down statefulset replica to 0.
		12. Delete statefulset, PVC and SC.
	*/

	ginkgo.It("Provisioning volume when storage class specified with Immediate BindingMode "+
		"with higher level allowed topologies and using default pod management policy "+
		"for statefulset", ginkgo.Label(p0, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Get allowed topologies for Storage Class (region1 > zone1 > building1)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 3)

		/* Create SC with Immediate BindingMode with higher level allowed topologies
		using shared datastore across cluster */
		scParameters := make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		scParameters["storagepolicyname"] = storagePolicyName
		storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC, "",
			"", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Creating StatefulSet with replica count 3 using default pod management policy
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Wait for StatefulSet pods to be in up and running state
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		/* Verify PV node affinity and that the PODS are running on appropriate
		node as specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running " +
			"on appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale up statefulset replicas to 5
		replicas += 5
		ginkgo.By("Scale up statefulset replica count to 5")
		err = scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset replicas to 1
		replicas -= 1
		ginkgo.By("Scale down statefulset replica count to 1")
		err = scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* "Verify newly created PV node affinity and that the new PODS are
		running on appropriate node as specified in the allowed topologies of SC */
		ginkgo.By("Verify newly created PV node affinity and that the new PODS are running " +
			"on appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset replicas to 0
		replicas = 0
		ginkgo.By("Scale down statefulset replica count to 0")
		err = scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-5
		Steps:
		1. Create SC with Immediate BindingMode with all 5 levels of allowedTopologies
			(provide multiple labels) and shared datasore URL present only in rack2 and rack3.
		(here in this case - region1 > zone1 > building1 > level1 > rack > (rack2 and rack3)
		2. Create StatefulSet with parallel pod management policy with replica count 3 using above SC.
		3. Wait for all StatefulSet Pods to be in up and running state and PVC to be in bound phase.
		5. Describe PV's. Since the datastore mentioned in SC is specific to rack2 and rack3,
			all the nodes should get deployed on rack2 and rack3 only. Node affinity should have
			details of all 5 levels.
		6. Verify that the pods are running on appropriate node.
		7. Scale up Statefulset replica count to 5.
		8. Scale down Statefulset replica count to 1.
		7. Describe newly created PV's and verify volumes are provisioned on appropriate node.
		PV should show proper node affinity details of all 5 levels.
		8. Bring down statefulset replica to 0.
		9. Delete statefulset , PVC, SC
	*/

	ginkgo.It("Provisioning volume when storage class specified with Immediate Bindingmode "+
		"shared datastore url between multiple topology labels and using parallel pod management "+
		"policy for statefulset", ginkgo.Label(p1, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sharedDataStoreUrlBetweenClusters := GetAndExpectStringEnvVar(datstoreSharedBetweenClusters)

		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > (rack2 and rack3)
		Taking Shared datstore between Rack2 and Rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1, leafNodeTag2)

		/* Create SC with Immediate BindingMode with multiple topology labels and datastore
		shared between those labels. */
		scParameters := make(map[string]string)
		scParameters["datastoreurl"] = sharedDataStoreUrlBetweenClusters
		storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC,
			"", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Create StatefulSet using parallel pod management policy
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Updating replicas to 3 and podManagement Policy as Parallel")
		*(statefulset.Spec.Replicas) = 3
		statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &storageclass.Name
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Wait for StatefulSet pods to be in up and running state
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		/* Verify PV node affinity and that the PODS are running on appropriate
		node as specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale up statefulset replicas to 5
		replicas += 5
		ginkgo.By("Scale up statefulset replica count to 5")
		err = scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset replicas to 1
		replicas -= 1
		ginkgo.By("Scale down statefulset replica count to 1")
		err = scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* Verify newly created PV node affinity and that the new PODS are running on
		appropriate node as specified in the allowed topologies of SC */
		ginkgo.By("Verify newly created PV node affinity and that the new PODS are running " +
			"on appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset replicas to 0
		replicas = 0
		ginkgo.By("Scale down statefulset replica count to 0")
		err = scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   TESTCASE-7
	   Steps:
	   1. Create SC with Immediate BindingMode with all 5 levels of allowedTopologies and
	   storage policy specific to rack3.
	   (here in this case - region1 > zone1 > building1 > level1 > rack > rack3)
	   2. Create PVC using above SC.
	   3. Create Deployment set with replica count 1 using above created PVC.
	   4. Vefiry Deployment pod is in up and running state and PVC in bound phase.
	   5. Describe PV and verify pv node affinity should have allowed topology details of SC.
	   6. Verify pod is created on the node as mentioned in the storage class.
	   7. Delete Deployment set
	   8. Delete PVC and SC
	*/
	ginkgo.It("Provisioning volume when storage class specified with "+
		"Immediate BindingMode with multiple allowed topologies and using DeploymentSet "+
		"pod", ginkgo.Label(p1, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var lables = make(map[string]string)
		lables["app"] = "nginx"
		replica := 1
		var pvclaims []*v1.PersistentVolumeClaim

		/* Get allowed topologies for Storage Class
		(region1 > zone1 > building1 > level1 > rack > rack3) */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag2)

		// create SC and PVC
		storagePolicyName := GetAndExpectStringEnvVar(storagePolicyForDatastoreSpecificToCluster)
		scParameters := make(map[string]string)
		scParameters["storagepolicyname"] = storagePolicyName

		ginkgo.By("Create StorageClass and PVC for Deployment")
		sc, pvclaim, err := createPVCAndStorageClass(client, namespace, nil,
			scParameters, diskSize, allowedTopologyForSC, "", false, "")
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
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()

		// Create Deployment Pod with replica count 1 using above PVC
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

		/* Verify PV node affinity and that the PODS are running on appropriate
		node as specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running " +
			"on appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx, client, deployment,
			namespace, allowedTopologyForSC, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-11
		Steps:
		1. Create SC with WFC BindingMode and allowed topology set to multiple labels
		without datastore URL.
		(here in this case - rack > (rack1,rack2,rack3))
		2. Create StatefulSet with default pod management policy with replica count 3 using above SC.
		3. Wait for StatefulSet pods to be in running state and PVC to be in Bound phase.
		4. Describe on the PV's and verify node affinity details.
		5. Verify, PV node affinity of all 5 levels should be displayed.
		5a. Verify, If a volume is provisioned on shared datastore, pv node affinity
			details contain all the availability zone details.
		5b. Verify, If a volume provisioned on a datastore that is shared within
			 the specific zone then node affinity will have details of specific zone and its node labels.
		6. Verify StatefuSet pods are created on nodes as mentioned in the storage class.
		7. Bring down statefulset replica to 0.
		8. Delete statefulset, PVC and SC.
	*/

	ginkgo.It("Provisioning volume when storage class specified with multiple labels "+
		"without specifying datastore url and using default pod management policy "+
		"for statefulset", ginkgo.Label(p2, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Get allowed topologies for Storage Class rack > (rack1,rack2,rack3)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)[4:]

		// Create SC with WFC BindingMode with allowed topology details.
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC, "",
			bindingMode, false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Creating StatefulSet with replica count 3 using default pod management policy
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Wait for StatefulSet pods to be in up and running state
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		/* Verify PV node affinity and that the PODS are running on appropriate node
		as specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset,
			namespace, allowedTopologies, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset to 0 replicas
		replicas -= 3
		ginkgo.By("Scale down statefulset replica count to 0")
		err = scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-12
		Steps:
		1. Create SC with Immediate BindingMode and allowed topology set to invalid topology
		label in any one level.
		(here in this case - region1 > zone1 > building1 > level1 > rack > rack15)
		2. Create PVC using above SC.
		3. Volume Provisioning should fail with error message displayed due to incorrect topology
		labels specified in SC
		4. Delete PVC and SC.
	*/

	ginkgo.It("Verify volume provisioning when storage class specified with invalid "+
		"topology label", ginkgo.Label(p2, block, vanilla, level5, stable, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Get allowed topologies for Storage Class
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)
		allowedTopologyForSC[4].Values = []string{"rack15"}

		/* Create SC with Immediate BindingMode and allowed topology set to
		invalid topology label in any one level */
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC, "", "", false, "")
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
		TESTCASE-14
		Create SC with one topology label with datastore URL
		Steps:
		1. Create SC with Immediate binding mode and one level topology detail
		specified with Datastore URL.
		(here in this case - rack > rack2 and datastore url specific to rack2)
		2. Create PVC using above created SC and wait for PVC to reach bound state.
		2. Describe PV and verify pv node affinity details should hold all the 5 levels of
		topology details.
		3. Create POD using above created PVC.
		4. Verify that the pod are running on appropriate node.
		5. Delete POD, PVC and SC
	*/

	ginkgo.It("Verify volume provisioning when storage class specified with one level "+
		"topology along with datstore url", ginkgo.Label(p2, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Get allowed topologies for Storage Class (rack > rack2)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1)[4:]

		/* Create SC with Immediate BindingMode with single level topology detail
		specified with datatsoreUrl */
		scParameters := make(map[string]string)
		DataStoreUrlSpecificToCluster := GetAndExpectStringEnvVar(datastoreUrlSpecificToCluster)
		scParameters["datastoreurl"] = DataStoreUrlSpecificToCluster
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil,
			scParameters, "", allowedTopologyForSC, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PVC to be bound
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims,
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := persistentvolumes[0]
		volHandle := pv.Spec.CSI.VolumeHandle

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var vmUUID string
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle,
			pod.Spec.NodeName))
		vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle,
					pod.Spec.NodeName))
		}()

		/* Verify PV node affinity and that the PODS are running on appropriate node as
		specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate " +
			"node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-13
		Crete SC with one label specified in storage class without datastore URL
		Steps:
		1. Create SC with Immediate BindingMode with only one level topology detail
		specified without datastore url.
		(here in this case - rack > rack1)
		2. Create PVC using above SC and wait for PVC to reach bound state.
		3. Create POD using above created PVC.
		4. Verify that the pod are running on appropriate node.
		5. Delete POD, PVC and SC
	*/
	ginkgo.It("Verify volume provisioning when storage class specified with single "+
		"level topology without datstore url", ginkgo.Label(p1, block, vanilla, level5,
		stable, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Get allowed topologies for Storage Class (rack > rack1)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)[4:]

		// Create SC with Immediate BindingMode with single level topology detail
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil,
			nil, "", allowedTopologyForSC, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PVC to be bound
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims,
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := persistentvolumes[0]
		volHandle := pv.Spec.CSI.VolumeHandle

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var vmUUID string
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle,
					pod.Spec.NodeName))
		}()

		/* Verify PV node affinity and that the PODS are running on appropriate node as
		specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-6
		Steps:
		1. Create FCD on the datastore present in datacenter > cluster2
		2. Create SC which points to region1 > zone1 > building1 > level1 > rack > rack2.
		3. Create PV using the above created FCD, SC and 'nodeSelectorTerms' of SC.
		4. Create PVC using above created SC and PV.
		5. Wait for PV, PVC to be in bound.
		6. Describe  PV and verify node affinity details. It should display all 5 levels.
		7. Create POD using the above PVC.
		8. POD should come up on the node which is present in the same zone as mentioned in
		the storage class.
		9. Delete POD, PVC, PV and SC.
	*/
	ginkgo.It("Verify static volume provisioning with FCD and storage class with allowed "+
		"topologies", ginkgo.Label(p1, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > rack2 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1)

		// Create SC with Immediate BindingMode and allowed topology set to 5 levels
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating FCD Disk")
		fcdID, err := e2eVSphere.createFCD(ctx, "BasicStaticFCD", diskSizeInMb,
			DSSharedToSpecificCluster.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
			v1.PersistentVolumeReclaimDelete, storageclass.Name, staticPVLabels,
			diskSize, allowedTopologyForSC)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		if err != nil {
			return
		}
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC")
		pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc.Spec.StorageClassName = &storageclass.Name
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc,
			metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(),
			namespace, pv, pvc))

		ginkgo.By("Verifying CNS entry is present in cache")
		_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the PV Claim")
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace),
				"Failed to delete PVC", pvc.Name)
			pvc = nil

			ginkgo.By("Verify PV should be deleted automatically")
			framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout))
			pv = nil

		}()

		ginkgo.By("Creating the Pod")
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		pod, err := createPod(client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle,
			pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")

		ginkgo.By("Verify container volume metadata is present in CNS cache")
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolume with VolumeID: %s", pv.Spec.CSI.VolumeHandle))
		_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := []types.KeyValue{{Key: "fcd-id", Value: fcdID}}
		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
			pvc.Name, pv.ObjectMeta.Name, pod.Name, labels...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(client, pod), "Failed to delete pod",
				pod.Name)

			ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", pod.Spec.NodeName))
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")
		}()

		/* Verify PV node affinity and that the PODS are running on appropriate node
		as specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		TESTCASE-15
		Steps:
		1. Create FCD on the datastore present in datacenter1 > cluster1/cluster2/cluster3.
		2. Create SC which points to region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3.
		3. Create PV using the above created FCD and SC and reclaim policy retain.
		4. Create PVC using above created SC and PV.
		5. Wait for PV, PVC to be in bound state.
		6. Create POD using above PVC.
		7. Wait for Pod to be in up and running state.
		8. Describe  PV and verify node affinity details. It should display all 5 levels.
		9. POD should come up on the appropriate node as mentioned in storage class.
		10. Delete POD and PVC.
		11. Delete claim ref from PV to bring it to available state.
		12. Recreate PVC with the same name used in step 4.
		13. PVC should be created and should reach bound state.
		14. Create Pod using above created PVC. Wait for Pod to reach running state.
		15. Describe PV and verify node affinity details. It should display all 5 levels.
		16. Verfiy Pod is running on appropriate node as mentioned in SC.
		17. Verify CNS metadata for PVC.
		18. Delete POD, PVC, PV and SC.

	*/
	ginkgo.It("Verify static volume provisioning with FCD and storage class specified "+
		"with datastore url and set of allowed "+
		"topologies", ginkgo.Label(p1, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* Get allowed topologies for Storage Class
		zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)

		// Create SC with Immediate BindingMode and allowed topology set to 5 levels
		scParameters := make(map[string]string)
		scParameters["datastoreurl"] = datastoreURL
		storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC, "", "",
			false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating FCD disk
		ginkgo.By("Creating FCD Disk")
		fcdID, err := e2eVSphere.createFCD(ctx, "BasicStaticFCD", diskSizeInMb,
			defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync "+
			"with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		// Creating label for PV. PVC will use this label as Selector to find PV.
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID

		// Creating PV using above created SC and FCD
		ginkgo.By("Creating the PV")
		pv := getPersistentVolumeSpecWithStorageClassFCDNodeSelector(fcdID,
			v1.PersistentVolumeReclaimRetain, storageclass.Name, staticPVLabels,
			diskSize, allowedTopologyForSC)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		if err != nil {
			return
		}
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Creating PVC using above created PV
		ginkgo.By("Creating the PVC")
		pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc.Spec.StorageClassName = &storageclass.Name
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc,
			metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(),
			namespace, pv, pvc))
		ginkgo.By("Verifying CNS entry is present in cache")
		_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the PV Claim")
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace),
				"Failed to delete PVC", pvc.Name)
			pvc = nil

			ginkgo.By("Deleting the PV")
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		// Creating Pod using above created PVC
		ginkgo.By("Creating the Pod")
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		pod, err := createPod(client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle,
			pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")

		ginkgo.By("Verify container volume metadata is present in CNS cache")
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolume with VolumeID: %s", pv.Spec.CSI.VolumeHandle))
		_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := []types.KeyValue{{Key: "fcd-id", Value: fcdID}}
		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
			pvc.Name, pv.ObjectMeta.Name, pod.Name, labels...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(client, pod), "Failed to delete pod",
				pod.Name)

			ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", pod.Spec.NodeName))
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")
		}()

		/* Verify PV node affinity and that the PODS are running on appropriate node as
		specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Deleting Pod
		ginkgo.By("Deleting the Pod")
		framework.ExpectNoError(fpod.DeletePodWithWait(client, pod), "Failed to delete pod", pod.Name)

		ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", pod.Spec.NodeName))
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle,
			pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")

		// Deleting PVC
		ginkgo.By("Delete PVC")
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name,
			*metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("PVC %s is deleted successfully", pvc.Name)

		// Verify PV exist and is in released status
		ginkgo.By("Check PV exists and is released")
		pv, err = waitForPvToBeReleased(ctx, client, pv.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("PV status after deleting PVC: %s", pv.Status.Phase)

		// Remove claim from PV and check its status.
		ginkgo.By("Remove claimRef from PV")
		pv.Spec.ClaimRef = nil
		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("PV status after removing claim : %s", pv.Status.Phase)

		// Recreate PVC with same name as created above
		ginkgo.By("ReCreating the PVC")
		pvclaim := getPersistentVolumeClaimSpec(namespace, nil, pv.Name)
		pvclaim.Spec.StorageClassName = &storageclass.Name
		pvclaim.Name = pvc.Name
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvclaim,
			metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for newly created PVC to bind to the existing PV
		ginkgo.By("Wait for the PVC to bind the lingering pv")
		err = fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, pv,
			pvclaim)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Creating POD to use this PVC, and verify volume has been attached.
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle,
			vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		/* Verify PV node affinity and that the PODS are running on appropriate node as
		specified in the allowed topologies of SC */
		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod, namespace,
			allowedTopologies, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify volume metadata for POD, PVC and PV
		ginkgo.By("Verify volume metadata for POD, PVC and PV")
		err = waitAndVerifyCnsVolumeMetadata(pv.Spec.CSI.VolumeHandle, pvclaim, pv, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})
})
