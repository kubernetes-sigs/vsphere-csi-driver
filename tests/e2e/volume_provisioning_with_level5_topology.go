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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

var _ = ginkgo.Describe("[csi-topology-vanilla-level5] Topology-Aware-Provisioning-With-Statefulset", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	var (
		client                  clientset.Interface
		namespace               string
		bindingMode             storagev1.VolumeBindingMode
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		storagePolicyName       string
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		topologylength := 5
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap, topologylength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologylength)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})

	/*
		TESTCASE-1
			Steps:
			1. Create SC without specifying any topology details using volumeBindingMode as WaitForFirstConsumer.
			2. Create StatefulSet with default pod management policy with replica count 3 using above SC.
			3. Wait for StatefulSet pods to be in up and running state.
			4. Since there is no Topology describe on SC, volume provisioning should happen on any availability zone.
			5. Describe on the PV's and verify node affinity details.
			5a. Verify, PV node affinity of all 5 levels should be displayed.
			5b. Verify, If a volume is provisioned on shared datastore, pv node affinity details contain all the availability zone details.
			5c. Verify, If a volume provisioned on a datastore that is shared within the specific zone then node affinity will have details of specific zone and its node labels.
			6. Verify StatefuSet pods are created on nodes as mentioned in the storage class.
			7. Bring down statefulset replica to 0.
			8. Delete statefulset, PVC and SC.
	*/

	ginkgo.It("Provisioning volume when no topology details specified in storage class and using default pod management policy for statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Creating StorageClass when no topology details are specified using WFC Binding mode
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "", bindingMode, false)
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

		// Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset, namespace, allowedTopologies)

		// Scale down statefulset to 0 replicas
		replicas -= 3
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas)

	})

	/*
		TESTCASE-2
			Steps:
			1. Create SC without specifying any topology details using volumeBindingMode as WaitForFirstConsumer.
			2. Create StatefulSet with parallel pod management policy with replica count 3 using above SC.
			3. Wait for StatefulSet pods to be in up and running state.
			4. Since there is no Topology describe on SC, volume provisioning should happen on any availability zone.
			5. Describe on the PV's and verify node affinity details.
			5a. Verify, PV node affinity of all 5 levels should be displayed.
			5b. Verify, If a volume is provisioned on shared datastore, pv node affinity details contain all the availability zone details.
			5c. Verify, If a volume provisioned on a datastore that is shared within the specific zone then node affinity will have details of specific zone and its node labels.
			6. Verify StatefuSet pods are created on nodes as mentioned in the storage class.
			7. Scale up the StatefulSet replica count to 5.
			8. Scale down the StatefulSet replica count to 1.
			9. Verify scale up and scale down of SttaefulSet pods are successful.
			10. Verify newly created StatefuSet pods are created on nodes as mentioned in the storage class.
			11. Describe PV and verify the node affinity details should display all 5 topology levels.
			12. Bring down all statefulset replica to 0
			13. Delete statefulset, PVC and SC.
	*/

	ginkgo.It("Provisioning volume when no topology details specified in storage class and using parallel pod management policy for statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Creating StorageClass when no topology details are specified using WFC Binding mode
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "", bindingMode, false)
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
		ginkgo.By("Updating replicas to 3 and podManagement Policy as Parallel")
		*(statefulset.Spec.Replicas) = 3
		statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = sc.Name
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

		// Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset, namespace, allowedTopologies)

		// Scale up statefulset replicas to 5
		replicas += 5
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas)

		// Scale down statefulset replicas to 1
		replicas -= 1
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas)

		// Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset, namespace, allowedTopologies)

		// Scale down statefulset replicas to 0
		replicas = 0
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas)

	})

	/*
		TESTCASE-3
			Steps:
			1. Create SC with volumeBindingMode as WaitForFirstConsumer with allowed topology details.
			2. Create StatefulSet with parallel pod management policy with replica count 3 using above SC.
			3. Wait for StatefulSet pods to be in up and running state.
			4. Describe PV's and verify node affinity details of all 5 levels should be displayed.
			(here in this case, region1 > zone1 > building1 > level1 > rack > rack3)
			5. Verify pods are created on nodes as mentioned in the storage class.
			6. Scale up StatefulSet replica count to 5.
			7. Verify StatefulSet scale up is successful.
			8. Verify newly created StatefuSet pods are created on nodes as mentioned in the storage class.
			9. Describe PV and verify node affinity details of all the 5 levels should be displayed.
			10. Bring down statefulset replica to 0.
			11. Delete statefulset, PVC and SC.
	*/

	ginkgo.It("Provisioning volume when storage class specified with allowed topologies and using parallel pod management policy for statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Get allowed topologies for Storage Class
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 5, 4, 2)

		// Create StorageClass with allowed Topologies
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologyForSC, "", bindingMode, false)
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
			Annotations["volume.beta.kubernetes.io/storage-class"] = sc.Name
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

		// Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset, namespace, allowedTopologies)

		// Scale up statefulset replicas to 5
		replicas += 5
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas)

		// Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset, namespace, allowedTopologies)

		// Scale down statefulset replicas to 0
		replicas = 0
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas)

	})

	/*
		TESTCASE-4
		Steps:
		1. Create SC with volumeBindingMode as Immediate with allowed topology details
		(here in this case allowed topology - region1 > zone1 > building1).
		2. Create StatefulSet with default pod management policy with replica count 3 using above SC.
		3. Wait for StatefulSet pods to be in up and running state.
		4. Describe PV's and verify node affinity details of all 5 levels should be displayed.
		4a. Verify volume provisioning can be on any rack. PV should have node affinity of all 5.
		(Ex: region1 > zone1 > building1 > (rack1/rack/rack3))
		5. Verify pods are created on nodes as mentioned in the storage class.
		6. Scale up StatefulSet replica count to 5.
		7. Scale up StatefulSet replica count to 1.
		8. Verify StatefulSet scale up and scale down is successful.
		9. Verify newly created StatefuSet pods are created on nodes as mentioned in the storage class.
		10. Describe newly created PV's and verify node affinity details of all the 5 levels should be displayed.
		11. Bring down statefulset replica to 0.
		12. Delete statefulset, PVC and SC.
	*/

	ginkgo.It("Provisioning volume when storage class specified with BindingMode as Immediate with allowed topology details and using default pod management policy for statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Get allowed topologies for Storage Class
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 3)

		// Create SC with BindingMode as Immediate with allowed topology details.
		scParameters := make(map[string]string)
		scParameters["storagepolicyname"] = storagePolicyName
		storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
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

		// Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset, namespace, allowedTopologies)

		// Scale up statefulset replicas to 5
		replicas += 5
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas)

		// Scale down statefulset replicas to 1
		replicas -= 1
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas)

		// Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset, namespace, nil)

		// Scale down statefulset replicas to 0
		replicas = 0
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas)
	})

	/*
	   TESTCASE-5
	   Steps:
	   1. Create SC with Immediate BindingMode with all 5 levels of allowedTopologies (provide multiple labels) and shared datasore URL present only in rack2 and rack3.
	   (here in this case - region1 > zone1 > building1 > level1 > rack > (rack2 and rack3)
	   2. Create StatefulSet with parallel pod management policy with replica count 3 using above SC.
	   3. Wait for all StatefulSet Pods to be in up and running state and PVC to be in bound phase.
	   5. Describe PV's. Since the datastore mentioned in SC is specific to rack2 and rack3, all the nodes should get deployed on rack2 and rack3 only. Node affinity should have details of all 5 levels.
	   6. Verify that the pods are running on appropriate node.
	   7. Scale up Statefulset replica count to 5.
	   8. Scale down Statefulset replica count to 1.
	   7. Describe on the newly created PV's and Verify volumes are provisioned on appropriate node . PV should show proper node affinity details of all 5 levels
	   8. Bring down statefulset replica to 0.
	   9. Delete statefulset , PVC, SC
	*/

	ginkgo.It("Provisioning volume when storage class specified with Immediate Bindingmode shared datastore url and allowed topologies using parallel pod management policy for statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sharedDataStoreUrl := GetAndExpectStringEnvVar(datstoreSharedBetweenClusters)
		// Get allowed topologies for Storage Class
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 5, 4, 1)

		// Create SC with BindingMode as Immediate with allowed topology details.
		scParameters := make(map[string]string)
		scParameters["datastoreurl"] = sharedDataStoreUrl
		storageclass, err := createStorageClass(client, scParameters, allowedTopologyForSC, "", "", false, "nginx-sc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
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
			Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
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

		// Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset, namespace, allowedTopologies)

		// Scale up statefulset replicas to 5
		replicas += 5
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas)

		// Scale down statefulset replicas to 1
		replicas -= 1
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas)

		// Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SCC
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client, statefulset, namespace, allowedTopologies)

		// Scale down statefulset replicas to 0
		replicas = 0
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas)
	})
	/*
	   TESTCASE-7
	   Steps:
	   1. Create SC with Immediate BindingMode with all 5 levels of allowedTopologies and storage policy specific to rack3.
	   (here in this case - region1 > zone1 > building1 > level1 > rack > rack3)
	   2. Create PVC using above SC.
	   3. Create Deployment set with replica count 1 using above created PVC.
	   4. Vefiry Deployment pod is in up and running state and PVC in bound phase.
	   5. Describe PV and verify pv node affinity should have allowed topology details of SC.
	   6. Verify pod is created on the node as mentioned in the storage class.
	   7. Delete Deployment set
	   8. Delete PVC and SC
	*/
	ginkgo.It("Provisioning volume when storage class specified with allowed topologies and using Deployment set pod", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		storagePolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameForNonSharedDatastores)
		var lables = make(map[string]string)
		lables["app"] = "nginx"
		replica := 1

		// Get allowed topologies for Storage Class
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 5, 4, 2)

		// Create SC with BindingMode as Immediate with allowed topology details.
		scParameters := make(map[string]string)
		scParameters["storagepolicyname"] = storagePolicyName

		ginkgo.By("Create StorageClass for Deployment")
		sc, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, allowedTopologyForSC, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create PVC using above SC
		ginkgo.By("Creating PVC")
		pvclaims = append(pvclaims, pvclaim)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()

		// Create Deployment Pod with replica count 1 using above PVC
		ginkgo.By("Create Deployments")
		deployment, err := createDeployment(ctx, client, int32(replica), lables, nil, namespace, pvclaims, "", false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC
		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node as specified in the allowed topologies of SC")
		verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx, client, deployment, namespace, allowedTopologyForSC)
	})

})
