/*
	Copyright 2024 The Kubernetes Authors.

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
	"time"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwx-topology] RWX-Topology", func() {
	f := framework.NewDefaultFramework("rwx-topology")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                  clientset.Interface
		namespace               string
		bindingMode             storagev1.VolumeBindingMode
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		leafNode                int
		leafNodeTag0            int
		leafNodeTag1            int
		leafNodeTag2            int
		topologyLength          int
		datastoreUrls           []string
		rack1DatastoreListMap   map[string]string
		rack2DatastoreListMap   map[string]string
		rack3DatastoreListMap   map[string]string
		sshClientConfig         *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd string
		allMasterIps            []string
		masterIp                string
		dataCenters             []*object.Datacenter
		clusters                []string
		datastoreUrlsRack1      []string
		datastoreUrlsRack2      []string
		datastoreUrlsRack3      []string
		scParameters            map[string]string
		storagePolicyName       string
		restConfig              *restclient.Config
		snapc                   *snapclient.Clientset
		pandoraSyncWaitTime     int
		accessmode              v1.PersistentVolumeAccessMode
		allowedTopologies       []v1.TopologySelectorLabelRequirement
	)

	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()

		// delete older stale entries of storage class if left in the setup
		storageClassList, err := client.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(storageClassList.Items) != 0 {
			for _, sc := range storageClassList.Items {
				gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
		}

		// fetch list of k8s nodes
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		// read export variables
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		//global variables
		scParameters = make(map[string]string)
		accessmode = v1.ReadWriteMany
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		// read topology map
		topologyLength, leafNode, leafNodeTag0, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap,
			topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)

		// fetching k8s master ip
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]

		// fetching datacenter details
		dataCenters, err = e2eVSphere.getAllDatacenters(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching cluster details
		clusters, err = getTopologyLevel5ClusterGroupNames(masterIp, sshClientConfig, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching list of datastores available in different racks
		rack1DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rack2DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rack3DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[2])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// create datastore map for each cluster
		for _, value := range rack1DatastoreListMap {
			datastoreUrlsRack1 = append(datastoreUrlsRack1, value)
		}

		for _, value := range rack2DatastoreListMap {
			datastoreUrlsRack2 = append(datastoreUrlsRack2, value)
		}

		for _, value := range rack3DatastoreListMap {
			datastoreUrlsRack3 = append(datastoreUrlsRack3, value)
		}

		datastoreUrls = append(datastoreUrls, datastoreUrlsRack1...)
		datastoreUrls = append(datastoreUrls, datastoreUrlsRack2...)
		datastoreUrls = append(datastoreUrls, datastoreUrlsRack3...)

		// snapshot
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		Deployment Pods with multiple replicas attached to single RWX PVC

		Steps:
		1. Create a StorageClass with BindingMode set to Immediate with no allowed topologies specified and with fsType as "nfs4"
		2. Create a PVC with "RWX" access mode.
		3. Wait for PVC to reach Bound state.
		4. Create deployment Pods with replica count 3 and attach it to above PVC.
		5. Wait for deployment Pods to reach Running state.
		6. As the SC lacks a specific topology, volume provisioning and Pod placement will occur in any available availability zones (AZs).
		7. Perform cleanup by deleting Pods, PVCs, and the SC.
	*/

	ginkgo.It("TC1Deployment Pods with multiple replicas attached to a single RWX PVC", ginkgo.Label(p0, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType
		replica := 3

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype with no allowed topology specified %q", accessmode, nfs4FSType))
		storageclass, pvclaim, pv, err := createRwxPvcWithStorageClass(client, namespace, labelsMap, scParameters, "", nil, "", false, accessmode, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv.Spec.CSI.VolumeHandle
		pvclaims := append(pvclaims, pvclaim)
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Deployment")
		deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nil, namespace, pvclaims, execRWXCommandPod, false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for deployment pods to be up and running")
		pods, err := fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		TESTCASE-2
		StatefulSet Pods with multiple replicas attached to multiple RWX PVCs with scaling operation

		Steps:
		1. Create a StorageClass with BindingMode set to WFFC with no allowed topologies specified and with fsType as "nfs4"
		2. Use the SC to deploy a StatefulSet with 3 replicas, ReadWriteMany access mode, and Parallel pod management policy. Allow time for PVCs and Pods to reach Bound and Running states, respectively.
		3. Increase StatefulSet replica count to 5. Verify scaling operation.
		4. Decrease replica count to 1. Verify smooth scaling operation.
		5. As the SC lacks a specific topology, volume provisioning and Pod placement will occur in any available availability zones (AZs) for new Pod and PVC.
		6. Perform cleanup by deleting Pods, PVCs, and the SC.
	*/

	ginkgo.It("TC2Scaling operations involving StatefulSet Pods with multiple replicas, each connected to RWX PVCs", ginkgo.Label(p0, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stsReplicas := 3
		scaleUpReplicaCount := 5
		scaleDownReplicaCount := 1
		scParameters[scParamFsType] = nfs4FSType

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype with no allowed topology specified and with WFFC binding mode %q", accessmode, nfs4FSType))
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", bindingMode, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true, int32(stsReplicas), false,
			nil, 0, false, false, false, "", v1.ReadWriteMany, sc, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount), int32(scaleDownReplicaCount),
			statefulset, false, namespace, nil, true, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-3
		Multiple standalone pods are connected to a single RWX PVC, with each pod configured to have distinct read/write access permissions
		SC → WFC Binding Mode with allowed topology specified i.e. region-1 > zone-1 > building-1 > level-1 > rack-3

		Steps:
		1. Create a Storage Class with BindingMode set to Immediate and allowed topologies for level-5 setup to
		   region-1 > zone-1 > building-1 > level-1 > rack-3 and for level-2 setup set region-1/zone-1
		2. Create RWX PVC using the SC created above.
		3. Wait for PVC to reach Bound state.
		4. Volume provisioning should happen on the datastore of the allowed topology as given in the SC.
		5. Create 3 standalone Pods using the PVC created in step #2.
		6. For Pod1 and Pod2 set read/write access and try reading/writing data into the volume. For the 3rd Pod set readOnly mode to true and verify
		that any write operation by Pod3 onto the volume should fail with an appropriate error message.
		7. Wait for Pods to reach Running Ready state.
		8. Pods can be created on any AZ.
		9. Verify CNS metadata for PVC and Pod.
		10. Perform cleanup by deleting Pod,PVC and SC.

	*/

	ginkgo.It("TC3Multiple Standalone Pods attached to a single RWX PVC", ginkgo.Label(p0, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scParameters[scParamFsType] = nfs4FSType
		no_pods_to_deploy := 3

		// Create allowed topologies for Storage Class: region1 > zone1 > building1 > level1 > rack > rack3
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, topologyLength, leafNode, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed topology set to cluster-3", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, pv, err := createRwxPvcWithStorageClass(client, namespace, nil, scParameters, "", allowedTopologyForSC, "", false, accessmode, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv.Spec.CSI.VolumeHandle
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(volHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create 3 standalone Pods using the same PVC")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim, false, execRWXCommandPod, no_pods_to_deploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err := fpod.DeletePodWithWait(client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
	})

	/*
		TESTCASE-4
		Deployment Pods with multiple replicas attached to single RWX PVC with vSAN storage policy of rack-1 defined in SC aloing with top level allowed topology
		SC → Immediate Binding Mode with allowed topology specified as region-1 > zone-1 > building-1 and Storage policy specified which is tagged to datastore in rack-1 (vSAN DS)

		Steps:
		1. Create a Storage Class with immediate Binding mode and allowed topology set to region-1 > zone-1 > building-1.
		2. Set a Storage policy specific to datastore in rack-1 (vSAN tagged Storage Policy) in SC.
		3. Create a RWX PVC using the Storage Class created in step #1.
		4. Wait for PVC to reach Bound state.
		5. Volume provisioning should happen on the datastore of the allowed topology as given in the SC.
		6. Create deployment Pod with relica count 3 using the PVC created in step #3.
		7. Set node selector teams so that all Pods should get created on rack-1
		8. Wait for deployment Pod to reach running ready state.
		9. Deployment Pods can be created on any worker node on any AZ.
		10. Verify CNS metadata for PVC and Pod.
		11. Perform scaleup operation on deployment Pods. Increase the replica count from 3 to 6.
		12. Verify scaling operation should go fine.
		13. Perform scale down operation on deployment Pods. Decrease the replica count from 6 to 2.
		14. Verify scale down operation went smooth.
		15. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("TC4", ginkgo.Label(p0, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		replica := 3
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType
		storagePolicyName = GetAndExpectStringEnvVar(envVsanDatastoreCluster1StoragePolicy)
		scParameters["storagepolicyname"] = storagePolicyName

		ginkgo.By("Set specific allowed topology for node selector terms")
		allowedTopologies = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)[4:]

		nodeSelectorTerms := getNodeSelectorMapForDeploymentPods(allowedTopologies)

		// Get allowed topologies for Storage Class (region1 > zone1 > building1)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 3)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaim, pv, err := createRwxPvcWithStorageClass(client, namespace, labelsMap, scParameters, "",
			allowedTopologyForSC, "", false, accessmode, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv.Spec.CSI.VolumeHandle
		pvclaims := append(pvclaims, pvclaim)
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume placement")
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(volHandle, datastoreUrls)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create Deployments and specify node selector terms")
		deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nodeSelectorTerms, namespace,
			pvclaims, execRWXCommandPod1, false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for deployment pods to be up and running")
		pods, err := fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a selected node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, true, pods, pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume metadata for POD, PVC and PV")
		err = waitAndVerifyCnsVolumeMetadata(volHandle, pvclaim, pv, &pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment to 6 replica")
		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err = fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod = pods.Items[0]
		rep := deployment.Spec.Replicas
		*rep = 6
		deployment.Spec.Replicas = rep

		deployment, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpod.WaitForPodNotFoundInNamespace(client, deployment.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to 2 replica")
		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err = fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod = pods.Items[0]
		rep = deployment.Spec.Replicas
		*rep = 2
		deployment.Spec.Replicas = rep

		deployment, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpod.WaitForPodNotFoundInNamespace(client, deployment.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-5
		Deployment Pods with multiple replicas attached to single RWX PVC
		SC → Immediate Binding Mode with allowed topology specified  as region-1 > zone-1 > building-1 > levl-1 > rack-2/rack-3 and
		datastore url specified which is shared across rack-2 and rack-3 (Non-vSAN FS datastore)

		Steps:
		1. Create a Storage Class with binding mode set to Immediate and with allowed topology set to
		region-1 > zone-1 > building-1 > levl-1 > rack-2/rack-3 for level-5
		2. Set datastore url shared across rack-2 and rack-3 (take a datastore which is not file Share compatible)
		3. Create RWX PVC using the SC created above.
		4. PVC creation will be stuck in a Pending state with an appropriate error due to non-VSAN FS datastore used.
		5. Perform cleanup by deleting PVC, SC.
	*/

	ginkgo.It("TC5", ginkgo.Label(p1, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType
		storagePolicyName = GetAndExpectStringEnvVar(envNonVsanFSDatastoreUrl)
		scParameters["storagepolicyname"] = storagePolicyName
		expectedErrMsg := "No compatible datastores found for storage policy"

		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > (rack2 and rack3)
		Taking Shared datstore between Rack2 and Rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no allowed topolgies specified", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters, "", allowedTopologyForSC, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid VSAN datastore is specified in storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
		TESTCASE-6
		Deployment Pod creation
		SC → Immediate Binding Mode with allowed topology specified as region-1 > zone-1 > building-1 > level-1 > rack-3 and
		storage policy specified which is tagged to datastore in rack-3 (vSAN DS)

		Steps:
		1. Create a Storage Class with volumeBindingMode set to Immediate and specify allowed topologies for the SC, such as region-1 > zone-1 > building-1 > level-1 > rack-3 and a storage policy specific to cluster3 (rack-3).
		2. Create a PVC with accessmode set to "RWX".
		3. Verify volume provisioning should happen on the SC allowed topology.
		3. Create a deployment Pod using the PVC created in step 2 with a replica count of 1 and configured with ReadWriteMany (RWX) access mode.
		4. Deployment Pod can be created on any AZ.
		5. Verify CNS metadata for PVC and Pod.
		6. Perform cleanup by deleting deployment Pod, PVC and SC.
	*/

	ginkgo.It("TC4", ginkgo.Label(p0, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		replica := 3
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType
		storagePolicyName = GetAndExpectStringEnvVar(envVsanDatastoreCluster1StoragePolicy)
		scParameters["storagepolicyname"] = storagePolicyName

		ginkgo.By("Set specific allowed topology for node selector terms")
		allowedTopologies = getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)[4:]

		nodeSelectorTerms := getNodeSelectorMapForDeploymentPods(allowedTopologies)

		// Get allowed topologies for Storage Class (region1 > zone1 > building1)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, 3)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaim, pv, err := createRwxPvcWithStorageClass(client, namespace, labelsMap, scParameters, "",
			allowedTopologyForSC, "", false, accessmode, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv.Spec.CSI.VolumeHandle
		pvclaims := append(pvclaims, pvclaim)
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume placement")
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(volHandle, datastoreUrls)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create Deployments and specify node selector terms")
		deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nodeSelectorTerms, namespace,
			pvclaims, execRWXCommandPod1, false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for deployment pods to be up and running")
		pods, err := fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a selected node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologies, true, pods, pv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume metadata for POD, PVC and PV")
		err = waitAndVerifyCnsVolumeMetadata(volHandle, pvclaim, pv, &pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment to 6 replica")
		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err = fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod = pods.Items[0]
		rep := deployment.Spec.Replicas
		*rep = 6
		deployment.Spec.Replicas = rep

		deployment, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpod.WaitForPodNotFoundInNamespace(client, deployment.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to 2 replica")
		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err = fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod = pods.Items[0]
		rep = deployment.Spec.Replicas
		*rep = 2
		deployment.Spec.Replicas = rep

		deployment, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpod.WaitForPodNotFoundInNamespace(client, deployment.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-7
		StatefulSet Workload with Parallel Pod Management and Scale-Up/Scale-Down Operations
		SC → WFC Binding Mode with allowed topology specified as region-1 > zone-1

		Steps:
		1. Create a Storage Class with volumeBindingMode set to WFFC and specify allowed topology (e.g., region-1 > zone-1).
		2. Create statefulSet with 5 replicas, each configured with ReadWriteMany (RWX) access mode and with default Pod management policy
		3. Ensure that all the PVCs and Pods are successfully created.
		-  The PVCs should be created in any available Availability Zone (AZ).
		-  The Pods should be created in any AZ
		4. Increase the replica count of statefulset to 5.
		5. Decrease the replica count of statefulset to 1
		6. Verify that the scaling up and scaling down of the StatefulSet is successful.
		7. Volume provisioning and Pod placement will occur in the available availability zones (AZs) specified in the SC during the scale-up operation.
		8. Verify CNS metadata for any one PVC and Pod.
		9. Perform cleanup by deleting StatefulSet, PVCs, and SC
	*/

	/*
		TESTCASE-8
		Storage Class Creation with Invalid Label Details in One Level
		SC → Immediate Binding Mode with invalid allowed topology specified  i.e. region-1 > zone-1 > building-1 > level-1 > rack-15

		Steps:
		1. Create a Storage Class with invalid label details, specifically using a label path such as "region-1 > zone-1 > building-1 > level-1 > rack-15"
		2. Create a PVC) using the SC created in step #1 with RWX access mode.
		3. As the label path is invalid, an appropriate error message should be displayed.
		4. Perform cleanup by deleting PVC and SC.
	*/

	ginkgo.It("TC8", ginkgo.Label(p1, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType
		storagePolicyName = GetAndExpectStringEnvVar(envNonVsanFSDatastoreUrl)
		scParameters["storagepolicyname"] = storagePolicyName
		expectedErrMsg := "No compatible datastores found for storage policy"

		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > (rack2 and rack3)
		Taking Shared datstore between Rack2 and Rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no allowed topolgies specified", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters, "", allowedTopologyForSC, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid VSAN datastore is specified in storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
				TESTCASE-9
				Create Storage Class with a single label in allowed topology
				SC → Immediate Binding Mode with specific allowed topology specified  i.e. k8s-rack > rack-1

				Steps:
		    1. Create a Storage Class with only one level of topology details specified i.e. k8s-rack > rack-1.
			2. Create a Persistent Volume Claim (PVC) using the Storage Class created in step #1, with ReadWriteMany (RWX) access mode.
		    Wait for PVC to reach Bound state.

		    Create a Pod using the PVC created in the step #2.
		    Wait for Pod to be in running state.

		    The PVC should get created on the AZ as specified in the SC allowed topology and the Pod placement should happen on the nodes of the specified allowed topology.

		    Perform cleanup by executing the following actions:
		        Delete the Pods.
		        Delete the Persistent Volume Claims (PVCs).
		        Delete the Storage Class (SC).
	*/

	/*
		TESTCASE-11 -----------> complete
		Volume expansion on file volumes
		SC → all levels of topology details with Immediate Binding mode

		Steps:
		1. Create Storage Class with all levels of topology details specified in the Storage Class and with Immediate Binding mode.
		2. Create PVC with RWX access mode using SC created above.
		3. Verify that the PVC transitions to the Bound state and PVC is created in any of the Availability Zone.
		4. Trigger Offline Volume Expansion on the PVC created in step #2
		5. Verify that the PVC resize operation fails with an appropriate error message, as volume expansion is not supported for File Volumes.
		6. Perform Cleanup by deleting PVC and SC.
	*/

	ginkgo.It("TC11", ginkgo.Label(p1, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType

		// Get allowed topologies for Storage Class i.e. region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, topologyLength)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q and set allowVolumeExpansion to true with all allowed topolgies specified", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters, "", allowedTopologyForSC, "", true, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for PVC and PV to reach Bound state")
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Modify PVC spec to trigger volume expansion, Expect to fail as file volume expansion is not supported")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)

		newPVC, err := expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim = newPVC
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Verify if controller resize failed")
		err = waitForPvResizeForGivenPvc(pvclaim, client, pollTimeoutShort)
		gomega.Expect(err).To(gomega.HaveOccurred())

		expectedErrMsg := "volume expansion is only supported for block volume type"
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(),
			fmt.Sprintf("Expected pvc creation failure with error message: %s", expectedErrMsg))
	})

	/*
		TESTCASE-12 ----------> complete
		PVC creation using shared datastore lacking capability of vSAN FS
		SC → default values, shared datastore url passed shared across all AZs lacking vSAN FS capability, Immediate Binding mode

		Steps:
		1. Create a Storage Class with default values and pass datastore Url shared across all AZs but lacking the vSAN FS capability with Immediate binding mode.
		2. Create a PVC using the SC created in step #1, with ReadWriteMany (RWX) access mode.
		3. PVC should get stuck in Pending state with appropriate error message because the datastore url passed in SC is not vSAN FS enabled datastore
		4. Perform cleanup by deleting PVC and SC
	*/

	ginkgo.It("TC12", ginkgo.Label(p1, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType
		datastoreurl := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		scParameters[scParamDatastoreURL] = datastoreurl

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no allowed topolgies specified", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters, "", nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid VSAN datastore is specified in storage class datastore url")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "failed to provision volume with StorageClass \"" + storageclass.Name + "\""
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
		TESTCASE-13
		Allowed topology specifies rack-2, but the storage policy is provided from rack-3
		SC → specific allowed topology i.e. k8s-rack:rack-2 and storage Policy specified tagged with rack-3 datastore

		Steps:
		1. Create a Storage Class  with specific allowed topology (rack-2) and storage policy tagged with rack-3 vSAN datastore
		2. Create a PVC using the SC created in step #1, with ReadWriteMany (RWX) access mode.
		3. PVC should get stuck in Pending state with an appropriate error message
		4. Perform cleanup by deleting PVC and SC
	*/

	ginkgo.It("TC13", ginkgo.Label(p2, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType
		storagePolicyName = GetAndExpectStringEnvVar(storagePolicyForDatastoreSpecificToCluster)
		scParameters["storagepolicyname"] = storagePolicyName

		/* Get allowed topologies for Storage Class
		(region1 > zone1 > building1 > level1 > rack > rack2) */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag1)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no allowed topolgies specified", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, labelsMap, scParameters, "", allowedTopologyForSC, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid VSAN datastore is specified in storage class datastore url")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "No compatible datastores found for storage policy"
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
		TESTCASE-14
		RWM with dynamic Snapshot
		SC → specific allowed topology i.e. region-1 > zone-1 > building-1 > level-1 > rack-1 with Immediate Binding mode

		Steps:
		1. Create a Storage Class with specific allowed topology of rack-1
		2. Create a PVC using the SC created in step #1, with ReadWriteMany (RWX) access mode.
		3. Verify PVC should reach Bound state.
		4. Verify PVC placement. It should provision on the same AZ as given in the SC allowed topology.
		5. Create deployment Pod with replica count 3.
		6. Pod placement can happen on any AZ.
		7. Create a dynamic volume snapshot for the PVC created in step #2.
		8. Verify dynamic volume snapshot should fail with an appropriate error message.
		9.  Perform cleanup by deleting VS, PVC and SC
	*/

	ginkgo.It("TC14", ginkgo.Label(p1, file, vanilla, level5, level2, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType
		replica := 3

		/* Get allowed topologies for Storage Class
		(region1 > zone1 > building1 > level1 > rack > rack2) */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength, leafNode, leafNodeTag0)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, pv, err := createRwxPvcWithStorageClass(client, namespace, labelsMap, scParameters, "", allowedTopologyForSC, "", false, v1.ReadWriteMany, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv.Spec.CSI.VolumeHandle
		pvclaims := append(pvclaims, pvclaim)
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Deployments")
		deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nil, namespace, pvclaims, "", false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for deployment pods to be up and running")
		pods, err := fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
				metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Create a dynamic volume snapshot and verify volume snapshot creation failed")
		volumeSnapshot, snapshotContent, _,
			_, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).To(gomega.HaveOccurred())

		defer func() {
			framework.Logf("Deleting volume snapshot content")
			err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, namespace, pandoraSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Deleting volume snapshot")
			deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

			framework.Logf("Wait till the volume snapshot is deleted")
			err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
				*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

	})
})
