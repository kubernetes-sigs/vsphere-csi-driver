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
	"path/filepath"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwx-nohci-singlevc-positive] RWX-Topology-NoHciMesh-SingleVc-Positive", func() {
	f := framework.NewDefaultFramework("rwx-nohci-singlevc-positive")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                   clientset.Interface
		namespace                string
		bindingModeWffc          storagev1.VolumeBindingMode
		bindingModeImm           storagev1.VolumeBindingMode
		topologyAffinityDetails  map[string][]string
		topologyCategories       []string
		leafNode                 int
		leafNodeTag0             int
		leafNodeTag1             int
		leafNodeTag2             int
		topologyLength           int
		scParameters             map[string]string
		accessmode               v1.PersistentVolumeAccessMode
		labelsMap                map[string]string
		replica                  int32
		createPvcItr             int
		createDepItr             int
		pvs                      []*v1.PersistentVolume
		pvclaim                  *v1.PersistentVolumeClaim
		pv                       *v1.PersistentVolume
		allowedTopologies        []v1.TopologySelectorLabelRequirement
		revertOriginalConfSecret bool
		vCenterUIUser            string
		vCenterUIPassword        string
		vCenterIP                string
		vCenterPort              string
		dataCenter               string
		clusterId                string
		csiNamespace             string
		csiReplicas              int32
		allMasterIps             []string
		masterIp                 string
		netPermissionIps         string
		err                      error
		topologySetupType        string
		allowedTopologyForSC     []v1.TopologySelectorLabelRequirement
		nodeSelectorTerms        map[string]string
		allowedTopologyForPod    []v1.TopologySelectorLabelRequirement
		depl                     []*appsv1.Deployment
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// connecting to vc
		bootstrap()

		// fetch list of k8s nodes
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		//global variables
		scParameters = make(map[string]string)
		labelsMap = make(map[string]string)
		accessmode = v1.ReadWriteMany
		bindingModeWffc = storagev1.VolumeBindingWaitForFirstConsumer
		bindingModeImm = storagev1.VolumeBindingImmediate

		//read topology map
		/*
			here 5 is the actual 5-level topology length (ex - region, zone, building, level, rack)
			4 represents the 4th level topology length (ex - region, zone, building, level)
			here rack further has 3 rack levels -> rack1, rack2 and rack3
			0 represents rack1
			1 represents rack2
			2 represents rack3
		*/

		topologyLength, leafNode, leafNodeTag0, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2
		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)

		//setting map values
		labelsMap["app"] = "test"
		scParameters[scParamFsType] = nfs4FSType

		// read original values of config secret
		vCenterUIUser = e2eVSphere.Config.Global.User
		vCenterUIPassword = e2eVSphere.Config.Global.Password
		vCenterIP = e2eVSphere.Config.Global.VCenterHostname
		vCenterPort = e2eVSphere.Config.Global.VCenterPort
		dataCenter = e2eVSphere.Config.Global.Datacenters
		clusterId = e2eVSphere.Config.Global.ClusterID

		// fetching csi namespace and csi pods replica count
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

		// fetch list of master IPs
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]

		// global variable declaration
		netPermissionIps = "*"
		revertOriginalConfSecret = true

		/* Reading the topology setup type (Level 2 or Level 5), and based on the selected setup type,
		executing the test case on the corresponding setup */
		topologySetupType = GetAndExpectStringEnvVar(envTopologySetupType)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Perform cleanup if any resource left in the setup before starting a new test")

		// perfrom cleanup of old stale entries of storage class if left in the setup
		storageClassList, err := client.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(storageClassList.Items) != 0 {
			for _, sc := range storageClassList.Items {
				gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
		}

		// perfrom cleanup of old stale entries of statefulset if left in the setup
		statefulsets, err := client.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(statefulsets.Items) != 0 {
			for _, sts := range statefulsets.Items {
				gomega.Expect(client.AppsV1().StatefulSets(namespace).Delete(ctx, sts.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
		}

		// perfrom cleanup of old stale entries of nginx service if left in the setup
		err = client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// perfrom cleanup of old stale entries of pvc if left in the setup
		pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(pvcs.Items) != 0 {
			for _, pvc := range pvcs.Items {
				gomega.Expect(client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
		}

		// perfrom cleanup of old stale entries of any deployment entry if left in the setup
		depList, err := client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(depList.Items) != 0 {
			for _, dep := range depList.Items {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		// perfrom cleanup of old stale entries of pv if left in the setup
		pvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(pvs.Items) != 0 {
			for _, pv := range pvs.Items {
				gomega.Expect(client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name,
					*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
			}
		}

		// revert config secert to its original details
		if !revertOriginalConfSecret {
			ginkgo.By("Reverting back csi-vsphere.conf to its original credentials")

			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
				vCenterPort, dataCenter, clusterId)

			ginkgo.By("Restart CSI driver")
			restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// setting all global variables to nil before starting new testcase
		pvs, pvclaim, pv, allowedTopologies, allowedTopologyForSC,
			nodeSelectorTerms, allowedTopologyForPod, depl = nil, nil, nil, nil, nil, nil, nil, nil

	})

	/*
		TESTCASE-1
		Deployment Pods with multiple replicas attached to single RWX PVC

		Steps:
		1. Create a StorageClass with BindingMode set to Immediate with no allowed topologies
		specified and with fsType as "nfs4"
		2. Create one PVC with "RWX" access mode.
		3. Wait for PVC to reach Bound state.
		4. Create deployment Pods with replica count 3 and attach it to above PVC.
		5. Wait for deployment Pods to reach Running state.
		6. As the SC lacks a specific topology, volume provisioning and Pod placement can occur in any AZ.
		7. Perform cleanup by deleting Pods, PVCs, and the SC.
	*/

	ginkgo.It("Deployment pods with multiple replicas attached to a "+
		"single rwx pvc", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 3
		createPvcItr = 1
		createDepItr = 1

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, nil, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Deployment")
		deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
			pvclaim, nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err = client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[0].Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		TESTCASE-2
		StatefulSet Pods with multiple replicas attached to multiple RWX PVCs with scaling operation

		Steps:
		1. Create a StorageClass with BindingMode set to WFFC with no allowed topologies specified and with fsType as "nfs4"
		2. Use the SC to deploy a StatefulSet with 3 replicas, ReadWriteMany access mode, and Parallel pod management policy.
		Allow time for PVCs and Pods to reach Bound and Running states, respectively.
		3. Increase StatefulSet replica count to 5. Verify scaling operation.
		4. Decrease replica count to 1. Verify smooth scaling operation.
		5. As the SC lacks a specific topology, volume provisioning and Pod placement will occur in any available
		availability zones (AZs) for new Pod and PVC.
		6. Perform cleanup by deleting Pods, PVCs, and the SC.
	*/

	ginkgo.It("Scaling operations involving statefulSet pods with multiple replicas, "+
		"each connected to rwx pvcs", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pod replica count
		stsReplicas := 3
		scaleUpReplicaCount := 5
		scaleDownReplicaCount := 1

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no "+
			"allowed topology specified and with WFFC binding mode", accessmode, nfs4FSType))
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", bindingModeWffc, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, nil, false, false, false, accessmode, sc, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
			int32(scaleDownReplicaCount), statefulset, false, namespace, nil, true, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-3
		Multiple standalone pods are connected to a single RWX PVC, with each pod configured to
		have distinct read/write access permissions
		SC → WFC Binding Mode with allowed topology specified i.e. region-1 > zone-1 > building-1 > level-1 > rack-3

		Steps:
		1. Create a Storage Class with BindingMode set to WFC and allowed topologies set to
		   region-1 > zone-1 > building-1 > level-1 > rack-3
		2. Create RWX PVC using the SC created above.
		3. Wait for PVC to reach Bound state.
		4. Volume provisioning should happen on the datastore of the allowed topology as given in the SC.
		5. Create 3 standalone Pods using the PVC created in step #2.
		6. For Pod1 and Pod2 set read/write access and try reading/writing data into the volume.
		For the 3rd Pod set readOnly mode to true and verify that any write operation by Pod3 onto the volume
		should fail with an appropriate error message.
		7. Wait for Pods to reach Running Ready state.
		8. Pods can be created on any AZ.
		9. Perform cleanup by deleting Pod,PVC and SC.

	*/

	ginkgo.It("Multiple standalone pods attached to a single rwx "+
		"pvc", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// standalone pod and pvc count
		noPodsToDeploy := 3
		createPvcItr = 1

		ginkgo.By("Setting allowed topology for storage class")
		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			region3 > zone3 */
			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 2}, // Key: k8s-region, Value: region3
				{KeyIndex: 1, ValueIndex: 2}, // Key: k8s-zone, Value: zone3
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			/* Create allowed topologies for Topology Level5  Storage Class:
			region1 > zone1 > building1 > level1 > rack > rack3 */
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag2)
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology set to cluster-3", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologyForSC, bindingModeWffc, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// taking pvclaims 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim, false, execRWXCommandPod,
			noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvs 0th index because we are creating only single RWX PVC in this case
		pv = pvs[0]

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))
	})

	/*
		TESTCASE-4
		Deployment Pods with multiple replicas attached to single RWX PVC with vSAN storage policy of rack-1
		defined in SC along with top level allowed topology
		SC → Immediate Binding Mode with allowed topology specified as region-1 > zone-1 > building-1
		and Storage policy specified which is tagged to datastore in rack-1 (vSAN DS)

		Steps:
		1. Create a Storage Class with immediate Binding mode and allowed topology set to region-1 > zone-1 > building-1.
		2. Set a Storage policy specific to datastore in rack-1 (vSAN tagged Storage Policy) in SC.
		3. Create a RWX PVC using the Storage Class created in step #1.
		4. Wait for PVC to reach Bound state.
		5. Volume provisioning should happen on the datastore of the allowed topology as given in the SC.
		6. Create deployment Pod with relica count 3 using the PVC created in step #3.
		7. Set node selector teams so that all Pods should get created on rack-1
		8. Wait for deployment Pod to reach running ready state.
		9. Deployment Pods should get created on any worker node of AZ1.
		10. Verify CNS metadata for PVC and Pod.
		11. Perform scaleup operation on deployment Pods. Increase the replica count from 3 to 6.
		12. Verify scaling operation should go fine.
		13. Perform scale down operation on deployment Pods. Decrease the replica count from 6 to 2.
		14. Verify scale down operation went smooth.
		15. Perform cleanup by deleting Pods, PVCs and SC.
	*/

	ginkgo.It("Pods attached to Rwx pvc with vSAN storage policy of rack-1 "+
		"defined in SC along with top level allowed topology "+
		"specified", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 3
		createPvcItr = 1
		createDepItr = 1

		//storage policy read of cluster-1(rack-1)
		storagePolicyName := GetAndExpectStringEnvVar(envVsanDsStoragePolicyCluster1)
		scParameters["storagepolicyname"] = storagePolicyName

		ginkgo.By("Set allowed topology for pod node selector terms and allowed topology for storage class")
		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			k8s-region -> region1/region2/region3 */
			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 0},
				{KeyIndex: 0, ValueIndex: 1},
				{KeyIndex: 0, ValueIndex: 2}, // Key: k8s-region, Value: region1/region2/region3
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Setting zone1 for pod node selector terms */
			keyValues = []KeyValue{
				{KeyIndex: 0, ValueIndex: 0}, // Key: k8s-region, Value: region1
				{KeyIndex: 1, ValueIndex: 0}, // Key: k8s-zone, Value: zone1
			}
			allowedTopologyForPod, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			// Get allowed topologies for Storage Class (region1 > zone1 > building1)
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories, 3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* setting rack1(cluster1) as node selector terms for deployment pod creation */
			allowedTopologyForPod = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag0)[4:]
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with higher "+
			"level allowed topology", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, storagePolicyName, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume placement, it should get created on the datastore as taken in the SC")
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on "+
			"the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create Deployments and specify node selector terms")
		deploymentList, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
			pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage, false, nil, createDepItr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err = client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[0].Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologyForPod, pods)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment to 6 replica")
		replica = 6
		deploymentList, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage, true, deploymentList[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologyForPod, pods)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment to 1 replica")
		replica = 1
		deploymentList, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, true,
			labelsMap, pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage,
			true, deploymentList[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaim, pv, &pods.Items[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-5
		Deployment Pods with multiple replicas attached to single RWX PVC
		SC → Immediate Binding Mode with allowed topology specified  as
		region-1 > zone-1 > building-1 > levl-1 > rack-2/rack-3
		and datastore url specified which is shared across rack-2 and rack-3 (Non-vSAN FS datastore)

		Steps:
		1. Create a Storage Class with binding mode set to Immediate and with allowed topology set to
		region-1 > zone-1 > building-1 > levl-1 > rack-2/rack-3 for level-5
		2. Set datastore url shared across rack-2 and rack-3 (take a datastore which is not file Share compatible)
		3. Create RWX PVC using the SC created above.
		4. PVC creation will be stuck in a Pending state with an appropriate error due to
		non-VSAN FS datastore used.
		5. Perform cleanup by deleting PVC, SC.
	*/

	ginkgo.It("RWX PVC created with non VSAN FS compatible "+
		"datastore", ginkgo.Label(p1, file, vanilla, level5, level2, newTest, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// non vSAN FS compatible datastore
		dsUrl := GetAndExpectStringEnvVar(envNonVsanDsUrl)
		scParameters["datastoreurl"] = dsUrl
		expectedErrMsg := "not found in candidate list for volume provisioning"

		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			region2 > zone2
			Taking Shared datstore which is not vSAN compatible for zone2 */

			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 1}, // Key: k8s-region, Value: region2
				{KeyIndex: 1, ValueIndex: 1}, // Key: k8s-zone, Value: zone2
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			/* Get allowed topologies for Storage Class
			region1 > zone1 > building1 > level1 > rack > (rack2 and rack3)
			Taking Shared datstore which is not vSAN compatible between Rack2 and Rack3 */
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag1, leafNodeTag2)
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topolgies specified "+
			"and tagged with non-compatible shared datastore", accessmode, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologyForSC,
			bindingModeImm, false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid VSAN datastore is specified in storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
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
		1. Create a Storage Class with volumeBindingMode set to Immediate and specify allowed topologies for the SC, such as
		region-1 > zone-1 > building-1 > level-1 > rack-3 and a storage policy specific to cluster3 (rack-3).
		2. Create a PVC with accessmode set to "RWX".
		3. Verify volume provisioning should happen on the SC allowed topology.
		3. Create a deployment Pod with node selector term set to rack-2 using the PVC created
		in step 2 with a replica count of 1 and configured with ReadWriteMany (RWX) access mode.
		4. Deployment Pod should get created on the given AZ as specified in the node selector term of deployment pod.
		5. Verify CNS metadata for PVC and Pod.
		7. Scaleup deployment pod replica count to 3.
		8. Verify deployment pod scaling operation went smooth.
		9. New deployment Pod should get created on the given AZ as specified in the node selector term of deployment pod.
		6. Perform cleanup by deleting deployment Pod, PVC and SC.
	*/

	ginkgo.It("RWX volume creation on one zone and pods scheduled on another zone with "+
		"immediate mode set in SC", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 1
		createPvcItr = 1
		createDepItr = 1

		// storage policy of cluster-3(rack-3)
		storagePolicyName := GetAndExpectStringEnvVar(envVsanDsStoragePolicyCluster3)
		scParameters["storagepolicyname"] = storagePolicyName

		ginkgo.By("Set allowed topology for Pods node selector terms and storage class creation")
		if topologySetupType == "Level2" {
			// Node selector term for deployment Pod region2 > zone2
			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 1}, // Key: k8s-region, Value: region2
				{KeyIndex: 1, ValueIndex: 1}, // Key: k8s-zone, Value: zone2
			}
			allowedTopologyForPod, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Create allowed topologies for Topology Level2 Storage Class:
			region3 > zone3
			*/
			keyValues = []KeyValue{
				{KeyIndex: 0, ValueIndex: 2}, // Key: k8s-region, Value: region3
				{KeyIndex: 1, ValueIndex: 2}, // Key: k8s-zone, Value: zone3
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			// Node selector term for deployment Pod region1 > zone1 > building1 > level1 > rack2
			allowedTopologyForPod = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag1)
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Get allowed topologies for Storage Class region1 > zone1 > building1 > level1 > rack3
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag2)
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology specified to cluster3", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace,
			labelsMap, scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode,
			"", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, storagePolicyName, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}()

		ginkgo.By("Verify volume placement")
		_, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has "+
			"happened on the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create Deployment pod with node selector terms specific to rack-2 but sc allowed topology set to rack-3")
		deploymentList, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false,
			labelsMap, pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage, false, nil, createDepItr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err = client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[0].Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaim, pv, &pods.Items[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologyForPod, pods)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment to 3 replica")
		replica = 3
		deploymentList, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nodeSelectorTerms, execRWXCommandPod,
			nginxImage, true, deploymentList[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologyForPod, pods)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-7
		StatefulSet Workload with Parallel Pod Management and Scale-Up/Scale-Down Operations
		SC → WFC Binding Mode with allowed topology specified as region-1 > zone-1

		Steps:
		1. Create a Storage Class with volumeBindingMode set to WFFC and specify allowed topology (e.g., region-1 > zone-1).
		2. Create statefulSet with 3 replicas, each configured with ReadWriteMany (RWX) access mode
		and with parallel Pod management policy and specify pod affinity
		3. Ensure that all the PVCs and Pods are successfully created.
		-  The PVCs should be created in any available Availability Zone (AZ).
		-  The Pods should be created in any AZ
		4. Increase the replica count of statefulset to 5.
		5. Decrease the replica count of statefulset to 1
		6. Verify that the scaling up and scaling down of the StatefulSet is successful.
		7. Volume provisioning and Pod placement will occur in the available availability
		zones (AZs) specified in the SC during the scale-up operation.
		8. Verify CNS metadata for any one PVC and Pod.
		9. Perform cleanup by deleting StatefulSet, PVCs, and SC
	*/

	ginkgo.It("Statefulset creation with RWX PVCs with top level allowed "+
		"topology specified", ginkgo.Label(p1, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stsReplicas := 3
		scaleUpReplicaCount := 5
		scaleDownReplicaCount := 1

		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			zone1, zone2 and zone3  i.e. all top level
			*/
			keyValues := []KeyValue{
				{KeyIndex: 1, ValueIndex: 0},
				{KeyIndex: 1, ValueIndex: 1},
				{KeyIndex: 1, ValueIndex: 2}, // Key: k8s-zone, All Values
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else if topologySetupType == "Level5" {
			// Get allowed topologies for Storage Class (region1 > zone1)
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories, 2)
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and "+
			"fstype %q with allowed topology specified at top level "+
			"and with WFFC binding mode", accessmode, nfs4FSType))
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters,
			allowedTopologyForSC, "", bindingModeWffc, false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create StatefulSet with replica count 3 with RWX PVC access mode with pod affinity set to true")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace, true,
			int32(stsReplicas), false, allowedTopologyForSC, true, false, false, accessmode, sc, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, _, _, datastoreUrls, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = volumePlacementVerificationForSts(ctx, client, statefulset, namespace, datastoreUrls)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, int32(scaleUpReplicaCount),
			int32(scaleDownReplicaCount),
			statefulset, false, namespace, nil, true, true, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-8
		Storage Class Creation with Invalid Label Details in One Level
		SC → Immediate Binding Mode with invalid allowed topology specified  i.e.
		region-1 > zone-1 > building-1 > level-1 > rack-15

		Steps:
		1. Create a Storage Class with invalid label details, specifically using a label path such as
		"region-1 > zone-1 > building-1 > level-1 > rack-15"
		2. Create a PVC) using the SC created in step #1 with RWX access mode.
		3. As the label path is invalid, an appropriate error message should be displayed.
		4. Perform cleanup by deleting PVC and SC.
	*/

	ginkgo.It("Storage class created with invalid allowed "+
		"topology", ginkgo.Label(p2, file, vanilla, level5, level2, stable, negative, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expectedErrMsg := "failed to fetch vCenter associated with topology segments"

		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			incorrect topology label
			*/
			keyValues := []KeyValue{
				{KeyIndex: 1, ValueIndex: 0},
				{KeyIndex: 1, ValueIndex: 1},
				{KeyIndex: 1, ValueIndex: 2}, // Key: k8s-zone, All Values
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			allowedTopologyForSC[0].Values = []string{"zone15"}
		} else if topologySetupType == "Level5" {

			// Get allowed topologies for Storage Class
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength)
			allowedTopologyForSC[4].Values = []string{"rack15"}
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with invalid "+
			"allowed topolgies specified", accessmode, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, labelsMap, scParameters, "",
			allowedTopologyForSC, "", false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since invalid allowed topology is specified in the storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
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
		1. Create a Storage Class with only one level of topology details specified i.e. k8s-rack > rack-1
		2. Create a PVC using the SC created in step #1, with ReadWriteMany (RWX) access mode
		3. Wait for PVC to reach Bound state.
		4. Create deployment Pod with replica count 1 and specify node selector as same as allowed topology of SC.
		5. Wait for deployment Pod to be in running state.
		6. PVC should get created on the AZ as specified in the SC allowed topology and the Pod
		 placement should happen on the AZ as specified in the node selector terms.
		7. Increase the replica count to 3.
		8. Wait for new pods to be in running state.
		9. New Pods should get created on the same AZ as specified in the node selector term of deployment
		7. Perform cleanup by deleting Pods, PVC and SC.
	*/

	ginkgo.It("Deployment pod creation with RWX pvc having only single level in "+
		"allowed topology", ginkgo.Label(p1, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// deployment pod and pvc count
		replica = 1
		createPvcItr = 1
		createDepItr = 1

		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			region1 > zone1 */
			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 0}, // Key: k8s-region, Value: region1
				{KeyIndex: 1, ValueIndex: 0}, // Key: k8s-zone, Value: zone1
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Setting zone1 for pod node selector terms */
			allowedTopologyForPod = allowedTopologyForSC
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			// Get allowed topologies for Storage Class (rack > rack1)
			ginkgo.By("Set node selector term for deployment pods so that all pods should get created " +
				"on one single AZ i.e. cluster-1(rack-1)")
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag0)[4:]
			allowedTopologyForPod = allowedTopologyForSC
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed topology "+
			"specified to single level cluster-1", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on "+
			"the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create Deployment")
		deploymentList, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
			pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage, false, nil, createDepItr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err = client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[0].Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaim, pv, &pods.Items[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologyForPod, pods)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment to 3 replica")
		replica = 3
		deploymentList, pods, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nodeSelectorTerms, execRWXCommandPod, nginxImage, true, deploymentList[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyDeploymentPodNodeAffinity(ctx, client, namespace, allowedTopologyForPod, pods)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-10
		Create Storage Class with a single label in allowed topology and with datastore url
		SC → top level allowed topology specified i.e. k8s-region:region-1 with WFC Binding mode and
		datastore url passed (vSAN ds of rack-3)

		Steps:
		1. Create Storage Class with only top level of topology details specified i.e. region-1
		2. Create PVC using the Storage Class created in step #1, with ReadWriteMany (RWX) access mode.
		3. Wait for PVC to reach Bound state.
		4. Create multiple Pods using the PVC created in the step #2 with node selector term specific to rack-3
		5. Wait for Pod to be in running state.
		6. PVC should get created on the AZ as specified in the SC allowed topology
		and the Pod placement should happen on any AZ.
		7. Perform cleanup by deleting Pods,PVC and SC.
	*/

	ginkgo.It("RWX Pvc creation with single level allowed topology "+
		"passed with datastore url", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pvc and pod count
		createPvcItr = 1
		no_pods_to_deploy := 3

		// vSAN datastore url of cluster-3(rack-3)
		datastoreurl := GetAndExpectStringEnvVar(envVsanDsUrlCluster3)
		scParameters[scParamDatastoreURL] = datastoreurl

		ginkgo.By("Set node selector terms and set allowed topology for Storage Class")
		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			region1,region2,region3 */

			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 0},
				{KeyIndex: 0, ValueIndex: 1},
				{KeyIndex: 0, ValueIndex: 2}, // Key: k8s-region, All Values
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			/* Setting zone3 for pod node selector terms */
			keyValues = []KeyValue{
				{KeyIndex: 0, ValueIndex: 2}, // Key: k8s-region, Value: region3
				{KeyIndex: 1, ValueIndex: 2}, // Key: k8s-zone, Value: zone3
			}
			allowedTopologyForPod, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			// Create allowed topologies for Storage Class: k8s-region > region-1
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories, 1)

			// allowed topology consider for node selector terms is rack3
			allowedTopologyForPod = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag2)
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology set at the top level", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, nil, scParameters, diskSize,
			allowedTopologyForSC, bindingModeWffc, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		//taking pvclaims 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]

		ginkgo.By("Create 3 standalone Pods using the same PVC")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nodeSelectorTerms, pvclaim, false,
			execRWXCommandPod, no_pods_to_deploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", datastoreurl)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//taking pvs 0th index because we are creating only single RWX PVC in this case
		pv = pvs[0]

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the "+
			"wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Verify standalone pods are scheduled on a selected node selector terms")
		for i := 0; i < len(podList); i++ {
			err = verifyStandalonePodAffinity(ctx, client, podList[i], allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		TESTCASE-11
		Volume expansion on file volumes
		SC → all levels of topology details with Immediate Binding mode

		Steps:
		1. Create Storage Class with all levels of topology details specified in the
		Storage Class and with Immediate Binding mode.
		2. Create PVC with RWX access mode using SC created above.
		3. Verify that the PVC transitions to the Bound state and PVC is created in any of the Availability Zone.
		4. Trigger Offline Volume Expansion on the PVC created in step #2
		5. Verify that the PVC resize operation fails with an appropriate error message, as
		volume expansion is not supported for File Volumes.
		6. Perform Cleanup by deleting PVC and SC.
	*/

	ginkgo.It("Volume expansion on file "+
		"volumes", ginkgo.Label(p2, file, vanilla, level5, level2, negative, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expectedErrMsg := "volume expansion is only supported for block volume type"

		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			region1,region2,region3 */

			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 0},
				{KeyIndex: 0, ValueIndex: 1},
				{KeyIndex: 0, ValueIndex: 2}, // Key: k8s-region, All Values
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			/* Get allowed topologies for Storage Class i.e.
			region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories, topologyLength)
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q and set allowVolumeExpansion "+
			"to true with all allowed topolgies specified", accessmode, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologyForSC, bindingModeImm, true, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for PVC and PV to reach Bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Modify PVC spec to trigger volume expansion, Expect to fail as file volume expansion is not supported")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("3Gi"))
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

		ginkgo.By("Volume expansion should fail for file volume")
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(),
			fmt.Sprintf("Expected pvc creation failure with error message: %s", expectedErrMsg))
	})

	/*
		TESTCASE-12
		PVC creation using shared datastore lacking capability of vSAN FS
		SC → default values, shared datastore url passed shared across all AZs
		lacking vSAN FS capability, Immediate Binding mode

		Steps:
		1. Create a Storage Class with default values and pass datastore Url shared across all AZs but
		lacking the vSAN FS capability with Immediate binding mode.
		2. Create a PVC using the SC created in step #1, with ReadWriteMany (RWX) access mode.
		3. PVC should get stuck in Pending state with appropriate error message because the datastore
		url passed in SC is not vSAN FS enabled datastore
		4. Perform cleanup by deleting PVC and SC
	*/

	ginkgo.It("RWX Pvc creation using shared datastore lacking "+
		"capability of vSAN FS", ginkgo.Label(p2, file, vanilla, level5, level2, negative, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		datastoreurl := GetAndExpectStringEnvVar(envNonVsanDsUrl)
		scParameters[scParamDatastoreURL] = datastoreurl

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no allowed "+
			"topolgies specified", accessmode, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, labelsMap, scParameters,
			diskSize, nil, bindingModeImm, false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid VSAN datastore is specified in storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
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
		SC → specific allowed topology i.e. k8s-rack:rack-2 and storage Policy specified tagged with
		rack-3 datastore

		Steps:
		1. Create a Storage Class  with specific allowed topology (rack-2) and storage policy
		tagged with rack-3 vSAN datastore
		2. Create a PVC using the SC created in step #1, with ReadWriteMany (RWX) access mode.
		3. PVC should get stuck in Pending state with an appropriate error message
		4. Perform cleanup by deleting PVC and SC
	*/

	ginkgo.It("Allowed topology specific to one zone and storage polocy "+
		"provided is from another zone", ginkgo.Label(p2, file, vanilla, level5, level2, negative, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expectedErrMsg := "No compatible datastores found for storage policy"

		// storage policy of rack3
		storagePolicyName := GetAndExpectStringEnvVar(envVsanDsStoragePolicyCluster3)
		scParameters["storagepolicyname"] = storagePolicyName

		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			region1,region2,region3 */

			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 1}, //Key: k8s-region, region-2
				{KeyIndex: 1, ValueIndex: 1}, // Key: k8s-zone, zone-2
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			/* Get allowed topologies for Storage Class
			(region1 > zone1 > building1 > level1 > rack > rack2) */
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag1)
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with no "+
			"allowed topolgies specified", accessmode, nfs4FSType))
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologyForSC, bindingModeImm, false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail provisioning volume since no valid datastore is specified in the storage class")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Expected failure message: %+q", expectedErrMsg))
		isFailureFound := checkEventsforError(client, namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)}, expectedErrMsg)
		gomega.Expect(isFailureFound).To(gomega.BeTrue(), "Unable to verify pvc create failure")
	})

	/*
		TESTCASE-15
		Deployment Pod with Scale-Up/Scale-Down Operations
		SC → WFC Binding Mode with allowed topology specified i.e.
		region-1 > zone-1 > building-1 > level-1 > rack-1 and rack-3

		Steps:
		1. Create Storage Class with WFFC binding mode and allowed topology set to
		region-1 > zone-1 > building-1 > level-1 > rack-1 and rack-3
		2. Create 4 PVCs with "RWX" access mode using SC created in step #1.
		3. Wait for PVCs to reach to Bound state.
		4. Create deployment Pod for each PVC with replica count 1.
		5. Verify volume placement. Provisioning should happen as per the allowed topology specified in the SC.
		6. Pods should get created on any AZ.
		7. Perform scaling up operation on deployment pod. Increase the replica count to 5
		8. New Pods should get created on any AZ. Volumes should get placed on the Az as given in the SC.
		8. Verify scaling up operation went smooth.
		9. Perform scale operation on deployment pod. Decrease the replica count to 2
		10. Verify scaling down operation went smooth.
		11. Perform cleanup by deleting Pods, PVC and SC
	*/

	ginkgo.It("Multiple deployment pods with scaling operation "+
		"attached to rwx pvcs", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pvc and deployment pod count
		replica = 1
		createPvcItr = 4
		createDepItr = 1

		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			region1,region3 and zone-1,zone-3 */

			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 0}, // Key: k8s-region, Value: region1
				{KeyIndex: 0, ValueIndex: 2}, // Key: k8s-region, Value: region3
				{KeyIndex: 1, ValueIndex: 0}, // Key: k8s-zone, Value: zone1
				{KeyIndex: 1, ValueIndex: 2}, // Key: k8s-zone, Value: zone3
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			/* Get allowed topologies for Storage Class
			(region1 > zone1 > building1 > level1 > rack > rack1/rack3) */
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag0, leafNodeTag2)
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeWffc, false, accessmode,
			"", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			for i := 0; i < len(pvclaims); i++ {
				pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaims[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Deployments")
		for i := 0; i < len(pvclaims); i++ {
			deploymentList, pods, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
				pvclaims[i], nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			depl = append(depl, deploymentList...)

			pv = getPvFromClaim(client, pvclaims[i].Namespace, pvclaims[i].Name)

			ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
			err = waitAndVerifyCnsVolumeMetadata(ctx, pv.Spec.CSI.VolumeHandle, pvclaims[i], pv, &pods.Items[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			framework.Logf("Delete deployment set")
			for i := 0; i < len(depl); i++ {
				err = client.AppsV1().Deployments(namespace).Delete(ctx, depl[i].Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var datastoreUrls []string
		datastoreUrls = append(datastoreUrls, datastoreUrlsRack1...)
		datastoreUrls = append(datastoreUrls, datastoreUrlsRack3...)
		for i := 0; i < len(pvs); i++ {
			isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pvs[i].Spec.CSI.VolumeHandle, datastoreUrls)
			gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the "+
				"wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))
		}

		ginkgo.By("Scale up deployment1 pods to 5 replicas")
		replica = 5
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[0], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down deployment2 pods to 2 replica")
		replica = 2
		_, _, err = createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica,
			true, labelsMap, pvclaim, nil, execRWXCommandPod, nginxImage, true, depl[1], 0)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-16
		Net Permissions -> ips = "*" and permissions = "READ_WRITE"
		SC → specific allowed topology i.e. k8s-rack:rack-1 with WFC Binding mode

		Steps:
		1. Create vSphere Configuration Secret - Set NetPermissions to "*" and Set permissions to "READ_WRITE"
		2. Install vSphere CSI Driver.
		3. Create Storage Class -  set allowed topology to rack-1 and set WFC Binding mode
		4. Create PVC using Storage Class - Set access mode to "RWX"
		5. Verify PVC state, ensure the PVC reaches the Bound state.
		6. Create 3 Pods using the PVC from step 4. Set Pod3 to "read-only" mode.
		7. Verify Pod States, confirm all three Pods are in an up and running state.
		8. Exec to Pod1 - Create a text file and write data into the mounted directory.
		9. Exec to Pod2 - Try to access the text file created by Pod1, cat the file and later write some data into it.
		Verify that Pod2 can read and write into the text file.
		10. Exec to Pod3 - Try to access the text file created by Pod1, cat the file and attempt to write some data into it.
		Verify that Pod3 can read but cannot write due to "READ_ONLY" permission.
		11. Perform cleanup by deleting Pods, PVC, and SC.
	*/

	ginkgo.It("Multiple standalone pods attached to a single rwx "+
		"pvc with netpermissions set to read/write", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Setting vsphere config secret netpermissions to readwrite access and with all set of ips allowed")
		err = setNetPermissionsInVsphereConfSecret(client, ctx, csiSystemNamespace, int32(csiReplicas),
			netPermissionIps, vsanfstypes.VsanFileShareAccessTypeREAD_WRITE, false)
		revertOriginalConfSecret = true
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
				vCenterPort, dataCenter, clusterId)

			ginkgo.By("Restart CSI driver")
			restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// standalone pod and pvc count
		noPodsToDeploy := 3
		createPvcItr = 1

		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			only zone1 */

			keyValues := []KeyValue{
				{KeyIndex: 1, ValueIndex: 0}, // Key: k8s-zone, Value: zone-1
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			// Create allowed topologies for Storage Class: region1 > zone1 > building1 > level1 > rack > rack1
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag0)
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with allowed "+
			"topology set to cluster-1(rack-1)", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap, scParameters,
			diskSize, allowedTopologyForSC, bindingModeWffc, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// taking pvclaims 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim, false, execRWXCommandPod,
			noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvs 0th index because we are creating only single RWX PVC in this case
		pv = pvs[0]

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))
	})

	/*
		TESTCASE-17
		Static PVC and PV with reclaim policy Retain

		Steps:
		1. Create 1 file share using the CNS Create Volume API with RWX access mode.
		2. Create a PV using the File Share created in step 1 with policy set to retain.
		3. Create a PVC using the PV created in step 2 configured with ReadWriteMany (RWX) access mode.
		4. Wait for both the PV and PVC to reach the Bound state.
		5. Create a Pod using the PVC from step 3. Set the node affinity so that the Pod should
		get created in the worker node of Az3/rack-3.
		7. Delete the Pod and the PVC.
		6. Wait for PV to change status to the Available state.
		7. Recreate the PVC with the same name used in step 4. The PVC should be created and reach the Bound state.
		8. Create a Pod and ensure it reaches the running state. Similar to the previous step, the Pod should be
		created in the worker node of AZ rack-3.
		9. Verify the CNS metadata for the PVC.
		10. Perform cleanup by deleting Pods, PVC, PV
	*/

	ginkgo.It("Create static pv using file share with "+
		"policy set to retain", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// creating file share on cluster3 vsan FS enabled datastore
		datastoreUrlCluster3 := GetAndExpectStringEnvVar(envVsanDsUrlCluster3)

		ginkgo.By("Creating file share volume")
		fileShareVolumeId, err := creatFileShareForSingleVc(datastoreUrlCluster3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PV with policy set to retain with rwx access mode")
		pvSpec := getPersistentVolumeSpecForFileShare(fileShareVolumeId,
			v1.PersistentVolumeReclaimRetain, labelsMap, accessmode)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pvSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete PV")
			framework.ExpectNoError(fpv.DeletePersistentVolume(ctx, client, pv.Name))
			framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name,
				poll, framework.ClaimProvisionTimeout))

			ginkgo.By("Verify fileshare volume got deleted")
			framework.ExpectNoError(e2eVSphere.waitForCNSVolumeToBeDeleted(fileShareVolumeId))
		}()

		ginkgo.By("Creating the PVC using pv created above and with rwx access mode")
		pvc := getPersistentVolumeClaimSpecForFileShare(namespace, labelsMap, pv.Name, accessmode)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims = append(pvclaims, pvc)
		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv, pvc))
		defer func() {
			ginkgo.By("Deleting the PV Claim")
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pvs[0].Spec.CSI.VolumeHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Set node selector term for standlone pods so that all pods should get " +
			"created on one single AZ i.e. cluster-3(rack-3)")
		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			k8s-region:region3, k8s-zone:zone3
			*/

			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 2}, // Key: k8s-region, Value: region3
				{KeyIndex: 1, ValueIndex: 2}, // Key: k8s-zone, Value: zone3
			}
			allowedTopologyForPod, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			// Create allowed topologies for Storage Class: region1 > zone1 > building1 > level1 > rack > rack3
			allowedTopologyForPod = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag2)
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating the Pod")
		pod, err := createPod(ctx, client, namespace, nodeSelectorTerms, pvclaims, false, execRWXCommandPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, pod), "Failed to delete pod", pod.Name)
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyStandalonePodAffinity(ctx, client, pod, allowedTopologyForPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the volume is accessible and available to the pod by creating an empty file")
		filepath := filepath.Join("/mnt/volume1", "/emptyFile.txt")
		_, err = e2eoutput.LookForStringInPodExec(namespace, pod.Name, []string{"/bin/touch", filepath}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, pvc.Name, pv.Name, pod.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the Pod")
		framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, pod), "Failed to delete pod", pod.Name)

		ginkgo.By("Delete PVC")
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name,
			*metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("PVC %s is deleted successfully", pvc.Name)

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
		pvclaim.Name = pvc.Name
		pvclaim.Spec.AccessModes = []v1.PersistentVolumeAccessMode{accessmode}
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvclaim,
			metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the PV Claim")
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for newly created PVC to bind to the existing PV")
		err = fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv,
			pvclaim)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating new Pod on another zone Az2")
		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			k8s-region:region2, k8s-zone:zone2
			*/
			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 1}, // Key: k8s-region, Value: region2
				{KeyIndex: 1, ValueIndex: 1}, // Key: k8s-zone, Value: zone2
			}
			allowedTopologyForPod, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			//rack-2 or zone-2 or cluster-2
			allowedTopologyForPod = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag1)
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating Pod using newly created PVC")
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvclaim)
		podNew, err := createPod(ctx, client, namespace, nodeSelectorTerms, pvclaims1, false, execRWXCommandPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, podNew), "Failed to delete pod", podNew.Name)
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyStandalonePodAffinity(ctx, client, podNew, allowedTopologyForPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs1, err := checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims1, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume metadata for POD, PVC and PV")
		err = waitAndVerifyCnsVolumeMetadata(ctx, pvs1[0].Spec.CSI.VolumeHandle, pvclaim, pv, podNew)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TESTCASE-18
		Static PVC creation
		Steps:
		1. Create 2 file shares using the CNS Create Volume API with the following configurations:
		Read-write mode.
		ReadOnly Netpermissions.
		2. Create Persistent Volumes (PVs) using the volume IDs of the previously created file shares
		3. Create Persistent Volume Claims (PVCs) using the PVs created in step 3.
		4. Wait for the PVs and PVCs to be in the Bound state.
		5. Volume provisioning will happen on exactly the datastore where the backing fileshare exists
		6. Create a POD using the PVC created in step 4. Make sure the POD is scheduled to come up on
		a node present in the same zone as mentioned in the Storage Class from step 1.
		7. Delete the POD, PVC, PV, and the Storage Class (SC).
	*/

	ginkgo.It("Create static pvs using file share with policy set to delete "+
		"with different read/write permissions", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// creating file share on cluster3 vsan FS enabled datastore
		datastoreUrlCluster3 := GetAndExpectStringEnvVar(envVsanDsUrlCluster3)

		// fetch file share volume id
		fileShareVolumeId1, err := creatFileShareForSingleVc(datastoreUrlCluster3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		fileShareVolumeId2, err := creatFileShareForSingleVc(datastoreUrlCluster3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PV with policy set to delete with ReadOnlyPermissions")
		pvSpec1 := getPersistentVolumeSpecForFileShare(fileShareVolumeId1,
			v1.PersistentVolumeReclaimDelete, labelsMap, v1.ReadOnlyMany)
		pv1, err := client.CoreV1().PersistentVolumes().Create(ctx, pvSpec1, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv1.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PV with policy set to delete with ReadWritePermissions")
		pvSpec2 := getPersistentVolumeSpecForFileShare(fileShareVolumeId2,
			v1.PersistentVolumeReclaimDelete, labelsMap, accessmode)
		pv2, err := client.CoreV1().PersistentVolumes().Create(ctx, pvSpec2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv2.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC where static PV is created with ReadOnly permissions")
		pvc1 := getPersistentVolumeClaimSpecForFileShare(namespace, labelsMap, pv1.Name, v1.ReadOnlyMany)
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc1, metav1.CreateOptions{})
		var pvclaims1 []*v1.PersistentVolumeClaim
		pvclaims1 = append(pvclaims1, pvc1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv1, pvc1))
		defer func() {
			ginkgo.By("Deleting the PV Claim")
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims1, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the PVC where static PV is created with ReadWrite permissions")
		pvc2 := getPersistentVolumeClaimSpecForFileShare(namespace, labelsMap, pv2.Name, accessmode)
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		var pvclaims2 []*v1.PersistentVolumeClaim
		pvclaims2 = append(pvclaims2, pvc2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv2, pvc2))
		defer func() {
			ginkgo.By("Deleting the PV Claim")
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims2, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Set node selector term for standlone pods so that all pods should get " +
			"created on one single AZ i.e. cluster-3(rack-3)")
		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			k8s-region:region3
			*/
			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 2}, // Key: k8s-region, Value: region3
			}
			allowedTopologyForPod, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologyForPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			// rack-3 or cluster-3
			allowedTopologyForPod = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag2)
			nodeSelectorTerms, err = getNodeSelectorMapForDeploymentPods(allowedTopologies)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating the Pod attached to ReadOnly permission PVC")
		pod1, err := createPod(ctx, client, namespace, nodeSelectorTerms, pvclaims1, false, execRWXCommandPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, pod1), "Failed to delete pod", pod1.Name)
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyStandalonePodAffinity(ctx, client, pod1, allowedTopologyForPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify the volume is accessible and available to the pod by creating an empty file")
		filepath := filepath.Join("/mnt/volume1", "/emptyFile.txt")
		_, err = e2eoutput.LookForStringInPodExec(namespace, pod1.Name, []string{"/bin/touch", filepath}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv1.Spec.CSI.VolumeHandle, pvc1.Name, pv1.Name, pod1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the Pod attached to ReadWrite permissions pvc")
		pod2, err := createPod(ctx, client, namespace, nodeSelectorTerms, pvclaims2, false, execRWXCommandPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the Pod")
			framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, pod2), "Failed to delete pod", pod2.Name)
		}()

		ginkgo.By("Verify deployment pods are scheduled on a specified node selector terms")
		err = verifyStandalonePodAffinity(ctx, client, pod2, allowedTopologyForPod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv2.Spec.CSI.VolumeHandle, pvc2.Name, pv2.Name, pod2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, _, datastoreUrlsRack3, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv1.Spec.CSI.VolumeHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))

		isCorrectPlacement = e2eVSphere.verifyPreferredDatastoreMatch(pv2.Spec.CSI.VolumeHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))
	})

	/*
		TESTCASE-21
		NetPermissions  -> [NetPermissions "B"], ips = "10.20.20.0/24", permissions = "READ_ONLY"

		SC → all allowed topologies specified with Immediate Binding mode with rack-1,rack-2 and rack-3

		Steps:
		1. Create vSphere Configuration Secret - Set NetPermissions to "10.20.20.0/24"
			specifying workerIP of rack-1. Set NetPermissions to "READ_ONLY"
		2. Install vSphere CSI Driver.
		3. Create Storage Class - Set allowed topology to all racks and Set Immediate Binding mode
		4. Create PVC with RWX access mode.
		5. Verify PVC creation status.
		6. Create deployment Pod and attach it to the volume created above.
		7. Deployment Pod creation should fail due to read/write permission set.
		8. Perform cleanup by deleting Pod, PVC and SC
	*/

	ginkgo.It("Setting netpermissions with specific ip and with "+
		"readonly permissions", ginkgo.Label(p1, file, vanilla, level5, level2, newTest, negative), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pods and pvc count
		replica = 1
		createPvcItr = 1
		createDepItr = 1

		ginkgo.By("Setting vsphere config secret netpermissions to readonly access and with specific netpermission ip")
		err = setNetPermissionsInVsphereConfSecret(client, ctx, csiSystemNamespace, int32(csiReplicas), masterIp,
			vsanfstypes.VsanFileShareAccessTypeREAD_ONLY, false)
		revertOriginalConfSecret = true
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
				vCenterPort, dataCenter, clusterId)

			ginkgo.By("Restart CSI driver")
			restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"no allowed topology specified", accessmode, nfs4FSType))
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologies, bindingModeImm, false, accessmode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Deployment and expecting deployment pod to fail to " +
			"come to ready state due to read only permission set in vsphere conf secret")
		deploymentList, _, err := createVerifyAndScaleDeploymentPods(ctx, client, namespace, replica, false, labelsMap,
			pvclaim, nil, execRWXCommandPod, nginxImage, false, nil, createDepItr)
		gomega.Expect(err).To(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err = client.AppsV1().Deployments(namespace).Delete(ctx, deploymentList[0].Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		TESTCASE-22
		PVC "ROX" access mode  and config secret Net Permissions  is set to "READ_WRITE"
		SC → specific allowed topology i.e. rack-1 with Immediate Binding mode

		Steps:
		1. Config secret is set with net permissions "READ_WRITE"
		2. Create Storage Class with allowed topology set to rack-1 and with Immediate Binding mode.
		3. Create PVC with access mode set to "ROM"
		4. PVC should reach to Bound state.
		5. Volume provision should happen on the AZ as specified in the SC.
		6. Create 3 Pods using above created PVC. Pod1 and Pod2 should have read-write permissions,
		but Pod-3 has "readonly" flag set to true.
		7. Verify all 3 Pods should reach Running state.
		8. Try reading/writing data on to the volume from Pod1 and Pod2 container.
		9. Since the access mode is set to "ROX", it should only allow reading of data from a volume
			but not writing. Verify reading/writing data from Pod-3.
		10. Verify Pod placement can happen on any AZ.
		11. Perform cleanup by deleting Pod, PVC and SC.
	*/

	ginkgo.It("Create pvc with ROX access mode and config secret "+
		"netpermissions is set with readwrite permissions", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pod and pvc count
		createPvcItr = 1
		noPodsToDeploy := 3

		ginkgo.By("Setting vsphere config secret netpermissions to readwrite with all allowed ips set")
		err = setNetPermissionsInVsphereConfSecret(client, ctx, csiSystemNamespace, int32(csiReplicas), netPermissionIps,
			vsanfstypes.VsanFileShareAccessTypeREAD_WRITE, false)
		revertOriginalConfSecret = true
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
				vCenterPort, dataCenter, clusterId)

			ginkgo.By("Restart CSI driver")
			restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			only zone1 */

			keyValues := []KeyValue{
				{KeyIndex: 1, ValueIndex: 0}, // Key: k8s-zone, Value: zone-1
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			// Create allowed topologies for Storage Class: region1 > zone1 > building1 > level1 > rack > rack1
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag0)
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"allowed topology of rack-1 specified", accessmode, nfs4FSType))
		ginkgo.By("Creating PVC with ROX access mode")
		readOnlyAccessMode := v1.ReadOnlyMany
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false,
			readOnlyAccessMode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		datastoreUrlsRack1, _, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack1)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create 3 standalone Pods using the same PVC with different read/write permissions")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim, false, execRWXCommandPod,
			noPodsToDeploy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			for i := 0; i < len(podList); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[i].Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, podList[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
	})

	/*
		TESTCASE-23
		PVC "ROX" access mode and config secret Net Permissions set to "READ_ONLY"
		SC → specific allowed topology → rack-2 → Immediate Binding mode

		Steps:
		1. Config secret is set with net permissions "READ_ONLY"
		2. Create Storage Class with allowed topology set to rack-2 and with Immediate Binding mode.
		3. Create PVC with access mode set to "ROM"
		4. PVC should reach to Bound state.
		5. Volume provision should happen on the AZ as specified in the SC.
		6. Create 3 Pods using above created PVC. Pod1 and Pod2 should have read-write permissions,
		but Pod-3 has "readonly" flag set to true.
		7. Verify all 3 Pods should reach Running state.
		8. Try reading/writing data on to the volume from Pod1 and Pod2 container.
		9. Since the access mode is set to "ROX", it should only allow reading of data from a
		volume but not writing. Verify reading/writing data from Pod-3.
		10. Verify Pod placement can happen on any AZ.
		11. Perform cleanup by deleting Pod, PVC and SC.
	*/

	ginkgo.It("Create pvc with rox access mode and config secret "+
		"netpermissions is set with readonly permissions", ginkgo.Label(p0, file, vanilla, level5, level2, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// pod and pvc count
		createPvcItr = 1
		noPodsToDeploy := 1

		ginkgo.By("Setting vsphere config secret netpermissions to readonly access and with specific netpermission ip")
		err = setNetPermissionsInVsphereConfSecret(client, ctx, csiSystemNamespace, int32(csiReplicas),
			netPermissionIps, vsanfstypes.VsanFileShareAccessTypeREAD_ONLY, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Reverting back csi-vsphere.conf with its original vcenter user " +
				"and its credentials")
			createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
				vCenterPort, dataCenter, clusterId)

			ginkgo.By("Restart CSI driver")
			restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
			gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		if topologySetupType == "Level2" {
			/* Create allowed topologies for Topology Level2 Storage Class:
			only region-2 and zone-2 */

			keyValues := []KeyValue{
				{KeyIndex: 0, ValueIndex: 1}, // Key: k8s-region, Value: region-2
				{KeyIndex: 1, ValueIndex: 1}, // Key: k8s-zone, Value: zone-2
			}
			allowedTopologyForSC, err = getAllowedTopologyForLevel2(allowedTopologies, keyValues)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if topologySetupType == "Level5" {
			// Create allowed topologies for Storage Class: region1 > zone1 > building1 > level1 > rack > rack2
			allowedTopologyForSC = getTopologySelector(topologyAffinityDetails, topologyCategories,
				topologyLength, leafNode, leafNodeTag1)
		}

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q with "+
			"allowed topology of rack-1 specified", accessmode, nfs4FSType))
		ginkgo.By("Creating PVC with ROX access mode")
		readOnlyAccessMode := v1.ReadOnlyMany
		storageclass, pvclaims, err := createStorageClassWithMultiplePVCs(client, namespace, labelsMap,
			scParameters, diskSize, allowedTopologyForSC, bindingModeImm, false,
			readOnlyAccessMode, "", nil, createPvcItr, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify PVC Bound state and CNS side verification")
		pvs, err = checkVolumeStateAndPerformCnsVerification(ctx, client, pvclaims, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// taking pvclaims and pvs 0th index because we are creating only single RWX PVC in this case
		pvclaim = pvclaims[0]
		pv = pvs[0]

		ginkgo.By("Verify volume placement, should match with the allowed topology specified")
		_, datastoreUrlsRack2, _, _, err := fetchDatastoreListMap(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, datastoreUrlsRack2)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong "+
			"datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create single standalone Pod and attach it to rwx pvc, " +
			"pod creation should fail due to readOnly permission set")
		podList, err := createStandalonePodsForRWXVolume(client, ctx, namespace, nil, pvclaim, false, execRWXCommandPod,
			noPodsToDeploy)
		gomega.Expect(err).To(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podList[0].Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, podList[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})
})
