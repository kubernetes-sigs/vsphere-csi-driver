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

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-topology-for-level5] Topology-Provisioning-For-Statefulset-Level5", func() {
	f := framework.NewDefaultFramework("rwx-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                  clientset.Interface
		namespace               string
		bindingMode             storagev1.VolumeBindingMode
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		leafNode                int
		leafNodeTag2            int
		topologyLength          int
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
		readOnly                bool
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
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		topologyLength, leafNode, _, _, leafNodeTag2 = 5, 4, 0, 1, 2
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap,
			topologyLength)

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

		// fetching datacenter details
		dataCenters, err = e2eVSphere.getAllDatacenters(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching cluster details
		clusters, err = getTopologyLevel5ClusterGroupNames(masterIp, sshClientConfig, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching list of datastores available in different racks
		rack1DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fmt.Println(rack1DatastoreListMap)
		rack2DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[1])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fmt.Println(rack2DatastoreListMap)
		rack3DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[2])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fmt.Println(rack3DatastoreListMap)

		for _, value := range rack1DatastoreListMap {
			datastoreUrlsRack1 = append(datastoreUrlsRack1, value)
		}

		for _, value := range rack2DatastoreListMap {
			datastoreUrlsRack2 = append(datastoreUrlsRack2, value)
		}

		for _, value := range rack3DatastoreListMap {
			datastoreUrlsRack3 = append(datastoreUrlsRack3, value)
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
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType
		replica := 3

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, pv, err := createRwxPVCwithStorageClass(client, namespace, labelsMap, scParameters, "", nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv[0].Spec.CSI.VolumeHandle
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
	})

	/*
		TESTCASE-2
		StatefulSet Pods with multiple replicas attached to multiple RWX PVCs with scaling operation
		StatefulSet pods, with multiple replicas, are associated with distinct RWX Persistent Volume Claims (PVCs), facilitating scaling operations with unique RWX storage connections for each pod instance.

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
		scaleUpReplicaCount := 6
		scaleDownReplicaCount := 1
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany, nfs4FSType))
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

		Steps:
		1. Create a Storage Class with BindingMode set to WFFC and allowed topologies set to region-1 > zone-1 > building-1 > level-1 > rack-3
		2. Create RWX PVC using the SC created above.
		3. Wait for PVC to reach Bound state.
		4. Volume provisioning should happen on the datastore of the allowed topology as specified in the SC.
		5. Create 3 standalone Pods using the PVC created in step #2.
		6. For Pod1 and Pod2 set read/write access and try reading/writing data into the volume. For the 3rd Pod set readOnly mode to true and verify
		that any write operation by Pod3 onto the volume should fail with an appropriate error message.
		7. Wait for Pods to reach Running Ready state.
		8. Verify CNS metadata for PVC and Pod.
		9. Perform cleanup by deleting Pod,PVC and SC.

	*/

	ginkgo.It("TC3Multiple Standalone Pods attached to a single RWX PVC", ginkgo.Label(p0, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType

		// Create allowed topologies for Storage Class: region1 > zone1 > building1 > level1 > rack > rack3)
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories, topologyLength, leafNode, leafNodeTag2)

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, pv, err := createRwxPVCwithStorageClass(client, namespace, nil, scParameters, "", allowedTopologyForSC, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv[0].Spec.CSI.VolumeHandle
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
		isCorrectPlacement := e2eVSphere.verifyPreferredDatastoreMatch(volHandle, datastoreUrlsRack3)
		gomega.Expect(isCorrectPlacement).To(gomega.BeTrue(), fmt.Sprintf("Volume provisioning has happened on the wrong datastore. Expected 'true', got '%v'", isCorrectPlacement))

		ginkgo.By("Create 3 standalone Pods using the same PVC")
		var pod *v1.Pod
		for i := 0; i < 3; i++ {
			if i == 2 {
				readOnly = true
			}
			pod, err = createStandalonePodsForRWXVolume(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod, readOnly)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume metadata for POD, PVC and PV")
			err = waitAndVerifyCnsVolumeMetadata(volHandle, pvclaim, pv[0], pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func(pod *v1.Pod) {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}(pod)

	})

	/*
		TESTCASE-4
		    1. Create a Storage Class with immediate Binding mode and allowed topology set to region-1 > zone-1 > building-1.
			2. Specify a Storage policy specific to datastore in rack-1 (vSAN tagged Storage Policy) in SC.
			3. Create a RWX PVC using the Storage Class created in step #1.
			4. Wait for PVC to reach Bound state.
			5. Volume provisioning should happen on the datastore of the allowed topology as specified in the SC.
			5. Create deployment Pod using the PVC created in step #3.
			6. Wait for deployment Pod to reach running ready state.
			7. Verify volume provisioning. Volume provisioned should happen on the same allowed topology as specified in the SC.
			8. Deployment Pod can be created on any AZ.
			9.


		    Allow some time for the Persistent Volume Claims (PVCs) to be created and reach the Bound state, and for the Pods to be created and reach a running state.
		    Volume provisioning and Pod placement will occur in the available availability zones (AZs) specified in the SC.

		    Increase the replica count of any one of the StatefulSets to 5.

		    Decrease the replica count of any one of the StatefulSets to 1.

		    Verify that the scaling up and scaling down of the StatefulSet is successful.
		    Volume provisioning and Pod placement will occur in the available availability zones (AZs) specified in the SC during the scale-up operation.
		    Verify CNS metadata for PVC and Pod.
		    Perform cleanup by deleting StatefulSets, PVCs, and the StorageClass (SC).
	*/

	ginkgo.It("TC4Deployment Pods with multiple replicas attached to a single RWX PVC", ginkgo.Label(p0, block, vanilla, level5, stable), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		replica := 3

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany, nfs4FSType))
		storageclass, pvclaim, pv, err := createRwxPVCwithStorageClass(client, namespace, nil, nil, "", nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv[0].Spec.CSI.VolumeHandle
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
		deployment, err := createDeployment(ctx, client, int32(replica), nil, nil, namespace, pvclaims, "", false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Scale down deployment to 0 replica")
		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pods, err := fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		rep := deployment.Spec.Replicas
		*rep = 3
		deployment.Spec.Replicas = rep
		deployment, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodNotFoundInNamespace(client, deployment.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

})
