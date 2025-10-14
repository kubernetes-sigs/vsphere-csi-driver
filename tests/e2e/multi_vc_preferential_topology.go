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
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

var _ = ginkgo.Describe("[multivc-preferential] MultiVc-Preferential", func() {
	f := framework.NewDefaultFramework("multivc-preferential")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		preferredDatastoreChosen    int
		allMasterIps                []string
		masterIp                    string
		preferredDatastorePaths     []string
		allowedTopologyRacks        []string
		sshClientConfig             *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd     string
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		ClusterdatastoreListVc      []map[string]string
		ClusterdatastoreListVc1     map[string]string
		ClusterdatastoreListVc2     map[string]string
		ClusterdatastoreListVc3     map[string]string
		parallelStatefulSetCreation bool
		stsReplicas                 int32
		scaleDownReplicaCount       int32
		scaleUpReplicaCount         int32
		stsScaleUp                  bool
		stsScaleDown                bool
		verifyTopologyAffinity      bool
		topValStartIndex            int
		topValEndIndex              int
		topkeyStartIndex            int
		scParameters                map[string]string
		storagePolicyInVc1Vc2       string
		parallelPodPolicy           bool
		nodeAffinityToSet           bool
		podAntiAffinityToSet        bool
		snapc                       *snapclient.Clientset
		pandoraSyncWaitTime         int
		err                         error
		csiNamespace                string
		csiReplicas                 int32
	)

	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name

		multiVCbootstrap()

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

		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		stsScaleUp = true
		stsScaleDown = true
		verifyTopologyAffinity = true
		scParameters = make(map[string]string)
		storagePolicyInVc1Vc2 = GetAndExpectStringEnvVar(envStoragePolicyNameInVC1VC2)
		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)

		//Get snapshot client using the rest config
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching k8s master ip
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]

		// fetching cluster details
		clientIndex := 0
		clusterComputeResource, _, err = getClusterNameForMultiVC(ctx, &multiVCe2eVSphere, clientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching list of datastores available in different VCs
		ClusterdatastoreListVc1, ClusterdatastoreListVc2,
			ClusterdatastoreListVc3, err = getDatastoresListFromMultiVCs(masterIp, sshClientConfig,
			clusterComputeResource[0])
		ClusterdatastoreListVc = append(ClusterdatastoreListVc, ClusterdatastoreListVc1,
			ClusterdatastoreListVc2, ClusterdatastoreListVc3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		allowedTopologyRacks = nil
		for i := 0; i < len(allowedTopologies); i++ {
			for j := 0; j < len(allowedTopologies[i].Values); j++ {
				allowedTopologyRacks = append(allowedTopologyRacks, allowedTopologies[i].Values[j])
			}
		}

		// read namespace
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

		//set preferred datatsore time interval
		setPreferredDatastoreTimeInterval(client, ctx, csiNamespace, csiReplicas)
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
		framework.Logf("Perform preferred datastore tags cleanup after test completion")
		err = deleteTagCreatedForPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Recreate preferred datastore tags post cleanup")
		err = createTagForPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/* Testcase-1:
		Add preferential tag in all the Availability zone's of VC1 and VC2  → change the preference during
		execution

	    1. Create SC default parameters without any topology requirement.
	    2. In each availability zone for any one datastore add preferential tag in VC1 and VC2
	    3. Create 3 statefulset with 10 replica's
	    4. Wait for all the  PVC to bound and pods to reach running state
	    5. Verify that since the preferred datastore is available, Volume should get created on the datastores
		 which has the preferencce set
	    6. Make sure common validation points are met on PV,PVC and POD
	    7. Change the Preference in any 2 datastores
	    8. Scale up the statefulset to 15 replica
	    9. The volumes should get provision on the datastores which has the preference
	    10. Clear the data
	*/
	ginkgo.It("[pq-multivc] Tag one datastore as preferred each in VC1 and VC2 and verify it is honored", ginkgo.Label(p0,
		block, vanilla, multiVc, vc70, preferential), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		parallelStatefulSetCreation = true
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil
		sts_count := 3
		stsReplicas = 10
		var dsUrls []string
		scaleUpReplicaCount = 15
		stsScaleDown = false

		ginkgo.By("Create SC with default parameters")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in VC1 and VC2")
		for i := 0; i < 2; i++ {
			paths, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologies[0].Values[i],
				preferredDatastoreChosen, ClusterdatastoreListVc[i], nil, i)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			preferredDatastorePaths = append(preferredDatastorePaths, paths...)

			// Get the length of the paths for the current iteration
			pathsLen := len(paths)

			for j := 0; j < pathsLen; j++ {
				// Calculate the index for ClusterdatastoreListVc based on the current iteration
				index := i + j

				if val, ok := ClusterdatastoreListVc[index][paths[j]]; ok {
					dsUrls = append(dsUrls, val)
				}
			}
		}

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Create service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Create 3 statefulset with 10 replicas")
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

		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulSets[i], namespace,
				preferredDatastorePaths, nil, true, true, dsUrls)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Remove preferred datatsore tag which is chosen for volume provisioning")
		for i := 0; i < len(preferredDatastorePaths); i++ {
			err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[i],
				allowedTopologies[0].Values[i], i)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		var preferredDatastorePathsNew []string
		ginkgo.By("Tag new preferred datastore for volume provisioning in VC1 and VC2")
		for i := 0; i < 2; i++ {
			paths, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologies[0].Values[i],
				preferredDatastoreChosen, ClusterdatastoreListVc[i], preferredDatastorePaths, i)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			preferredDatastorePathsNew = append(preferredDatastorePathsNew, paths...)
			pathsLen := len(paths)
			for j := 0; j < pathsLen; j++ {
				index := i + j
				if val, ok := ClusterdatastoreListVc[index][paths[j]]; ok {
					dsUrls = append(dsUrls, val)
				}
			}
		}
		preferredDatastorePaths = append(preferredDatastorePaths, preferredDatastorePathsNew...)
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			for i := 0; i < len(preferredDatastorePathsNew); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePathsNew[i],
					allowedTopologies[0].Values[i], i)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		for i := 0; i < len(statefulSets); i++ {
			err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
				scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
				allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulSets[i], namespace,
				preferredDatastorePaths, nil, true, true, dsUrls)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/* Testcase-2:
	Create SC with storage policy available in VC1 and VC2, set the preference in VC1 datastore only

	Steps//
	1. Create storage policy with same name on both VC1 and VC2
	2. Add preference tag on the datastore which is on VC1 only
	3. Create statefulset using the above policy
	4. Since the preference tag is added in VC1, volume provisioning should  happned on VC1's datastore only
	[no, first preference will be given to Storage Policy mentioned in the Storage Class]
	5. Make sure common validation points are met on PV,PVC and POD
	6. Reboot VC1
	7. Scale up the stateful set to replica 15  → What should be the behaviour here
	8. Since the VC1 is presently in reboot state, new volumes should start coming up on VC2
	Once VC1  is up again the datastore preference should take preference
	[no, until all VCs comes up PVC provision will be stuck in Pending state]
	9. Verify the node affinity on all PV's
	10. Make sure POD has come up on appropriate nodes .
	11. Clean up the data
	*/

	ginkgo.It("[pq-multivc] Create SC with storage policy available in VC1 and VC2 and set the "+
		"preference in VC1 datastore only", ginkgo.Label(p0, block, vanilla, multiVc, vc70,
		preferential), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		/* here we are considering storage policy of VC1 and VC2 and the allowed topology is k8s-zone -> zone-1
		in case of 2-VC setup and 3-VC setup
		*/

		stsReplicas = 1
		scParameters[scParamStoragePolicyName] = storagePolicyInVc1Vc2
		topValStartIndex = 0
		topValEndIndex = 2
		var dsUrls []string
		stsScaleDown = false
		scaleUpReplicaCount = 7
		var multiVcClientIndex = 0
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		datastoreURLVC1 := GetAndExpectStringEnvVar(envPreferredDatastoreUrlVC1)
		datastoreURLVC2 := GetAndExpectStringEnvVar(envPreferredDatastoreUrlVC2)

		/*
			fetching datstore details passed in the storage policy
			Note: Since storage policy is specified in the SC, the first preference for volume provisioning
			will be given to the datastore given in the storage profile and second preference will then be
			given to the preferred datastore chosen
		*/
		for _, vCenterList := range ClusterdatastoreListVc {
			for _, url := range vCenterList {
				if url == datastoreURLVC1 || url == datastoreURLVC2 {
					dsUrls = append(dsUrls, url)
				}
			}
		}

		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		ginkgo.By("Create StorageClass with storage policy specified")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, allowedTopologies, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in VC1")
		preferredDatastorePaths, err := tagPreferredDatastore(masterIp, sshClientConfig,
			allowedTopologies[0].Values[0],
			preferredDatastoreChosen, ClusterdatastoreListVc[0], nil, multiVcClientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pathsLen := len(preferredDatastorePaths)
		for j := 0; j < pathsLen; j++ {
			if val, ok := ClusterdatastoreListVc[0][preferredDatastorePaths[j]]; ok {
				dsUrls = append(dsUrls, val)
			}
		}
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[i],
					allowedTopologies[0].Values[0], i)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Create StatefulSet and verify pv affinity and pod affinity details")
		service, statefulset, err := createStafeulSetAndVerifyPVAndPodNodeAffinty(ctx, client, namespace,
			parallelPodPolicy, stsReplicas, nodeAffinityToSet, allowedTopologies,
			podAntiAffinityToSet, parallelStatefulSetCreation, false, "", sc, verifyTopologyAffinity, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace,
			preferredDatastorePaths, nil, false, false, dsUrls)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Rebooting VC")
		framework.Logf("vcAddress - %s ", vcAddress)
		err = invokeVCenterReboot(ctx, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForHostToBeUp(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Done with reboot")

		essentialServices := []string{spsServiceName, vsanhealthServiceName, vpxdServiceName}
		checkVcenterServicesRunning(ctx, vcAddress, essentialServices)

		//After reboot
		multiVCbootstrap()

		ginkgo.By("Perform scaleup/scaledown operation on statefulsets and " +
			"verify pv affinity and pod affinity")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
			scaleDownReplicaCount, statefulset, parallelStatefulSetCreation, namespace,
			allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace,
			preferredDatastorePaths, nil, false, false, dsUrls)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* Testcase-3:
	Create/Restore Snapshot of PVC using single datastore preference

	Steps//

	1. Assign preferential tag to any one datastore under any one VC
	2. Create SC with allowed topology set to all the volumes
	[here, for verification of snapshot, considering single allowed topology of VC3 ]
	3. Create PVC-1 with the above SC
	4. Wait for PVC-1 to reach Bound state.
	5. Describe PV-1 and verify node affinity details
	6. Verify volume should be provisioned on the selected preferred datastore
	7. Create SnapshotClass, Snapshot of PVC-1.
	8. Verify snapshot state. It should be in ready-to-use state.
	9. Verify snapshot should be created on the preferred datastore.
	10. Restore snapshot to create PVC-2
	11. Wait for PVC-2 to reach Bound state.
	12. Describe PV-2 and verify node affinity details
	13. Verify volume should be provisioned on the selected preferred datastore
	14. Create Pod from restored PVC-2.
	15. Make sure common validation points are met on PV,PVC and POD
	16. Make sure POD is running on the same node as mentioned in the node affinity details.
	17. Perform Cleanup. Delete Snapshot, Pod, PVC, SC
	18. Remove datastore preference tags as part of cleanup.
	*/

	ginkgo.It("[pq-multivc] Assign preferred datatsore to any one VC and verify create restore "+
		"snapshot", ginkgo.Label(p0,
		block, vanilla, multiVc, vc70, snapshot, preferential), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil
		var dsUrls []string
		var multiVcClientIndex = 2
		topValStartIndex = 2
		topValEndIndex = 3

		// Considering k8s-zone -> zone-3 i.e. VC3 allowed topology
		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in VC3")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, sshClientConfig,
			allowedTopologies[0].Values[0],
			preferredDatastoreChosen, ClusterdatastoreListVc[2], nil, multiVcClientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pathsLen := len(preferredDatastorePaths)
		for j := 0; j < pathsLen; j++ {
			if val, ok := ClusterdatastoreListVc[2][preferredDatastorePaths[j]]; ok {
				dsUrls = append(dsUrls, val)
			}
		}
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[0],
				allowedTopologies[0].Values[0], multiVcClientIndex)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Create StorageClass and PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, nil,
			nil, diskSize, allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PVC to be in Bound phase
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := multiVCe2eVSphere.queryCNSVolumeWithResultInMultiVC(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class, volume snapshot")
		volumeSnapshot, volumeSnapshotClass, snapshotId := createSnapshotClassAndVolSnapshot(ctx, snapc, namespace,
			pvclaim, volHandle, false)
		defer func() {
			ginkgo.By("Perform cleanup of snapshot created")
			performCleanUpForSnapshotCreated(ctx, snapc, namespace, volHandle, volumeSnapshot, snapshotId,
				volumeSnapshotClass, pandoraSyncWaitTime)
		}()

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)
		pvclaim2, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
				volHandle2, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle2,
					pod.Spec.NodeName))
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(client, pod, preferredDatastorePaths,
			ClusterdatastoreListVc[2], dsUrls)

		ginkgo.By("Verify PV node affinity and that the PODS are running on " +
			"appropriate node as specified in the allowed topologies of SC")
		err = verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx, client, pod,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* Testcase-4

		Create/Restore Snapshot of PVC, datastore preference change

		1. Assign preferential tag to any one datastore under any one VC
	    2. Create SC with allowed topology set to all the volumes
	    3. Create PVC-1 with the above SC
	    4. Wait for PVC-1 to reach Bound state.
	    5. Describe PV-1 and verify node affinity details
	    6. Verify volume should be provisioned on the selected preferred datastore
	    7. Make sure common validation points are met on PV,PVC and POD
	    8. Change datastore preference(ex- from NFS-2 to vSAN-2)
	    9. Create SnapshotClass, Snapshot of PVC-1
	    10. Verify snapshot state. It should be in ready-to-use state.
	    11. Restore snapshot to create PVC-2
	    12. PVC-2 should get stuck in Pending state and proper error message should be displayed.
	    13. Perform Cleanup. Delete Snapshot, Pod, PVC, SC
	    14. Remove datastore preference tags as part of cleanup.
	*/

	ginkgo.It("[pq-multivc] Assign preferred datatsore to any one VC and verify create restore snapshot "+
		"and later change datastore preference", ginkgo.Label(p1, block, vanilla, multiVc,
		vc70, snapshot, negative, preferential), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil
		var dsUrls []string
		var multiVcClientIndex = 2
		topValStartIndex = 2
		topValEndIndex = 3

		// Considering k8s-zone -> zone-3 i.e. VC3 allowed topology
		ginkgo.By("Set specific allowed topology")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in VC3")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, sshClientConfig,
			allowedTopologies[0].Values[0],
			preferredDatastoreChosen, ClusterdatastoreListVc[2], nil, multiVcClientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pathsLen := len(preferredDatastorePaths)
		for j := 0; j < pathsLen; j++ {
			if val, ok := ClusterdatastoreListVc[2][preferredDatastorePaths[j]]; ok {
				dsUrls = append(dsUrls, val)
			}
		}
		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[0],
				allowedTopologies[0].Values[0], multiVcClientIndex)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Create StorageClass and PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client, namespace, nil,
			nil, diskSize, allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PVC to be in Bound phase
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaim},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = multiVCe2eVSphere.waitForCNSVolumeToBeDeletedInMultiVC(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := multiVCe2eVSphere.queryCNSVolumeWithResultInMultiVC(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Creating pod")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(client,
				volHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle,
					pod.Spec.NodeName))
		}()

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		verifyVolumeProvisioningForStandalonePods(client, pod, preferredDatastorePaths,
			ClusterdatastoreListVc[2], dsUrls)

		ginkgo.By("Remove preferred datastore tag chosen for volume provisioning")
		err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[0],
			allowedTopologyRacks[2], multiVcClientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in VC3")
		preferredDatastorePaths, err = tagPreferredDatastore(masterIp, sshClientConfig,
			allowedTopologies[0].Values[0], preferredDatastoreChosen, ClusterdatastoreListVc[2],
			preferredDatastorePaths, multiVcClientIndex)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

		ginkgo.By("Create volume snapshot class, volume snapshot")
		volumeSnapshot, volumeSnapshotClass, snapshotId := createSnapshotClassAndVolSnapshot(ctx, snapc, namespace,
			pvclaim, volHandle, false)
		defer func() {
			ginkgo.By("Perform cleanup of snapshot created")
			performCleanUpForSnapshotCreated(ctx, snapc, namespace, volHandle, volumeSnapshot, snapshotId,
				volumeSnapshotClass, pandoraSyncWaitTime)
		}()

		ginkgo.By("Create PVC from snapshot")
		pvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, storageclass, nil,
			v1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)
		pvclaim2, err := fpv.CreatePVC(ctx, client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to fail provisioning volume within the topology")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound,
			client, pvclaim2.Namespace, pvclaim2.Name, framework.Poll, framework.ClaimProvisionTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())
		expectedErrMsg := "failed to get the compatible shared datastore for create volume from snapshot"
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Expected error : %q", expectedErrMsg))
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})
})
