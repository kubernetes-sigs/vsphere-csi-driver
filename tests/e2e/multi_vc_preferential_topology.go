/*
	Copyright 2020 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[Preferential-Topology] Preferential-Topology-Provisioning", func() {
	f := framework.NewDefaultFramework("preferential-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string

		topologyLength int

		preferredDatastoreChosen int

		allMasterIps []string
		masterIp     string

		dataCenters []*object.Datacenter
		clusters    []string
		//csiReplicas             int32
		//csiNamespace            string
		preferredDatastorePaths []string
		allowedTopologyRacks    []string

		sshClientConfig             *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd     string
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		ClusterdatastoreListVC      []map[string]string
		ClusterdatastoreListVC1     map[string]string
		ClusterdatastoreListVC2     map[string]string
		ClusterdatastoreListVC3     map[string]string
		parallelStatefulSetCreation bool
		stsReplicas                 int32
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
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		//csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)

		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		topologyLength = 5
		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)

		// csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
		// 	ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// csiReplicas = *csiDeployment.Spec.Replicas

		// fetching k8s master ip
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]

		// fetching datacenter details
		dataCenters, err = multiVCe2eVSphere.getAllDatacentersForMultiVC(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching cluster details
		client_index := 0
		clusters, err = getTopologyLevel5ClusterGroupNames(masterIp, sshClientConfig, dataCenters, true, client_index)
		fmt.Println(clusters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching list of datastores available in different racks
		ClusterdatastoreListVC1, ClusterdatastoreListVC2,
			ClusterdatastoreListVC3, err = getListOfDatastoresByClusterName1(masterIp, sshClientConfig, clusters[0], true)
		ClusterdatastoreListVC = append(ClusterdatastoreListVC, ClusterdatastoreListVC1,
			ClusterdatastoreListVC2, ClusterdatastoreListVC3)
		fmt.Println(ClusterdatastoreListVC1)
		fmt.Println(ClusterdatastoreListVC2)
		fmt.Println(ClusterdatastoreListVC3)
		fmt.Println(ClusterdatastoreListVC)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//set preferred datatsore time interval
		//setPreferredDatastoreTimeInterval(client, ctx, csiNamespace, namespace, csiReplicas)

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
		framework.Logf("Perform preferred datastore tags cleanup after test completion")
		err = deleteTagCreatedForPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Recreate preferred datastore tags post cleanup")
		err = createTagForPreferredDatastore(masterIp, sshClientConfig, allowedTopologyRacks)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/* Testcase-1:
		Add preferential tag in all the Availability zone's of VC1 and VC2  → change the preference during execution

	    Steps
	    Preferential FSS "topology-preferential-datastores" should be set and csi-vsphere-config should have
		the preferential tag.

	    1. Create SC  default parameters without any topology requirement.
	    2. In each availability zone for any one datastore add preferential tag in VC1 and VC2
	    3. Create 3 statefulset with 10 replica's
	    4. Wait for all the  PVC to bound and pod's to reach running state
	    5. Verify that since the prefered datastore is available, Volume should get created on the datastores
		 which has the preferencce set
	    6. Make sure common validation points are met on PV,PVC and POD
	    Change the Preference in any 2 datastores
	    scale up the statefull set to 15 replica
	    The volumes should get provision on the datastores which has the preference
	    Clear the data
	*/
	ginkgo.It("TagTest single preferred datastore each in VC1 and VC2 and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		parallelStatefulSetCreation = true
		sts_count := 3
		stsReplicas = 2

		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, nil, "",
			"", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		preferredDatastoreChosen = 1
		preferredDatastorePaths = nil

		// choose preferred datastore
		ginkgo.By("Tag preferred datastore for volume provisioning in VC1 and VC2")
		for i := 0; i < 2; i++ {
			paths, err := tagPreferredDatastore(masterIp, sshClientConfig, allowedTopologies[0].Values[i], preferredDatastoreChosen, ClusterdatastoreListVC[i], nil, true, i)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			preferredDatastorePaths = append(preferredDatastorePaths, paths...)
		}

		defer func() {
			ginkgo.By("Remove preferred datastore tag")
			for i := 0; i < len(preferredDatastorePaths); i++ {
				err = detachTagCreatedOnPreferredDatastore(masterIp, sshClientConfig, preferredDatastorePaths[i],
					allowedTopologies[0].Values[i], true, i)
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Waiting for %v for preferred datastore to get refreshed in the environment",
			preferredDatastoreTimeOutInterval)
		time.Sleep(preferredDatastoreTimeOutInterval)

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

		ginkgo.By("Verify volume is provisioned on the preferred datatsore")
		for i := 0; i < len(statefulSets); i++ {
			err = verifyVolumeProvisioningForStatefulSet(ctx, client, statefulSets[i], namespace,
				preferredDatastorePaths, ClusterdatastoreListVC[i], true, true)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/* Testcase-2:
		Create SC with storage policy available in VC1 and VC2 , set the preference in VC1 datastore only

	    Steps
		Preferential FSS "topology-preferential-datastores" should be set and csi-vsphere-config should have the preferential tag.

	    1. Create SC  default parameters without any topology requirement.
	    2. In each availability zone for any one datastore add preferential tag in VC1 and VC2
	    3. Create 3 statefulset with 10 replica's
	    4. Wait for all the  PVC to bound and pod's to reach running state
	    5. Verify that since the prefered datastore is available, Volume should get created on the datastores
		 which has the preferencce set
	    6. Make sure common validation points are met on PV,PVC and POD
	    Change the Preference in any 2 datastores
	    scale up the statefull set to 15 replica
	    The volumes should get provision on the datastores which has the preference
	    Clear the data
	*/

})

// func writeConfigToSecretString(cfg e2eTestConfig) (string, error) {
// 	result := fmt.Sprintf("[Global]\ninsecure-flag = \"%s\"\ncluster-distribution = \"%s\"\nquery-limit = %d\n"+
// 		"csi-fetch-preferred-datastores-intervalinmin = %d\nlist-volume-threshold = %d\n\n"+
// 		"[VirtualCenter \"10.161.119.92\"]\ninsecure-flag = \"%s\"\nuser = \"%s\"\npassword = \"%s\"\nport = \"%s\"\n"+
// 		"datacenters = \"%s\"\n\n"+
// 		"[VirtualCenter \"10.78.160.225\"]\ninsecure-flag = \"%s\"\nuser = \"%s\"\npassword = \"%s\"\nport = \"%s\"\n"+
// 		"datacenters = \"%s\"\n\n"+
// 		"datacenters = \"%s\"\n\n"+
// 		"[Labels]\ntopology-categories = \"%s\"",
// 		cfg.Global.InsecureFlag, cfg.Global.ClusterDistribution, cfg.Global.QueryLimit,
// 		cfg.Global.CSIFetchPreferredDatastoresIntervalInMin, cfg.Global.ListVolumeThreshold,
// 		cfg.VirtualCenter1.InsecureFlag, cfg.VirtualCenter1.User, cfg.VirtualCenter1.Password, cfg.VirtualCenter1.Port,
// 		cfg.VirtualCenter1.Datacenters,
// 		cfg.VirtualCenter2.InsecureFlag, cfg.VirtualCenter2.User, cfg.VirtualCenter2.Password, cfg.VirtualCenter2.Port,
// 		cfg.VirtualCenter2.Datacenters,
// 		cfg.VirtualCenter3.InsecureFlag, cfg.VirtualCenter3.User, cfg.VirtualCenter3.Password, cfg.VirtualCenter3.Port,
// 		cfg.VirtualCenter3.Datacenters,
// 		cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume,
// 		cfg.Labels.TopologyCategories)
// 	return result, nil
// }
