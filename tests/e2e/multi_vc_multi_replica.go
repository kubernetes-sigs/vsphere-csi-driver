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

var _ = ginkgo.Describe("[csi-multi-vc-topology] Multi-VC", func() {
	f := framework.NewDefaultFramework("csi-multi-vc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		topologyLength              int
		sshClientConfig             *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd     string
		statefulSetReplicaCount     int32
		k8sVersion                  string
		stsScaleUp                  bool
		stsScaleDown                bool
		verifyTopologyAffinity      bool
		parallelStatefulSetCreation bool
		scaleUpReplicaCount         int32
		scaleDownReplicaCount       int32
	)
	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name

		multiVCbootstrap()

		topologyLength = 5
		stsScaleUp = true
		stsScaleDown = true
		verifyTopologyAffinity = true
		parallelStatefulSetCreation = true

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
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		// fetching k8s version
		v, err := client.Discovery().ServerVersion()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		k8sVersion = v.Major + "." + v.Minor

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

	/*
		Verify the behaviour when CSI Provisioner, CSI Attacher, Vsphere syncer is deleted repeatedly
		during workload creation

		1. Identify the CSI-Controller-Pod where CSI Provisioner, CSI Attacher and vsphere-syncer are the leader
		2. Create SC with allowed topology set to different availability zones  spread across multiple VC's
		3. Create three Statefulsets each with replica 5
		4. While the Statefulsets is creating PVCs and Pods, kill CSI-Provisioner, CSI-attacher identified
		in the step 1
		5. Wait for some time for all the PVCs and Pods to come up
		6. Verify the PV node affinity and the nodes on which Pods have come should be appropriate
		7. Scale-up/Scale-down the Statefulset and kill the vsphere-syncer identified in the step 1
		8. Wait for some time,  Statefulset workload is patched with proper count
		9. Make sure common validation points are met on PV,PVC and POD
		10. Clean up the data
	*/

	ginkgo.It("Verify behaviour when CSI-Provisioner, CSI-Attacher, Vsphere-Syncer is "+
		"deleted repeatedly during workload creation in multivc", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sts_count := 3

		ginkgo.By("Get current leader where CSI-Provisioner, CSI-Attacher and " +
			"Vsphere-Syncer is running and find the master node IP where these containers pods are running")
		leader1, k8sMasterIP1, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, provisionerContainerName)
		framework.Logf("CSI-Provisioner is running on Leader Pod %s "+
			"which is running on master node %s", leader1, k8sMasterIP1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		leader2, k8sMasterIP2, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, attacherContainerName)
		framework.Logf("CSI-Attacher is running on Leader Pod %s "+
			"which is running on master node %s", leader2, k8sMasterIP2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		leader3, k8sMasterIP3, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, syncerContainerName)
		framework.Logf("Vsphere-Syncer is running on Leader Pod %s "+
			"which is running on master node %s", leader3, k8sMasterIP3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create SC with allowed topology spread across multiple VCs")
		storageclass, err := createStorageClass(client, nil, allowedTopologies, "", "", false, "nginx-sc")
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

		ginkgo.By("Creating multiple StatefulSets specs in parallel")
		statefulSets := createParallelStatefulSetSpec(namespace, sts_count, statefulSetReplicaCount)

		ginkgo.By("Trigger multiple StatefulSets creation in parallel. During StatefulSets " +
			"creation, kill CSI-Provisioner, CSI-Attacher container in between")
		var wg sync.WaitGroup
		wg.Add(sts_count)
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i], statefulSetReplicaCount, &wg)
			if i == 1 {
				ginkgo.By("Kill CSI-Provisioner container")
				err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP1, provisionerContainerName,
					k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if i == 2 {
				ginkgo.By("Kill CSI-Attacher container")
				err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP2, attacherContainerName,
					k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		wg.Wait()

		ginkgo.By("Waiting for StatefulSets Pods to be in Ready State")
		time.Sleep(60 * time.Second)

		ginkgo.By("Verify that all parallel triggered StatefulSets Pods creation should be in up and running state")
		for i := 0; i < len(statefulSets); i++ {
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], statefulSetReplicaCount)
			gomega.Expect(CheckMountForStsPods(client, statefulSets[i], mountPath)).NotTo(gomega.HaveOccurred())

			ssPods := GetListOfPodsInSts(client, statefulSets[i])
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name))
			gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, parallelStatefulSetCreation, true)
		}

		for i := 0; i < len(statefulSets); i++ {
			ginkgo.By("Perform scaleup/scaledown operation on statefulset and " +
				"verify pv and pod affinity details")
			if i == 0 {
				stsScaleUp = false
				scaleDownReplicaCount = 3
				framework.Logf("Scale down StatefulSet1 replica count to 3")
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
			}
			if i == 1 {
				scaleUpReplicaCount = 9
				stsScaleDown = false
				framework.Logf("Scale up StatefulSet2 replica count to 9 and in between " +
					"kill vsphere syncer container")
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)

				framework.Logf("Kill Vsphere-Syncer container")
				err = execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIP3, syncerContainerName,
					k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			}
			if i == 2 {
				framework.Logf("Scale up StatefulSet3 replica count to 7 and later scale down" +
					"the replica count to 2")
				scaleUpReplicaCount = 7
				scaleDownReplicaCount = 2
				performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
			}
		}
	})
})
