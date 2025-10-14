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

var _ = ginkgo.Describe("[multivc-multireplica] MultiVc-MultiReplica", func() {
	f := framework.NewDefaultFramework("multivc-multireplica")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		allowedTopologies           []v1.TopologySelectorLabelRequirement
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

		stsScaleUp = true
		stsScaleDown = true
		verifyTopologyAffinity = true
		parallelStatefulSetCreation = true

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
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{})
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

	ginkgo.It("[pq-multivc] Verify behaviour when CSI-Provisioner, CSI-Attacher, Vsphere-Syncer is "+
		"deleted repeatedly during workload creation in multivc", ginkgo.Label(p1,
		block, vanilla, multiVc, vc70), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sts_count := 3
		statefulSetReplicaCount = 5

		ginkgo.By("Get current leader where CSI-Provisioner, CSI-Attacher and " +
			"Vsphere-Syncer is running and find the master node IP where these containers are running")
		csiProvisionerLeader, csiProvisionerControlIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, provisionerContainerName)
		framework.Logf("CSI-Provisioner is running on Leader Pod %s "+
			"which is running on master node %s", csiProvisionerLeader, csiProvisionerControlIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		csiAttacherLeaderleader, csiAttacherControlIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, attacherContainerName)
		framework.Logf("CSI-Attacher is running on Leader Pod %s "+
			"which is running on master node %s", csiAttacherLeaderleader, csiAttacherControlIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vsphereSyncerLeader, vsphereSyncerControlIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshClientConfig, syncerContainerName)
		framework.Logf("Vsphere-Syncer is running on Leader Pod %s "+
			"which is running on master node %s", vsphereSyncerLeader, vsphereSyncerControlIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create SC with allowed topology spread across multiple VCs")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "",
			"", false)
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
				err = execDockerPauseNKillOnContainer(sshClientConfig, csiProvisionerControlIp, provisionerContainerName,
					k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if i == 2 {
				ginkgo.By("Kill CSI-Attacher container")
				err = execDockerPauseNKillOnContainer(sshClientConfig, csiAttacherControlIp, attacherContainerName,
					k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
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
			err = verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies, parallelStatefulSetCreation)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Perform scaleup/scaledown operation on statefulset and verify pv and pod affinity details")
		for i := 0; i < len(statefulSets); i++ {
			if i == 0 {
				stsScaleUp = false
				scaleDownReplicaCount = 3
				framework.Logf("Scale down StatefulSet1 replica count to 3")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if i == 1 {
				scaleUpReplicaCount = 9
				stsScaleDown = false
				framework.Logf("Scale up StatefulSet2 replica count to 9 and in between " +
					"kill vsphere syncer container")
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				framework.Logf("Kill Vsphere-Syncer container")
				err = execDockerPauseNKillOnContainer(sshClientConfig, vsphereSyncerControlIp, syncerContainerName,
					k8sVersion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			}
			if i == 2 {
				framework.Logf("Scale up StatefulSet3 replica count to 7 and later scale down" +
					"the replica count to 2")
				scaleUpReplicaCount = 7
				scaleDownReplicaCount = 2
				err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client, scaleUpReplicaCount,
					scaleDownReplicaCount, statefulSets[i], parallelStatefulSetCreation, namespace,
					allowedTopologies, stsScaleUp, stsScaleDown, verifyTopologyAffinity)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})
})
