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
	"strconv"
	"sync"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
)

var _ = ginkgo.Describe("[csi-topology-vanilla-level5] Topology-Aware-Provisioning-With-Statefulset-Level5", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-topology-aware-provisioning")
	var (
		client                  clientset.Interface
		namespace               string
		bindingMode             storagev1.VolumeBindingMode
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		topologyLength          int
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
		//topologyLength, leafNode, leafNodeTag0, leafNodeTag1, leafNodeTag2 = 5, 4, 0, 1, 2
		topologyLength = 5

		topologyMap := GetAndExpectStringEnvVar(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap, topologyLength)
		allowedTopologies = createAllowedTopolgies(topologyMap, topologyLength)
		fmt.Println(allowedTopologies)
	})

	ginkgo.It("Volume provisioning when CSI Provisioner is deleted during statefulset creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		controller_name := "csi-provisioner"
		sshClientConfig := &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)

		// Create SC with Immediate BindingMode
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC, "", "", false, "nginx-sc")
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

		k8sMasterIP, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx, client,
			sshClientConfig, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = executeDockerPauseKillCmd(sshClientConfig, k8sMasterIP, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		statefulSets := createParallelStatefulSetSpec(namespace, 3)

		var wg sync.WaitGroup
		//wg := sync.WaitGroup{}
		wg.Add(3)
		var statefulSetReplicaCount int32 = 3
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)
		}
		wg.Wait()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies)
		}

		// Scale down statefulset to 5 replicas
		statefulSetReplicaCount -= 1
		ginkgo.By("Scale down statefulset replica count to 0")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
		}

		k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx, client,
			sshClientConfig, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = executeDockerPauseKillCmd(sshClientConfig, k8sMasterIP, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset to 0 replicas
		statefulSetReplicaCount -= 0
		ginkgo.By("Scale down statefulset replica count to 0")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
		}
	})

	ginkgo.It("Volume provisioning when CSI Attacher is deleted during statefulset creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		controller_name := "csi-attacher"
		sshClientConfig := &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		/* Get allowed topologies for Storage Class
		region1 > zone1 > building1 > level1 > rack > rack1/rack2/rack3 */
		allowedTopologyForSC := getTopologySelector(topologyAffinityDetails, topologyCategories,
			topologyLength)

		// Create SC with Immediate BindingMode
		storageclass, err := createStorageClass(client, nil, allowedTopologyForSC, "", "", false, "nginx-sc")
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

		k8sMasterIP, err := getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx, client,
			sshClientConfig, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = executeDockerPauseKillCmd(sshClientConfig, k8sMasterIP, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		statefulSets := createParallelStatefulSetSpec(namespace, 3)

		var wg sync.WaitGroup
		//wg := sync.WaitGroup{}
		wg.Add(3)
		var statefulSetReplicaCount int32 = 3
		for i := 0; i < len(statefulSets); i++ {
			go createParallelStatefulSets(client, namespace, statefulSets[i],
				statefulSetReplicaCount, &wg)
		}
		wg.Wait()

		ginkgo.By("Verify PV node affinity and that the PODS are running on appropriate node")
		for i := 0; i < len(statefulSets); i++ {
			verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, client,
				statefulSets[i], namespace, allowedTopologies)
		}

		// Scale down statefulset to 5 replicas
		statefulSetReplicaCount -= 1
		ginkgo.By("Scale down statefulset replica count to 0")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
		}

		k8sMasterIP, err = getK8sMasterNodeIPWhereControllerLeaderIsRunning(ctx, client,
			sshClientConfig, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = executeDockerPauseKillCmd(sshClientConfig, k8sMasterIP, controller_name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Scale down statefulset to 0 replicas
		statefulSetReplicaCount -= 0
		ginkgo.By("Scale down statefulset replica count to 0")
		for i := 0; i < len(statefulSets); i++ {
			scaleDownStatefulSetPod(ctx, client, statefulSets[i], namespace, statefulSetReplicaCount)
		}

	})
})

func createParallelStatefulSets(client clientset.Interface, namespace string,
	statefulset *appsv1.StatefulSet, replicas int32, wg *sync.WaitGroup) {
	defer wg.Done()
	ginkgo.By("Creating statefulset")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	framework.Logf(fmt.Sprintf("Creating statefulset %v/%v with %d replicas and selector %+v",
		statefulset.Namespace, statefulset.Name, *(statefulset.Spec.Replicas), statefulset.Spec.Selector))
	_, err := client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
	framework.ExpectNoError(err)

}

func createParallelStatefulSetSpec(namespace string, no_of_sts int) []*appsv1.StatefulSet {
	stss := []*appsv1.StatefulSet{}
	var statefulset *appsv1.StatefulSet
	for i := 0; i < no_of_sts; i++ {
		statefulset = GetStatefulSetFromManifest(namespace)
		statefulset.Name = "thread-" + strconv.Itoa(i) + "-" + statefulset.Name
		stss = append(stss, statefulset)
	}
	return stss
}
