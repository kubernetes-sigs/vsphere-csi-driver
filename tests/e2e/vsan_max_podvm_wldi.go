/*
Copyright 2025 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/drain"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ bool = ginkgo.Describe("[podvm-domain-isolation-vsan-max] PodVM-WLDI-Vsan-Max", func() {

	f := framework.NewDefaultFramework("domain-isolation")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                  clientset.Interface
		namespace               string
		storageProfileId        string
		vcRestSessionId         string
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		storagePolicyName       string
		topkeyStartIndex        int
		topologyAffinityDetails map[string][]string
		topologyCategories      []string
		labelsMap               map[string]string
		labels_ns               map[string]string
		err                     error
		zone1                   string
		zone2                   string
		statuscode              int
		nodeList                *v1.NodeList
		uncordon                bool
		filteredNodes           *v1.NodeList
		dh                      drain.Helper
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// making vc connection
		client = f.ClientSet
		bootstrap()

		// reading vc session id
		if vcRestSessionId == "" {
			vcRestSessionId = createVcSession4RestApis(ctx)
		}

		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		// reading topology map set for management domain and workload domain
		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap)

		// required for pod creation
		labels_ns = map[string]string{}
		labels_ns[admissionapi.EnforceLevelLabel] = string(admissionapi.LevelPrivileged)
		labels_ns["e2e-framework"] = f.BaseName

		//setting map values
		labelsMap = make(map[string]string)
		labelsMap["app"] = "test"

		//zones used in the test
		zone1 = topologyAffinityDetails[topologyCategories[0]][0]
		zone2 = topologyAffinityDetails[topologyCategories[0]][1]

		dh = drain.Helper{
			Ctx:                 ctx,
			Client:              client,
			Force:               true,
			IgnoreAllDaemonSets: true,
			Out:                 ginkgo.GinkgoWriter,
			ErrOut:              ginkgo.GinkgoWriter,
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dumpSvcNsEventsOnTestFailure(client, namespace)

		framework.Logf("Collecting supervisor PVC events before performing PV/PVC cleanup")
		eventList, err := client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, item := range eventList.Items {
			framework.Logf("%q", item.Message)
		}

		if uncordon {
			ginkgo.By("Uncordoning of nodes")
			for i := range filteredNodes.Items {
				node := &filteredNodes.Items[i]
				ginkgo.By("Uncordoning node: " + node.Name)
				err := drain.RunCordonOrUncordon(&dh, node, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	/*
		vSAN Max vSAN Stretch zonal mount of datastore on Az2 with cordon/uncodeon of nodes

		Steps:
		1. Create a wcp namespace and tagged it to Az1, Az2 zone.
		2. Read a zonal storage policy which is compatible only with Az2 zone using Immediate Binding mode.
		3. Create pvc using zonal policy of Az2
		4. Wait for PVC and PV to reach Bound state.
		5. Verify PVC has csi.vsphere.volume-accessible-topology annotation with Az2
		6. Verify PV has node affinity rule for Az2
		7. Create a Pod using pvc created above
		8. wait for Pod to reach running state
		9. Verify pod node annotation.
		10. Cordon all the nodes in Az2
		11. Delete and re-create the above pod.
		12. Scheduling should fail for the re-created pod as there are no nodes available in Az2
		13. Uncordon the nodes before cleanup
	*/

	ginkgo.It("vSAN Max vSAN Stretch zonal mount of datastore on Az2 "+
		"with cordon/uncordon of nodes", ginkgo.Label(p0, wldi, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// reading zonal storage policy of zone-2 workload domain
		storagePolicyName = GetAndExpectStringEnvVar(envZonal2StoragePolicyName)
		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)

		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4
			so topValStartIndex=1 and topValEndIndex=2 will fetch the 1st index value from topology map string
		*/
		topValStartIndex := 0
		topValEndIndex := 2

		ginkgo.By("Create a WCP namespace tagged to Az1, Az2 but policy tagged is of Az2 only")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1, zone2}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read Az2 storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create PVC")
		pvclaim, persistentVolumes, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle

		ginkgo.By("Create Pod and attach it to a Pvc")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		annotations := pod.Annotations
		vmUUID, exists := annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, pod, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Fetch the node details of Az2")
		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4
			so topValStartIndex=1 and topValEndIndex=2 will fetch the 1st index value from topology map string i.e
			fetching Az2 zone
		*/
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, 1,
			2)
		allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
		filteredNodes := fetchAllNodesOfSpecificZone(nodeList, allowedTopologiesMap)

		for _, node := range filteredNodes.Items {
			ginkgo.By("Cordoning of node: " + node.Name)
			err = drain.RunCordonOrUncordon(&dh, &node, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Draining of node: " + node.Name)
			err = drain.RunNodeDrain(&dh, node.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		defer func() {
			if uncordon {
				ginkgo.By("Uncordoning of nodes")
				for i := range filteredNodes.Items {
					node := &filteredNodes.Items[i]
					ginkgo.By("Uncordoning node: " + node.Name)
					err := drain.RunCordonOrUncordon(&dh, node, false)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				uncordon = false
			}
		}()

		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Recreate Pod and attach it to same above PVC, " +
			"expecting pod to fail as no nodes are available for scheduling")
		_, err = createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execRWXCommandPod1)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Uncordoning of nodes")
		for i := range filteredNodes.Items {
			node := &filteredNodes.Items[i]
			ginkgo.By("Uncordoning node: " + node.Name)
			err := drain.RunCordonOrUncordon(&dh, node, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		uncordon = true
	})
})
