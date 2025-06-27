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
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ bool = ginkgo.Describe("[tkg-domain-isolation-vsan-max] TKG-WLDI-Vsan-Max", func() {

	f := framework.NewDefaultFramework("tkg-domain-isolation-vsan-max")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                      clientset.Interface
		namespace                   string
		vcRestSessionId             string
		allowedTopologies           []v1.TopologySelectorLabelRequirement
		replicas                    int32
		topologyAffinityDetails     map[string][]string
		topologyCategories          []string
		labelsMap                   map[string]string
		labels_ns                   map[string]string
		sharedStoragePolicyNameWffc string
	)

	ginkgo.BeforeEach(func() {
		namespace = getNamespaceToRunTests(f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// making vc connection
		client = f.ClientSet
		bootstrap()

		// reading vc session id
		if vcRestSessionId == "" {
			vcRestSessionId = createVcSession4RestApis(ctx)
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

		// reading shared storage policy
		sharedStoragePolicyNameWffc = GetAndExpectStringEnvVar(envIsolationSharedStoragePolicyNameLateBidning)

		svcNamespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)

		// Read testbedInfo.json and populate tbinfo
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		dumpSvcNsEventsOnTestFailure(client, namespace)

		framework.Logf("Collecting supervisor PVC events before performing PV/PVC cleanup")
		eventList, err := client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, item := range eventList.Items {
			framework.Logf("%q", item.Message)
		}
	})

	/*
		Testcase-5 & 6
		vSAN Max with Fault Domains with HCI mounted datastore - Block & file Volume

		Steps:
		1. Deploy statefulsets with 3 replica on namespace-4 in the TKG cluster
		1.1 Use vsan-shared policy with WFFC volume binding mode storageclass 1
		1.2 Access Mode as ReadWriteOnce and ReadWriteMany
		2. Bring down one of the host from vsphere cluster in zone-2
		3. Bring down one of the host from vSAN max fault domain-1
		4. Bring up host from zone-2 and host from fault domain-1
		5. Bring down both hosts from vsphere cluster in zone-3
		6. Bring down both hosts from vSAN max fault domain-2
		7. Bring up all hosts from zone-3 and all hosts from fault domain-2
	*/

	ginkgo.It("vSAN Max with Fault Domains with HCI mounted datastore", ginkgo.Label(p0, wldi, vc90u1), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// get the vsan-max FD name
		vsanMaxFdName := GetAndExpectStringEnvVar(vsanMaxFaultDomainName)

		// statefulset replica count
		replicas = 3

		// Get the fault domain and host map
		fdMap := createFaultDomainMap(ctx, &e2eVSphere)

		// Flag to check the status of hosts
		isHostDown := false
		isFdHostDown := false

		ginkgo.By("Read shared storage policy tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, sharedStoragePolicyNameWffc, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset with ReadWriteOnce")
		statefulsetRwo := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, true, allowedTopologies,
			true, true, "", "", storageclass, storageclass.Name)
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulsetRwo, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating statefulset with ReadWriteMany")
		statefulsetRwm := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, true, allowedTopologies,
			true, true, "", v1.ReadWriteMany, storageclass, storageclass.Name)
		defer func() {
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulsetRwm, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Get all hosts of zone2
		zone2 := topologyAffinityDetails[topologyCategories[0]][1]
		nodes := getNodesFromZone(ctx, client, zone2)
		gomega.Expect(len(nodes) > 0).To(gomega.BeTrue())
		// Get host IP of zone2
		hostIp := nodes[0].Status.Addresses[1].Address
		// Get VM name of the host from zone2
		hostlist := getVmNameFromHostIp([]string{hostIp})
		gomega.Expect(len(hostlist) > 0).To(gomega.BeTrue())
		// Power off the ESX host
		err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, hostlist, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isHostDown = true
		defer func() {
			if isHostDown {
				err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, hostlist, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitForHostToBeUp(hostIp, time.Minute*40)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		err = waitForHostToBeDown(ctx, hostIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Bring down one of the host from vSAN max fault domain-1
		fdHostVmName := ""
		fdHostIp := ""
		for hostIp, site := range fdMap {
			if strings.Contains(site, vsanMaxFdName) {
				fdHostIp = hostIp
				fdHostVmName = getVmNameFromHostIp([]string{hostIp})
				err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, fdHostVmName, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isFdHostDown = true
				break
			}
		}
		defer func() {
			if isFdHostDown {
				err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, fdHostVmName, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitForHostToBeUp(fdHostIp, time.Minute*40)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		err = waitForHostToBeDown(ctx, fdHostIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check for TKG VM and STS pod status
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		gomega.Expect(len(nodeList.Items) == 6).To(gomega.BeTrue())
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwo, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwm, replicas)

		// Bring up host from zone-2 and host from fault domain-1
		err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, hostlist, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isHostDown = false
		err = waitForHostToBeUp(hostIp, time.Minute*40)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, fdHostVmName, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isFdHostDown = false
		err = waitForHostToBeUp(fdHostIp, time.Minute*40)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Bring down all hosts from vsphere cluster in zone-3
		zone3 := topologyAffinityDetails[topologyCategories[0]][2]
		nodes = getNodesFromZone(ctx, client, zone3)
		gomega.Expect(len(nodes) > 0).To(gomega.BeTrue())
		var nodeIps []string
		for _, node := range nodes {
			nodeIps = append(nodeIps, node.Status.Addresses[1].Address)
		}
		hostlist = getVmNameFromHostIp(nodeIps)
		err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, hostlist, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isHostDown = true
		defer func() {
			if isHostDown {
				err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, hostlist, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, nodeIp := range nodeIps {
					err = waitForHostToBeUp(nodeIp, time.Minute*40)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

			}
		}()
		for _, nodeIp := range nodeIps {
			err = waitForHostToBeDown(ctx, nodeIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		//Bring down all hosts from vSAN max fault domain-2
		var fdHostIps []string
		var fdHostVmNames []string
		for hostIp, site := range fdMap {
			if strings.Contains(site, vsanMaxFdName) {
				fdHostIps = append(fdHostIps, hostIp)
				fdHostVmName = getVmNameFromHostIp([]string{hostIp})
				fdHostVmNames = append(fdHostVmNames, fdHostVmName)
				err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, fdHostVmName, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isFdHostDown = true
			}
		}
		defer func() {
			if isFdHostDown {
				for _, fdHostVmName := range fdHostVmNames {
					err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, fdHostVmName, true)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					for _, hostIp := range fdHostIps {
						err = waitForHostToBeUp(hostIp, time.Minute*40)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
		}()
		for _, hostIp := range fdHostIps {
			err = waitForHostToBeDown(ctx, hostIp)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		//Check for TKG VM and STS pod status
		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		gomega.Expect(len(nodeList.Items) > 0).To(gomega.BeTrue())
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwo, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwm, replicas)
	})

})
