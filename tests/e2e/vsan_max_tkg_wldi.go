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
	"math/rand"
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
		nodeList                    *v1.NodeList
		err                         error
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

		// fetching tkg node list
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

		// reading shared storage policy
		sharedStoragePolicyNameWffc = GetAndExpectStringEnvVar(envIsolationSharedStoragePolicyNameLateBidning)

		svcNamespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)

		// Read testbedInfo.json and populate tbinfo
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))

		/* This function initializes the fds struct by categorizing ESXi hosts into primary,
		secondary, and witness based on their fault domain in a vSAN stretched cluster */
		initialiseFdsVar(ctx)
		err = waitForAllNodes2BeReady(ctx, client)
		framework.ExpectNoError(err, "cluster not completely healthy")
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("Power on the hosts")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			powerOnHostParallel(fds.hostsDown)
			fds.hostsDown = nil
		}

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

	ginkgo.JustAfterEach(func() {
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			powerOnHostParallel(fds.hostsDown)
			fds.hostsDown = nil
		}

		fds.primarySiteHosts = nil
		fds.secondarySiteHosts = nil
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

	ginkgo.It("vSAN Max with Fault Domains with HCI mounted datastore", ginkgo.Label(p0, wldi, vc90), func() {
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
		isFaultDomainHostDown := false
		isClusterDown := false
		isFaultDomainDown := false

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

		// Bring down a host from zone2
		zone2 := topologyAffinityDetails[topologyCategories[0]][1]
		poweredOffHostIps := powerOffHostsFromZone(ctx, zone2, false, 1)
		isHostDown = true
		defer func() {
			if isHostDown {
				powerOnHostParallel(poweredOffHostIps)
			}
		}()

		// Bring down one of the host from vSAN max fault domain-1
		poweredOffFdHostIps := powerOffHostsFromFaultDomain(ctx, vsanMaxFdName, fdMap, false, 1)
		isFaultDomainHostDown = true
		defer func() {
			if isFaultDomainHostDown {
				powerOnHostParallel(poweredOffFdHostIps)
			}
		}()

		// Check for TKG VM and STS pod status
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		gomega.Expect(len(nodeList.Items) == 6).To(gomega.BeTrue())
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwo, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwm, replicas)

		// Bring up host from zone-2 and host from fault domain-1
		powerOnHostParallel(poweredOffHostIps)
		isHostDown = false
		powerOnHostParallel(poweredOffFdHostIps)
		isFaultDomainHostDown = false

		//Bring down all hosts from vsphere cluster in zone-3
		zone3 := topologyAffinityDetails[topologyCategories[0]][2]
		poweredOffHostIps = powerOffHostsFromZone(ctx, zone3, true, 0)
		isClusterDown = true
		defer func() {
			if isClusterDown {
				powerOnHostParallel(poweredOffHostIps)
			}
		}()

		//Bring down all hosts from vSAN max fault domain-2
		poweredOffFdHostIps = powerOffHostsFromFaultDomain(ctx, vsanMaxFdName, fdMap, true, 0)
		isFaultDomainDown = true
		defer func() {
			if isFaultDomainDown {
				powerOnHostParallel(poweredOffFdHostIps)
			}
		}()

		//Check for TKG VM and STS pod status
		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		gomega.Expect(len(nodeList.Items) > 0).To(gomega.BeTrue())
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwo, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwm, replicas)
	})

	/*
		Testcase - 1 & 2
		vSAN Max with vSAN Stretch Cluster mounted to zones using HCI mesh - Block and File Volume
		With WFFC binding mode and RWO/RWM access
		Steps:
		1. Read storage class which is shared across 3 zones i.e. az1, az2 and az3
		2. Deploy statefulset with 3 replica count with access mode as RWO and RWM (Block and File volumes) using
		storage policy read in step #1
		3. Verify PV affinity, Pod node affinity and pvc annotation for all pods,pvcs created in step #2.
		4. Bring down one of the host from Az2 zone
		5. Bring down one of the host from primary site
		6. Verify sts pods and replicas
		6. Bring up host from zone-2 and host from primary site
		5. Bring down 2 hosts from Az2 zone
		6. Bring down all hosts from primary site
		Verify sts pods and replicas
		7. Bring up all hosts from Az2 and all hosts from primary site
	*/

	ginkgo.It("vSAN Max on vSAN Stretch domain isolation with "+
		"HCI mounted datastore", ginkgo.Label(p0, wldi, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// statefulset replica count
		replicas = 3

		// Flag to check the status of hosts
		isHostDown := false
		isClusterDown := false

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

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, statefulsetRwm, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down one host from zone2")
		zone2 := topologyAffinityDetails[topologyCategories[0]][1]
		poweredOffHostIps := powerOffHostsFromZone(ctx, zone2, false, 1)
		isHostDown = true
		defer func() {
			if isHostDown {
				powerOnHostParallel(poweredOffHostIps)
			}
		}()

		ginkgo.By("Bring down one host in primary site")
		// this will randomly fetch 1 esxi hosts from the primary site hostlist
		rand.New(rand.NewSource(time.Now().UnixNano()))
		max, min := 3, 0
		randomValue := rand.Intn(max-min) + min
		host := fds.primarySiteHosts[randomValue]
		hostFailure(ctx, host, true)
		defer func() {
			ginkgo.By("Bring up host in primary site")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				hostFailure(ctx, host, false)
				fds.hostsDown = nil
			}
		}()

		// Check for TKG VM and STS pod status
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		gomega.Expect(len(nodeList.Items) == 6).To(gomega.BeTrue())
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwo, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwm, replicas)

		// Bring up host from Az2 and host from primary site
		ginkgo.By("Bring up host in Az2")
		powerOnHostParallel(poweredOffHostIps)
		isHostDown = false

		ginkgo.By("Bring up host in primary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			hostFailure(ctx, host, false)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down all host in Az2")
		poweredOffHostIps = powerOffHostsFromZone(ctx, zone2, true, 0)
		isClusterDown = true
		defer func() {
			if isClusterDown {
				powerOnHostParallel(poweredOffHostIps)
			}
		}()

		ginkgo.By("Bring down full primary site")
		siteFailover(ctx, true)
		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}
		}()

		//Check for TKG VM and STS pod status
		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		gomega.Expect(len(nodeList.Items) > 0).To(gomega.BeTrue())
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwo, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulsetRwm, replicas)
	})
})
