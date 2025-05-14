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
	"os"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

var _ bool = ginkgo.Describe("[domain-isolation-vmsvc] Domain-Isolation-VmServiceVm", func() {

	f := framework.NewDefaultFramework("vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                  clientset.Interface
		namespace               string
		vcRestSessionId         string
		allowedTopologies       []v1.TopologySelectorLabelRequirement
		topkeyStartIndex        int
		topologyCategories      []string
		labelsMap               map[string]string
		labels_ns               map[string]string
		err                     error
		zone2                   string
		statuscode              int
		vmClass                 string
		contentLibId            string
		datastoreURL            string
		vmopC                   ctlrclient.Client
		cnsopC                  ctlrclient.Client
		nodeList                *v1.NodeList
		topologyAffinityDetails map[string][]string
		storagePolicyNameZone2  string
		storageProfileIdZone2   string
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

		// fetching nodes list
		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		// reading topology map for management and workload domain
		topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
		allowedTopologies = createAllowedTopolgies(topologyMap)

		// Set namespace labels to allow privileged pod creation
		labels_ns = map[string]string{}
		labels_ns[admissionapi.EnforceLevelLabel] = string(admissionapi.LevelPrivileged)
		labels_ns["e2e-framework"] = f.BaseName

		//setting labels map on pvc
		labelsMap = make(map[string]string)
		labelsMap["app"] = "test"

		//fetching zones
		topologyAffinityDetails, topologyCategories = createTopologyMapLevel5(topologyMap)
		zone2 = topologyAffinityDetails[topologyCategories[0]][1]

		// get or set vm class required for VM creation
		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}

		// fetch shared vsphere datatsore url
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		dsRef := getDsMoRefFromURL(ctx, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		// reading zonal storage policy of zone-2 workload domain
		storagePolicyNameZone2 = GetAndExpectStringEnvVar(envZonal2StoragePolicyName)
		storageProfileIdZone2 = e2eVSphere.GetSpbmPolicyID(storagePolicyNameZone2)

		// read or create content library if it is empty
		if contentLibId == "" {
			contentLibId, err = createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
				dsRef.Value)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		/* Sets up a Kubernetes client with a custom scheme, adds the vmopv1 API types to the scheme,
		and ensures that the client is properly initialized without errors */
		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		framework.Logf("Collecting supervisor PVC events before performing PV/PVC cleanup")
		dumpSvcNsEventsOnTestFailure(client, namespace)
		eventList, err := client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, item := range eventList.Items {
			framework.Logf("%q", item.Message)
		}
	})

	/*
		Testcase-1
		Brief:
		Dynamic volume attached to Vm Service VM
		Singlezone -> zone-2 tagged to WCP namespace
		zonal storage policy compatible only with zone-2
		Immediate Binding mode

		Steps:
		1. Create a WCP namespace tagged to workload domain zone-2 with a zonal storage policy
		(compatible only with zone-2 workload domain) and assign it to the namespace.
		2. Create a PVC using the above policy. (Policy created using Immediate Binding mode)
		3. Wait for PVC to come to Bound State.
		4. Verify PVC annotation.
		5. Verify PV affinity. It should show zone-2 topology affinity details.
		6. Create a VMservice VM using PVC created in step #2.
		7. Wait for VM to get an IP and to be in a power-on state.
		8. Once the VM is up, verify that the volume is accessible inside the VM.
		9. Write some IO to the CSI volumes read it back from them and verify the data integrity
		10. Verify VM node annotation.
		11. Perform cleanup: Delete VM, PVC and Namespace.
	*/

	ginkgo.It("Volume attachment to vm using zonal policy", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var secretName string
		var vm *vmopv1.VirtualMachine
		var vmlbsvc *vmopv1.VirtualMachineService

		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4
			so topValStartIndex=1 and topValEndIndex=2 will fetch the 1st index value from topology map string
		*/
		topValStartIndex := 1
		topValEndIndex := 2

		ginkgo.By("Create a WCP namespace and tag zone-2 to it")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		allowedTopologiesMap := convertToTopologyMap(allowedTopologies)

		// creating namespace with zonal2 storage policy
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileIdZone2}, getSvcId(vcRestSessionId),
			[]string{zone2}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read zonal-2 storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyNameZone2, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow namespace to get created and "+
			"storage policy and zonal tag to get added to it",
			oneMinuteWaitTimeInSeconds))
		time.Sleep(time.Duration(oneMinuteWaitTimeInSeconds) * time.Second)

		ginkgo.By("Create a PVC using zonal-2 storage policy")
		pvc, err := createPVC(ctx, client, namespace, labelsMap, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volHandle := pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			ginkgo.By("Deleting loadbalancing service, VM and its bootstrap data")
			err = deleteVmServiceVmWithItsConfig(ctx, client, vmopC,
				vmlbsvc, namespace, vm, secretName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Delete PVC")
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Waiting for CNS volumes to be deleted")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Refresh PVC state")
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume affinity annotation state")
		err = verifyVolumeAnnotationAffinity(pvc, pv, allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vm service vm")
		secretName, vm, vmlbsvc, err = createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*v1.PersistentVolumeClaim{pvc}, vmClass, storageclass.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify vm affinity annotation state")
		err = verifyVmServiceVmAnnotationAffinity(vm, allowedTopologiesMap, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Testcase-2
		Brief:
		Static volume attached to Vm Service VM
		Multizones -> zone-2 and zone-4 tagged to WCP namespace
		storage policy compatible only with zone-2 and zone-4
		Immediate Binding mode

		Steps:
		1. Create a WCP namespace tagged to workload domain zone-2 and zone-4 with a shared storage policy
		(compatible only with zone-2 and zone-4 workload domain) and assign it to the namespace.
		2. Create FCD using the above policy.
		3. Create a static PV and PVC using cns register volume API
		4. Wait for static PV and PVC to reach the Bound state.
		5. Verify PVC annotation.
		6. Verify PV affinity. It should show zone-2 and zone-4 topology affinity details.
		7. Create a VMservice VM using the static volume created in step #3.
		8. Wait for VM to get an IP and to be in a power-on state.
		9. Once the VM is up, verify that the volume is accessible inside the VM.
		10. Write some IO to the CSI volumes and read it back from them and verify the data integrity
		11. Verify VM node annotation.
		12. Perform cleanup: Delete VM, PVC and Namespace.
	*/

	ginkgo.It("Static volume attachment to vm using shared policy", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var secretName string
		var vm *vmopv1.VirtualMachine
		var vmlbsvc *vmopv1.VirtualMachineService

		// reading shared storage policy of zone-2 and zone-4 workload domain and its datastore url
		storagePolicyNameZone24 := GetAndExpectStringEnvVar(envSharedZone2Zone4StoragePolicyName)
		storageProfileIdZone24 := e2eVSphere.GetSpbmPolicyID(storagePolicyNameZone24)
		storageDatastoreUrlZone24 := GetAndExpectStringEnvVar(envSharedZone2Zone4DatastoreUrl)

		ginkgo.By("Create a WCP namespace and tag zone-2 and zone-4 to it")
		zone4 := allowedTopologies[0].Values[3]
		allowedTopologies[0].Values = []string{zone2, zone4}
		allowedTopologiesMap := convertToTopologyMap(allowedTopologies)
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileIdZone24}, getSvcId(vcRestSessionId),
			[]string{zone2, zone4}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyNameZone24, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create static volume")
		fcdID, defaultDatastore, staticPvc, staticPv, err := createStaticVolumeOnSvc(ctx, client,
			namespace, storageDatastoreUrlZone24, storagePolicyNameZone24)
		defer func() {
			ginkgo.By("Deleting loadbalancing service, VM and its bootstrap data")
			err = deleteVmServiceVmWithItsConfig(ctx, client, vmopC,
				vmlbsvc, namespace, vm, secretName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Delete FCD")
			err := e2eVSphere.deleteFCD(ctx, fcdID, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Delete PVC")
			err = fpv.DeletePersistentVolumeClaim(ctx, client, staticPvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(staticPv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Refresh PVC state")
		staticPvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, staticPvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume affinity annotation state")
		err = verifyVolumeAnnotationAffinity(staticPvc, staticPv, allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vm service vm")
		secretName, vm, vmlbsvc, err = createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*v1.PersistentVolumeClaim{staticPvc}, vmClass, storageclass.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify vm affinity annotation state")
		err = verifyVmServiceVmAnnotationAffinity(vm, allowedTopologiesMap, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

})
