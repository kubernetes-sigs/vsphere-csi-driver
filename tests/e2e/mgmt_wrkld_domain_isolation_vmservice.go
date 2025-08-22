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

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"

	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
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
		zone1                   string
		zone2                   string
		zone3                   string
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
		snapc                   *snapclient.Clientset
		restConfig              *rest.Config
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
		zone1 = topologyAffinityDetails[topologyCategories[0]][0]
		zone2 = topologyAffinityDetails[topologyCategories[0]][1]
		zone3 = topologyAffinityDetails[topologyCategories[0]][2]

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
				dsRef.Value, &e2eVSphere)
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

		// Get snapshot client using the rest config
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
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

	ginkgo.It("Volume attachment to vm using zonal policy", ginkgo.Label(p0, wldi, vmServiceVm, vc90), func() {
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
			[]string{storageProfileIdZone2}, getSvcId(vcRestSessionId, &e2eVSphere),
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
			[]*v1.PersistentVolumeClaim{pvc}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify attached volumes are accessible and validate data integrity")
		err = verifyVolumeAccessibilityAndDataIntegrityOnVM(ctx, vm, vmopC, namespace)
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

	ginkgo.It("Static volume attachment to vm using shared policy", ginkgo.Label(p0, wldi, vmServiceVm, vc90), func() {
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
			[]string{storageProfileIdZone24}, getSvcId(vcRestSessionId, &e2eVSphere),
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
			err = e2eVSphere.deleteFCD(ctx, fcdID, defaultDatastore.Reference())
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
			[]*v1.PersistentVolumeClaim{staticPvc}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify attached volumes are accessible and validate data integrity")
		err = verifyVolumeAccessibilityAndDataIntegrityOnVM(ctx, vm, vmopC, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify vm affinity annotation state")
		err = verifyVmServiceVmAnnotationAffinity(vm, allowedTopologiesMap, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-3

	   Steps:
	   1. Create a WCP namespace tagged to management domain zone-1 and workload domain zone-2
	   with a shared storage policy (accessible to all zones) and assign it to the namespace.
	   2. Create 2 PVCs (pvc-1, pvc-2)
	   3. Wait for PVCs to come to Bound State.
	   4. Verify PVCs annotation.
	   5. Verify PV affinity. It should show all zones topology affinity details as the shared
	   storage policy is being used.
	   6. Create VM Service VM (vm-1) and attach it to pvc-1
	   7. Wait for VM to get an IP and to be in a power-on state.
	   8. Once the VM is up, verify that the volume is accessible inside the VM.
	   9. Verify VM node annotation and affinity.
	   9. Write some IO to the CSI volumes and read it back from them and verify the data integrity
	   10. Mark zone-2 for removal.
	   11. Create a new VM service VM (vm-2).
	   12. Wait for VM to get an IP and to be in a power-on state.
	   13. Modify vm-1 and vm-2 specs. Detach pvc-1 from vm-1 and attach it to vm-2
	   14. Verify vm-2 affinity and annotation. It should get created only on zone-1 now.
	   15. Create new Vm service VM (vm-3) and attach it to pvc-2
	   16. verify that pvc2 is accessible in vm3
	   17. Wait for VM to get an IP and to be in a power-on state.
	   18. Once the VM is up, verify that the volume is accessible inside the VM.
	   19. Write some IO to the CSI volumes and read it back from them and verify the data integrity
	   20. Verify VM node annotation.
	   21. Perform cleanup: Delete VM, PVC and Namespace.
	*/

	ginkgo.It("VM creations when zone is marked for removal", ginkgo.Label(p0, wldi, vmServiceVm, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var vm1, vm2, vm3 *vmopv1.VirtualMachine

		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4
			so topValStartIndex=0 and topValEndIndex=2 will fetch the 0th and 1st index value from
			topology map string
		*/
		topValStartIndex := 0
		topValEndIndex := 2

		// topology map for verifying vm node annotation and affinity
		fetchSpecificAllowedTopology := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex,
			topValStartIndex,
			topValEndIndex)
		allowedTopologiesMapForVM := convertToTopologyMap(fetchSpecificAllowedTopology)

		// reading shared storage policy which is accessible to all zones
		storagePolicyName := GetAndExpectStringEnvVar(envIsolationSharedStoragePolicyName)
		storageProfileId := e2eVSphere.GetSpbmPolicyID(storagePolicyName)

		ginkgo.By("Create a WCP namespace and tag zone-1 and zone-2 to it using shared " +
			"policy which is accessible to all zones")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1, zone2}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow namespace to get created and "+
			"storage policy and zonal tag to get added to it",
			oneMinuteWaitTimeInSeconds))
		time.Sleep(time.Duration(oneMinuteWaitTimeInSeconds) * time.Second)

		ginkgo.By("Create PVCs using shared storage policy")
		pvc1, err := createPVC(ctx, client, namespace, labelsMap, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc2, err := createPVC(ctx, client, namespace, labelsMap, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVCs to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc1, pvc2}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv1 := pvs[0]
		pv2 := pvs[1]

		ginkgo.By("Getting PVC latest state to fetch affinity and topology annotation details")
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volumes affinity annotation state")
		allowedTopologiesMap := convertToTopologyMap(allowedTopologies)
		err = verifyVolumeAnnotationAffinity(pvc1, pv1, allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = verifyVolumeAnnotationAffinity(pvc2, pv2, allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vm service vm1")
		_, vm1, _, err = createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*v1.PersistentVolumeClaim{pvc1}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify attached volumes are accessible and validate data integrity")
		err = verifyVolumeAccessibilityAndDataIntegrityOnVM(ctx, vm1, vmopC, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify vm1 affinity annotation state")
		err = verifyVmServiceVmAnnotationAffinity(vm1, allowedTopologiesMapForVM, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Mark zone-2 for removal")
		statusCode := markZoneForRemovalFromNs(namespace, zone2, vcRestSessionId)
		gomega.Expect(statusCode).To(gomega.Equal(status_code_success))

		ginkgo.By("Create vm service vm2")
		_, vm2, _, err = createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*v1.PersistentVolumeClaim{}, vmClass, storageclass.Name, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify vm2 affinity annotation state")
		allowedTopologiesMapForVM = passZonesToStayInMap(allowedTopologiesMapForVM, zone1)
		err = verifyVmServiceVmAnnotationAffinity(vm2, allowedTopologiesMapForVM, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Edit vm1 spec and remove volume attached to it")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm1.Spec.Volumes = []vmopv1.VirtualMachineVolume{}
		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Edit vm2 spec and attach volume of vm1 to vm2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm2.Spec.Volumes = append(vm2.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: pvc1.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc1.Name},
			}})
		err = vmopC.Update(ctx, vm2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Wait and verify pvc1 is attached to vm2")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvc1})).To(gomega.Succeed())

		ginkgo.By("Create vm service vm3 and attach it to a pvc2 volume")
		_, vm3, _, err = createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*v1.PersistentVolumeClaim{pvc2}, vmClass, storageclass.Name, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify vm affinity annotation state")
		vm3, err = getVmsvcVM(ctx, vmopC, vm3.Namespace, vm3.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = verifyVmServiceVmAnnotationAffinity(vm3, allowedTopologiesMapForVM, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-7
	   Test Steps:
	   1. Create a WCP namespace tagged to workload domain zones 1, 2 and 3 with a zonal storage policy
	   (compatible with zones 1,2 and 3) and assign it to the namespace.
	   2. Create 2 PVCs(pvc-1, pvc-2) using the above policy. (Policy created using Immediate Binding mode)
	   3. Verify that PVC reached Boud state.
	   4. Verify PVC annotation.
	   5. Verify PV affinity. It should show zone-1, zone-2 and zone-3 topology affinity details.
	   6. Create 2 VM Service VMs(vm-1, vm-2) and attach them to PVC. vm-1 is attach to pvc-1 and vm-2 is
	   attach to pvc-2.
	   7. Wait for VM to get an IP and to be in a power-on state.
	   8. Once the VM is up, verify that the volume is accessible inside the VM.
	   9. Write some IO to the CSI volumes read it back from them and verify the data integrity
	   10. Verify VM node annotation.
	   11. Verify CNS volume metadata.
	   12. Mark zone-2 for removal.
	   13. Take snapshots of pvc-1 and pvc-2.
	   14. Verify snapshot is taken successfully and ready to state is set to true.
	   15. Restore the snapshot to create new volumes (pvc-3, pvc-4).
	   16. Verify that new PVCs reached Boud state.
	   17. Verify PVC annotation.
	   18. Verify PV affinity. It should show zone-1, zone-4 topology affinity details.
	   19. Attach pvc-4 to vm-1 and pvc-3 to vm-2.
	   20. Verify attachment of volume to vm went successful.
	   21. Perform cleanup: Delete VM, PVC and Namespace.
	*/

	ginkgo.It("Vm Service creation with volumes and restored snapshots attached", ginkgo.Label(p0,
		wldi, vmServiceVm, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4
			so topValStartIndex=0 and topValEndIndex=3 will fetch the 0th to 2nd index value from topology map
			string
		*/
		topValStartIndex := 0
		topValEndIndex := 3

		// topology map for verifying vm node annotation and affinity
		allowedTopologies := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		allowedTopologiesMap := convertToTopologyMap(allowedTopologies)

		// reading shared storage policy which is compatible with zone-1, zone-2 and zone-3
		storagePolicyName := GetAndExpectStringEnvVar(envSharedZone1Zone2Zone3StoragePolicyName)
		storageProfileId := e2eVSphere.GetSpbmPolicyID(storagePolicyName)

		ginkgo.By("Create a WCP namespace and tag zone-1, zone-2 and zone-3 to it using shared " +
			"policy compatible with only these 3 zones")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1, zone2, zone3}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create PVCs using shared storage policy")
		pvc1, err := createPVC(ctx, client, namespace, labelsMap, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc2, err := createPVC(ctx, client, namespace, labelsMap, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVCs to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc1, pvc2}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv1 := pvs[0]
		pv2 := pvs[1]

		ginkgo.By("Getting PVC latest state to fetch affinity and topology annotation details")
		pvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volumes affinity annotation state")
		err = verifyVolumeAnnotationAffinity(pvc1, pv1, allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = verifyVolumeAnnotationAffinity(pvc2, pv2, allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vm service vm1")
		_, vm1, _, err := createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*v1.PersistentVolumeClaim{pvc1}, vmClass, storageclass.Name, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify attached volumes are accessible and validate data integrity")
		err = verifyVolumeAccessibilityAndDataIntegrityOnVM(ctx, vm1, vmopC, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify vm1 affinity annotation state")
		err = verifyVmServiceVmAnnotationAffinity(vm1, allowedTopologiesMap, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create vm service vm2")
		_, vm2, _, err := createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
			[]*v1.PersistentVolumeClaim{pvc2}, vmClass, storageclass.Name, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify vm2 affinity annotation state")
		err = verifyVmServiceVmAnnotationAffinity(vm2, allowedTopologiesMap, nodeList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Mark zone-2 for removal")
		statusCode := markZoneForRemovalFromNs(namespace, zone2, vcRestSessionId)
		gomega.Expect(statusCode).To(gomega.Equal(status_code_success))

		ginkgo.By("Read volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Take volume snapshots of both PVCs")
		volumeSnapshot1, _, _,
			_, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc1, pv1.Spec.CSI.VolumeHandle, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volumeSnapshot2, _, _,
			_, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc2, pv2.Spec.CSI.VolumeHandle, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restore sanpshot-1 and snapshot-2 to create new volumes")
		restorepvc1, restorepv1, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot1, diskSize, false)
		restorepvc2, restorepv2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot2, diskSize, false)

		ginkgo.By("Getting PVC latest state to fetch affinity and topology annotation details")
		restorepvc1, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, restorepvc1.Name,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		restorepvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, restorepvc2.Name,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volumes affinity annotation state for restored volumes")
		err = verifyVolumeAnnotationAffinity(restorepvc1, restorepv1[0], allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = verifyVolumeAnnotationAffinity(restorepvc2, restorepv2[0], allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Edit vm1 spec and attach restore volume2 to vm1 and restore volume1 to vm2")
		vm1.Spec.Volumes = append(vm1.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: restorepvc2.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: restorepvc2.Name},
			}})

		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Wait and verify restorepvc2 is attached to vm1")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc1, restorepvc2})).To(gomega.Succeed())

		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm2.Spec.Volumes = append(vm2.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: restorepvc1.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: restorepvc1.Name},
			}})

		err = vmopC.Update(ctx, vm2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Wait and verify restorepvc1 is attached to vm2")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvc2, restorepvc1})).To(gomega.Succeed())
	})

	/*
	   Testcase-8
	   PVCs created with requested topology annotation specified

	   Test Steps:
	   1. Create a WCP namespace tagged to all zones and using a shared storage policy.
	   2. Create PVC-1 with csi.vsphere.volume-requested-topology annotation on zone-1 using zonal policy with immediate
	   volume binding mode on namespace created in step #1.
	   3. Create PVC-2 with csi.vsphere.volume-requested-topology annotation on zone-2 using zonal policy with immediate
	   volume binding mode on namespace created in step #1.
	   4. Create PVC-3 with csi.vsphere.volume-requested-topology annotation on zone-3 using zonal policy with immediate
	   volume binding mode on namespace created in step #1.
	   5. Verify all PVCs reach to Boud state.
	   6. Verify PVC annotation.
	   7. Verify PVs affinity. It should show the exact affinity as mentioned in PVCs annotation during creation.
	   8. Create 3 VM Service VMs(VM-1, VM-2, VM-3) and attach them to PVCs.
	   9. VM-1 is attach to PVC-1 and VM-2 is attach to PVC-2 and VM-3 is attac to PVC-3.
	   10. Wait for VM to get an IP and to be in a power-on state.
	   11. Once the VM is up, verify that the volume is accessible inside the VM.
	   12. Write some IO to the CSI volumes read it back from them and verify the data integrity
	   13. Verify VM node annotation.
	   14. Perform cleanup: Delete VM, PVC and Namespace.
	*/

	ginkgo.It("Volume attachment to VM with requested allowed topology", ginkgo.Label(p0,
		wldi, vmServiceVm, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pvclaims []*v1.PersistentVolumeClaim
		var pvs []*v1.PersistentVolume
		createBootstrapSecret := true

		/*
			EX - zone -> zone-1, zone-2, zone-3, zone-4
			so topValStartIndex=0 and topValEndIndex=3 will fetch the 0th to 2nd index value from topology map string
		*/
		topValStartIndex := 0
		topValEndIndex := 3

		// topology map for verifying vm node annotation and affinity
		allowedTopologies := setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		allowedTopologiesMap := convertToTopologyMap(allowedTopologies)

		// reading shared storage policy which is compatible with zone-1, zone-2 and zone-3
		storagePolicyName := GetAndExpectStringEnvVar(envSharedZone1Zone2Zone3StoragePolicyName)
		storageProfileId := e2eVSphere.GetSpbmPolicyID(storagePolicyName)

		ginkgo.By("Create a WCP namespace and tag zone-1, zone-2 and zone-3 to it using shared " +
			"policy compatible with only these 3 zones")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone1, zone2, zone3}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read shared storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating pvc with requested topology annotation. Set pvc-1 to zone-1, pvc-2 to zone-2 and pvc-3 to zone-3")
		pvc1, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclass, "", zone1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Creating pvc with requested topology annotation set to zone2")
		pvc2, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclass, "", zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc3, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclass, "", zone3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims = append(pvclaims, pvc1, pvc2, pvc3)

		ginkgo.By("Wait for PVCs to be in bound state")
		pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc1, pvc2, pvc3}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())

		ginkgo.By("Verify volumes affinity annotation state")
		for i, pvc := range pvclaims {
			pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name,
				metav1.GetOptions{}) // refresh pvc state
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = verifyVolumeAnnotationAffinity(pvc, pvs[i], allowedTopologiesMap, topologyCategories)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create vm service vm for each pvc")
			if i != 1 {
				createBootstrapSecret = false
			}
			_, vm, _, err := createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
				[]*v1.PersistentVolumeClaim{pvc}, vmClass, storageclass.Name, createBootstrapSecret)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify attached volumes are accessible and validate data integrity")
			err = verifyVolumeAccessibilityAndDataIntegrityOnVM(ctx, vm, vmopC, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify vm affinity annotation state")
			err = verifyVmServiceVmAnnotationAffinity(vm, allowedTopologiesMap, nodeList)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})
})
