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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ bool = ginkgo.Describe("[domain-isolation-vmsvc] Domain-Isolation-VmServiceVm", func() {

	f := framework.NewDefaultFramework("vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client             clientset.Interface
		namespace          string
		vcRestSessionId    string
		allowedTopologies  []v1.TopologySelectorLabelRequirement
		topkeyStartIndex   int
		topologyCategories []string
		labelsMap          map[string]string
		labels_ns          map[string]string
		zone2              string
		vmClass            string
		contentLibId       string
		datastoreURL       string
		//vmopC                   ctlrclient.Client
		//cnsopC                  ctlrclient.Client
		nodeList                *v1.NodeList
		topologyAffinityDetails map[string][]string
		storagePolicyNameZone2  string
		storageProfileIdZone2   string
		restConfig              *rest.Config
		err                     error
		pandoraSyncWaitTime     int
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
				dsRef.Value, &e2eVSphere)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		/* Sets up a Kubernetes client with a custom scheme, adds the vmopv1 API types to the scheme,
		and ensures that the client is properly initialized without errors */
		// vmopScheme := runtime.NewScheme()
		// gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		// vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// cnsOpScheme := runtime.NewScheme()
		// gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		// cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Get snapshot client using the rest config
		restConfig = getRestConfigClient()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

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
	   Basic test
	   Steps:
	   zonal NS → zonal DS → zonal FCD
	   az1 cluster → az1 policy → FCD created with Az1 datastore url

	   Without setting any annotation on BluePrint PVC and using az1 storage policy

	   1. Create namespace "test-ns" and assign az1 policy to NS with sufficient storage quota.
	   Note that only Az1 zone and Az1 compatible storage profile is added to test-ns NS
	   2. Create a dynamic PVC using above storage profile and in the above ns using "immediate" binding mode
	   and "DELETE" policy.
	   3. Verify the PVC annotation and PV affinity. It should list Az1 details only.
	   4. Wait for PVC to reach the Bound state
	   5. Create a VM Service VM and attach it to above created dynamic PVC
	   6. Verify VM got created only on Az1 node.
	   7. Wait for VM to get and IP and to be in a power-on state
	   8. Create a blueprint PVC using "DataSource Ref", specify CRD annotation "apiGroup:
	   vmoperator.vmware.com" and it should point to above created VM name.
	   9. PVC created from "DataSource Ref" will be stuck in a pending state.
	   10. Create a FCD using an API call using zonal datastore url and zonal storage policy
	   pointing to Az1 only
	   11. Create a CNSRegisterVolume by passing the above created "FCD id" and "PVC name" created in step #11
	   12. Verify static volume created successfully.
	   13. Verify newly created static PVC/PV state.
	   14. Verify static PV affinity. It should display only Az1 affinity.
	   15. Verify static PVC. No annotation should come on pvc as it was not set initially.
	   16. Perform cleanup.
	*/

	ginkgo.It("TC1", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		curtime := time.Now().Unix()
		curtimestring := strconv.FormatInt(curtime, 10)

		/*
			EX - zone -> az1, az2, az3, az4
			so topValStartIndex=1 and topValEndIndex=2 will fetch the 1st index value from topology map string
		*/
		topValStartIndex := 1
		topValEndIndex := 2

		ginkgo.By("Create wcp namespace with az2 zone and az2 zonal storage policy")
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, topValStartIndex,
			topValEndIndex)
		allowedTopologiesMap := convertToTopologyMap(allowedTopologies)
		namespace, statuscode, err := createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileIdZone2}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{zone2}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read az2 storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyNameZone2, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow namespace to get created and "+
			"storage policy and zonal tag to get added to it",
			oneMinuteWaitTimeInSeconds))
		time.Sleep(time.Duration(oneMinuteWaitTimeInSeconds) * time.Second)

		ginkgo.By("Create a PVC using az2 storage policy")
		pvc, err := createPVC(ctx, client, namespace, labelsMap, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volHandle := pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By("Refresh PVC state")
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume affinity annotation state")
		err = verifyVolumeAnnotationAffinity(pvc, pv, allowedTopologiesMap, topologyCategories)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for VM images to get listed under namespace and create VM")
		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Create VM and attach the above created dynamic PVC to it")
		// _, vm1, _, err := createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
		// 	[]*v1.PersistentVolumeClaim{pvc}, vmClass, storageclass.Name, true)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Verify attached volumes are accessible and validate data integrity")
		// err = verifyVolumeAccessibilityAndDataIntegrityOnVM(ctx, vm1, vmopC, namespace)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Verify vm affinity annotation state")
		// err = verifyVmServiceVmAnnotationAffinity(vm1, allowedTopologiesMap, nodeList)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create blueprint pvc using VM DataSourceRef and using az2 storage policy")
		blueprintPvc, err := createBluePrintPVC(ctx, client, namespace, storageclass, v1.ReadWriteOnce,
			"testvm", diskSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create FCD Disk")
		zone2datastoreURL := GetAndExpectStringEnvVar(envDatastoreUrlZone2)
		defaultDatastore := readDatastoreUrlForFcdCreation(ctx, zone2datastoreURL)
		fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
			"staticfcd"+curtimestring, storageProfileIdZone2, diskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create CNS register volume with above created FCD ")
		cnsRegisterVolume := getCNSRegisterVolumeSpecForBluePrintPvc(namespace, fcdID,
			blueprintPvc.Name, v1.ReadWriteOnce)

		err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx, restConfig,
			namespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

		ginkgo.By("Verify blueprint pvc and statically generated pv status")
		blueprintPvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, blueprintPvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		staticPv := getPvFromClaim(client, namespace, blueprintPvc.Name)
		verifyBidirectionalReferenceOfPVandPVC(ctx, client, blueprintPvc, staticPv, fcdID)

		// ginkgo.By("Create VM and attach static blueprint pvc to it")
		// _, vm2, _, err := createVmServiceVm(ctx, client, vmopC, cnsopC, namespace,
		// 	[]*v1.PersistentVolumeClaim{pvc}, vmClass, storageclass.Name, true)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Verify attached volumes are accessible and validate data integrity")
		// err = verifyVolumeAccessibilityAndDataIntegrityOnVM(ctx, vm2, vmopC, namespace)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Verify vm affinity annotation state")
		// err = verifyVmServiceVmAnnotationAffinity(vm2, allowedTopologiesMap, nodeList)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
