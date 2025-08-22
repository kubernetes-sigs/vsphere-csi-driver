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
	"strconv"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ bool = ginkgo.Describe("[domain-isolation-negative] Management-Workload-Domain-Isolation-Negative", func() {

	f := framework.NewDefaultFramework("domain-isolation-negative")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                     clientset.Interface
		namespace                  string
		vcRestSessionId            string
		allowedTopologies          []v1.TopologySelectorLabelRequirement
		topkeyStartIndex           int
		topologyAffinityDetails    map[string][]string
		topologyCategories         []string
		labelsMap                  map[string]string
		labels_ns                  map[string]string
		err                        error
		zone1                      string
		zone2                      string
		sharedStoragePolicyName    string
		statuscode                 int
		replicas                   int32
		isVsanHealthServiceStopped bool
		isWcpServicestopped        bool
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

		// reading topology map set for management doamin and workload domain
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

		// reading shared storage policy
		sharedStoragePolicyName = GetAndExpectStringEnvVar(envIsolationSharedStoragePolicyName)
		if sharedStoragePolicyName == "" {
			ginkgo.Skip("Skipping the test because WORKLOAD_ISOLATION_SHARED_STORAGE_POLICY is not set")
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if isVsanHealthServiceStopped {
			framework.Logf("Bringing vsanhealth up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}

		if isWcpServicestopped {
			ginkgo.By(fmt.Sprintln("Starting WCP on the vCenter host"))
			err := invokeVCenterServiceControl(ctx, startOperation, wcpServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

	/*
		Testcase 7
		Verify workloads with rename and vsan-Health down.
		At the same time on the zonal/shared datastore with both type of zone(marked for delete and normal)

		Test steps:
		1. Create a wcp namespace and tag zone-1, zone-2 to it and add 2 different zonal policy each from zone1 and zone2.
		2. Create volumes, pods, statefulset
		3. Verify volume and pod status
		4. Mark zone-1 for removal
		5. Perform vsan-health down and once service is down perform rename of datastores of zone1 and zone2
		6. Bring-up vsan health service.
		7. Perform scaling operation.
		8. Perform restore operation.
		9. Create new volumes which Imm and WFFC binding mode and attach it to deployment pods.
		10. Verify pv affinity and pvc annotation
		11. Perform cleanup
	*/
	ginkgo.It("Verify workloads with vsan-health down", ginkgo.Label(p0, wldi, snapshot, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Read zonal policy of zone-1 & zone-2
		storagePolicyNameZ1 := GetAndExpectStringEnvVar(envZonal1StoragePolicyName)
		storageProfileIdZ1 := e2eVSphere.GetSpbmPolicyID(storagePolicyNameZ1)
		storagePolicyNameZ2 := GetAndExpectStringEnvVar(envZonal2StoragePolicyName)
		storageProfileIdZ2 := e2eVSphere.GetSpbmPolicyID(storagePolicyNameZ2)

		replicas = 3

		ginkgo.By("Creating wcp namespace tagged to any 2 zones")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(
			vcRestSessionId,
			[]string{storageProfileIdZ1, storageProfileIdZ2},
			getSvcId(vcRestSessionId, &e2eVSphere), []string{zone1, zone2}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read zonal class")
		storageclassZ1, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyNameZ1, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		storageclassZ2, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyNameZ2, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating pvc")
		pvclaim1, _, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclassZ1, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to reach Bound state.")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create Pod to attach to Pvc-1")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		// startIndex=0 & endIndex=1 to set allowedTopologies to zone-1
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, 0,
			1)
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, pod, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating service")
		_ = CreateService(namespace, client)

		ginkgo.By("Creating statefulset")
		sts := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, false, nil,
			false, true, "", "", storageclassZ2, storagePolicyNameZ2)
		// startIndex=1 & endIndex=2 to set allowedTopologies to zone-2
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, 1,
			2)
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, sts, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Mark zone-2 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace,
			zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// get ds name
		pv := getPvFromClaim(client, namespace, pvclaim1.Name)
		volHandle := pv.Spec.CSI.VolumeHandle
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		randomStr := strconv.Itoa(r1.Intn(1000))
		datastoreName, dsRef, _ := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)

		ginkgo.By("Stop Vsan-health service")
		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if isVsanHealthServiceStopped {
				ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
				err := invokeVCenterServiceControl(ctx, startOperation, vsanhealthServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
				time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
			}
		}()

		ginkgo.By("Rename datastore to a new name")
		framework.Logf("Original datastore name: %s", datastoreName)
		e2eVSphere.renameDs(ctx, datastoreName+randomStr, &dsRef)
		defer func() {
			framework.Logf("Renaming datastore back to original name")
			e2eVSphere.renameDs(ctx, datastoreName, &dsRef)
		}()

		ginkgo.By("Start Vsan-health service on the vCenter host")
		err = invokeVCenterServiceControl(ctx, startOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		isVsanHealthServiceStopped = false

		ginkgo.By("Perform scaling operation on statefulset. Increase the replica count to 4 when zone is marked for removal")
		ginkgo.By("Scaleup any one StatefulSets replica")
		_, scaleupErr := scaleStatefulSetPods(client, sts, 4)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())

		pods, err := fss.GetPodList(ctx, client, sts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pod := range pods.Items {
			if pod.Name == "web-3" {
				if pod.Status.Phase != v1.PodPending {
					framework.Failf("Expected pod to be in: %s state but is in: %s state", v1.PodPending,
						pod.Status.Phase)
				}
			}
		}
	})

	/*
		Testcase 8
		Create STS/Deployment with large disk, mark the zone for delete before the creation is completed and verify

		Test steps:
		1. Create a wcp ns with zone1, zone2 added to it and storage policy added is of zone2 only.
		2. Create volumes, pods, statefulset with large thick disk
		3. Verify volume and pod status
		4. Mark zone-2 for removal when workload creation is in progress
		5. Verify volume and pod status
		6. Now delete deployment pods, statefulset pods from the NS
		7. Verify if pods gets created automatically through replica controller.
		8. If pods gets created automatically, proceed for test cleanup
		9. If it fails, verify the error message.
		10. Perfrom cleanup
	*/
	ginkgo.It("Large disk with zone removal", ginkgo.Label(p0, wldi, snapshot, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Read zonal policy of zone-2
		storagePolicyNameZ2 := GetAndExpectStringEnvVar(envZonal2StoragePolicyName)
		storageProfileIdZ2 := e2eVSphere.GetSpbmPolicyID(storagePolicyNameZ2)

		replicas = 3

		ginkgo.By("Creating wcp namespace tagged to any 2 zones")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(
			vcRestSessionId,
			[]string{storageProfileIdZ2},
			getSvcId(vcRestSessionId, &e2eVSphere), []string{zone1, zone2}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read zonal class")
		storageclassZ2, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyNameZ2, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating pvc")
		pvclaim1, _, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSizeLarge, storageclassZ2, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating service")
		_ = CreateService(namespace, client)

		ginkgo.By("Creating statefulset")
		sts := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, false, nil,
			false, true, "", "", storageclassZ2, storagePolicyNameZ2)

		ginkgo.By("Mark zone-2 for removal from wcp namespace")
		err = markZoneForRemovalFromWcpNs(vcRestSessionId, namespace,
			zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to reach Bound state.")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// startIndex=1 & endIndex=2 to set allowedTopologies to zone-2
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, 1,
			2)
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, sts, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform scaling operation on statefulset. Increase the replica count to 4 when zone is marked for removal")
		ginkgo.By("Scaleup any one StatefulSets replica")
		_, scaleupErr := scaleStatefulSetPods(client, sts, 4)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())

		// Validate pods are in Pending state
		pods, err := fss.GetPodList(ctx, client, sts)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pod := range pods.Items {
			if pod.Name == "web-3" {
				if pod.Status.Phase != v1.PodPending {
					framework.Failf("Expected pod to be in: %s state but is in: %s state", v1.PodPending,
						pod.Status.Phase)
				}
			}
		}
	})

	/*
		Testcase 10
		Mark zone for removal when WCP is down, create new STS/Deployment/VMService on the same zone before recovering WCP

		Test steps:
		1. Create a wcp namespace and add zone-2 and zone-3 to it.
		Assign shared storage policy, zonal2 and zonal3 policy to this ns
		2. Create volumes, pods and statefulsets.
		3. Verify volumes created successfully and Pods reach running state.
		4. Perform wcp service down and at the same time mark zone2 for removal.
		5. Perform scaling operation when zone2 is marked for removal and wcp service is down.
		6. Now create new pvc with requested topology of the zone2 which is marked for removal
		7. Volume creation should fail with an appropriate error message
		8. Create new volumes when wcp service is down and zone2 is marked for removal
		9. Create deployment, VM Service VM and attach it to the volume created in step #6
		10. Expecting that all these workloads should get created when wcp service is down.
		11. Now recover wcp.
		12. Validate the data
		13. If workload creation fails, valid the error.
		14. If it passes, perform cleanup.
	*/
	ginkgo.It("Workload creation when WCP is down", ginkgo.Label(p0, wldi, snapshot, vc90), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Read zonal policy of zone-1, zone-2 and shared storage policy
		storagePolicyNameZ1 := GetAndExpectStringEnvVar(envZonal1StoragePolicyName)
		storageProfileIdZ1 := e2eVSphere.GetSpbmPolicyID(storagePolicyNameZ1)
		storagePolicyNameZ2 := GetAndExpectStringEnvVar(envZonal2StoragePolicyName)
		storageProfileIdZ2 := e2eVSphere.GetSpbmPolicyID(storagePolicyNameZ2)
		sharedProfileId := e2eVSphere.GetSpbmPolicyID(sharedStoragePolicyName)

		replicas = 3

		ginkgo.By("Creating wcp namespace tagged to any 2 zones")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(
			vcRestSessionId,
			[]string{storageProfileIdZ2, storageProfileIdZ1, sharedProfileId},
			getSvcId(vcRestSessionId, &e2eVSphere), []string{zone2, zone1}, "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read storage class")
		storageclassZ1, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyNameZ1, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		storageclassZ2, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyNameZ2, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		sharedStorageClass, err := client.StorageV1().StorageClasses().Get(ctx, sharedStoragePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating pvc")
		pvclaim1, _, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclassZ1, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, _, err = createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclassZ2, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating service")
		_ = CreateService(namespace, client)

		ginkgo.By("Creating statefulset")
		sts := createCustomisedStatefulSets(ctx, client, namespace, true, replicas, false, nil,
			false, true, "", "", sharedStorageClass, storagePolicyNameZ2)

		ginkgo.By("Perform wcp service down and at the same time mark zone2 for removal")
		expectedStatusCodes := []int{status_code_failure}
		var wg sync.WaitGroup
		errChan := make(chan error)
		wg.Add(2)
		go markZoneForRemovalFromWcpNsWithWg(vcRestSessionId, namespace,
			zone2, expectedStatusCodes, &wg)
		go stopServiceWithWg(ctx, vcAddress, wcpServiceName, &wg, errChan)
		isWcpServicestopped = true
		wg.Wait()
		gomega.Expect(errChan).NotTo(gomega.HaveOccurred())
		defer func() {
			if isWcpServicestopped {
				ginkgo.By(fmt.Sprintln("Starting WCP on the vCenter host"))
				err := invokeVCenterServiceControl(ctx, startOperation, wcpServiceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Perform a scaling operation on the StatefulSet, increasing the replica count to 4.")
		err = performScalingOnStatefulSetAndVerifyPvNodeAffinity(ctx, client,
			4, 0, sts, true, namespace, allowedTopologies, true, false, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, sts, nil, nil, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create new pvc with requested topology of the zone2 which is marked for removal")
		pvc3, err := createPvcWithRequestedTopology(ctx, client, namespace, nil, "", storageclassZ2, "", zone2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpv.WaitForPersistentVolumeClaimPhase(ctx,
			v1.ClaimPending, client, pvc3.Namespace, pvc3.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		expectedErrMsg := "failed to provision volume with StorageClass"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		errorOccurred := checkEventsforError(client, pvc3.Namespace,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvc3.Name)}, expectedErrMsg)
		gomega.Expect(errorOccurred).To(gomega.BeTrue())

		ginkgo.By("Creating a Deployment")
		dep, err := createDeployment(ctx, client, 1, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim1}, execRWXCommandPod1, false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify svc pv affinity, pvc annotation and pod node affinity")
		// startIndex=0 & endIndex=1 to set allowedTopologies to zone-1
		allowedTopologies = setSpecificAllowedTopology(allowedTopologies, topkeyStartIndex, 0,
			1)
		err = verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx, client, nil, nil, dep, namespace,
			allowedTopologies)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

})
