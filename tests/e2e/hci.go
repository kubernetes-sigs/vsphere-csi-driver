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
	"strings"

	"github.com/vmware/govmomi/find"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	admissionapi "k8s.io/pod-security-admission/api"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

var _ bool = ginkgo.Describe("hci", func() {
	f := framework.NewDefaultFramework("hci")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                  clientset.Interface
		namespace               string
		remoteStoragePolicyName string
		scParameters            map[string]string
		migratedVms             []vim25types.ManagedObjectReference
		vmknic4VsanDown         bool
		nicMgr                  *object.HostVirtualNicManager
		isHostInMaintenanceMode bool
		targetHostSystem        *object.HostSystem
		isTargetHostPoweredOff  bool
		targetHostSystemName    string
		remoteDsUrl             string
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		remoteDsUrl = GetAndExpectStringEnvVar(envRemoteHCIDsUrl)
		scParameters = make(map[string]string)
		targetHostSystemName = ""
		remoteStoragePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForHCIRemoteDatastores)
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))
		err = waitForAllNodes2BeReady(ctx, client)
		framework.ExpectNoError(err, "cluster not completely healthy")

		for _, vm := range migratedVms {
			e2eVSphere.svmotionVM2DiffDs(ctx, object.NewVirtualMachine(e2eVSphere.Client.Client, vm.Reference()),
				remoteDsUrl)
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for _, vm := range migratedVms {
			e2eVSphere.svmotionVM2DiffDs(ctx, object.NewVirtualMachine(e2eVSphere.Client.Client, vm.Reference()),
				remoteDsUrl)
		}
		if vmknic4VsanDown {
			ginkgo.By("Enable vsan network on the host's vmknic in remote cluster")
			err := nicMgr.SelectVnic(ctx, vsanLabel, GetAndExpectStringEnvVar(envVmknic4Vsan))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmknic4VsanDown = false
		}
		if isHostInMaintenanceMode {
			ginkgo.By("Exit host from maintenance mode in remote cluster")
			exitHostMM(ctx, targetHostSystem, mmStateChangeTimeout)
			isHostInMaintenanceMode = false
		}
		if isTargetHostPoweredOff {
			ginkgo.By("Power on the host used in step 3")
			err := vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, targetHostSystemName, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isTargetHostPoweredOff = false
		}
	})

	/*
	   relocate vm between local and remote ds
	   Steps:
	   1	create a SC which points to remote vsan ds
	   2	create 5 pvcs each on remote vsan ds and wait for them to be bound
	   3	attach a pod to each of the PVCs created in step 2
	   4	wait for all pods to be running and verify that the respective pvcs are accessible
	   5	storage vmotion remote workers to local datastore
	   6	verify that volumes are accessible for all the pods
	   7	storage vmotion remote workers to back to remote datastore
	   8	verify that volumes are accessible for all the pods
	   9	cleanup all the pods, pvcs and SCs created for the test
	*/
	ginkgo.It("Relocate vm between local and remote ds", ginkgo.Label(p0, block, vanilla, hci), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Create a SC which points to remote vsan ds")
		scParameters = map[string]string{}
		scParameters[scParamStoragePolicyName] = remoteStoragePolicyName
		storageClassName := "remote"
		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		remoteSc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, remoteSc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvcs := []*v1.PersistentVolumeClaim{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		ginkgo.By("create 5 pvcs each on remote vsan ds")
		for i := 0; i < 5; i++ {
			pvc, err := createPVC(ctx, client, namespace, nil, "", remoteSc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvc)
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc})
		}

		ginkgo.By("Wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvcs {
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[i].Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("Wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("Storage vmotion workers to local vsan datastore")
		workervms := getWorkerVmMoRefs(ctx, client)
		for _, vm := range workervms {
			e2eVSphere.svmotionVM2DiffDs(ctx, object.NewVirtualMachine(e2eVSphere.Client.Client, vm.Reference()),
				GetAndExpectStringEnvVar(envSharedDatastoreURL))
			migratedVms = append(migratedVms, vm)
		}

		ginkgo.By("Verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		ginkgo.By("Storage vmotion workers back to remote datastore")
		for _, vm := range workervms {
			e2eVSphere.svmotionVM2DiffDs(ctx, object.NewVirtualMachine(e2eVSphere.Client.Client, vm.Reference()),
				GetAndExpectStringEnvVar(envRemoteHCIDsUrl))
			migratedVms = append(migratedVms[:0], migratedVms[1:]...)
		}

		ginkgo.By("Verify that volumes are accessible for all the pods that belong to remote workers")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

	})

	/*
		vsan partition in remote cluster
		Steps:
		1	Create an environment with 4 VC clusters with cluster 4 as vsan remote host for the other 3 clusters, and
		    create a sts with 3 replicas
		2	disable vsan network on one the hosts in cluster4
		3	there should not be any visible impact to the replicas and PVs should be accessible (but they will be become
			non-compliant, since hftt=1)
		4	perform sts scale up and down and verify they are successful
		5	write some data to one of the PVs from statefulset and read I/O back and verify its integrity
		6	undo the changes done in step2
		7	verify the PVs are accessible and compliant now
		8	perform sts scale up and down and verify they are successful
		9	write some data to one of the PVs from statefulset and read I/O back and verify its integrity
		10	cleanup all objects created during the test
	*/
	ginkgo.It("Vsan partition in remote cluster", ginkgo.Label(p0, block, vanilla, hci), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error

		scParameters = map[string]string{}
		scParameters[scParamStoragePolicyName] = remoteStoragePolicyName
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("create a sts with 3 replicas")
		var replicas int32 = 3
		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false, false, replicas, "", "")
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

		ginkgo.By("Disable vsan network on one the host's vmknic in remote cluster")
		workervms := getWorkerVmMoRefs(ctx, client)
		targetHost := e2eVSphere.getHostFromVMReference(ctx, workervms[0].Reference())
		hostMoRef := vim25types.ManagedObjectReference{Type: "HostSystem", Value: targetHost.Value}
		targetHostSystem = object.NewHostSystem(e2eVSphere.Client.Client, hostMoRef)
		framework.Logf("Target host name: %s, MOID: %s", targetHostSystem.Name, targetHost.Value)
		nicMgr, err = targetHostSystem.ConfigManager().VirtualNicManager(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmknic4VsanDown = true
		err = nicMgr.DeselectVnic(ctx, vsanLabel, GetAndExpectStringEnvVar(envVmknic4Vsan))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vmknic4VsanDown {
				ginkgo.By("enable vsan network on the host's vmknic in remote cluster")
				err = nicMgr.SelectVnic(ctx, vsanLabel, GetAndExpectStringEnvVar(envVmknic4Vsan))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				vmknic4VsanDown = false
			}
		}()

		ginkgo.By("Verify PVs are accessible")
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

		ginkgo.By("Perform sts scale up and down and verify they are successful")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset, replicas+1, true, true)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := fss.GetPodList(ctx, client, statefulset)
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset, ssPods, replicas-1, true, true)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enable vsan network on the host's vmknic in remote cluster")
		err = nicMgr.SelectVnic(ctx, vsanLabel, GetAndExpectStringEnvVar(envVmknic4Vsan))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmknic4VsanDown = false

		ginkgo.By("Perform sts scale up and down and verify they are successful")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset, replicas+2, true, true)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods = fss.GetPodList(ctx, client, statefulset)
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset, ssPods, replicas-2, true, true)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

	})

	/*
		Put esx node on remote cluster in and out of maintenance mode
		steps:
		1.	Create an environment as described in the testbed layout above
		2.	put one of the esx host from the remote cluster in maintenance mode with ensureAccessibility
		3.	after 5 minutes sts replicas up and running and the PVs are accessible
		4.	remove the esx node used in step2 from maintenance mode
		5.	perform sts scale up and down and verify they are successful
		6.	cleanup all objects created during the test
	*/
	ginkgo.It("Put esx node on remote cluster in and out of maintenance mode", ginkgo.Label(
		p0, block, vanilla, hci), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error

		scParameters = map[string]string{}
		scParameters[scParamStoragePolicyName] = remoteStoragePolicyName
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a sts with 3 replicas")
		var replicas int32 = 3
		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false, false, replicas, "", "")

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		ginkgo.By("Put one of the esx host from the remote cluster in maintenance mode with ensureAccessibility")
		workervms := getWorkerVmMoRefs(ctx, client)
		framework.Logf("workervms: %v", workervms)
		targetHost := e2eVSphere.getHostFromVMReference(ctx, workervms[0].Reference())
		hostMoRef := vim25types.ManagedObjectReference{Type: "HostSystem", Value: targetHost.Value}
		targetHostSystem = object.NewHostSystem(e2eVSphere.Client.Client, hostMoRef)
		framework.Logf("target host name: %s, MOID: %s", targetHostSystem.Name(), targetHost.Value)
		enterHostIntoMM(ctx, targetHostSystem, ensureAccessibilityMModeType, mmStateChangeTimeout, true)
		isHostInMaintenanceMode = true
		defer func() {
			if isHostInMaintenanceMode {
				ginkgo.By("Exit host from maintenance mode in remote cluster")
				exitHostMM(ctx, targetHostSystem, mmStateChangeTimeout)
				isHostInMaintenanceMode = false
			}
		}()

		ginkgo.By("Verify after 5 minutes sts are replicas up")
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)

		ginkgo.By("Verify PVs are accessible")
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

		ginkgo.By("Exit host from maintenance mode in remote cluster")
		if isHostInMaintenanceMode {
			exitHostMM(ctx, targetHostSystem, mmStateChangeTimeout)
			isHostInMaintenanceMode = false
		}

		ginkgo.By("Perform sts scale up and down and verify they are successful")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset, replicas+1, true, true)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := fss.GetPodList(ctx, client, statefulset)
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset, ssPods, replicas-1, true, true)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
	})

	/*
		A host down in one AZ
		steps:
		1	Create an environment as described in the testbed layout above
		2	Create statefulset2 with 1 replica
		3	power off a host which has a k8s-worker with attached PVs in cluster1
		4	wait for 5-10 mins, verify that the k8s-worker is restarted and brought up on another host
		5	scale up statefulset2 and scale down statefulset1 to 2 replicas
		6	power on the host used in step 3
		7	reverse the operations done in step 5 and verify they are successful
		8	cleanup all objects created during the test
	*/
	ginkgo.It("A host down in one AZ", ginkgo.Label(p0, block, vanilla, hci), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		var replicas1 int32 = 3
		var replicas2 int32 = 1
		hostIp := ""

		scParameters = map[string]string{}
		scParameters[scParamStoragePolicyName] = remoteStoragePolicyName
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create 2 sts with 3 replica and 1 replica respectively")
		sts1 := createCustomisedStatefulSets(ctx, client, namespace, false, replicas1,
			false, nil, false, true, "", "", sc, "")
		sts2 := createCustomisedStatefulSets(ctx, client, namespace, false, replicas2,
			false, nil, false, true, "web-nginx", "", sc, "")
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}()

		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods1 := fss.GetPodList(ctx, client, sts1)
		pods2 := fss.GetPodList(ctx, client, sts2)
		workersPodMap := make(map[string]int)
		targetWorkerName := ""
		max := 0
		for _, pod := range append(pods1.Items, pods2.Items...) {
			workersPodMap[pod.Spec.NodeName] += 1
			if workersPodMap[pod.Spec.NodeName] > max {
				targetWorkerName = pod.Spec.NodeName
			}
		}

		ginkgo.By("Power off a host which has a k8s-worker with attached PVs in cluster1")
		workerNode, err := client.CoreV1().Nodes().Get(ctx, targetWorkerName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		targetHost := e2eVSphere.getHostFromVMReference(ctx, getHostMoref4K8sNode(
			ctx, client, workerNode))
		hostMoRef := vim25types.ManagedObjectReference{Type: "HostSystem", Value: targetHost.Value}
		targetHostSystem = object.NewHostSystem(e2eVSphere.Client.Client, hostMoRef)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		var datacenters []string
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters,
			",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		for _, dc := range datacenters {
			defDc, err := finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defDc)
			hosts, err := finder.HostSystemList(ctx, "*")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, host := range hosts {
				if host.Reference().Reference().Value == targetHost.Value {
					hostInfo := host.Common.InventoryPath
					hostIpInfo := strings.Split(hostInfo, "/")
					hostIp = hostIpInfo[len(hostIpInfo)-1]
					targetHostSystemName = host.Name()
					break
				}
			}
			if targetHostSystemName != "" {
				break
			}
		}
		framework.Logf("Target host name: %s, MOID: %s", targetHostSystemName, targetHost.Value)

		for _, esxInfo := range tbinfo.esxHosts {
			if hostIp == esxInfo["ip"] {
				targetHostSystemName = esxInfo["vmName"]
				err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, targetHostSystemName, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isTargetHostPoweredOff = true
			}
		}

		defer func() {
			if isTargetHostPoweredOff {
				ginkgo.By("Power on the host used in step 3")
				err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, targetHostSystemName, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isTargetHostPoweredOff = false
			}
		}()

		ginkgo.By("Wait for 5-10 mins, verify that the k8s-worker is restarted and brought up on another host")
		wait4AllK8sNodesToBeUp(ctx, client, nodes)
		gomega.Expect(waitForAllNodes2BeReady(ctx, client)).To(gomega.Succeed())

		ginkgo.By("Scale up statefulset2 and scale down statefulset1 to 2 replicas")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, sts2, replicas2+1, true, true)
		gomega.Expect(fss.CheckMount(ctx, client, sts2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods1 := fss.GetPodList(ctx, client, sts1)
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, sts1, ssPods1, replicas1-1, true, true)
		gomega.Expect(fss.CheckMount(ctx, client, sts1, mountPath)).NotTo(gomega.HaveOccurred())

		ginkgo.By("Power on the host used in step 3")
		if isTargetHostPoweredOff {
			err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, targetHostSystemName, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isTargetHostPoweredOff = false
		}

		ginkgo.By("Scale up statefulset2 to 3 replicas and scale down statefulset1 to 1 replica")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, sts2, replicas2+2, true, true)
		gomega.Expect(fss.CheckMount(ctx, client, sts2, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods1 = fss.GetPodList(ctx, client, sts1)
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, sts1, ssPods1, replicas1-2, true, true)
		gomega.Expect(fss.CheckMount(ctx, client, sts1, mountPath)).NotTo(gomega.HaveOccurred())

	})

})
