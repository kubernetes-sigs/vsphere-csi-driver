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
	"net"
	"strings"

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
		// storagePolicyName       string
		client                  clientset.Interface
		namespace               string
		remoteStoragePolicyName string
		scParameters            map[string]string
		migratedVms             []vim25types.ManagedObjectReference
		vmknic4VsanDown         bool
		nicMgr                  *object.HostVirtualNicManager
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		remoteStoragePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForHCIRemoteDatastores)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for _, vm := range migratedVms {
			e2eVSphere.svmotionVM2DiffDs(ctx, object.NewVirtualMachine(e2eVSphere.Client.Client, vm.Reference()),
				GetAndExpectStringEnvVar(envRemoteHCIDsUrl))
		}
		if vmknic4VsanDown {
			ginkgo.By("enable vsan network on the host's vmknic in cluster4")
			err := nicMgr.SelectVnic(ctx, "vsan", GetAndExpectStringEnvVar(envVmknic4Vsan))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmknic4VsanDown = false
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
	ginkgo.It("relocate vm between local and remote ds", ginkgo.Label(p0, block, vanilla, hci), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("create a SC which points to remote vsan ds")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = remoteStoragePolicyName
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
			pvc, err := createPVC(client, namespace, nil, "", remoteSc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvc)
			pvclaims2d = append(pvclaims2d, []*v1.PersistentVolumeClaim{pvc})
		}

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvcs {
				err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[i].Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("storage vmotion workers to local vsan datastore")
		workervms := getWorkerVmMos(ctx, client)
		for _, vm := range workervms {
			e2eVSphere.svmotionVM2DiffDs(ctx, object.NewVirtualMachine(e2eVSphere.Client.Client, vm.Reference()),
				GetAndExpectStringEnvVar(envSharedDatastoreURL))
			migratedVms = append(migratedVms, vm)
		}

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		ginkgo.By("storage vmotion workers back to remote datastore")
		for _, vm := range workervms {
			e2eVSphere.svmotionVM2DiffDs(ctx, object.NewVirtualMachine(e2eVSphere.Client.Client, vm.Reference()),
				GetAndExpectStringEnvVar(envRemoteHCIDsUrl))
			migratedVms = append(migratedVms[:0], migratedVms[1:]...)
		}

		ginkgo.By("verify that volumes are accessible for all the pods that belong to remote workers")
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
	ginkgo.It("vsan partition in remote cluster", ginkgo.Label(p0, block, vanilla, hci), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error

		storageClassName := "nginx-sc"
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = remoteStoragePolicyName
		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("create a sts with 3 replicas")
		statefulset := GetStatefulSetFromManifest(namespace)
		framework.Logf("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

		ginkgo.By("disable vsan network on one the host's vmknic in cluster4")
		workervms := getWorkerVmMos(ctx, client)
		targetHost := e2eVSphere.getHostFromVMReference(ctx, workervms[0].Reference())
		targetHostSystem := object.NewHostSystem(e2eVSphere.Client.Client, targetHost.Reference())
		nicMgr, err = targetHostSystem.ConfigManager().VirtualNicManager(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmknic4VsanDown = true
		err = nicMgr.DeselectVnic(ctx, "vsan", GetAndExpectStringEnvVar(envVmknic4Vsan))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("enable vsan network on the host's vmknic in cluster4")
			err = nicMgr.SelectVnic(ctx, "vsan", GetAndExpectStringEnvVar(envVmknic4Vsan))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmknic4VsanDown = false
		}()

		ginkgo.By("verify PVs are accessible")
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

		ginkgo.By("perform sts scale up and down and verify they are successful")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset, replicas+1, true, true)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPods := fss.GetPodList(client, statefulset)
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset, ssPods, replicas-2, true, true)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
	})

})

func getWorkerVmMos(ctx context.Context, client clientset.Interface) []vim25types.ManagedObjectReference {
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	vmIp2MoRefMap := vmIpToMoRefMap(ctx)

	workervms := []vim25types.ManagedObjectReference{}
	for _, node := range nodes.Items {
		cpvm := false
		for label := range node.Labels {
			if label == "node-role.kubernetes.io/control-plane" {
				cpvm = true
			}
		}
		if !cpvm {
			addrs := node.Status.Addresses
			externalIpFound := false
			ip := ""
			for _, addr := range addrs {
				if addr.Type == v1.NodeExternalIP && (net.ParseIP(addr.Address)).To4() != nil {
					externalIpFound = true
					ip = addr.Address
					break
				}
			}
			if !externalIpFound {
				for _, addr := range addrs {
					if addr.Type == v1.NodeInternalIP && (net.ParseIP(addr.Address)).To4() != nil {
						ip = addr.Address
						break
					}
				}
			}
			gomega.Expect(ip).NotTo(gomega.BeEmpty())
			workervms = append(workervms, vmIp2MoRefMap[ip])
		}
	}
	return workervms
}

func vmIpToMoRefMap(ctx context.Context) map[string]vim25types.ManagedObjectReference {
	vmIp2MoMap := make(map[string]vim25types.ManagedObjectReference)
	vmObjs := e2eVSphere.getAllVms(ctx)
	for _, mo := range vmObjs {
		if !strings.Contains(mo.Name(), "k8s") {
			continue
		}
		ip, err := mo.WaitForIP(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ip).NotTo(gomega.BeEmpty())
		vmIp2MoMap[ip] = mo.Reference()
		framework.Logf("VM with IP %s is named %s and its moid is %s", ip, mo.Name(), mo.Reference().Value)
	}
	return vmIp2MoMap
}
