/*
Copyright 2024 The Kubernetes Authors.

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
	"strings"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"

	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"

	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

var _ bool = ginkgo.Describe("[vsan-stretch-vmsvc] vm service with csi vol tests", func() {

	f := framework.NewDefaultFramework("vsan-stretch-vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                     clientset.Interface
		namespace                  string
		datastoreURL               string
		storagePolicyName          string
		storageClassName           string
		storageProfileId           string
		vcRestSessionId            string
		vmi                        string
		vmClass                    string
		csiNs                      string
		vmopC                      ctlrclient.Client
		cnsopC                     ctlrclient.Client
		isVsanHealthServiceStopped bool
		isSPSserviceStopped        bool
		vcAddress                  string
		nodeList                   *v1.NodeList
		snapc                      *snapclient.Clientset
		guestClusterRestConfig     *rest.Config
		pandoraSyncWaitTime        int
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		var err error

		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		bootstrap()

		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))
		initialiseFdsVar(ctx)

		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		vcRestSessionId = createVcSession4RestApis(ctx)
		//csiNs = GetAndExpectStringEnvVar(envCSINamespace)

		storageClassName = strings.ReplaceAll(storagePolicyName, " ", "-") // since this is a wcp setup
		storageClassName = strings.ToLower(storageClassName)
		framework.Logf("storageClassName: %s", storageClassName)

		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		dsRef := getDsMoRefFromURL(ctx, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)
		contentLibId := createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
			dsRef.Value, GetAndExpectStringEnvVar(envContentLibraryUrlSslThumbprint))

		framework.Logf("Create a WCP namespace for the test")
		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}
		namespace = createTestWcpNs(
			vcRestSessionId, storageProfileId, vmClass, contentLibId, getSvcId(vcRestSessionId))

		time.Sleep(5 * time.Minute)

		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi = waitNGetVmiForImageName(ctx, vmopC, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())

		if !guestCluster {
			restConfig = getRestConfigClient()
			snapc, err = snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			guestClusterRestConfig = getRestConfigClientForGuestCluster(guestClusterRestConfig)
			snapc, err = snapclient.NewForConfig(guestClusterRestConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

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
		if isVsanHealthServiceStopped {
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}

		if isSPSserviceStopped {
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", spsServiceName))
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isSPSserviceStopped)
		}
		dumpSvcNsEventsOnTestFailure(client, namespace)
		delTestWcpNs(vcRestSessionId, namespace)
		gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
	})

	/*
		VMService - primary site down
		Steps:
		1. Create a few PVCs using the storageclass as mentioned in testbed structure.
		2. Create a VMservice VM from each PVC created in step2.
		3. Verify all PVC's metadata on CNS.
		4. Once the VMs are up verify that the volume is accessible inside the VM.
		5. Write data on volumes created.
		6. Bring down the primary site by powering off the hosts in primary site.
		7. Verify that the supervisor cluster should be in running and ready
			state after site failover.
		8. Verify that all the k8s constructs created in the test are running fine.
		9. Perform volume lifecycle actions which should work fine.
		10. Verify the data written in step 5.
		11. Bring primary site up and wait for testbed to be back to normal.
		12. Delete all objects created in this test.
	*/
	ginkgo.It("VMService - primary site down",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 5
			var err error

			ginkgo.By("Creating StorageClass")

			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				for i, pvc := range pvclaimsList {
					ginkgo.By("Delete PVCs")
					err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Waiting for CNS volumes to be deleted")
					volHandle := pvs[i].Spec.CSI.VolumeHandle
					gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creating VM bootstrap data")
			secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
			defer func() {
				ginkgo.By("Deleting VM bootstrap data")
				err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Creating VM")
			vms := createVMServiceVmWithMultiplePvcs(
				ctx, vmopC, namespace, vmClass, pvclaimsList, vmi, storageClassName, secretName)
			defer func() {
				for _, vm := range vms {
					ginkgo.By("Deleting VM")
					err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
						Name:      vm.Name,
						Namespace: namespace,
					}})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Bring down the primary site")
			siteFailover(ctx, true)

			defer func() {
				ginkgo.By("Bring up the primary site before terminating the test")
				if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
					siteRestore(true)
					fds.hostsDown = nil
				}
			}()

			ginkgo.By("Wait for k8s cluster to be healthy")
			if vanillaCluster {
				wait4AllK8sNodesToBeUp(nodeList)
			}
			if guestCluster {
				err = waitForAllNodes2BeReady(ctx, client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			time.Sleep(5 * time.Minute)
			// Check if csi pods are running fine after site failure
			ginkgo.By("Check if csi pods are running fine after site failure")
			err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, len(csipods.Items),
				time.Duration(pollTimeout*2))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			for _, vm := range vms {
				_, err := wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By("Perform volume and application lifecycle actions")
			performVolumeLifecycleActionForVmServiceVM(ctx, client, vmopC, cnsopC, vmClass, namespace, vmi, sc, secretName)

			ginkgo.By("Bring up the primary site")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			// wait for the VMs to move back
			err = waitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	/*
		Secondary site down
		Steps:
		1. Create a few PVCs using the storageclass as mentioned in testbed structure.
		2. Create a VMservice VM from each PVC created in step2.
		3. Verify all PVC's metadata on CNS.
		4. Once the VMs are up verify that the volume is accessible inside the VM.
		5. Write data on volumes created.
		6. Bring down the secondary site by powering off the hosts in secondary site.
		7. Verify that the supervisor cluster should be in running and ready state after site failover.
		8. Verify that all the k8s constructs created in the test are running fine.
		9. Perform volume lifecycle actions which should work fine.
		10.Verify the data written in step 5.
		11.Bring secondary site up and wait for testbed to be back to normal.
		12.Delete all objects created in this test.
	*/
	ginkgo.It("VMService - secondary site down",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 10
			var err error

			ginkgo.By("Get StorageClass for volume creation")

			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				for i, pvc := range pvclaimsList {
					ginkgo.By("Delete PVCs")
					err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Waiting for CNS volumes to be deleted")
					volHandle := pvs[i].Spec.CSI.VolumeHandle
					gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creating VM bootstrap data")
			secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
			defer func() {
				ginkgo.By("Deleting VM bootstrap data")
				err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Creating VM")
			vms := createVMServiceVmWithMultiplePvcs(
				ctx, vmopC, namespace, vmClass, pvclaimsList, vmi, storageClassName, secretName)
			defer func() {
				for _, vm := range vms {
					ginkgo.By("Deleting VM")
					err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
						Name:      vm.Name,
						Namespace: namespace,
					}})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Bring down the secondary site")
			siteFailover(ctx, false)

			defer func() {
				ginkgo.By("Bring up the secondary site before terminating the test")
				if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
					siteRestore(false)
					fds.hostsDown = nil
				}
			}()

			ginkgo.By("Wait for k8s cluster to be healthy")
			if vanillaCluster {
				wait4AllK8sNodesToBeUp(nodeList)
			}
			if guestCluster {
				err = waitForAllNodes2BeReady(ctx, client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			time.Sleep(5 * time.Minute)
			// Check if csi pods are running fine after site failure
			ginkgo.By("Check if csi pods are running fine after site failure")
			err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, len(csipods.Items),
				time.Duration(pollTimeout*2))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			for _, vm := range vms {
				_, err := wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By("Verify volume lifecycle actions when there is a fault induced")
			performVolumeLifecycleActionForVmServiceVM(ctx, client, vmopC, cnsopC, vmClass, namespace, vmi, sc, secretName)

			ginkgo.By("Bring up the secondary site")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(false)
				fds.hostsDown = nil
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			// wait for the VMs to move back
			err = waitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	/*
		VMService VM creation while primary site goes down¯
		Steps:
		1. Create 10 PVCS using the storageclass as mentioned in testbed structure
		   and verify that it goes to bound state.
		2. Create VMService VM with each PVC created in step1.
		3. While VMService VM creation is going on, bring down the primary site
		   by powering off the hosts in primary site in parallel.
		4. Verify that the supervisor cluster should be in running and ready state after site failover.
		5. Verify that all the PVCs created in step 2 are running fine.
		6. Perform volume lifecycle actions which should work fine.
		7. Bring primary site up and wait for testbed to be back to normal.
		8. Delete all objects created in the test.
	*/
	ginkgo.It("VMService VM creation while primary site goes down",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 9
			var vmCount = 9
			var err error
			var vms []*vmopv1.VirtualMachine

			ginkgo.By("Creating StorageClass")
			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				for i, pvc := range pvclaimsList {
					ginkgo.By("Delete PVCs")
					err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Waiting for CNS volumes to be deleted")
					volHandle := pvs[i].Spec.CSI.VolumeHandle
					gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creating VM bootstrap data")
			secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
			defer func() {
				ginkgo.By("Deleting VM bootstrap data")
				err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ch := make(chan *vmopv1.VirtualMachine)
			var wg sync.WaitGroup
			var lock sync.Mutex
			ginkgo.By("Creating VM in parallel to site failure")
			wg.Add(2)
			go createVMServiceVmInParallel(ctx, vmopC, namespace, vmClass, pvclaimsList,
				vmi, storageClassName, secretName, vmCount, ch, &wg, &lock)
			go func() {
				for v := range ch {
					vms = append(vms, v)
				}
			}()
			go siteFailureInParallel(ctx, true, &wg)
			wg.Wait()
			close(ch)

			defer func() {
				ginkgo.By("Bring up the primary site before terminating the test")
				if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
					siteRestore(true)
					fds.hostsDown = nil
				}
			}()

			defer func() {
				for _, vm := range vms {
					ginkgo.By("Deleting VM")
					err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
						Name:      vm.Name,
						Namespace: namespace,
					}})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Wait for k8s cluster to be healthy")
			if vanillaCluster {
				wait4AllK8sNodesToBeUp(nodeList)
			}
			if guestCluster {
				err = waitForAllNodes2BeReady(ctx, client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			// Check if csi pods are running fine after site failure
			ginkgo.By("Check if csi pods are running fine after site failure")
			err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, len(csipods.Items),
				time.Duration(pollTimeout*2))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			ginkgo.By("Verify volume lifecycle actions when there is a fault induced")
			performVolumeLifecycleActionForVmServiceVM(ctx, client, vmopC, cnsopC, vmClass, namespace, vmi, sc, secretName)

			ginkgo.By("Bring up the primary site")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			// wait for the VMs to move back
			err = waitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	/*
		VMService VM deletion while secondary site goes down
		Steps:

		1. Create 10 PVCS using the storageclass as mentioned in testbed structure
		   and verify they go into bound state.
		2. Create VMService VM with each PVC created in step1.
		3. Verify all PVC's metadata on CNS.
		4. Once the VMs are up verify that the volume is accessible inside the VM.
		5. Delete all the VMs created in step2.
		6. While VMService VM deletion is going on,
			bring down the secondary site by powering off the hosts in secondary site in parallel.
		7. Verify that the supervisor cluster should be in running and ready state after site failover.
		8. Verify all the VMservice vms created in step2 are deleted successfully.
		9. Perform volume lifecycle actions which should work fine.
		10.Bring secondary site up and wait for testbed to be back to normal.
		11.Delete all objects created in this test.
	*/
	ginkgo.It("VMService VM deletion while secondary site goes down",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 10
			var err error

			ginkgo.By("Creating StorageClass")

			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				for i, pvc := range pvclaimsList {
					ginkgo.By("Delete PVCs")
					err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Waiting for CNS volumes to be deleted")
					volHandle := pvs[i].Spec.CSI.VolumeHandle
					gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creating VM bootstrap data")
			secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
			defer func() {
				ginkgo.By("Deleting VM bootstrap data")
				err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Creating VM")
			vms := createVMServiceVmWithMultiplePvcs(
				ctx, vmopC, namespace, vmClass, pvclaimsList, vmi, storageClassName, secretName)
			defer func() {
				for _, vm := range vms {
					ginkgo.By("Deleting VM")
					_, err := getVmsvcVM(ctx, vmopC, namespace, vm.Name)
					if !apierrors.IsNotFound(err) {
						err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
							Name:      vm.Name,
							Namespace: namespace,
						}})
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}()

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			var wg sync.WaitGroup
			ginkgo.By("Deleting VM in parallel to secondary site failure")
			wg.Add(2)
			go deleteVMServiceVmInParallel(ctx, vmopC, vms, namespace, &wg)
			go siteFailureInParallel(ctx, false, &wg)
			wg.Wait()

			defer func() {
				ginkgo.By("Bring up the secondary site before terminating the test")
				if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
					siteRestore(false)
					fds.hostsDown = nil
				}
			}()

			ginkgo.By("Wait for k8s cluster to be healthy")
			if vanillaCluster {
				wait4AllK8sNodesToBeUp(nodeList)
			}
			if guestCluster {
				err = waitForAllNodes2BeReady(ctx, client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			time.Sleep(5 * time.Minute)
			// Check if csi pods are running fine after site failure
			ginkgo.By("Check if csi pods are running fine after site failure")
			err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, len(csipods.Items),
				time.Duration(pollTimeout*2))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify all the VMservice vms created before " +
				"secondary site failure are deleted successfully")
			for _, vm := range vms {
				_, err := getVmsvcVM(ctx, vmopC, namespace, vm.Name)
				gomega.Expect(err).To(gomega.HaveOccurred())
			}

			ginkgo.By("Bring up the secondary site")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(false)
				fds.hostsDown = nil
			}

			ginkgo.By("Wait for k8s cluster to be healthy")
			// wait for the VMs to move back
			err = waitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	/*
	   PSOD hosts on secondary site
	   Steps:
	   1.  Create a few PVCs using the storageclass as mentioned in testbed structure.
	   2. Verify all PVC's metadata on CNS.
	   3. Create a VMservice VM from each PVC created in step2.
	   4. Write data on volumes created.
	   5. While VMService VM creation is going on,PSOD all hosts
	      in secondary site in parallel.
	   6. Verify that the supervisor cluster should be in running
	      and ready state after site failover.
	   7. Verify that all the k8s constructs created in step 2 are running fine.
	   8. Perform volume lifecycle actions which should work fine.
	   9. Verify the data written in step 3.
	   10.Wait for psod timeout to be over and wait for testbed to be back to normal.
	   11.Delete all objects created in this test.
	*/
	ginkgo.It("VMService - psod hosts on secondary site",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var vms []*vmopv1.VirtualMachine

			ginkgo.By("Creating StorageClass")
			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, 10, nil)

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				for i, pvc := range pvclaimsList {
					ginkgo.By("Delete PVCs")
					err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Waiting for CNS volumes to be deleted")
					volHandle := pvs[i].Spec.CSI.VolumeHandle
					gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creating VM bootstrap data")
			secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
			defer func() {
				ginkgo.By("Deleting VM bootstrap data")
				err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ch := make(chan *vmopv1.VirtualMachine)
			var wg sync.WaitGroup
			var lock sync.Mutex
			ginkgo.By("Creating VM in parallel to site failure")
			wg.Add(2)
			go createVMServiceVmInParallel(ctx, vmopC, namespace, vmClass,
				pvclaimsList, vmi, storageClassName, secretName, 10, ch, &wg, &lock)
			go func() {
				for v := range ch {
					vms = append(vms, v)
				}
			}()
			go psodHostsInParallel(true, "600", &wg)
			wg.Wait()
			close(ch)

			if vanillaCluster {
				wait4AllK8sNodesToBeUp(nodeList)
			}
			if vanillaCluster || guestCluster {
				err = waitForAllNodes2BeReady(ctx, client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			time.Sleep(5 * time.Minute)

			ginkgo.By("Check if csi pods are running fine after site recovery")
			err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, len(csipods.Items),
				time.Duration(pollTimeout*2))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			ginkgo.By("Verify volume lifecycle actions when there is a fault induced")
			performVolumeLifecycleActionForVmServiceVM(ctx, client, vmopC, cnsopC,
				vmClass, namespace, vmi, sc, secretName)

			ginkgo.By("Bring up the primary site")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}

			wait4AllK8sNodesToBeUp(nodeList)
			err = waitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	/*
		VMService - witness failure
		Steps:
		1.	Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Create a statefulset, deployment with volumes from the stretched datastore
		3.	Bring down the primary site
		4.	Verify that the VMs hosted by esx servers are brought up on the other site
		5.	Verify that the k8s cluster is healthy and all the k8s constructs created in step 2 are running and volume
			and application lifecycle actions work fine
		6.	Bring primary site up and wait for testbed to be back to normal
		7.	Delete all objects created in step 2 and 5
	*/
	ginkgo.It("VMService - witness failure",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 10
			var err error

			ginkgo.By("Creating StorageClass")
			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Wait for k8s cluster to be healthy")
			if vanillaCluster {
				wait4AllK8sNodesToBeUp(nodeList)
			}
			if guestCluster {
				err = waitForAllNodes2BeReady(ctx, client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By("Bring down witness host")
			toggleWitnessPowerState(ctx, true)
			defer func() {
				ginkgo.By("Bring up the witness host before terminating the test")
				if fds.witnessDown != "" {
					toggleWitnessPowerState(ctx, false)
				}
			}()

			// Check if csi pods are running fine after site failure
			ginkgo.By("Check if csi pods are running fine after site failure")
			err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, len(csipods.Items),
				time.Duration(pollTimeout*2))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				for i, pvc := range pvclaimsList {
					ginkgo.By("Delete PVCs")
					err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Waiting for CNS volumes to be deleted")
					volHandle := pvs[i].Spec.CSI.VolumeHandle
					gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creating VM bootstrap data")
			secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
			defer func() {
				ginkgo.By("Deleting VM bootstrap data")
				err := client.CoreV1().Secrets(namespace).Delete(ctx,
					secretName, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Creating VM")
			vms := createVMServiceVmWithMultiplePvcs(
				ctx, vmopC, namespace, vmClass, pvclaimsList, vmi, storageClassName, secretName)
			defer func() {
				for _, vm := range vms {
					ginkgo.By("Deleting VM")
					err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
						Name:      vm.Name,
						Namespace: namespace,
					}})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			ginkgo.By("Check storage compliance")
			comp := checkVmStorageCompliance(storagePolicyName)
			if comp {
				framework.Failf("Expected VM and storage compliance to be false but found true")
			}

			ginkgo.By("Bring up witness host")
			if fds.witnessDown != "" {
				toggleWitnessPowerState(ctx, false)
			}

			time.Sleep(5 * time.Minute)
			ginkgo.By("Check storage compliance")
			comp = checkVmStorageCompliance(storagePolicyName)
			if !comp {
				framework.Failf("Expected VM and storage compliance to be true but found false")
			}

		})

	/*
		Primary site network isolation
		Steps:
		1.	Create a few PVCs using the storageclass as mentioned in testbed structure.
		2. Verify all PVC's metadata on CNS.
		3. Create a VMservice VM from each PVC created in step2.
		4. Write data on volumes created.
		5. While VMService VM creation is going on, isolate the primary site from the witness and the secondary site
			such that both witness and secondary site can't talk to primary site. VC will have
			access to both secondary site and primary site.
		6. Verify that the supervisor cluster should be in running and ready state after site failover.
		7. Verify all the k8s constructs created in step 2 are running and volume and application lifecycle actions work fine
		8. Once the VMs are up verify that the volume is accessible inside the VM.
		9. Perform volume lifecycle actions which should work fine.
		10. Verify the CNS metadata entries.
		11. Re-establish primary site network and wait for testbed to be back to normal
		12. Delete all objects created in this test.
	*/
	ginkgo.It("VMService - Primary site network isolation",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 10
			var err error

			ginkgo.By("Creating StorageClass")

			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				for i, pvc := range pvclaimsList {
					ginkgo.By("Delete PVCs")
					err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Waiting for CNS volumes to be deleted")
					volHandle := pvs[i].Spec.CSI.VolumeHandle
					gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creating VM bootstrap data")
			secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
			defer func() {
				ginkgo.By("Deleting VM bootstrap data")
				err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Creating VM")
			vms := createVMServiceVmWithMultiplePvcs(
				ctx, vmopC, namespace, vmClass, pvclaimsList, vmi, storageClassName, secretName)
			defer func() {
				for _, vm := range vms {
					ginkgo.By("Deleting VM")
					err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
						Name:      vm.Name,
						Namespace: namespace,
					}})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Cause a network failure on primary site
			ginkgo.By("Isolate secondary site from witness and primary site")
			siteNetworkFailure(false, false)
			defer func() {
				ginkgo.By("Bring up the primary site before terminating the test")
				siteNetworkFailure(false, true)
			}()

			ginkgo.By("Wait for k8s cluster to be healthy")
			if vanillaCluster {
				wait4AllK8sNodesToBeUp(nodeList)
			}
			if guestCluster {
				err = waitForAllNodes2BeReady(ctx, client)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			// Check if csi pods are running fine after site failure
			ginkgo.By("Check if csi pods are running fine after site failure")
			err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, len(csipods.Items),
				time.Duration(pollTimeout*2))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for all claims to be in bound state")
			pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			for _, vm := range vms {
				_, err := wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By("Verify volume lifecycle actions when there is a fault induced")
			performVolumeLifecycleActionForVmServiceVM(ctx, client, vmopC, cnsopC,
				vmClass, namespace, vmi, sc, secretName)

			ginkgo.By("Bring up the primary site")
			siteNetworkFailure(false, true)

			ginkgo.By("Wait for k8s cluster to be healthy")
			// wait for the VMs to move back
			err = waitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	/*
		VMService - Volume snapshot creation while secondary site goes down
		Steps:
		1. Create 10 PVCS using the storageclass as mentioned in testbed structure and verify that it goes to bound state.
		2. Create VMService VM with each PVC created in step1.
		3. While VMService VM creation is going on, bring down the primary site by powering off the hosts in primary site in parallel.
		4. Verify that the supervisor cluster should be in running and ready state after site failover.
		5. Verify that all the PVCs created in step 2 are running fine.
		6. Perform volume lifecycle actions which should work fine.
		7. Bring primary site up and wait for testbed to be back to normal.
		8. Delete all objects created in the test.
	*/
	ginkgo.It("VMService - Volume snapshot creation while secondary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvcCount int = 10
		var vmCount = 10
		var err error
		var vmlbsvcs []*vmopv1.VirtualMachineService
		var pvclaimsList []*v1.PersistentVolumeClaim
		var volHandles []string
		var volumeSnapshotList []*snapV1.VolumeSnapshot

		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run

		sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, persistentVolumes, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, nil, "",
				diskSize, sc, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
			volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
			if guestCluster {
				volHandle = getVolumeIDFromSupervisorCluster(volHandle)
			}
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			volHandles = append(volHandles, volHandle)

			defer func() {
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		vms := createVMServiceVmWithMultiplePvcs(
			ctx, vmopC, namespace, vmClass, pvclaimsList, vmi, storageClassName, secretName)
		defer func() {
			for _, vm := range vms {
				ginkgo.By("Deleting VM")
				err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
					Name:      vm.Name,
					Namespace: namespace,
				}})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		for _, vm := range vms {
			vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm.Name)
			vmlbsvcs = append(vmlbsvcs, vmlbsvc)
			defer func() {
				ginkgo.By("Deleting loadbalancing service for ssh with the VM")
				err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
					Name:      vmlbsvc.Name,
					Namespace: namespace,
				}})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}

		ginkgo.By("Wait for VM to come up and get an IP")
		for j, vm := range vms {
			_, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Wait and verify PVCs are attached to the VM")
			gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
				[]*v1.PersistentVolumeClaim{pvclaimsList[j]})).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify PVCs are accessible to the VM")
			ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
			vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			/*for i, vol := range vm.Status.Volumes {
				volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
				verifyDataIntegrityOnVmDisk(vmIp, volFolder)
			}*/
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ch := make(chan *snapV1.VolumeSnapshot)
		var wg sync.WaitGroup
		var lock *sync.Mutex
		ginkgo.By("Creating volumeSnapshot in parallel to secondary site failure")
		wg.Add(vmCount)
		go createDynamicSnapshotInParallel(ctx, namespace, snapc,
			pvclaimsList, volumeSnapshotClass.Name, ch, lock, &wg)
		go func() {
			for v := range ch {
				volumeSnapshotList = append(volumeSnapshotList, v)
			}
		}()
		go siteFailureInParallel(ctx, false, &wg)
		wg.Wait()
		close(ch)

		defer func() {
			ginkgo.By("Bring up the secondary site")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(false)
				fds.hostsDown = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		if vanillaCluster {
			wait4AllK8sNodesToBeUp(nodeList)
		}
		if guestCluster {
			err = waitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Check if csi pods are running fine after site failure
		ginkgo.By("Check if csi pods are running fine after site failure")
		err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, int(csipods.Size()), time.Duration(pollTimeout*2))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		performVolumeLifecycleActionForVmServiceVM(ctx, client, vmopC, cnsopC,
			vmClass, namespace, vmi, sc, secretName)

		ginkgo.By("Verify volume snapshot is created")
		for i, volumeSnapshot := range volumeSnapshotList {
			volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize)) != 0 {
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "unexpected restore size")
			}

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			snapshotContent, err = waitForVolumeSnapshotContentReadyToUse(*snapc, ctx, snapshotContent.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Get volume snapshot ID from snapshot handle")
			_, snapshotId, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, snapshotContent)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = waitForCNSSnapshotToBeCreated(volHandles[i], snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify restore volume from snapshot is successfull")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshot, diskSize, true)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshot, pandoraSyncWaitTime, volHandles[i], snapshotId, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(false)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		VMService - Restore volume from snapshot while secondary site goes down
		Steps:
		1. Create 10 PVCS using the storageclass as mentioned in testbed structure and verify that it goes to bound state.
		2. Create VMService VM with each PVC created in step1.
		3. While VMService VM creation is going on, bring down the primary site by powering off the hosts in primary site in parallel.
		4. Verify that the supervisor cluster should be in running and ready state after site failover.
		5. Verify that all the PVCs created in step 2 are running fine.
		6. Perform volume lifecycle actions which should work fine.
		7. Bring primary site up and wait for testbed to be back to normal.
		8. Delete all objects created in the test.
	*/
	ginkgo.It("VMService - Restore volume from snapshot while secondary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvcCount int = 10
		var err error
		var vmlbsvcs []*vmopv1.VirtualMachineService
		var pvclaimsList, restoreVolList []*v1.PersistentVolumeClaim
		var volHandles []string
		var volumeSnapshotList []*snapV1.VolumeSnapshot
		var snapshotIds []string

		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run

		sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, persistentVolumes, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, nil, "",
				diskSize, sc, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
			volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
			if guestCluster {
				volHandle = getVolumeIDFromSupervisorCluster(volHandle)
			}
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			volHandles = append(volHandles, volHandle)

			defer func() {
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		vms := createVMServiceVmWithMultiplePvcs(
			ctx, vmopC, namespace, vmClass, pvclaimsList, vmi, storageClassName, secretName)
		defer func() {
			for _, vm := range vms {
				ginkgo.By("Deleting VM")
				err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
					Name:      vm.Name,
					Namespace: namespace,
				}})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		for _, vm := range vms {
			vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm.Name)
			vmlbsvcs = append(vmlbsvcs, vmlbsvc)
			defer func() {
				ginkgo.By("Deleting loadbalancing service for ssh with the VM")
				err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
					Name:      vmlbsvc.Name,
					Namespace: namespace,
				}})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}

		ginkgo.By("Wait for VM to come up and get an IP")
		for j, vm := range vms {
			vmIp, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Wait and verify PVCs are attached to the VM")
			gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
				[]*v1.PersistentVolumeClaim{pvclaimsList[j]})).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify PVCs are accessible to the VM")
			ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
			vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for i, vol := range vm.Status.Volumes {
				volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
				verifyDataIntegrityOnVmDisk(vmIp, volFolder)
			}
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		for i, pvclaim := range pvclaimsList {
			volumeSnapshot, _, _,
				_, _, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				pvclaim, volHandles[i], diskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeSnapshotList = append(volumeSnapshotList, volumeSnapshot)
			//snapshotContents = append(snapshotContents, snapshotContent)
			snapshotIds = append(snapshotIds, snapshotId)

		}
		defer func() {
			for i, volumeSnapshot := range volumeSnapshotList {
				ginkgo.By("Delete dynamic volume snapshot")
				_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
					volumeSnapshot, pandoraSyncWaitTime, volHandles[i], snapshotIds[i], true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			}
		}()

		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the secondary site while creating pvcs")
		var ch chan *v1.PersistentVolumeClaim
		var wg sync.WaitGroup
		var lock *sync.Mutex
		wg.Add(2)
		go restoreVolumeFromSnapshotInParallel(ctx, client, namespace, sc, volumeSnapshotList, ch, lock, &wg)
		go func() {
			for v := range ch {
				restoreVolList = append(restoreVolList, v)
			}
		}()
		go siteFailureInParallel(ctx, false, &wg)
		wg.Wait()

		ginkgo.By("Wait for k8s cluster to be healthy")
		if vanillaCluster {
			wait4AllK8sNodesToBeUp(nodeList)
		}
		if guestCluster {
			err = waitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Check if csi pods are running fine after site failure
		ginkgo.By("Check if csi pods are running fine after site failure")
		err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, int(csipods.Size()), time.Duration(pollTimeout*2))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		performVolumeLifecycleActionForVmServiceVM(ctx, client, vmopC, cnsopC,
			vmClass, namespace, vmi, sc, secretName)

		for i, _ := range volumeSnapshotList {

			persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
				[]*v1.PersistentVolumeClaim{restoreVolList[i]}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
			if guestCluster {
				volHandle2 = getVolumeIDFromSupervisorCluster(volHandle2)
			}
			gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

			// Create a Pod to use this PVC, and verify volume has been attached
			ginkgo.By("Creating pod to attach PV to the node")
			pod, err := createPod(ctx, client, namespace, nil,
				[]*v1.PersistentVolumeClaim{restoreVolList[i]}, false, execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			var vmUUID string
			nodeName := pod.Spec.NodeName

			if vanillaCluster {
				vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
			} else if guestCluster {
				vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle2, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle2, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			ginkgo.By("Verify the volume is accessible and Read/write is possible")
			cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat /mnt/volume1/Pod1.html "}
			output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
			gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

			wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
			e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
			output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
			gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
		}

		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(false)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		VMService VM snapshot deletion while primary site goes down
		Steps:
		1. Create 10 PVCS using the storageclass as mentioned in testbed structure and verify that it goes to bound state.
		2. Create VMService VM with each PVC created in step1.
		3. While VMService VM creation is going on, bring down the primary site by powering off the hosts in primary site in parallel.
		4. Verify that the supervisor cluster should be in running and ready state after site failover.
		5. Verify that all the PVCs created in step 2 are running fine.
		6. Perform volume lifecycle actions which should work fine.
		7. Bring primary site up and wait for testbed to be back to normal.
		8. Delete all objects created in the test.
	*/
	ginkgo.It("VMService VM snapshot deletion while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvcCount int = 10
		var err error
		var vmlbsvcs []*vmopv1.VirtualMachineService
		var pvclaimsList []*v1.PersistentVolumeClaim
		var volHandles, snapshotIds []string
		var volumeSnapshotList []*snapV1.VolumeSnapshot

		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run

		sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create multiple PVCs")
		for i := 0; i < pvcCount; i++ {
			pvclaim, persistentVolumes, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, nil, "",
				diskSize, sc, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaimsList = append(pvclaimsList, pvclaim)
			volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
			if guestCluster {
				volHandle = getVolumeIDFromSupervisorCluster(volHandle)
			}
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			volHandles = append(volHandles, volHandle)

			defer func() {
				err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		vms := createVMServiceVmWithMultiplePvcs(
			ctx, vmopC, namespace, vmClass, pvclaimsList, vmi, storageClassName, secretName)
		defer func() {
			for _, vm := range vms {
				ginkgo.By("Deleting VM")
				err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
					Name:      vm.Name,
					Namespace: namespace,
				}})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		for _, vm := range vms {
			vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm.Name)
			vmlbsvcs = append(vmlbsvcs, vmlbsvc)
			defer func() {
				ginkgo.By("Deleting loadbalancing service for ssh with the VM")
				err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
					Name:      vmlbsvc.Name,
					Namespace: namespace,
				}})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}

		ginkgo.By("Wait for VM to come up and get an IP")
		for j, vm := range vms {
			vmIp, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Wait and verify PVCs are attached to the VM")
			gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
				[]*v1.PersistentVolumeClaim{pvclaimsList[j]})).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify PVCs are accessible to the VM")
			ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
			vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for i, vol := range vm.Status.Volumes {
				volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
				verifyDataIntegrityOnVmDisk(vmIp, volFolder)
			}
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name,
					metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot")
		for i, pvclaim := range pvclaimsList {
			volumeSnapshot, _, _,
				_, _, snapshotId, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				pvclaim, volHandles[i], diskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeSnapshotList = append(volumeSnapshotList, volumeSnapshot)
			//snapshotContents = append(snapshotContents, snapshotContent)
			snapshotIds = append(snapshotIds, snapshotId)

		}
		defer func() {
			for i, volumeSnapshot := range volumeSnapshotList {
				ginkgo.By("Delete dynamic volume snapshot")
				_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
					volumeSnapshot, pandoraSyncWaitTime, volHandles[i], snapshotIds[i], true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			}
		}()

		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var wg sync.WaitGroup
		ginkgo.By("Deleting volumeSnapshot in parallel to primary site failure")
		wg.Add(2)
		go deleteVolumeSnapshotInParallel(ctx, namespace, snapc, volumeSnapshotList, &wg)
		go siteFailureInParallel(ctx, false, &wg)
		wg.Wait()

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}
		}()

		defer func() {
			for _, volumeSnapshot := range volumeSnapshotList {
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace,
					volumeSnapshot.Name, pandoraSyncWaitTime)
			}
		}()
		ginkgo.By("Wait for k8s cluster to be healthy")
		if vanillaCluster {
			wait4AllK8sNodesToBeUp(nodeList)
		}
		if guestCluster {
			err = waitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		time.Sleep(5 * time.Minute)
		// Check if csi pods are running fine after site failure
		ginkgo.By("Check if csi pods are running fine after site failure")
		err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, int(csipods.Size()), time.Duration(pollTimeout*2))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all claims to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		performVolumeLifecycleActionForVmServiceVM(ctx, client, vmopC, cnsopC,
			vmClass, namespace, vmi, sc, secretName)

		ginkgo.By("Verify all volume snapshots are deleted")
		for i, volumeSnapshot := range volumeSnapshotList {
			framework.Logf("Wait until the volume snapshot content is deleted")
			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Verify snapshot entry %v is deleted from CNS for volume %v", snapshotIds[i], volHandles[i])
			err = waitForCNSSnapshotToBeDeleted(volHandles[i], snapshotIds[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Verify snapshot entry is deleted from CNS")
			err = verifySnapshotIsDeletedInCNS(volHandles[i], snapshotIds[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Deleting volume snapshot again to check 'Not found' error")
			deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)
		}

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(true)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})
})
