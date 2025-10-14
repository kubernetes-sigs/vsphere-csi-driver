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
	"strings"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"

	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
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
		nodeList                   *v1.NodeList
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

		vcRestSessionId = createVcSession4RestApis(ctx)

		if !latebinding {
			ginkgo.By("Reading Immediate binding mode storage policy")
			storageClassName = strings.ReplaceAll(storagePolicyName, " ", "-")
			storageClassName = strings.ToLower(storageClassName)
			framework.Logf("storageClassName: %s", storageClassName)
		} else {
			ginkgo.By("Reading late binding mode storage policy")
			storageClassName = strings.ReplaceAll(storagePolicyName, " ", "-")
			storageClassName = strings.ToLower(storageClassName)
			storageClassName = storageClassName + lateBinding
			framework.Logf("storageClassName: %s", storageClassName)
		}

		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		dsRef := getDsMoRefFromURL(ctx, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)
		contentLibId, err := createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
			dsRef.Value, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Create a WCP namespace for the test")
		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}
		namespace = createTestWcpNs(
			vcRestSessionId, storageProfileId, vmClass, contentLibId, getSvcId(vcRestSessionId, &e2eVSphere))

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
	ginkgo.It("[pq-vmsvc-vsanstretch] VMService - primary site down",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 5
			var err error
			var pvs []*v1.PersistentVolume

			ginkgo.By("Creating StorageClass")

			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			if !latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state when " +
					"using an Immediate binding mode storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
				ginkgo.By("Performing cleanup...")
				for _, vm := range vms {
					ginkgo.By("Deleting VM")
					err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
						Name:      vm.Name,
						Namespace: namespace,
					}})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
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

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			if latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state after the " +
					"volume is attached to the VM using a late-binding storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
	ginkgo.It("[pq-vmsvc-vsanstretch] VMService - secondary site down",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 10
			var err error
			var pvs []*v1.PersistentVolume

			ginkgo.By("Get StorageClass for volume creation")

			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			if !latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state when " +
					"using an Immediate binding mode storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
				ginkgo.By("Performing cleanup...")
				for _, vm := range vms {
					ginkgo.By("Deleting VM")
					err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
						Name:      vm.Name,
						Namespace: namespace,
					}})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

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

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			if latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state after the " +
					"volume is attached to the VM using a late-binding storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
	ginkgo.It("[pq-f-vmsvc-vsanstretch] VMService VM creation while primary site goes down",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 9
			var vmCount = 9
			var err error
			var vms []*vmopv1.VirtualMachine
			var pvs []*v1.PersistentVolume

			ginkgo.By("Creating StorageClass")
			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			if !latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state when " +
					"using an Immediate binding mode storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
				ginkgo.By("Performing cleanup...")
				for _, vm := range vms {
					ginkgo.By("Deleting VM")
					err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
						Name:      vm.Name,
						Namespace: namespace,
					}})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

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

			if latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state after the " +
					"volume is attached to the VM using a late-binding storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
	ginkgo.It("[pq-f-vmsvc-vsanstretch] VMService VM deletion while secondary site goes down",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 10
			var err error
			var pvs []*v1.PersistentVolume

			ginkgo.By("Creating StorageClass")

			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			if !latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state when " +
					"using an Immediate binding mode storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
				ginkgo.By("Performing cleanup...")
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

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			if latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state after the " +
					"volume is attached to the VM using a late-binding storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
	ginkgo.It("[pq-f-vmsvc-vsanstretch] VMService - psod hosts on secondary site",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var vms []*vmopv1.VirtualMachine
			var pvs []*v1.PersistentVolume

			ginkgo.By("Creating StorageClass")
			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, 10, nil)

			if !latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state when " +
					"using an Immediate binding mode storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
			defer func() {
				ginkgo.By("Performing cleanup...")
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

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			if latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state after the " +
					"volume is attached to the VM using a late-binding storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
	ginkgo.It("[pq-f-vmsvc-vsanstretch] VMService - witness failure",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 10
			var err error
			var pvs []*v1.PersistentVolume

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

			if !latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state when " +
					"using an Immediate binding mode storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
				ginkgo.By("Performing cleanup...")
				for _, vm := range vms {
					ginkgo.By("Deleting VM")
					err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
						Name:      vm.Name,
						Namespace: namespace,
					}})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

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

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			if latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state after the " +
					"volume is attached to the VM using a late-binding storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
	ginkgo.It("[pq-f-vmsvc-vsanstretch] VMService - Primary site network isolation",
		ginkgo.Label(p0, vmsvc, vsanStretch, block, wcp), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var pvcCount int = 10
			var err error
			var pvs []*v1.PersistentVolume

			ginkgo.By("Creating StorageClass")

			sc, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create multiple PVCs")
			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)

			if !latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state when " +
					"using an Immediate binding mode storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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

			ginkgo.By("Creates a loadbalancing service for ssh with each VM" +
				"and waits for VM IP to come up to come up and verify PVCs are accessible in the VM")
			createVMServiceandWaitForVMtoGetIP(ctx, vmopC, cnsopC, namespace, vms, pvclaimsList, true, true)

			if latebinding {
				ginkgo.By("Validating that the PVC transitions to Bound state after the " +
					"volume is attached to the VM using a late-binding storage policy")
				pvs, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

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
})
