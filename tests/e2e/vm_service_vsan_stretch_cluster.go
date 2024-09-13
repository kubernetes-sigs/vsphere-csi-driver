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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
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
		vcAddress                  string
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

		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		vcRestSessionId = createVcSession4RestApis(ctx)
		csiNs = GetAndExpectStringEnvVar(envCSINamespace)

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
		vmi = waitNGetVmiForImageName(ctx, vmopC, namespace, vmImageName)
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
		1.	Create a few PVCs using the storageclass as mentioned in testbed structure.
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
	ginkgo.It("VMService - primary site down", func() {
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

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		for _, vm := range vms {
			vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm.Name)
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
			wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		}
		if guestCluster {
			err = waitForAllNodes2BeReady(ctx, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		time.Sleep(5 * time.Minute)
		// Check if csi pods are running fine after site failure
		ginkgo.By("Check if csi pods are running fine after site failure")
		err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, int32(csipods.Size()), 0, pollTimeout*2)
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
})
