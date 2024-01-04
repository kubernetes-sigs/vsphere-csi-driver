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
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
)

var _ bool = ginkgo.Describe("[vmsvc] vm service with csi vol tests", func() {

	f := framework.NewDefaultFramework("vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client            clientset.Interface
		namespace         string
		datastoreURL      string
		storagePolicyName string
		storageClassName  string
		storageProfileId  string
		vcRestSessionId   string
		vmi               string
		vmClass           string
		vmopC             ctlrclient.Client
		cnsopC            ctlrclient.Client
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		var err error
		topologyFeature := os.Getenv(topologyFeature)
		if topologyFeature != topologyTkgHaName {
			nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}
			storagePolicyName = GetAndExpectStringEnvVar(envZonalStoragePolicyName)
		} else {
			storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		}
		bootstrap()
		vcRestSessionId = createVcSession4RestApis()

		storageClassName = strings.ReplaceAll(storagePolicyName, "_", "-") // since this is a wcp setup

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
		dumpSvcNsEventsOnTestFailure(client, namespace)
		delTestWcpNs(vcRestSessionId, namespace)
		gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
	})

	/*
	   Basic test
	   Steps:
	   1   Assign a spbm policy to test namespace with sufficient quota
	   2   Create a PVC say pvc1
	   3   Create a VMservice VM say vm1, pvc1
	   4   verify pvc1 CNS metadata.
	   5   Once the vm1 is up verify that the volume is accessible inside vm1
	   6   Delete vm1
	   7   delete pvc1
	   8   Remove spbm policy attached to test namespace

	   statically provisioned CSI volumes
	   Steps:
	   1   Assign a spbm policy to test namespace with sufficient quota
	   2   Create two FCDs
	   3   Create a static PV/PVC using cns register volume API
	   4   Create a VMservice VM and with the pvcs created in step 3
	   5   Verify CNS metadata for pvcs.
	   6   Write some IO to the CSI volumes and read it back from them and verify the data integrity
	   7   Delete VM service VM
	   8   delete pvcs
	   9   Remove spbm policy attached to test namespace
	*/
	ginkgo.It("verify vmservice vm creation with a pvc in its spec", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pandoraSyncWaitTime int
		var err error
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		datastore := getDsMoRefFromURL(ctx, datastoreURL)
		ginkgo.By("Creating FCD Disk")
		fcdID, err := e2eVSphere.createFCD(ctx, fcdName, diskSizeInMb, datastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By(fmt.Sprintf("Creating the PV with the fcdID %s", fcdID))
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID
		staticPv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimDelete, nil, ext4FSType)
		staticPv, err = client.CoreV1().PersistentVolumes().Create(ctx, staticPv, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = e2eVSphere.waitForCNSVolumeToBeCreated(staticPv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating a static PVC")
		staticPvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, staticPv.Name)
		staticPvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(
			ctx, staticPvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Create a PVC")
		pvc, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc, staticPvc}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volHandle := pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			ginkgo.By("Delete PVCs")
			err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, staticPvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Waiting for CNS volumes to be deleted")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(staticPv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating VM")
		vm := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc, staticPvc}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VM to come up and get an IP")
		vmIp, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*v1.PersistentVolumeClaim{pvc, staticPvc})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
			verifyDataIntegrityOnVmDisk(vmIp, volFolder)
		}
	})

	/*
	   hot detach and attach
	   Steps:
	   1   Assign a spbm policy to test namespace with sufficient quota
	   2   create few pvcs say pvc1, pvc2
	   3   Create two VMservice VMs, say vm1(with pvc1) and vm2(with pvc2)
	   4   write some data to a file in pvc2 from vm2
	   5   modify vm1 and vm2 specs to detach pvc2 from vm2 and attach it to vm1
	   6   verify that pvc2 is accessible in vm1 and can read and verify the contents of the file written in step 6
	       from vm1
	   7   Delete VM service VMs
	   8   delete pvcs from step2
	   9   Remove spbm policy attached to test namespace
	*/
	ginkgo.It("hot detach and attach pvc to vmservice vms", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Create a PVC")
		pvc, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volHandle := pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			ginkgo.By("Delete PVCs")
			err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Waiting for CNS volumes to be deleted")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating VMs")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{}, vmi, storageClassName, secretName)
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc1 := createService4Vm(ctx, vmopC, namespace, vm1.Name)
		vmlbsvc2 := createService4Vm(ctx, vmopC, namespace, vm2.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing services for ssh for the VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmIp2, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVC is attached to the VM2")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvc})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC is accessible to the VM2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volFolder := formatNVerifyPvcIsAccessible(vm2.Status.Volumes[0].DiskUuid, 1, vmIp2)

		ginkgo.By("write some data to a file in pvc2 from vm2")
		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		framework.Logf("Creating a 100mb test data file %v", testdataFile)
		op, err := exec.Command(
			"bash", "-c", "dd if=/dev/urandom bs=1M count=1 | tr -dc 'a-zA-Z0-9' >"+testdataFile).Output()
		// using 'tr' to filter out non-alphanumeric characters
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		op, err = exec.Command("md5sum", testdataFile).Output()
		fmt.Println("md5sum", string(op[:]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		op, err = exec.Command("ls", "-l", testdataFile).Output()
		fmt.Println(string(op[:]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		framework.Logf("Copying test data file to VM")
		copyFileToVm(vmIp2, testdataFile, volFolder+"/f1")

		_ = execSshOnVmThroughGatewayVm(vmIp2, []string{"ls -l " + volFolder + "/f1", "md5sum " + volFolder + "/f1"})

		ginkgo.By("modify vm1 and vm2 specs to detach pvc2 from vm2 and attach it to vm1")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vm1.Spec.Volumes, vm2.Spec.Volumes = vm2.Spec.Volumes, vm1.Spec.Volumes
		err = vmopC.Update(ctx, vm2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVC is attached to the VM1")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc})).To(gomega.Succeed())
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify PVC is detached from VM2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(verifyPvcsAreAttachedToVmsvcVm(
			ctx, cnsopC, vm2, []*v1.PersistentVolumeClaim{pvc})).To(gomega.BeFalse())

		ginkgo.By("verify data in pvc2 from vm1")
		framework.Logf("Mounting the volume")
		volFolder = mountFormattedVol2Vm(vm1.Status.Volumes[0].DiskUuid, 1, vmIp1)
		vmFileData := fmt.Sprintf("/tmp/vmdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		_ = execSshOnVmThroughGatewayVm(vmIp1, []string{"md5sum " + volFolder + "/f1"})
		framework.Logf("Fetching file from the VM")
		copyFileFromVm(vmIp1, volFolder+"/f1", vmFileData)
		defer func() {
			c := []string{"rm", "-f", vmFileData}
			op, err = exec.Command(c[0], c[1:]...).Output()
			framework.Logf("Command: %c, output: %v", c, op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		framework.Logf("Comparing file fetched from the VM with test data file")
		c := []string{"md5sum", testdataFile, vmFileData}
		op, err = exec.Command(c[0], c[1:]...).Output()
		framework.Logf("Command: %c, output: %v", c, op)
		lines := strings.Split(string(op[:]), "\n")
		gomega.Expect(strings.Fields(lines[0])[0]).To(gomega.Equal(strings.Fields(lines[1])[0]))
	})

	/*
	   attach PVC used by one VM to another VM while in use
	   Steps:
	   1   Assign a spbm policy to test namespace with sufficient quota
	   2   Create a PVC say pvc1
	   3   Create two VMservice VMs, say vm1 and vm2
	   4   Attach pvc1 to vm1
	   5   Try to attach pvc1 to vm2 which should fail
	   6   Delete VM service VMs
	   7   Delete pvc1
	   8   Remove spbm policy attached to test namespace

	   attach PVC used by one VM (powered off) to another VM
	   Steps:
	   1   Assign a spbm policy to test namespace with sufficient quota
	   2   Create a PVC say pvc1
	   3   Create two VMservice VMs, say vm1 and vm2
	   4   Attach pvc1 to vm1
	   5   power off vm1
	   6   Try to attach pvc1 to vm2 which should fail
	   7   Delete VM service VMs
	   8   Delete pvc1
	   9   Remove spbm policy attached to test namespace

	   attach PVC used by one VM to another VM (powered off)
	   Steps:
	   1   Assign a spbm policy to test namespace with sufficient quota
	   2   Create a PVC say pvc1
	   3   Create two VMservice VMs, say vm1 and vm2
	   4   Attach pvc1 to vm1
	   5   power off vm2
	   6   Try to attach pvc1 to vm2 which should fail
	   7   Power on vm2 and verify it comes up fine
	   8   Delete VM service VMs
	   9   Delete pvc1
	   10   Remove spbm policy attached to test namespace
	*/
	ginkgo.It("attach PVC used by one VM to another VM while in use", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Create a PVC")
		pvc1, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc2, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcs := []*v1.PersistentVolumeClaim{pvc1, pvc2}
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvcs, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete PVCs")
			err = fpv.DeletePersistentVolumeClaim(client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Waiting for CNS volumes to be deleted")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[1].Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating VMs")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc1}, vmi, storageClassName, secretName)
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc2}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc1 := createService4Vm(ctx, vmopC, namespace, vm1.Name)
		vmlbsvc2 := createService4Vm(ctx, vmopC, namespace, vm2.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing services for ssh for the VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmIp2, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to respective VMs")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc1})).To(gomega.Succeed())
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvc2})).To(gomega.Succeed())

		ginkgo.By("Verify PVCs are accessible to respective VMs")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = formatNVerifyPvcIsAccessible(vm1.Status.Volumes[0].DiskUuid, 1, vmIp1)
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = formatNVerifyPvcIsAccessible(vm2.Status.Volumes[0].DiskUuid, 1, vmIp2)

		ginkgo.By("edit vm1 spec and try to attach pvc2 to vm1, which should fail")
		vm1.Spec.Volumes = append(vm1.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: pvc2.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc2.Name},
			}})
		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = wait4PvcAttachmentFailure(ctx, vmopC, vm1, pvc2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("edit vm1 spec and remove pvc2")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm1.Spec.Volumes = vm1.Spec.Volumes[:1]
		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Power off vm2")
		vm2 = setVmPowerState(ctx, vmopC, vm2, vmopv1.VirtualMachinePoweredOff)
		vm2, err = wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to attach pvc2 to vm1 which should fail")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm1.Spec.Volumes = append(vm1.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: pvc2.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc2.Name},
			}})
		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = wait4PvcAttachmentFailure(ctx, vmopC, vm1, pvc2)
		framework.Logf("Error found: %s", err.Error())
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("edit vm1 spec and remove pvc2")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm1.Spec.Volumes = vm1.Spec.Volumes[:1]
		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Try to attach pvc1 to vm2 which should fail")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm2.Spec.Volumes = append(vm2.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: pvc1.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc1.Name},
			}})
		err = vmopC.Update(ctx, vm2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = wait4PvcAttachmentFailure(ctx, vmopC, vm2, pvc1)
		framework.Logf("Error found: %s", err.Error())
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("edit vm2 spec and remove pvc1")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm2.Spec.Volumes = vm2.Spec.Volumes[:1]
		err = vmopC.Update(ctx, vm2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Power on vm2")
		vm2 = setVmPowerState(ctx, vmopC, vm2, vmopv1.VirtualMachinePoweredOn)
		vm2, err = wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   VM and PVC both belong to same zone
	   Steps:
	   1   Assign a zonal spbm policy to test namespace with sufficient quota
	   2   Create two PVCs say pvc1, pvc2 under zone1
	   3   Create a VMservice VM say vm1 under zone1 with pvc1
	   4   Once the vm1 is up verify that the volume is accessible inside vm1
	   5   verify pvc1 CNS metadata.
	   6   Attach pvc2 to vm1 and verify that the volume is accessible inside vm1
	   7   Delete vm1
	   8   delete pvc1, pvc2
	   9   Remove spbm policy attached to test namespace in step1
	*/
	ginkgo.It("[vmsvc-stretched] VM and PVC both belong to same zone", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topologyHaMap := GetAndExpectStringEnvVar(topologyHaMap)
		allowedTopos := createAllowedTopolgies(topologyHaMap, tkgshaTopologyLevels)
		allowedTopologyHAMap := createAllowedTopologiesMap(allowedTopos)
		pvcAnnotations := make(map[string]string)
		topoList := []string{}
		zones := []string{}
		rand.NewSource(time.Now().UnixNano())

		for key, val := range allowedTopologyHAMap {
			for _, topoVal := range val {
				str := `{"` + key + `":"` + topoVal + `"}`
				topoList = append(topoList, str)
				zones = append(zones, topoVal)
			}
		}
		framework.Logf("topoList: %v", topoList)
		annotationVal := "[" + strings.Join(topoList, ",") + "]"
		pvcAnnotations[tkgHARequestedAnnotationKey] = annotationVal
		framework.Logf("annotationVal :%s, pvcAnnotations: %v", annotationVal, pvcAnnotations)

		ginkgo.By("Creating Pvc with Immediate topology storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcSpec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", storageclass, nil, "")
		pvcSpec.Annotations = pvcAnnotations
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvcSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for SV PVC to come to bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID := pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeID))
		}()

		ginkgo.By("Verify SV PV has has required PV node affinity details")
		_, err = verifyVolumeTopologyForLevel5(pvs[0], allowedTopologyHAMap)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("SVC PV: %s has required PV node affinity details", pvs[0].Name)

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM")
		vm := createVmServiceVmWithPvcsWithZone(ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi,
			storageClassName, secretName, zones[rand.Intn(len(zones))])
		defer func() {
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VM to come up and get an IP")
		vmIp, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*v1.PersistentVolumeClaim{pvc})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm.Status.Volumes {
			_ = formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
		}

	})

	/*
	   VM and PVC both belong to different zones (creation and post creation)
	   Steps:
	   1	Assign a zonal spbm policy to test namespace with sufficient quota
	   2	Create a PVC say pvc1 under zone2
	   3	Create a VMservice VM say vm1 under zone1 with pvc1
	   4	verify that vm1 does not come up
	   5	remove pvc1 from vm1 spec
	   6	Verify that the vm1 comes up now
	   7	Attach pvc1 to vm1 and verify that the volume attachment fails
	   8	Delete vm1
	   9	delete pvc1
	   10   Remove spbm policy attached to test namespace in step1
	*/
	ginkgo.It("[vmsvc-stretched] VM and PVC both belong to same zone", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topologyHaMap := GetAndExpectStringEnvVar(topologyHaMap)
		allowedTopos := createAllowedTopolgies(topologyHaMap, tkgshaTopologyLevels)
		allowedTopologyHAMap := createAllowedTopologiesMap(allowedTopos)
		pvcAnnotations := make(map[string]string)
		topoList := []string{}
		zones := []string{}
		rand.NewSource(time.Now().UnixNano())

		for key, val := range allowedTopologyHAMap {
			for _, topoVal := range val {
				str := `{"` + key + `":"` + topoVal + `"}`
				topoList = append(topoList, str)
				zones = append(zones, topoVal)
			}
		}
		framework.Logf("topoList: %v", topoList)
		annotationVal := "[" + topoList[0] + "]" // just selecting one topology
		pvcAnnotations[tkgHARequestedAnnotationKey] = annotationVal
		framework.Logf("annotationVal :%s, pvcAnnotations: %v", annotationVal, pvcAnnotations)

		ginkgo.By("Create a PVC say pvc1 under zone2")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcSpec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", storageclass, nil, "")
		pvcSpec.Annotations = pvcAnnotations
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvcSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for PVC to come to bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID := pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeID))
		}()

		ginkgo.By("Verify PV has required PV node affinity details")
		_, err = verifyVolumeTopologyForLevel5(pvs[0], allowedTopologyHAMap)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("PV %s has required node affinity details", pvs[0].Name)

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM")
		vm := createVmServiceVmWithPvcsWithZone(ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi,
			storageClassName, secretName, zones[1]) // wrong zone
		defer func() {
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			wait4VmSvcVm2BeDeleted(ctx, vmopC, vm)
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("verify that vm1 does not come up")
		_, err = waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("remove pvc1 from vm1 spec")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm.Spec.Volumes = nil
		err = vmopC.Update(ctx, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the vm1 comes up now")
		_, err = waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Attach pvc2 to vm1 and verify that the volume attachment fails")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm.Spec.Volumes = []vmopv1.VirtualMachineVolume{{
			Name: pvc.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
			}}}
		err = vmopC.Update(ctx, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = wait4PvcAttachmentFailure(ctx, vmopC, vm, pvc)
		framework.Logf("Error found: %s", err.Error())
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

})
