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

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/types"

	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
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
		client                     clientset.Interface
		namespace                  string
		datastoreURL               string
		storagePolicyName          string
		storageClassName           string
		storageProfileId           string
		vcRestSessionId            string
		vmi                        string
		vmClass                    string
		vmopC                      ctlrclient.Client
		cnsopC                     ctlrclient.Client
		isVsanHealthServiceStopped bool
		isSPSserviceStopped        bool
		vcAddress                  string
		restConfig                 *restclient.Config
		snapc                      *snapclient.Clientset
		pandoraSyncWaitTime        int
		err                        error
		dsRef                      types.ManagedObjectReference
		labelsMap                  map[string]string
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// client connection
		client = f.ClientSet
		bootstrap()

		// fetch the testbed type for executing testcases
		topologyFeature := os.Getenv(topologyFeature)

		// fetching nodes and reading storage policy name
		if topologyFeature != topologyTkgHaName {
			nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}
			storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		} else {
			storagePolicyName = GetAndExpectStringEnvVar(envZonalStoragePolicyName)
		}

		// fetching vc ip and creating creating vc session
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		vcRestSessionId = createVcSession4RestApis(ctx)

		// reading storage class name for wcp setup "wcpglobal_storage_profile"
		storageClassName = strings.ReplaceAll(storagePolicyName, "_", "-") // since this is a wcp setup

		// fetching shared datastore url
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)

		// reading datastore morf reference
		dsRef = getDsMoRefFromURL(ctx, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		// reading storage profile id of "wcpglobal_storage_profile"
		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)

		/* creating/reading content library
		   "https://wp-content-pstg.broadcom.com/vmsvc/lib.json" */
		contentLibId := createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
			dsRef.Value, GetAndExpectStringEnvVar(envContentLibraryUrlSslThumbprint))

		/*
		   [ ~ ]# kubectl get vmclass -n csi-vmsvcns-2227
		   NAME                CPU   MEMORY
		   best-effort-small   2     4Gi

		   [ ~ ]# kubectl get vmclass best-effort-small -n csi-vmsvcns-2227 -o jsonpath='{.spec}' | jq
		*/
		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}

		framework.Logf("Create a WCP namespace for the test")
		// creating wcp test namespace and setting vmclass, contlib, storage class fields in test ns
		namespace = createTestWcpNs(
			vcRestSessionId, storageProfileId, vmClass, contentLibId, getSvcId(vcRestSessionId))

		// Get snapshot client using the rest config
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// setting resource quota for storage policy tagged to supervisor namespace
		//setStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, rqLimitScaleTest)

		// creating vm schema
		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/*
		   *** reading vm image name "ubuntu-2004-cloud-init-21.4-kube-v1.20.10" ***

		   [ ~ ]# kubectl get vmimage -o wide -n csi-vmsvcns-2227 | grep ubuntu-2004-cloud-init-21.4-kube-v1.20.10
		   vmi-819319608e5ba43d1   ubuntu-2004-cloud-init-21.4-kube-v1.20.10     OVF    kube-v1.20.10   ubuntu64Guest

		   [ ~ ]# kubectl get vmimage vmi-819319608e5ba43d1 -n csi-vmsvcns-2227 -o jsonpath='{.spec}' | jq
		*/
		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi = waitNGetVmiForImageName(ctx, vmopC, namespace, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())

		// reading full sync wait time
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		//setting map values
		labelsMap = make(map[string]string)
		labelsMap["app"] = "test"
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
	   Testcase-1
	   Dynamic PVC → VM → Snapshot
	   Steps:
	   1. Create a PVC using the storage class (storage policy) tagged to the supervisor namespace
	   2. Wait for PVC to reach the Bound state.
	   3. Create a VM service VM using the PVC created in step #1
	   4. Wait for the VM service to be up and in the powered-on state.
	   5. Once the VM is up, verify that the volume is accessible inside the VM
	   6. Write some data into the volume.
	   7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   8. Create a volume snapshot for the PVC created in step #1.
	   9. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   10. Verify CNS metadata for a PVC
	   11. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("VMM1", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvc, pvs, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		/*
		   [ ~ ]# kubectl get secret -n csi-vmsvcns-2227
		   NAME                TYPE     DATA   AGE
		   vm-bootstrap-data   Opaque   1      72s

		   [ ~ ]# kubectl get secret -n csi-vmsvcns-2227 -o yaml
		*/
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM")
		/*
		   [ ~ ]# kubectl get vm -o wide -n csi-vmsvcns-2227
		   NAME               POWER-STATE   CLASS               IMAGE                   PRIMARY-IP4   AGE
		   csi-test-vm-2668   PoweredOn     best-effort-small   vmi-819319608e5ba43d1                 94s

		   [ ~ ]# kubectl describe vm -n csi-vmsvcns-2227
		*/
		vm := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		/*
		   [ ~ ]# kubectl get network -n csi-vmsvcns-2227
		   NAME      AGE
		   primary   21m

		   [ ~ ]# kubectl get network -n csi-vmsvcns-2227 -o jsonpath='{.items[0].spec}' | jq

		   [ ~ ]# kubectl get vmservice -n csi-vmsvcns-2227
		   NAME                   TYPE           AGE
		   csi-test-vm-2668-svc   LoadBalancer   2m17s
		   root@4203ec75780f15c3cd295b6bad330232 [ ~ ]#

		   [ ~ ]# kubectl get svc -n csi-vmsvcns-2227
		   NAME                   TYPE           CLUSTER-IP       EXTERNAL-IP     PORT(S)   AGE
		   csi-test-vm-2668-svc   LoadBalancer   172.24.176.180   192.168.130.7   22/TCP    2m32s
		   root@4203ec75780f15c3cd295b6bad330232 [ ~ ]#

		   [ ~ ]# kubectl get endpoints -n csi-vmsvcns-2227
		   NAME                   ENDPOINTS   AGE
		   csi-test-vm-2668-svc   <none>      2m40s
		   root@4203ec75780f15c3cd295b6bad330232 [ ~ ]#
		*/
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
		/*
		   [ ~ ]# kubectl get cnsnodevmattachment -n csi-vmsvcns-2227  -o yaml
		*/
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
			verifyDataIntegrityOnVmDisk(vmIp, volFolder)
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
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-2
	   Static PVC → VM → Snapshot
	   Steps:
	   1. Create FCD
	   2. Create a static PV and PVC using cns register volume API
	   3. Wait for PV and PVC to reach the Bound state.
	   4. Create a VM service VM using the PVC created in step #2
	   5. Wait for the VM service to be up and in the powered-on state.
	   6. Once the VM is up, verify that the volume is accessible inside the VM
	   7. Write some data into the volume.
	   8. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   9. Create a volume snapshot for the PVC created in step #1.
	   10. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   11. Verify CNS metadata for a PVC
	   12. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("VMM2", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Creating FCD Disk")
		fcdID, err := e2eVSphere.createFCD(ctx, fcdName, diskSizeInMb, dsRef.Reference())
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
		volHandle := staticPv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		err = e2eVSphere.waitForCNSVolumeToBeCreated(staticPv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating a static PVC")
		staticPvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, staticPv.Name)
		staticPvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(
			ctx, staticPvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating VM")
		vm := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{staticPvc}, vmi, storageClassName, secretName)
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
			[]*v1.PersistentVolumeClaim{staticPvc})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
			verifyDataIntegrityOnVmDisk(vmIp, volFolder)
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
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			staticPvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-3
	   Dynamic PVC  → VM → Snapshot → RestoreVol → VM
	   Steps:
	   1. Create a dynamic PVC using the storage class (storage policy) tagged to the supervisor namespace
	   2. Wait for dynamic PVC to reach the Bound state.
	   3. Create a VM service VM using dynamic PVC.
	   4. Wait for the VM service to be up and in the powered-on state.
	   5. Once the VM is up, verify that the volume is accessible inside the VM
	   6. Write some IO to the CSI volumes, read it back from them and verify the data integrity
	   7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   8. Create a volume snapshot for the dynamic PVC created in step #1
	   9. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   10. Verify CNS metadata for a PVC
	   11. Create a new PVC from the snapshot created in step #11.
	   12. Wait for PVC to reach the Bound state.
	   13. Create a VM service VM using the PVC created in step #14
	   Wait for the VM service to be up and in the powered-on state.
	   Once the VM is up, verify that the volume is accessible inside the VM
	   Verify reading/writing data in the volume.
	   Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("VMM3", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvc, pvs, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Creating VM")
		vm := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName)
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
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
			verifyDataIntegrityOnVmDisk(vmIp, volFolder)
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume from a snapshot")
		pvc2, pv2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass, volumeSnapshot, diskSize, false)
		volHandle2 := pv2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM")
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc2}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc2 := createService4Vm(ctx, vmopC, namespace, vm2.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VM to come up and get an IP")
		vmIp2, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvc2})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm2.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp2)
			verifyDataIntegrityOnVmDisk(vmIp2, volFolder)
		}

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Testcase-4
		Dynamic PVC → VM → Snapshot1 and Snapshot2 → PVC1 and PVC2 → VM
		Steps:
		1. Create a PVC using the storage class (storage policy) tagged to the supervisor namespace
		2. Wait for PVC to reach the Bound state.
		3. Create a VM service VM using the PVC created in step #1
		4. Wait for the VM service to be up and in the powered-on state.
		5. Once the VM is up, verify that the volume is accessible inside the VM
		6. Write some IO to the CSI volumes, read it back from them and verify the data integrity
		7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
		8. Take 2 snapshots (snapshot-1, snapshot-2) for the PVC created in step #1.
		9. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
		10. Verify CNS metadata for a PVC created in step #1
		11. Create PVC-1 from Snapshot-1, PVC-2 from Snapshot-2
		12. Wait for PVCs to reach the Bound state.
		13. Create a VM service VM using pvc-1 and pvc-2 created in step #11
		14. Wait for the VM service to be up and in the powered-on state.
		15. Once the VM is up, verify that the volume is accessible inside the VM
		16. Write some IO to the CSI volumes, read it back from them and verify the data integrity
		17. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("VMM4", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvc, pvs, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Creating VM")
		vm := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName)
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
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
			verifyDataIntegrityOnVmDisk(vmIp, volFolder)
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot-1 for the volume")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot-2 for the volume")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume from a snapshot")
		pvc2, pv2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass, volumeSnapshot1, diskSize, false)
		volHandle2 := pv2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		ginkgo.By("Create a volume from a snapshot")
		pvc3, pv3, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass, volumeSnapshot2, diskSize, false)
		volHandle3 := pv3[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle3).NotTo(gomega.BeEmpty())

		ginkgo.By("Creating VM")
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc2, pvc3}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc2 := createService4Vm(ctx, vmopC, namespace, vm2.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VM to come up and get an IP")
		vmIp2, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvc2, pvc3})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm2.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp2)
			verifyDataIntegrityOnVmDisk(vmIp2, volFolder)
		}

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated1, snapshotContentCreated1, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot1, pandoraSyncWaitTime, volHandle, snapshotId1, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated2, snapshotContentCreated2, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot2, pandoraSyncWaitTime, volHandle, snapshotId2, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-5
	   Offline  resize
	   PVC → VM → Snapshot → Delete VM → restore snapshot → Offline restored PVC resize → Create new VM  → Snapshot

	   Steps:
	   1. Create a PVC using the storage class (storage policy) tagged to the supervisor namespace
	   2. Wait for PVC to reach the Bound state.
	   3. Create a VM service VM using the PVC created in step #1
	   4. Wait for the VM service to be up and in the powered-on state.
	   5. Once the VM is up, verify that the volume is accessible inside the VM
	   6. Write some data into the volume.
	   7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   8. Take snapshots of the PVC created in step #1
	   9. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   10. Verify CNS metadata for a PVC created in step #1
	   11. Delete VM service VM created in step #3.
	   12. Delete volume snapshot
	   13. Perform offline volume resizing and verify that the operation went successfully.
	   14. Create a new VM service VM with PVC created in step #1
	   15. Once the VM is up, verify that the volume is accessible and the filesystem on the volume has expanded.
	   16. Perform online volume resizing and verify that the operation went successfully.
	   17. Take a snapshot of the PVC created in step #1.
	   18. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   19. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("VMM5", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvc, pvs, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Creating VM")
		vm := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName)
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
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
			verifyDataIntegrityOnVmDisk(vmIp, volFolder)
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot-1 for the volume")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume from a snapshot")
		pvc2, pv2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass, volumeSnapshot1, diskSize, false)
		volHandle2 := pv2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		ginkgo.By("Expanding the current pvc")
		currentPvcSize := pvc2.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("4Gi"))
		newDiskSize := "6Gi"
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvc2, err = expandPVCSize(pvc2, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc2).NotTo(gomega.BeNil())
		pvcSize := pvc2.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvc2.Name)
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvc2, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle2))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(6144)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb !=
			newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion +%v ",
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating VM")
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc2}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc2 := createService4Vm(ctx, vmopC, namespace, vm2.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VM to come up and get an IP")
		vmIp2, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvc2})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm2.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp2)
			verifyDataIntegrityOnVmDisk(vmIp2, volFolder)
		}

		ginkgo.By("Create volume snapshot-2 for the volume")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc2, volHandle2, newDiskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete volume snapshot-1")
		snapshotCreated1, snapshotContentCreated1, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot1, pandoraSyncWaitTime, volHandle, snapshotId1, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete volume snapshot-2")
		snapshotCreated2, snapshotContentCreated2, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot2, pandoraSyncWaitTime, volHandle2, snapshotId2, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
	   Testcase-6
	   PVC-1, PVC-2 → VM-1(PVC-1), VM-2(PVC-2), VM-3(no vol attach) → VolS-1 (PVC-1), VolS-2(PVC-2) → RestoreVol-1 (PVC-1), RestoreVol-2 (PVC-2)→ Attach VM-1 (PVC-1, RestoreVol-2) -> RestoreVol-1 Attach -> VM-3

	   Steps:
	   1. Create 2 PVCs (PVC-1, PVC-2) using the storage class (storage policy) tagged to the supervisor namespace
	   2. Wait for PVCs to reach the Bound state.
	   3. Create two VM service VMs (VM-1, VM-2) such that VM-1 is attached to PVC-1, VM-2 is attached to PVC-2
	   4. Create VM-3 but not attched to any volume
	   5. Wait for the VM service to be up and in the powered-on state.
	   6. Once the VM is up, verify that the volume is accessible inside the VM
	   7. Write some data to a file in PVC-1 from VM-1 and in PVC-2 from VM-2.
	   8. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   9. Take snapshots of the PVC-1 (vols-1) and PVC-2(vols-2) created in step #1
	   10. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   11. Create a new PVC using the snapshot created in step #8. (RestoreVol-2 from vols-2)
	   12. Modify VM-1 spec to attach RestoreVol-2 to VM-1
	   13. Verify that RestoreVol-2  is accessible in VM-1 and can read and verify the contents of the file written in step 6 from VM-1
	   14. Create a new PVC using the snapshot created in step #8. (RestoreVol-1 from vols-1)
	   15. Attach RestoreVol-1 to VM-3
	   16. Verify reading writing data in the volume from VM3
	   17. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("VMM6", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC1")
		pvc1, pvs1, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle1 := pvs1[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle1).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create PVC2")
		pvc2, pvs2, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle2 := pvs2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM1 and attaching it to PVC1")
		vm1 := createVmServiceVmWithPvcs(ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc1}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VM1")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM1")
		vmlbsvc1 := createService4Vm(ctx, vmopC, namespace, vm1.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing services for ssh for the VM1")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VM1 to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVC1 is attached to the VM1")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1, []*v1.PersistentVolumeClaim{pvc1})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC is accessible to the VM1")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volFolder := formatNVerifyPvcIsAccessible(vm1.Status.Volumes[0].DiskUuid, 1, vmIp1)

		ginkgo.By("write some data to a file in pvc1 from vm1")
		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile1 := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		framework.Logf("Creating a 100mb test data file %v", testdataFile1)
		op, err := exec.Command(
			"bash", "-c", "dd if=/dev/urandom bs=1M count=1 | tr -dc 'a-zA-Z0-9' >"+testdataFile1).Output()
		// using 'tr' to filter out non-alphanumeric characters
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		op, err = exec.Command("md5sum", testdataFile1).Output()
		fmt.Println("md5sum", string(op[:]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		op, err = exec.Command("ls", "-l", testdataFile1).Output()
		fmt.Println(string(op[:]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile1).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		framework.Logf("Copying test data file to VM1")
		copyFileToVm(vmIp1, testdataFile1, volFolder+"/f1")

		_ = execSshOnVmThroughGatewayVm(vmIp1,
			[]string{"ls -l " + volFolder + "/f1", "md5sum " + volFolder + "/f1", "sync"})

		ginkgo.By("Creating VM2 and attaching it to PVC2")
		vm2 := createVmServiceVmWithPvcs(ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc2}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VM2")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM2")
		vmlbsvc2 := createService4Vm(ctx, vmopC, namespace, vm2.Name)
		defer func() {
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VM2 to come up and get an IP")
		vmIp2, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVC2 is attached to the VM2")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2, []*v1.PersistentVolumeClaim{pvc2})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVC is accessible to the VM2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volFolder = formatNVerifyPvcIsAccessible(vm2.Status.Volumes[0].DiskUuid, 1, vmIp2)

		ginkgo.By("write some data to a file in pvc2 from vm2")
		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile2 := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		framework.Logf("Creating a 100mb test data file %v", testdataFile2)
		op, err = exec.Command(
			"bash", "-c", "dd if=/dev/urandom bs=1M count=1 | tr -dc 'a-zA-Z0-9' >"+testdataFile2).Output()
		// using 'tr' to filter out non-alphanumeric characters
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		op, err = exec.Command("md5sum", testdataFile2).Output()
		fmt.Println("md5sum", string(op[:]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		op, err = exec.Command("ls", "-l", testdataFile2).Output()
		fmt.Println(string(op[:]))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile2).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		framework.Logf("Copying test data file to VM2")
		copyFileToVm(vmIp2, testdataFile2, volFolder+"/f1")

		_ = execSshOnVmThroughGatewayVm(vmIp2,
			[]string{"ls -l " + volFolder + "/f1", "md5sum " + volFolder + "/f1", "sync"})

		ginkgo.By("Creating VM3")
		vm3 := createVmServiceVmWithPvcs(ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VM3")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm3.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM3")
		vmlbsvc3 := createService4Vm(ctx, vmopC, namespace, vm3.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing services for ssh for the VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc3.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp3, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm3.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create snapshot-1 for PVC1")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc1, volHandle1, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot-2 for the volume")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvc2, volHandle2, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create restorevol1 from snapshot1")
		restorepvc1, restorepv1, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass, volumeSnapshot1, diskSize, false)
		restorevolHandle1 := restorepv1[0].Spec.CSI.VolumeHandle
		gomega.Expect(restorevolHandle1).NotTo(gomega.BeEmpty())

		ginkgo.By("Create restorevol1 from snapshot1")
		restorepvc2, restorepv2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass, volumeSnapshot2, diskSize, false)
		restorevolHandle2 := restorepv2[0].Spec.CSI.VolumeHandle
		gomega.Expect(restorevolHandle2).NotTo(gomega.BeEmpty())

		ginkgo.By("Modify vm1 spec to attach restorevol1 snapshot to vm1")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/* Try both ways whichever works */
		err = updateVmWithNewPvc(ctx, vmopC, vm1.Name, namespace, restorepvc2)
		restorevol2 := vmopv1.VirtualMachineVolume{
			Name: restorepvc2.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: restorepvc2.Name},
			},
		}
		vm1.Spec.Volumes = []vmopv1.VirtualMachineVolume{restorevol2}
		err = vmopC.Update(ctx, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify restorevol2 is attached to the VM1")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{restorepvc2})).To(gomega.Succeed())
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify data in restorevol2 from vm1")
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
		c := []string{"md5sum", testdataFile1, vmFileData}
		op, err = exec.Command(c[0], c[1:]...).Output()
		framework.Logf("Command: %c, output: %v", c, op)
		lines := strings.Split(string(op[:]), "\n")
		gomega.Expect(strings.Fields(lines[0])[0]).To(gomega.Equal(strings.Fields(lines[1])[0]))

		ginkgo.By("Attach restorevol1 to vm3 ")
		/* Try both ways whichever works */
		err = updateVmWithNewPvc(ctx, vmopC, vm3.Name, namespace, restorepvc1)

		restorevol1 := vmopv1.VirtualMachineVolume{
			Name: restorepvc1.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: restorepvc1.Name},
			},
		}
		vm3.Spec.Volumes = []vmopv1.VirtualMachineVolume{restorevol1}
		err = vmopC.Update(ctx, vm3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify restorevol3 is attached to the VM3")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{restorepvc1})).To(gomega.Succeed())
		vm1, err = getVmsvcVM(ctx, vmopC, vm3.Namespace, vm3.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify data in restorevol1 from vm3")
		framework.Logf("Mounting the volume")
		volFolder = mountFormattedVol2Vm(vm1.Status.Volumes[0].DiskUuid, 1, vmIp3)
		vmFileData = fmt.Sprintf("/tmp/vmdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		_ = execSshOnVmThroughGatewayVm(vmIp3, []string{"md5sum " + volFolder + "/f1"})

		framework.Logf("Fetching file from vm3")
		copyFileFromVm(vmIp3, volFolder+"/f1", vmFileData)
		defer func() {
			c := []string{"rm", "-f", vmFileData}
			op, err = exec.Command(c[0], c[1:]...).Output()
			framework.Logf("Command: %c, output: %v", c, op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		framework.Logf("Comparing file fetched from the VM with test data file")
		c = []string{"md5sum", testdataFile2, vmFileData}
		op, err = exec.Command(c[0], c[1:]...).Output()
		framework.Logf("Command: %c, output: %v", c, op)
		lines = strings.Split(string(op[:]), "\n")
		gomega.Expect(strings.Fields(lines[0])[0]).To(gomega.Equal(strings.Fields(lines[1])[0]))

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated1, snapshotContentCreated1, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot1, pandoraSyncWaitTime, volHandle1, snapshotId1, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated2, snapshotContentCreated2, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot2, pandoraSyncWaitTime, volHandle2, snapshotId2, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*Testcase-7
	  PVC →  Pod → Snapshot → VM (PVC) → should fail → Delete Pod → VM (PVC) → should succeed → RestoreVol (PVC) → Pod

	  Steps:
	  1. Create a PVC using the storage class (storage policy) tagged to the supervisor namespace
	  2. Wait for PVCs to reach the Bound state.
	  3. Create a Pod using the PVC created in step #1.
	  4. Wait for Pod to reach running state.
	  5. Write some data in the volume from Pod.
	  6. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	  7. Take a snapshot of the PVC created in step #1
	  8. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	  9. Create a VM service VM using the above PVC. It should fail.
	  Verify that an appropriate error message should be displayed.
	  Delete the Pod created in step #3 to ensure the volume is detached from the Pod.
	  Try creating a VM service again using the PVC created above.
	  Wait for the VM service to be up and in the powered-on state.
	  Once the VM is up, verify that the volume is accessible inside the VM
	  Write some data to the volume from the VM.
	  Create a new PVC from the snapshot created in step #7.
	  Wait for PVC to reach Bound state.
	  Create a new Pod and attach it to the newly created volume.
	  Confirm that the Pod reaches the running state and that read and write operations can be performed on the volume.
	  Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/
	ginkgo.It("attach a PVC attached to a pod to VM", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvclaim, pvs, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
			diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create a Pod using the volume created above and write data into the volume")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pvs[0].Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		var vmUUID string
		var exists bool

		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pvs[0].Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		output := readFileFromPod(namespace, pod.Name, filePathPod1)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		writeDataOnFileFromPod(namespace, pod.Name, filePathPod1, "Hello message from test into Pod1")
		output = readFileFromPod(namespace, pod.Name, filePathPod1)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

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

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(ctx, volHandle, pvclaim, pvs[0], pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By("Creating VM")
		vm := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{}, vmi, storageClassName, secretName)
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
			ginkgo.By("Deleting loadbalancing services for ssh for the VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("edit vm spec and try to attach pvclaim to vm1, which should fail")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm.Spec.Volumes = []vmopv1.VirtualMachineVolume{{Name: pvclaim.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name},
			}}}
		err = vmopC.Update(ctx, vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = wait4PvcAttachmentFailure(ctx, vmopC, vm, pvclaim)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("delete pod")
		deletePodsAndWaitForVolsToDetach(ctx, client, []*v1.Pod{pod}, true)

		ginkgo.By("retry to attach pvc2 to vm1 and verify it is accessible from vm1")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*v1.PersistentVolumeClaim{pvclaim})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = formatNVerifyPvcIsAccessible(vm.Status.Volumes[0].DiskUuid, 1, vmIp)

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
			verifyDataIntegrityOnVmDisk(vmIp, volFolder)
		}

		ginkgo.By("Create volume using the snapshot")
		pvclaim2, pvs2, pod2 := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot, diskSize, true)
		volHandle2 := pvs2[0].Spec.CSI.VolumeHandle
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify volume metadata for deployment pod, pvc and pv")
		err = waitAndVerifyCnsVolumeMetadata(ctx, volHandle2, pvclaim2, pvs2[0], pod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*Testcase-8

		vSAN-Health and SPS service down
		PVC → VM → Multiple Snapshots(vols-1, vols-2, vols-3) and in parallel vSAN Health service down → snapshot should not pass → start vsan-health service → Snapshot Verification → Restore Vols(ResVol-1, ResVol-2, ResVol-3) → New VM(ResVol-1, ResVol-2) → sps service down → Attach RestoreVol (ResVol-3)) → new VM → should not pass → bring up services → volume  attachment should pass


			  Steps:
	    Create a PVC using the storage class (storage policy) tagged to the supervisor namespace
	    Wait for PVC to reach the Bound state.
	    Create a VM service VM using the PVC created in step #1
	    Wait for the VM service to be up and in the powered-on state.
	    Once the VM is up, verify that the volume is accessible inside the VM
	    Write some data into the volume.
	    Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	    Create multiple volume snapshots(vols-1, vols-2, vols-3) for the PVC created in step #1.
	    Stop the vSAN Health service while a snapshot creation operation is in progress.
	    Snapshot creation readyToUse status should be 'false' and creation should be stuck. Verify the error message.
	    Bring up the vsan-health service.
	    Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	    Create PVCs (ResVol-1, ResVol-2, ResVol-3) from the snapshot created in step #8.
	    Wait for PVCs to reach the Bound state.
	    Create a VM service VM using the restored PVCs (ResVol-1, ResVol-2).
	    Wait for the VM service to be up and in the powered-on state.
	    Once the VM is up, verify that the volume is accessible inside the VM
	    Write some data into the volume.
	    Now bring down the SPS service
	    Attach a restored volume (ResVol-3) to the VM service created in step #15.
	    Verify that the attachment is stuck and does not go through
	    Bring up the SPS service.
	    Wait for ResVol-3 to get attached and verify volume is accessible to the VM created in step #15.
	    Perform read/write operation on the volume from VM.
	    Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("Create VM or attach/detach PVC to VM when vsan-health is down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pvcs := []*v1.PersistentVolumeClaim{}
		vms := []*vmopv1.VirtualMachine{}
		vmlbsvcs := []*vmopv1.VirtualMachineService{}

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Create a PVC")
		for i := 0; i < 3; i++ {
			pvc, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvcs = append(pvcs, pvc)
		}

		ginkgo.By("Waiting for all claims to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvcs, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete PVCs")
			for i := 0; i < 3; i++ {
				framework.Logf("Deleting pvc %s", pvcs[0].Name)
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvcs[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("Waiting for CNS volume %s to be deleted", pvs[i].Spec.CSI.VolumeHandle)
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[i].Spec.CSI.VolumeHandle)
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

		ginkgo.By("Creating VMs 2 and 3")
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{}, vmi, storageClassName, secretName)
		vm3 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvcs[2]}, vmi, storageClassName, secretName)

		vms = append(vms, vm2, vm3)

		defer func() {
			ginkgo.By("Deleting VMs")
			for _, vm := range vms {
				framework.Logf("Deleting VM %s", vm.Name)
				err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
					Name:      vm.Name,
					Namespace: namespace,
				}})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Wait for VMs 2 and 3 to come up and get an IP")
		vmIp3, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm3.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmIp2, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating loadbalancing service for ssh for vm2 and vm3")
		vmlbsvc2 := createService4Vm(ctx, vmopC, namespace, vm2.Name)
		vmlbsvc3 := createService4Vm(ctx, vmopC, namespace, vm3.Name)
		vmlbsvcs = append(vmlbsvcs, vmlbsvc2, vmlbsvc3)
		defer func() {
			ginkgo.By("Deleting loadbalancing services for ssh for the VMs")
			for _, vmlbsvc := range vmlbsvcs {
				framework.Logf("Deleting loadbalancing service %s", vmlbsvc.Name)
				err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
					Name:      vmlbsvc.Name,
					Namespace: namespace,
				}})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Verify pvc3 is attached to vm3")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm3,
			[]*v1.PersistentVolumeClaim{pvcs[2]})).To(gomega.Succeed())
		ginkgo.By("Verify pvc3 is accessible to VM3")
		vm3, err = getVmsvcVM(ctx, vmopC, vm3.Namespace, vm3.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = formatNVerifyPvcIsAccessible(vm3.Status.Volumes[0].DiskUuid, 1, vmIp3)

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", vsanhealthServiceName))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(ctx, vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isVsanHealthServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
			}
		}()

		ginkgo.By("Creating vm1 with pvc1")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvcs[0]}, vmi, storageClassName, secretName)
		vms = append(vms, vm1)

		ginkgo.By("Attach pvc2 to vm2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm2.Spec.Volumes = append(vm2.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: pvcs[1].Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvcs[1].Name},
			}})
		err = vmopC.Update(ctx, vm2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Detach pvc3 from vm3")
		vm3, err = getVmsvcVM(ctx, vmopC, vm3.Namespace, vm3.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm3.Spec.Volumes = []vmopv1.VirtualMachineVolume{}
		err = vmopC.Update(ctx, vm3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Verifying pvc1 attachment to vm1 failed")
		err = wait4PvcAttachmentFailure(ctx, vmopC, vm1, pvcs[0])
		framework.Logf("Error found: %s", err.Error())
		gomega.Expect(err).To(gomega.HaveOccurred())
		ginkgo.By("Verifying vm1 is stuck in creation")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = wait4PvcAttachmentFailure(ctx, vmopC, vm1, pvcs[0])
		framework.Logf("Error found: %s", err.Error())
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(vm1.Status.PowerState).To(gomega.Equal(vmopv1.VirtualMachinePoweredOff))

		ginkgo.By("Verifying pvc2 attachment to vm2 failed")
		err = wait4PvcAttachmentFailure(ctx, vmopC, vm2, pvcs[1])
		framework.Logf("Error found: %s", err.Error())
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Verify pvc3 is still attached to vm3")
		gomega.Expect(
			verifyPvcsAreAttachedToVmsvcVm(ctx, cnsopC, vm3, []*v1.PersistentVolumeClaim{pvcs[2]})).To(gomega.BeTrue())

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By("Wait for VM1 to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating loadbalancing service for ssh for vm1")
		vmlbsvc1 := createService4Vm(ctx, vmopC, namespace, vm1.Name)
		vmlbsvcs = append(vmlbsvcs, vmlbsvc1)

		ginkgo.By("Verify pvc1 is attached to VM1")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvcs[0]})).To(gomega.Succeed())
		ginkgo.By("Verify pvc1 is accessible to VM1")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = formatNVerifyPvcIsAccessible(vm1.Status.Volumes[0].DiskUuid, 1, vmIp1)

		ginkgo.By("Verify pvc2 is attached to VM2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvcs[1]})).To(gomega.Succeed())
		ginkgo.By("Verify pvc2 is accessible to VM2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = formatNVerifyPvcIsAccessible(vm2.Status.Volumes[0].DiskUuid, 1, vmIp2)

		ginkgo.By("Verify pvc3 is detached from VM3")
		wait4Pvc2Detach(ctx, vmopC, vm3, pvcs[2])

	})
})
