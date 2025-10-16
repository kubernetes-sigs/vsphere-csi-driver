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
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/types"
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

var _ bool = ginkgo.Describe("[snapshot-vmsvc] Snapshot VM Service VM", func() {

	f := framework.NewDefaultFramework("vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                     clientset.Interface
		namespace                  string
		datastoreURL               string
		storageClassName           string
		storageProfileId           string
		vcRestSessionId            string
		vmi                        string
		vmClass                    string
		vmopC                      ctlrclient.Client
		cnsopC                     ctlrclient.Client
		isVsanHealthServiceStopped bool
		isSPSserviceStopped        bool
		restConfig                 *restclient.Config
		snapc                      *snapclient.Clientset
		pandoraSyncWaitTime        int
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
		if topologyFeature != topologyTkgHaName && topologyFeature != podVMOnStretchedSupervisor {
			nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}
			storageClassName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		} else {
			storageClassName = GetAndExpectStringEnvVar(envZonalStoragePolicyName)
		}

		// creating vc session
		vcRestSessionId = createVcSession4RestApis(ctx)

		// fetching shared datastore url
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)

		// reading datastore morf reference
		dsRef = getDsMoRefFromURL(ctx, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		// reading storage profile id of "wcpglobal_storage_profile"
		storageProfileId = e2eVSphere.GetSpbmPolicyID(storageClassName)

		// creating/reading content library
		contentLibId, err := createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
			dsRef.Value, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}

		framework.Logf("Create a WCP namespace for the test")
		// creating wcp test namespace and setting vmclass, contlib, storage class fields in test ns
		namespace = createTestWcpNs(
			vcRestSessionId, storageProfileId, vmClass, contentLibId, getSvcId(vcRestSessionId, &e2eVSphere))

		framework.Logf("Verifying storage policies usage for each storage class")
		restConfig = getRestConfigClient()
		ListStoragePolicyUsages(ctx, client, restConfig, namespace, []string{storageClassName})

		// creating vm schema
		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// reading vm image name
		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi = waitNGetVmiForImageName(ctx, vmopC, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())

		// Get snapshot client using the rest config
		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

	ginkgo.It("[cf-wcp] Taking snapshot of a vm service vm attached to a dynamic "+
		"volume", ginkgo.Label(p0, block, wcp, snapshot, vmServiceVm, vc80), func() {

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

		isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
		if !isPrivateNetwork {
			ginkgo.By("Verify PVCs are accessible to the VM")
			ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
			vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for i, vol := range vm.Status.Volumes {
				if vol.Name == pvc.Name {
					volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
					verifyDataIntegrityOnVmDisk(vmIp, volFolder)
				}
			}
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
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

	ginkgo.It("[ef-f-vmsvc] Taking snapshot of a vm service vm attached to a static "+
		"volume", ginkgo.Label(p0, block, wcp, snapshot, vmServiceVm, vc80), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var fcdID string
		curr_time := time.Now().Unix()
		curTimeString := strconv.FormatInt(curr_time, 10)
		pvcName := "cns-pvc-" + curTimeString
		framework.Logf("pvc name :%s", pvcName)

		ginkgo.By("Creating FCD (CNS Volume)")
		fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
			"staticfcd"+curTimeString, storageProfileId, diskSizeInMb, dsRef.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create CNS register volume with above created FCD ")
		cnsRegisterVolume := getCNSRegisterVolumeSpec(ctx, namespace, fcdID, "", pvcName, v1.ReadWriteOnce)
		err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx, restConfig,
			namespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

		ginkgo.By(" verify created PV, PVC and check the bidirectional reference")
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := getPvFromClaim(client, namespace, pvcName)
		volHandle := pv.Spec.CSI.VolumeHandle
		verifyBidirectionalReferenceOfPVandPVC(ctx, client, pvc, pv, fcdID)

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
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId, false)
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

	ginkgo.It("[ef-vmsvc] Restoring snapshot and attaching it to a vm service "+
		"vm", ginkgo.Label(p0, block, wcp, snapshot, vmServiceVm, vc80), func() {

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
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
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

	ginkgo.It("[ef-vmsvc] Restoring multiple snapshots and attaching it to a single vm service "+
		"vm", ginkgo.Label(p1, block, wcp, snapshot, vmServiceVm, vc80), func() {

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
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Create a dynamic volume snapshot-2 for the volume")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
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
	   PVC → VM → Snapshot → Delete VM → restore snapshot →
	   Offline restored PVC resize → Create new VM  → Snapshot

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

	ginkgo.It("[ef-vmsvc] Offline volume expansion and later attaching it to a vm service "+
		"vm", ginkgo.Label(p1, block, wcp, snapshot, vmServiceVm, vc80), func() {

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
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a volume from a snapshot")
		pvc2, pv2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass, volumeSnapshot1, diskSize, false)
		volHandle2 := pv2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

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
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+2, vmIp2)
			verifyDataIntegrityOnVmDisk(vmIp2, volFolder)
		}

		ginkgo.By("Create volume snapshot-2 for the volume")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc2, volHandle2, newDiskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
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
	   PVC-1, PVC-2 → VM-1(PVC-1), VM-2(PVC-2), VM-3(no vol attach) → VolS-1 (PVC-1), VolS-2(PVC-2) →
	   RestoreVol-1 (PVC-1), RestoreVol-2 (PVC-2)→ Attach VM-1 (PVC-1, RestoreVol-2) ->
	   RestoreVol-1 Attach -> VM-3

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
	   13. Verify that RestoreVol-2  is accessible in VM-1 and can read and verify the
	   contents of the file written in step 6 from VM-1
	   14. Create a new PVC using the snapshot created in step #8. (RestoreVol-1 from vols-1)
	   15. Attach RestoreVol-1 to VM-3
	   16. Verify reading writing data in the volume from VM3
	   17. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("[ef-vmsvc] Attaching new restore snapshots to vm service vms", ginkgo.Label(p1, block,
		wcp, snapshot, vmServiceVm, vc80), func() {

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

		ginkgo.By("Creating VMs")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc1}, vmi, storageClassName, secretName)
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc2}, vmi, storageClassName, secretName)
		vm3 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					framework.Logf("virtualmachines.vmoperator.vmware.com not found")
				} else {
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "VM deletion failed with error: %s", err)
				}
			}

			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					framework.Logf("virtualmachines.vmoperator.vmware.com not found")
				} else {
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "VM deletion failed with error: %s", err)
				}
			}

			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm3.Name,
				Namespace: namespace,
			}})
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					framework.Logf("virtualmachines.vmoperator.vmware.com not found")
				} else {
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "VM deletion failed with error: %s", err)
				}
			}
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc1 := createService4Vm(ctx, vmopC, namespace, vm1.Name)
		vmlbsvc2 := createService4Vm(ctx, vmopC, namespace, vm2.Name)
		vmlbsvc3 := createService4Vm(ctx, vmopC, namespace, vm3.Name)
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

			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc3.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmIp2, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmIp3, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm3.Name)
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

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create snapshot-1 for PVC1")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc1, volHandle1, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Create a dynamic volume snapshot-2 for the volume")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc2, volHandle2, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create restorevol1 from snapshot1")
		restorepvc1, restorepv1, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot1, diskSize, false)
		restorevolHandle1 := restorepv1[0].Spec.CSI.VolumeHandle
		gomega.Expect(restorevolHandle1).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorepvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restorevolHandle1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create restorevol2 from snapshot2")
		restorepvc2, restorepv2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot2, diskSize, false)
		restorevolHandle2 := restorepv2[0].Spec.CSI.VolumeHandle
		gomega.Expect(restorevolHandle2).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorepvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restorevolHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Modify VM1 spec to attach restore snapshot-2 to VM1")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = updateVmWithNewPvc(ctx, vmopC, vm1.Name, namespace, restorepvc2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify restorevol2 is attached to the VM1")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc1, restorepvc2})).To(gomega.Succeed())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		// Refresh VM information
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Format the PVC and verify accessibility
		volFolder := formatNVerifyPvcIsAccessible(vm1.Status.Volumes[0].DiskUuid, 2, vmIp1)
		// Verify data integrity on the VM disk
		verifyDataIntegrityOnVmDisk(vmIp1, volFolder)

		ginkgo.By("Attach restore snapshot-1 to vm3")
		vm3, err = getVmsvcVM(ctx, vmopC, vm3.Namespace, vm3.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = updateVmWithNewPvc(ctx, vmopC, vm3.Name, namespace, restorepvc1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify restore snapshot-1 is attached to the VM3")
		vm3, err = getVmsvcVM(ctx, vmopC, vm3.Namespace, vm3.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm3,
			[]*v1.PersistentVolumeClaim{restorepvc1})).To(gomega.Succeed())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm3, err = getVmsvcVM(ctx, vmopC, vm3.Namespace, vm3.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm3.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp3)
			verifyDataIntegrityOnVmDisk(vmIp3, volFolder)
		}

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

		err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
			Name:      vm3.Name,
			Namespace: namespace,
		}})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
	  PVC →  Pod → Snapshot → VM (PVC) → should fail → Delete Pod → VM (PVC) →
	  should succeed → RestoreVol (PVC) → Pod

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
	ginkgo.It("[ef-f-vmsvc] Attaching same volume to a pod and vm service vm", ginkgo.Label(p1, block, wcp, snapshot,
		vmServiceVm, vc80), func() {

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

		ginkgo.By("Create a volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvclaim, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {

			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
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

	/*
	   Testcase-9
	   Power on-off VM

	   Workflow Path:

	   PVC → VM1 → Snapshot → Power-off VM1 → VM2 (PVC) → should fail → RestoreVol → VM2(RestoreVol) → should succeed
	   1. Create a PVC using the storage class (storage policy) tagged to the supervisor namespace
	   2. Wait for PVC to reach the Bound state.
	   3. Create a VM service VM (VM-1) and attach it to the PVC created in step #1.
	   4. Wait for the VM service to be up and in the powered-on state.
	   5. Once the VM is up, verify that the volume is accessible inside the VM
	   6. Write some data into the volume.
	   7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   8. Take a snapshot of the PVC created in step #1.
	   9. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   10. Now, Power off VM-1
	   11. Create a new VM service VM (VM-2) and attach it to the PVC created in step #1, it should fail.
	   Verify an appropriate error message should be displayed.
	   12. Create a new PVC from the snapshot created in step #8
	   13. Edit the VM-2 spec and attach it to the volume created in step #12.
	   14. Wait for the VM service to be up and in the powered-on state.
	   15. Once the VM is up, verify that the volume is accessible inside the VM
	   16. Write some data into the volume.
	   17. Power on VM-1 and verify it comes up fine
	   18. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("[pq-neg-vmsvc] Power on and off operation on a vm service vm with snapshot", ginkgo.Label(p0, block,
		wcp, snapshot, vmServiceVm, vc80), func() {

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
		ginkgo.By("Creating VMs")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName)
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
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

		ginkgo.By("Wait and verify PVC is attached to the VM1")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm1.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp1)
			verifyDataIntegrityOnVmDisk(vmIp1, volFolder)
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Power off vm1")
		vm1 = setVmPowerState(ctx, vmopC, vm1, vmopv1.VirtualMachinePoweredOff)
		vm1, err = wait4Vm2ReachPowerStateInSpec(ctx, vmopC, vm1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("edit vm2 spec and try to attach pvc to vm2, which should fail")
		vm2.Spec.Volumes = append(vm2.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: pvc.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
			}})
		err = vmopC.Update(ctx, vm2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = wait4PvcAttachmentFailure(ctx, vmopC, vm2, pvc)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Removing the volume attached to Vm1 from vm2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm2.Spec.Volumes = nil
		err = vmopC.Update(ctx, vm2)
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		ginkgo.By("modify vm2 spec to attach restore volume to vm2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vm2.Spec.Volumes = append(vm2.Spec.Volumes, vmopv1.VirtualMachineVolume{Name: pvc2.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc2.Name},
			}})
		err = vmopC.Update(ctx, vm2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait and verify PVC2 is attached to the VM2")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvc2})).To(gomega.Succeed())
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
	   Testcase-8

	   vSAN-Health and SPS service down

	   Workflow Path:
	   PVC → VM → Multiple Snapshots(vols-1, vols-2, vols-3) and in parallel vSAN Health service down →
	   snapshot should not pass → start vsan-health service → Snapshot Verification →
	   Restore Vols(ResVol-1, ResVol-2, ResVol-3) → New VM(ResVol-1, ResVol-2) →
	   sps service down → Attach RestoreVol (ResVol-3)) →
	   new VM → should not pass → bring up services → volume attachment should pass

	   1. Create a PVC using the storage class (storage policy) tagged to the supervisor namespace
	   2. Wait for PVC to reach the Bound state.
	   3. Create a VM service VM using the PVC created in step #1
	   4. Wait for the VM service to be up and in the powered-on state.
	   5. Once the VM is up, verify that the volume is accessible inside the VM
	   6. Write some data into the volume.
	   7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   8. Create multiple volume snapshots(vols-1, vols-2, vols-3) for the PVC created in step #1.
	   9. Stop the vSAN Health service while a snapshot creation operation is in progress.
	   10. Snapshot creation readyToUse status should be 'false' and creation should be stuck. Verify the error message.
	   11. Bring up the vsan-health service.
	   12. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   13. Create PVCs (ResVol-1, ResVol-2, ResVol-3) from the snapshot created in step #8.
	   14. Wait for PVCs to reach the Bound state.
	   15. Create a VM service VM using the restored PVCs (ResVol-1, ResVol-2).
	   16. Wait for the VM service to be up and in the powered-on state.
	   17. Once the VM is up, verify that the volume is accessible inside the VM
	   18. Write some data into the volume.
	   19. Now bring down the SPS service
	   20. Attach a restored volume (ResVol-3) to the VM service created in step #15.
	   21. Verify that the attachment is stuck and does not go through
	   22. Bring up the SPS service.
	   23. Wait for ResVol-3 to get attached and verify volume is accessible to the VM created in step #15.
	   24. Perform read/write operation on the volume from VM.
	   25. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("pq-neg-vmsvc] vCenter services down operation during snapshot creation and restoration taken for "+
		"a vm service vm", ginkgo.Label(p1, block, wcp, snapshot, vmServiceVm, negative, vc80), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		volumeOpsScale := 3
		var snapshotIds []string
		volumesnapshots := make([]*snapV1.VolumeSnapshot, volumeOpsScale)
		snapshotContents := make([]*snapV1.VolumeSnapshotContent, volumeOpsScale)

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

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		ginkgo.By("Create a volume snapshot")
		framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating snapshot %v", i)
			volumesnapshots[i], _ = snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvc.Name), metav1.CreateOptions{})
			framework.Logf("Volume snapshot name is : %s", volumesnapshots[i].Name)
		}

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		for i := 0; i < volumeOpsScale; i++ {
			ginkgo.By("Verify volume snapshot is created")
			volumesnapshots[i], err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumesnapshots[i].Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(volumesnapshots[i].Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContents[i], err = snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*volumesnapshots[i].Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(*snapshotContents[i].Status.ReadyToUse).To(gomega.BeTrue())

			framework.Logf("Get volume snapshot ID from snapshot handle")
			snapshotId, _, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, snapshotContents[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotIds = append(snapshotIds, snapshotId)
		}
		defer func() {
			for i := 0; i < volumeOpsScale; i++ {
				ginkgo.By("Delete dynamic volume snapshot")
				_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
					volumesnapshots[i], pandoraSyncWaitTime, volHandle, snapshotIds[i], true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create volume-1 from snapshot-1")
		pvc1, pv1, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumesnapshots[0], diskSize, false)
		volHandle1 := pv1[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle1).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume-2 from snapshot-2")
		pvc2, pv2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumesnapshots[1], diskSize, false)
		volHandle2 := pv2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle1).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create volume-3 from snapshot-3")
		pvc3, pv3, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumesnapshots[2], diskSize, false)
		volHandle3 := pv3[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle3).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc3.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", spsServiceName))
		isSPSserviceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(ctx, spsServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if isSPSserviceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", spsServiceName))
				startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSserviceStopped)
			}
		}()

		ginkgo.By("Creating VM-1")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc1, pvc2}, vmi, storageClassName, secretName)
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc3}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verifying vm1 and vm2 is stuck in creation")
		framework.Logf("sleeping for a min...")
		time.Sleep(time.Minute)
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(vm1.Status.PowerState).NotTo(gomega.Equal(vmopv1.VirtualMachinePoweredOn))

		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(vm2.Status.PowerState).NotTo(gomega.Equal(vmopv1.VirtualMachinePoweredOn))

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", spsServiceName))
		startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSserviceStopped)

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

			ginkgo.By("Deleting loadbalancing services for ssh for the VMs")
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

		ginkgo.By("Verify pvc1 and pvc2 is attached to VM1")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc1, pvc2})).To(gomega.Succeed())

		ginkgo.By("Verify pvc3 is attached to VM2")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvc3})).To(gomega.Succeed())

		ginkgo.By("Verify pvc1 and pvc2 is accessible to VM1")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = formatNVerifyPvcIsAccessible(vm1.Status.Volumes[0].DiskUuid, 1, vmIp1)
		_ = formatNVerifyPvcIsAccessible(vm1.Status.Volumes[1].DiskUuid, 1, vmIp1)

		ginkgo.By("Verify pvc3 is accessible to VM2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = formatNVerifyPvcIsAccessible(vm2.Status.Volumes[0].DiskUuid, 1, vmIp2)
	})

	/*
	   Testcase-10
	   vMotion volume from one DS to another

	   Workflow Path:
	   PVC → Relocate PVC -> VM-1 → Snapshot →  RestoreVol -> Reloacte RestoreVol -> VM-2 -> Snapshot

	   1. Create 2 PVCs (PVC-1, PVC-2) using the storage class (storage policy) tagged to the supervisor namespace
	   2. Wait for PVCs to reach the Bound state.
	   3. Create 2 VM service VMs. Attach VM-1 to PVC-1, VM-2 to PVC-2.
	   4. Wait until the VM service is up and in a powered-on state
	   5. Once the VM is up, verify that the volume is accessible inside the VM
	   6. Write some data into the volume.
	   7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   8. Take snapshots (vols-1, vols-2) of the PVCs created in step #1.
	   9. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   10. Relocate PVC-1 to a different datastore.
	   11. Verify that PVC-1 is still accessible.
	   12. Take a snapshot of relocated PVC-1.
	   13. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   14. Create a new PVC from the volume snapshot created in step #12
	   15. Wait for PVC to reach Bound state.
	   16. Create a new VM and attach it to the restored volume.
	   17. Wait until the VM service is up and in a powered-on state
	   18. Once the VM is up, verify that the volume is accessible inside the VM
	   19. Perform read write operation in the volume.
	   Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("[pq-neg-vmsvc] Storage vmotion of a vm from one datastore to another with "+
		"snapshot", ginkgo.Label(p1, block, wcp, snapshot, vmServiceVm, vc80), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var datastoreUrls []string

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC-1")
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

		// read nfs datastore url
		nfsDatastoreUrl := os.Getenv(envSharedNFSDatastoreURL)
		if nfsDatastoreUrl == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedNFSDatastoreURL))
		}

		// read shared vmfs
		sharedVmfsDatastoreURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedVmfsDatastoreURL == "" {
			ginkgo.Skip(fmt.Sprintf("Env %v is missing", envSharedVMFSDatastoreURL))
		}
		datastoreUrls = append(datastoreUrls, nfsDatastoreUrl, sharedVmfsDatastoreURL)

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent := fetchDsUrl4CnsVol(e2eVSphere, volHandle)
		framework.Logf("Volume: %s is present on %s", volHandle, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(volHandle, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl := ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
				break
			}
		}

		ginkgo.By("Relocate FCD to another datastore")
		dsRefDest := getDsMoRefFromURL(ctx, destDsUrl)
		_, err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, volHandle, dsRefDest, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		defer func() {
			ginkgo.By("Deleting VM bootstrap data")
			err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VMs")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc1 := createService4Vm(ctx, vmopC, namespace, vm1.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing services for ssh for the VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to respective VMs")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc})).To(gomega.Succeed())

		ginkgo.By("Verify PVCs are accessible to respective VMs")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = formatNVerifyPvcIsAccessible(vm1.Status.Volumes[0].DiskUuid, 1, vmIp1)

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create volume from snapshot")
		restorepvc, restorepv, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot1, diskSize, false)
		restoreVolHandle := restorepv[0].Spec.CSI.VolumeHandle
		gomega.Expect(restoreVolHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorepvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restoreVolHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify if VolumeID is created on the given datastores")
		dsUrlWhereVolumeIsPresent = fetchDsUrl4CnsVol(e2eVSphere, restoreVolHandle)
		framework.Logf("Volume: %s is present on %s", restoreVolHandle, dsUrlWhereVolumeIsPresent)
		e2eVSphere.verifyDatastoreMatch(restoreVolHandle, datastoreUrls)

		// Get the destination ds url where the volume will get relocated
		destDsUrl = ""
		for _, dsurl := range datastoreUrls {
			if dsurl != dsUrlWhereVolumeIsPresent {
				destDsUrl = dsurl
				break
			}
		}

		ginkgo.By("Relocate FCD to another datastore")
		dsRefDest = getDsMoRefFromURL(ctx, destDsUrl)
		_, err = e2eVSphere.cnsRelocateVolume(e2eVSphere, ctx, restoreVolHandle, dsRefDest, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, restorepvc, restoreVolHandle, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating VMs")
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{restorepvc}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc2 := createService4Vm(ctx, vmopC, namespace, vm2.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing services for ssh for the VMs")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc2.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp2, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to respective VMs")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{restorepvc})).To(gomega.Succeed())

		ginkgo.By("Verify PVCs are accessible to respective VMs")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_ = formatNVerifyPvcIsAccessible(vm1.Status.Volumes[0].DiskUuid, 1, vmIp2)

		ginkgo.By("Delete snapshot-1")
		snapshotCreated1, snapshotContentCreated1, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot1, pandoraSyncWaitTime, volHandle, snapshotId1, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete snapshot-2")
		snapshotCreated2, snapshotContentCreated2, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot2, pandoraSyncWaitTime, restoreVolHandle, snapshotId2, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-11
	   Workflow Path:

	   PVC-1, PVC-2 → VM-1(PVC-1), VM-2(PVC-2) → Snapshot (volS-1,volS-2 of PVC-1, pvc-2) →
	   RestoreVol (PVC-3, PVC-4) → Attach RestoreVol1 (VM-1), RestoreVol2 (VM-2) → verify data

	   1. Create 2 PVCs (PVC-1, PVC-2) using the storage class (storage policy) tagged to the supervisor namespace
	   2. Wait for PVCs to reach the Bound state.
	   3. Create 2 VM service VMs. Attach VM-1 to PVC-1, VM-2 to PVC-2.
	   4. Wait until the VM service is up and in a powered-on state
	   5. Once the VM is up, verify that the volume is accessible inside the VM
	   6. Write some data to PVC-1 from VM-1.
	   7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   8. Take 2 snapshots (vols-1, vols-2) of PVC-1
	   9. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   10. Create a new PVC (PVC-3, PVC-4) from the snapshots created in step #8.
	   11. Attach PVC-3 to VM-1 and PVC-4 to VM-2
	   12. Read and verify data in PVC-3 and PVC-4
	   13. Write fresh data on PVC-3 and PVC-4
	   14. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("[ef-vmsvc] Creation of multiple vm service vms and attaching it to restore snapshots", ginkgo.Label(p1,
		block, wcp, snapshot, vmServiceVm, vc80), func() {

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
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					framework.Logf("virtualmachines.vmoperator.vmware.com not found")
				} else {
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "VM deletion failed with error: %s", err)
				}
			}

			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm2.Name,
				Namespace: namespace,
			}})
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					framework.Logf("virtualmachines.vmoperator.vmware.com not found")
				} else {
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "VM deletion failed with error: %s", err)
				}
			}
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

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create snapshot-1 for PVC1")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc1, volHandle1, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dynamic volume snapshot-2 for the volume")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc2, volHandle2, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create restorevol1 from snapshot1")
		restorepvc1, restorepv1, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot1, diskSize, false)
		restorevolHandle1 := restorepv1[0].Spec.CSI.VolumeHandle
		gomega.Expect(restorevolHandle1).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorepvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restorevolHandle1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create restorevol2 from snapshot2")
		restorepvc2, restorepv2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot2, diskSize, false)
		restorevolHandle2 := restorepv2[0].Spec.CSI.VolumeHandle
		gomega.Expect(restorevolHandle2).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorepvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restorevolHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Modify VM1 spec to attach restore snapshot-2 to VM1")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = updateVmWithNewPvc(ctx, vmopC, vm1.Name, namespace, restorepvc2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify restorevol2 is attached to the VM1")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc1, restorepvc2})).To(gomega.Succeed())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		// Refresh VM information
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Format the PVC and verify accessibility
		volFolder := formatNVerifyPvcIsAccessible(vm1.Status.Volumes[0].DiskUuid, 2, vmIp1)
		// Verify data integrity on the VM disk
		verifyDataIntegrityOnVmDisk(vmIp1, volFolder)

		ginkgo.By("Attach restore snapshot-1 to vm2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = updateVmWithNewPvc(ctx, vmopC, vm2.Name, namespace, restorepvc1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify restore snapshot-1 is attached to the VM2")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm2,
			[]*v1.PersistentVolumeClaim{pvc2, restorepvc1})).To(gomega.Succeed())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		// Refresh VM information
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Format the PVC and verify accessibility
		volFolder = formatNVerifyPvcIsAccessible(vm2.Status.Volumes[0].DiskUuid, 2, vmIp2)
		// Verify data integrity on the VM disk
		verifyDataIntegrityOnVmDisk(vmIp2, volFolder)

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

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated1, snapshotContentCreated1, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot1, pandoraSyncWaitTime, volHandle1, snapshotId1, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated2, snapshotContentCreated2, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot2, pandoraSyncWaitTime, volHandle2, snapshotId2, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-12
	   Workflow Path:

	   PVC → VM → Write data → Snapshot1 → Write new data → Snapshot2 → Write new data → Snapshot3 →
	   RestoreVol from snapshot3 → VM → Read/Write and verify data

	   Steps:
	   1.Create PVC using the storage class (storage policy) tagged to the supervisor namespace
	   2. Wait for PVC to reach the Bound state.
	   3. Create a VM service VM. Attach VM to PVC-1
	   4. Wait until the VM service is up and in a powered-on state
	   5. Once the VM is up, verify that the volume is accessible inside the VM
	   6. Write some data to PVC-1 from VM-1.
	   7. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   8. Create a volume snapshot (snapshot1) using the above snapshot class (step #3) and PVC (step #1).
	   9. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   10. Write new data into the volume
	   11.Take a new snapshot (snapshot2) of PVC-1.
	   12. Write new data into the volume.
	   13. Take a new snapshot (snapshot3) of PVC-1.
	   14. Create a new PVC using the snapshot3.
	   15. Wait for the new PVC (PVC-2) to reach the Bound state.
	   16. Create a new VM service VM and attach it to volume PVC-2
	   18. Wait until the VM service is up and in a powered-on state
	   19. Once the VM is up, verify that the volume is accessible inside the VM
	   20. Read, write and verify data in the volume.
	   21. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("[ef-vmsvc] Recurring taking snapshots of a vm service vm", ginkgo.Label(p1, block, wcp,
		vmServiceVm, snapshot, vc80), func() {

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
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName)
		defer func() {
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc1 := createService4Vm(ctx, vmopC, namespace, vm1.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc1.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VM to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm1,
			[]*v1.PersistentVolumeClaim{pvc})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm1.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp1)
			verifyDataIntegrityOnVmDisk(vmIp1, volFolder)
		}

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create snapshot-1")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volFolder := formatNVerifyPvcIsAccessible(vm1.Status.Volumes[0].DiskUuid, 2, vmIp1)
		verifyDataIntegrityOnVmDisk(vmIp1, volFolder)

		ginkgo.By("Create snapshot-2")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volFolder = formatNVerifyPvcIsAccessible(vm1.Status.Volumes[0].DiskUuid, 3, vmIp1)
		verifyDataIntegrityOnVmDisk(vmIp1, volFolder)

		ginkgo.By("Create snapshot-3")
		volumeSnapshot3, snapshotContent3, snapshotCreated3,
			snapshotContentCreated3, snapshotId3, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated3 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot3.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot3.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated3 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent3, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Restore volume from latest snapshot")
		restorepvc, restorepv, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot3, diskSize, false)
		restorevolHandle := restorepv[0].Spec.CSI.VolumeHandle
		gomega.Expect(restorevolHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorepvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restorevolHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VM")
		vm2 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{restorepvc}, vmi, storageClassName, secretName)
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
			[]*v1.PersistentVolumeClaim{restorepvc})).NotTo(gomega.HaveOccurred())

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

		ginkgo.By("Delete dynamic volume snapshot")
		snapshotCreated3, snapshotContentCreated3, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot3, pandoraSyncWaitTime, volHandle, snapshotId3, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-13
	   Stretched WCP: VM and PVC both belong to the same zone, Immediate Binding mode
	   Workflow Path:
	   PVC → VM-1 → Snapshot → RestoreVol → Attach to VM-1

	   Steps:
	   Assign a zonal SPBM policy to test the namespace with sufficient quota
	   Create a PVC (PVC-1)
	   Wait for PVC-1 to reach Bound state.
	   Verify PV node affinity
	   Create a VM service VM using PVC-1 created in step #2.
	   Wait until the VM service is up and in a powered-on state
	   Once the VM is up verify that the volume is accessible inside the VM
	   Verify PVC CNS metadata.
	   Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   Take a snapshot  of PVC
	   Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   Restore Snapshot to create a new PVC (PVC-2)
	   Wait for PVC-2 to reach the Bound state.
	   Verify PV node affinity.
	   Attach this newly created PVC-2 to the VM created in step #5
	   Verify that the volume is accessible inside the VM.
	   Read, write and verify data in the volume.
	   Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("[ef-f-stretched-svc][stretched-svc] Taking snapshot of a vm service vm in a stretched supervisor "+
		"cluster", ginkgo.Label(p0, block, stretchedSvc, vmServiceVm, snapshot, vc80), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topologyHaMap := GetAndExpectStringEnvVar(topologyHaMap)
		allowedTopos := createAllowedTopolgies(topologyHaMap)
		allowedTopologyHAMap := createAllowedTopologiesMap(allowedTopos)
		pvcAnnotations := make(map[string]string)
		topoList := []string{}

		for key, val := range allowedTopologyHAMap {
			for _, topoVal := range val {
				str := `{"` + key + `":"` + topoVal + `"}`
				topoList = append(topoList, str)
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
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		topologykey := pvs[0].Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values[0]
		volumeID := pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
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
		vm := createVmServiceVmWithPvcsWithZone(ctx, vmopC, namespace, vmClass,
			[]*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName, topologykey)
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

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volumeID, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Restore volume from snapshot")
		restorepvc, restorepv, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot, diskSize, false)
		restorevolHandle := restorepv[0].Spec.CSI.VolumeHandle
		gomega.Expect(restorevolHandle).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorepvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restorevolHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify SV PV has has required PV node affinity details")
		_, err = verifyVolumeTopologyForLevel5(restorepv[0], allowedTopologyHAMap)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("SVC PV: %s has required PV node affinity details", restorepv[0].Name)

		ginkgo.By("Creating VM")
		vm2 := createVmServiceVmWithPvcsWithZone(ctx, vmopC, namespace, vmClass,
			[]*v1.PersistentVolumeClaim{restorepvc}, vmi, storageClassName, secretName, topologykey)
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
			[]*v1.PersistentVolumeClaim{restorepvc})).NotTo(gomega.HaveOccurred())

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
			volumeSnapshot, pandoraSyncWaitTime, volumeID, snapshotId, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Testcase-14
	   VMotion and storage VMotion within the same zone
	   Immediate Binding mode
	   Workflow Path:
	   PVC-1 → VM → Write data → Snapshot1  → RestoreVol1  → new VM(RestoreVol1) → Write data ->
	   Snapshot2 -> RestoreVol2 -> new VM(RestoreVol3) -> Snapshot
	   1. Assign a zonal SPBM policy to test the namespace with a sufficient quota
	   which matches two shared datastores say ds1 and ds2 in the same zone
	   2. Create two PVCs say pvc1, and pvc2 (assuming that both of them are provisioned on ds1)
	   3. Create a VM service VM say vm1 under the same zone with pvc1, pvc2
	   4. Wait until the VM service is up and in a powered-on state
	   5. Once the vm1 is up write some data to pvc1 and pvc2
	   6. Get VolumeSnapshotClass "volumesnapshotclass-delete" from supervisor cluster
	   7. Take snapshots of both the PVCs.
	   8. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   9. VMotion VM to a different host in the same zone
	   10. Verify data in pvc1, pvc2 written in step 4
	   11. Write new data to pvc1, pvc2
	   12. Storage VMotion pvc1 and pvc2 to a different datastore (ds2) in the same zone
	   13. Verify reading, and writing data on the volume.
	   14. Take volume snapshots of both migrated PVCs i.e. Snapshot3(PVC-1), Snapshot4(PVC-2)
	   15. Snapshot Verification: Execute and verify the steps mentioned in the Create snapshot mandatory checks
	   16. Create new PVCs i.e. RestoreVol1 from Snapshot1 → RestoreVol2 from Snapshot2
	   17. Create new PVCs i.e. RestoreVol3 from Snapshot3 → RestoreVol4 from Snapshot4
	   18. Create a VM service VM using volumes created in step #16.
	   19. Wait until the VM service is up and in a powered-on state
	   20. Once the vm1 is up write some data to the attached volumes.
	   21. Create new PVCs i.e. RestoreVol3 from Snapshot3 → RestoreVol4 from Snapshot4
	   22. Create a VM service VM using volumes created in step #17
	   23. Wait until the VM service is up and in a powered-on state
	   24. Once the VM is up write some data to the attached volumes.
	   25. Cleanup: Execute and verify the steps mentioned in the Delete snapshot mandatory checks
	*/

	ginkgo.It("[ef-f-stretched-svc][stretched-svc] Restoring snapshots and attaching it to a new vm service vms in "+
		"a stretched supervisor env", ginkgo.Label(p0, block, stretchedSvc, snapshot, vc80), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topologyHaMap := GetAndExpectStringEnvVar(topologyHaMap)
		allowedTopos := createAllowedTopolgies(topologyHaMap)
		allowedTopologyHAMap := createAllowedTopologiesMap(allowedTopos)
		pvcAnnotations := make(map[string]string)
		topoList := []string{}

		for key, val := range allowedTopologyHAMap {
			for _, topoVal := range val {
				str := `{"` + key + `":"` + topoVal + `"}`
				topoList = append(topoList, str)
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
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		topologykey := pvs[0].Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values[0]
		volumeId := pvs[0].Spec.CSI.VolumeHandle

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeId))
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
		vm := createVmServiceVmWithPvcsWithZone(ctx, vmopC, namespace, vmClass,
			[]*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName, topologykey)
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

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create snapshot-1")
		volumeSnapshot1, snapshotContent1, snapshotCreated1,
			snapshotContentCreated1, snapshotId1, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volumeId, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated1 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot1.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot1.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated1 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent1, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Restore volume from snapshot-1")
		restorepvc1, restorepv1, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot1, diskSize, false)
		restorevolHandle1 := restorepv1[0].Spec.CSI.VolumeHandle
		gomega.Expect(restorevolHandle1).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorepvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restorevolHandle1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify SV PV has has required PV node affinity details")
		_, err = verifyVolumeTopologyForLevel5(restorepv1[0], allowedTopologyHAMap)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("SVC PV: %s has required PV node affinity details", restorepv1[0].Name)

		ginkgo.By("Creating VM")
		vm2 := createVmServiceVmWithPvcsWithZone(ctx, vmopC, namespace, vmClass,
			[]*v1.PersistentVolumeClaim{restorepvc1}, vmi, storageClassName, secretName, topologykey)
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
			[]*v1.PersistentVolumeClaim{restorepvc1})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm2, err = getVmsvcVM(ctx, vmopC, vm2.Namespace, vm2.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm2.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp2)
			verifyDataIntegrityOnVmDisk(vmIp2, volFolder)
		}

		ginkgo.By("Create volume snapshot-2")
		volumeSnapshot2, snapshotContent2, snapshotCreated2,
			snapshotContentCreated2, snapshotId2, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, restorepvc1, restorevolHandle1, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated2 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot2.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot2.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated2 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent2, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Restore volume from snapshot-2")
		restorepvc2, restorepv2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass,
			volumeSnapshot2, diskSize, false)
		restorevolHandle2 := restorepv2[0].Spec.CSI.VolumeHandle
		gomega.Expect(restorevolHandle2).NotTo(gomega.BeEmpty())
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, restorepvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(restorevolHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify SV PV has has required PV node affinity details")
		_, err = verifyVolumeTopologyForLevel5(restorepv2[0], allowedTopologyHAMap)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("SVC PV: %s has required PV node affinity details", restorepv2[0].Name)

		ginkgo.By("Creating VM")
		vm3 := createVmServiceVmWithPvcsWithZone(ctx, vmopC, namespace, vmClass,
			[]*v1.PersistentVolumeClaim{restorepvc2}, vmi, storageClassName, secretName, topologykey)
		defer func() {
			ginkgo.By("Deleting VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
				Name:      vm3.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc3 := createService4Vm(ctx, vmopC, namespace, vm3.Name)
		defer func() {
			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc3.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for VM to come up and get an IP")
		vmIp3, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm3.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm3,
			[]*v1.PersistentVolumeClaim{restorepvc2})).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm3, err = getVmsvcVM(ctx, vmopC, vm3.Namespace, vm3.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm3.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp3)
			verifyDataIntegrityOnVmDisk(vmIp3, volFolder)
		}

		ginkgo.By("Create volume snapshot-3")
		volumeSnapshot3, snapshotContent3, snapshotCreated3,
			snapshotContentCreated3, snapshotId3, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, restorepvc2, restorevolHandle2, diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotCreated3 {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot3.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot3.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if snapshotContentCreated3 {
				err = deleteVolumeSnapshotContent(ctx, snapshotContent3, snapc, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Delete volume snapshot-1")
		snapshotCreated1, snapshotContentCreated1, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot1, pandoraSyncWaitTime, volumeId, snapshotId1, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete volume snapshot-2")
		snapshotCreated2, snapshotContentCreated2, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot2, pandoraSyncWaitTime, restorevolHandle1, snapshotId2, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete volume snapshot-3")
		snapshotCreated3, snapshotContentCreated3, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot3, pandoraSyncWaitTime, restorevolHandle2, snapshotId3, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
