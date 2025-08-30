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
	"os"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
)

var _ bool = ginkgo.Describe("[multi-writer-positive] Multi-Writer-Positive", func() {

	f := framework.NewDefaultFramework("multi-writer")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                             clientset.Interface
		vmopC                              ctlrclient.Client
		cnsopC                             ctlrclient.Client
		snapc                              *snapclient.Clientset
		nodeList                           *v1.NodeList
		vcRestSessionId                    string
		namespace                          string
		pvclaimlabels                      map[string]string
		rawBlockVolumeMode                 = v1.PersistentVolumeBlock
		vmClass                            string
		contentLibId                       string
		err                                error
		statuscode                         int
		datastoreURL                       string
		sharedVmfsStoragePolicy            string
		sharedVmfsStorageProfileId         string
		multiWriterCapableStoragePolicy    string
		multiWriterCapableStorageProfileId string
		rwoaccessMode                      = v1.ReadWriteOnce
		rwmaccessMode                      = v1.ReadWriteMany
	)

	//parameters required for multi writer volume and vm creation
	type MultiWriterVolumeVmConfig struct {
		CreatePvcCountLimit    int
		ParallelVolumeCreation bool
		VmCreationCount        int
		ParallelVmCreation     bool
		LateBinding            bool
		SharingMode            string
		ControllerNumber       int32
		UnitNumber             int32
		DiskMode               string
		BusSharing             string
		ControllerType         string
	}

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

		//setting labels map on pvc
		pvclaimlabels = make(map[string]string)
		pvclaimlabels["app"] = "multi-writer"

		// reading clustered vmdk enabled sharedVMFS Thick storage policy
		sharedVmfsStoragePolicy = GetAndExpectStringEnvVar(envSharedVmfsThickPolicy)
		sharedVmfsStorageProfileId = e2eVSphere.GetSpbmPolicyID(sharedVmfsStoragePolicy)

		// reading multi-writer capable shared storage policy
		multiWriterCapableStoragePolicy = GetAndExpectStringEnvVar(envMultiWriterSharedStoragePolicy)
		multiWriterCapableStorageProfileId = e2eVSphere.GetSpbmPolicyID(multiWriterCapableStoragePolicy)

		// fetch shared vsphere datatsore url
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		dsRef := getDsMoRefFromURL(ctx, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		framework.Logf("Read vm class for creating vm service vm")
		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}

		// read or create content library if it is empty
		if contentLibId == "" {
			contentLibId, err = createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
				dsRef.Value, &e2eVSphere)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		/* Sets up a Kubernetes client with a custom scheme, adds the vmopv1 API types to the scheme,
		and ensures that the client is properly initialized without errors */
		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		restConfig = getRestConfigClient()
		snapc, err = snapclient.NewForConfig(restConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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
	   Testcase-1
	   7 VMs deployed with WSFC configuration using immediate sharedVMFS EZT Thick storage policy

	   Test Steps:
	   1. Create a wcp namespace and tag sharedVMFS EZT Thick storage policy to it
	   1. Create PVC by setting access mode to "RWX" and volumeMode: Block using above
	   storage policy but with immediate binding mode
	   2. Wait for PVC to reach Bound state.
	   3. Deploy 7 VMs with WSFC configuration and attach above pvc to all 7 VMs
	   4. Wait for all VMs to get an IP and to be in power on state.
	   5. Verify CR cnsnodevmattachment for PVC and VMs
	   6. Verify volume attached successfully with PVC for all the VMs
	   7. Write some IO to the volume from VMs
	   8. Perform cleanup: Delete VMs, PVCs.
	   9. Verify cleanup happened successfully.
	*/

	ginkgo.It("TC1", ginkgo.Label(p0, vmServiceVm, vc901, newTest), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := MultiWriterVolumeVmConfig{
			CreatePvcCountLimit:    1,
			ParallelVolumeCreation: false,
			VmCreationCount:        7,
			ParallelVmCreation:     true,
			LateBinding:            false,
			SharingMode:            "",
			ControllerNumber:       0,
			UnitNumber:             0,
			DiskMode:               "independent_persistent",
			BusSharing:             "Physical",
			ControllerType:         "Paravirtual",
		}

		ginkgo.By("Create a WCP namespace and add clustered vmdk enabled sharedVMfs thick storage policy")
		namespace, statuscode, err := createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedVmfsStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read clustered vmdk enabled sharedVMfs thick storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, sharedVmfsStoragePolicy,
			metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create RWX raw block volume using sharedVMfs thick storage policy")
		pvc, err := createPVCsOnScaleWithDifferentConfigurations(ctx, client, namespace, pvclaimlabels,
			diskSize, storageclass, rwmaccessMode, rawBlockVolumeMode,
			cfg.CreatePvcCountLimit, cfg.ParallelVolumeCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deploy WSFC configured 7 VMs")
		_, vms, _, err := createVmServiceOnScaleWithDifferentSettings(ctx, client, vmopC, cnsopC, namespace,
			pvc, vmClass, storageclass.Name, cfg.VmCreationCount, cfg.ParallelVmCreation,
			cfg.SharingMode, cfg.DiskMode, cfg.BusSharing,
			cfg.ControllerType, cfg.ControllerNumber, cfg.UnitNumber)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify pvc bound state and cns metadata verification")
		_, _, err = verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify cns node vm attachment CR for pvc and vm")
		for _, vm := range vms {
			verifyCRDInSupervisor(ctx, f, vm.Name+"-"+pvc[0].Name,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
			verifyIsAttachedInSupervisor(ctx, f, vm.Name+"-"+pvc[0].Name, crdVersion, crdGroup)
		}

		ginkgo.By("Verify attached volumes are accessible and validate data integrity on all VMs")
		for _, vm := range vms {
			framework.Logf("Verifying raw block volumes on VM: %s", vm.Name)
			err := verifyRawBlockVolumeAccessibilityAndDataIntegrityOnVM(ctx, vm, vmopC, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Data integrity check failed for VM %s", vm.Name))
		}
	})

	/*
	   Testcase-2
	   Snapshot Usecase:
	   Raw Block RWX PVC → Snapshot → Parallel 2 VMs creation

	   Test Steps:
	   1. Create PVC by setting access mode to "RWX" and volumeMode: Block to create
	   a raw block volume using vSAN Max storage policy and using WFFC binding mode
	   2. PVC will be stuck in a pending state due to WFFC binding mode
	   3. Take a snapshot of PVC created in step #2
	   4. Wait for Snapshot to get created.
	   5. Trigger 2 Oracle RAC VMs creation in parallel and attach it to PVC created in step1
	   6. Wait for VMs to get an IP and to be in power on state.
	   7. Verify volume attached successfully for vm1-pvc and vm2-pvc
	   8. Verify CR cnsnodevmattachment for the PVC and VMs. Should display 2 entries.
	   9. Write some IO to the volume from VMs.
	   10. Perform cleanup: Delete VMs, Snapshot, PVCs
	   11. Verify cleanup happened successfully.
	*/

	ginkgo.It("TC2", ginkgo.Label(p0, vmServiceVm, vc901, newTest), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := MultiWriterVolumeVmConfig{
			CreatePvcCountLimit:    1,
			ParallelVolumeCreation: false,
			VmCreationCount:        2,
			ParallelVmCreation:     true,
			LateBinding:            true,
			SharingMode:            "Multiwriter",
			ControllerNumber:       0,
			UnitNumber:             0,
			DiskMode:               "independent_persistent",
			BusSharing:             "",
			ControllerType:         "Paravirtual",
		}

		ginkgo.By("Create a WCP namespace and add multi-writer enabled shared storage policy to ns")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{multiWriterCapableStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read multi writer capable WFFC shared storage " +
			"policy which is tagged to wcp namespace")
		multiWriterCapableStoragePolicy = multiWriterCapableStoragePolicy + "-latebinding"
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, multiWriterCapableStoragePolicy,
			metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create RWX raw block volume using multi writer capable shared storage policy")
		pvc, err := createPVCsOnScaleWithDifferentConfigurations(ctx, client, namespace, pvclaimlabels,
			diskSize, storageclass, rwmaccessMode, rawBlockVolumeMode,
			cfg.CreatePvcCountLimit, cfg.ParallelVolumeCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot of raw block volume")
		_, _, _, _, _, _, err = createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc[0], "", diskSize, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deploy Oracle RAC configured 2 VMs")
		_, vms, _, err := createVmServiceOnScaleWithDifferentSettings(ctx, client, vmopC, cnsopC, namespace,
			pvc, vmClass, storageclass.Name, cfg.VmCreationCount, cfg.ParallelVmCreation,
			cfg.SharingMode, cfg.DiskMode, cfg.BusSharing,
			cfg.ControllerType, cfg.ControllerNumber, cfg.UnitNumber)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify pvc bound state and cns metadata verification")
		_, _, err = verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify cns node vm attachment CR for pvc and vm")
		for _, vm := range vms {
			verifyCRDInSupervisor(ctx, f, vm.Name+"-"+pvc[0].Name,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
			verifyIsAttachedInSupervisor(ctx, f, vm.Name+"-"+pvc[0].Name, crdVersion, crdGroup)
		}

		ginkgo.By("Verify attached volumes are accessible and validate data integrity on all VMs")
		for _, vm := range vms {
			framework.Logf("Verifying raw block volumes on VM: %s", vm.Name)
			err := verifyRawBlockVolumeAccessibilityAndDataIntegrityOnVM(ctx, vm, vmopC, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Data integrity check failed for VM %s", vm.Name))
		}
	})

	/*
	   Testcase-3
	   Snapshot Usecase:

	   Raw Block RWX PVC + RWO PVC → Both PVCs Snapshot → VM1 → detach Raw Block RWX PVC volume →
	   snapshot of Raw Block RWX PVC →Restore all PVCs → Attach all restored PVCs to VM2

	   Test Steps:
	   1. Create 2 PVCs one with RWO raw block, second with RWX raw block with immediate binding mode using
	   shared multi writer enabled policy
	   2. Wait for PVCs to reach Bound state.
	   3. Take volume snapshot of both the PVCs.
	   4. Verify snapshot created successfully.
	   5. Create VM1 with WSFC configuration and attach it to both the PVCs
	   6. Wait for VM1 to get an IP and to be in a power on state
	   7. Verify volume attachment passes for pvc1-vm as it is a "RWX raw block volume"
	   8. Verify volume attachment fails for pvc2-vm as it is a "RWO raw block volume"
	   9. Verify cnsnodeattachment CR for both the PVCs with VM
	   Power-off VM and detach PVC1 from VM
	   Verify volume detached is successful.
	   Now take volume snapshot of PVC1 again.
	   Verify snapshot created successfully.
	   Now restore all the snapshots created so far in step 3 and 12
	   Verify restored PVCs reached Bound state.
	   Create VM2 with the configuration mentioned in the testcase specification and attach it to all the restored PVCs
	   Volume attachment should fail for restored pvc which is "RWO raw block volume". Verify an error message
	   For other "RWX raw block volume" restored volumes, attachment should go fine.
	   Perform cleanup: Delete VMs, Snapshot, PVCs
	   Verify cleanup happened successfully.
	*/

	ginkgo.It("TC3", ginkgo.Label(p0, vmServiceVm, vc901, newTest), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var snapshots []*snapV1.VolumeSnapshot

		cfg := MultiWriterVolumeVmConfig{
			CreatePvcCountLimit:    1,
			ParallelVolumeCreation: false,
			VmCreationCount:        1,
			ParallelVmCreation:     false,
			LateBinding:            false,
			SharingMode:            "",
			ControllerNumber:       0,
			UnitNumber:             0,
			DiskMode:               "independent_persistent",
			BusSharing:             "Physical",
			ControllerType:         "Paravirtual",
		}

		var pvcsList []*v1.PersistentVolumeClaim

		ginkgo.By("Create a WCP namespace and add multi-writer enabled shared storage policy to ns")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{multiWriterCapableStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read multi writer capable Immediate shared storage " +
			"policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, multiWriterCapableStoragePolicy,
			metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create RWX raw block volume using multi writer capable shared storage policy")
		pvc1, err := createPVCsOnScaleWithDifferentConfigurations(ctx, client, namespace, pvclaimlabels,
			diskSize, storageclass, rwmaccessMode, rawBlockVolumeMode,
			cfg.CreatePvcCountLimit, cfg.ParallelVolumeCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcsList = append(pvcsList, pvc1...)

		ginkgo.By("Create RWO raw block volume using multi writer capable shared storage policy")
		pvc2, err := createPVCsOnScaleWithDifferentConfigurations(ctx, client, namespace, pvclaimlabels,
			diskSize, storageclass, rwoaccessMode, rawBlockVolumeMode,
			cfg.CreatePvcCountLimit, cfg.ParallelVolumeCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvcsList = append(pvcsList, pvc2...)

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create volume snapshot of raw block volume")
		for _, pvc := range pvcsList {
			snapshot, _, _, _, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
				volumeSnapshotClass, pvc, "", diskSize, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshots = append(snapshots, snapshot)
		}
		ginkgo.By("Deploy WSFC configured VM, expecting volume attachment to " +
			"fail for RWO raw block volume but should pass for RWX raw block volume")
		_, vms, _, err := createVmServiceOnScaleWithDifferentSettings(ctx, client, vmopC, cnsopC, namespace,
			pvcsList, vmClass, storageclass.Name, cfg.VmCreationCount, cfg.ParallelVmCreation,
			cfg.SharingMode, cfg.DiskMode, cfg.BusSharing,
			cfg.ControllerType, cfg.ControllerNumber, cfg.UnitNumber)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Verify pvc bound state and cns metadata verification")
		_, _, err = verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, pvcsList)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Verify cns node vm attachment CR for pvc and vm")
		// for _, vm := range vms {
		// 	verifyCRDInSupervisor(ctx, f, vm.Name+"-"+pvclaimList[0].Name,
		// 		crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		// 	verifyIsAttachedInSupervisor(ctx, f, vm.Name+"-"+pvclaimList[0].Name, crdVersion, crdGroup)
		// }

		ginkgo.By("Verify attached volumes are accessible and validate data integrity on all VMs")
		for _, vm := range vms {
			framework.Logf("Verifying raw block volumes on VM: %s", vm.Name)
			err := verifyRawBlockVolumeAccessibilityAndDataIntegrityOnVM(ctx, vm, vmopC, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Data integrity check failed for VM %s", vm.Name))
		}
	})

})
