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
	"os"

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
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
)

var _ bool = ginkgo.Describe("[multi-writer-positive] Multi-Writer-Positive", func() {

	f := framework.NewDefaultFramework("multi-writer")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	var (
		client                             clientset.Interface
		vmopC                              ctlrclient.Client
		cnsopC                             ctlrclient.Client
		nodeList                           *v1.NodeList
		vcRestSessionId                    string
		namespace                          string
		pvclaimlabels                      map[string]string
		rawBlockVolumeMode                 = v1.PersistentVolumeBlock
		vmClass                            string
		contentLibId                       string
		err                                error
		datastoreURL                       string
		sharedVmfsStoragePolicy            string
		sharedVmfsStorageProfileId         string
		multiWriterCapableStoragePolicy    string
		multiWriterCapableStorageProfileId string
		rwoaccessMode                      = v1.ReadWriteOnce
		rwmaccessMode                      = v1.ReadWriteMany
		pvcDiskSize                        = "2Gi"
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
		datastoreURL = GetAndExpectStringEnvVar(envMultiWriterSharedDatastoreURL)
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
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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
	   PVC Creation with Different Configurations

	   Test Steps:

	   1. Create two PVCs:
	   ->  PVC1 with volumeMode: not set, accessModes: RWO, BindingMode: Immediate, storage policy -> multi-writer compatible shared storage policy
	   ->  PVC2 with volumeMode: Block, accessModes: RWO, BindingMode: Immediate, storage policy -> multi-writer compatible shared storage policy
	   2. Wait until both PVCs reach the Bound state.
	   3. Deploy a VM Service VM configured with Oracle RAC settings, attaching it to both PVCs, with the following parameters:
	   ->  SharingMode: MultiWriter
	   ->  DiskMode: independent_persistent
	   ->  ControllerType: Paravirtual
	   ->  ControllerNumber: 0
	   ->  UnitNumber: 0
	   4. Wait for the VM to acquire an IP address and transition to Powered On state.
	   5. Verify that volume attachment fails for the VM with both PVCs, since the configuration does not meet the multi-writer requirements.
	   6. Confirm that an appropriate error message is displayed.
	   7. Perform cleanup by deleting NS. Namespace deletion should automatically take care
	   of deleting all the volumes and workloads
	   8. Verify that cleanup completes successfully.
	*/

	ginkgo.It("Negative-TC1", ginkgo.Label(p1, vmServiceVm, vc901, newTest, multiWriter, negative), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := MultiWriterVolumeVmConfig{
			CreatePvcCountLimit:    1,
			ParallelVolumeCreation: false,
			VmCreationCount:        1,
			ParallelVmCreation:     false,
			LateBinding:            false,
			SharingMode:            "MultiWriter",
			ControllerNumber:       0,
			UnitNumber:             0,
			DiskMode:               "independent_persistent",
			ControllerType:         "Paravirtual",
		}

		ginkgo.By("Create a WCP namespace and add multi-writer compatible storage policy")
		namespace, statuscode, err := createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{multiWriterCapableStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, multiWriterCapableStoragePolicy,
			metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify test namespace created successfully in supervisor")
		configStatus := getTestWcpNs(vcRestSessionId, namespace)
		framework.Logf("Namespace %s is in Config Status: %s", namespace, configStatus)
		gomega.Expect(configStatus).To(gomega.Equal("RUNNING"))

		ginkgo.By("Create PVC1")
		pvc1, err := createPVCsOnScaleWithDifferentConfigurations(ctx, client, namespace, pvclaimlabels,
			pvcDiskSize, storageclass, rwoaccessMode, "",
			cfg.CreatePvcCountLimit, cfg.ParallelVolumeCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC2")
		pvc2, err := createPVCsOnScaleWithDifferentConfigurations(ctx, client, namespace, pvclaimlabels,
			pvcDiskSize, storageclass, rwoaccessMode, rawBlockVolumeMode,
			cfg.CreatePvcCountLimit, cfg.ParallelVolumeCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs Bound state")
		allPVCs := append(pvc1, pvc2...)
		pvcs, _, err := verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, allPVCs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deploy Oracle RAC configured VM Service VM")
		_, vms, _, err := createVmServiceOnScaleWithDifferentSettings(ctx, client, vmopC, cnsopC, namespace,
			pvcs, vmClass, storageclass.Name, cfg.VmCreationCount, cfg.ParallelVmCreation,
			cfg.SharingMode, cfg.DiskMode, cfg.BusSharing,
			cfg.ControllerType, cfg.ControllerNumber, cfg.UnitNumber)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify cns volume metadata verification")
		_, _, err = verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, pvcs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify CNS NodeBatchAttachment CR for PVCs and VM")
		verifyCRDInSupervisor(ctx, f, vms[0].Name+"-"+pvcs[0].Name,
			crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		verifyIsAttachedInSupervisor(ctx, f, vms[0].Name+"-"+pvcs[0].Name, crdVersion, crdGroup)

		ginkgo.By("Verify both PVC attachment should fail due to incorrect multi-writer configurations passed")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vms[0],
			pvcs)).To(gomega.HaveOccurred())
	})

	/*
		Testcase-3
		VM Creation with Different Configurations

		Test Steps:

		1. Create a PVC with the following configuration: volumeMode: Block, accessModes: RWX, BindingMode: Immediate,
		Storage Policy: ClusteredVMDK-enabled SharedVMFS storage policy
		2. Verify the PVC reaches the Bound state.
		3. Create Oracle RAC VM1 with the following settings and attach it to the PVC: SharingMode: MultiWriter
		DiskMode: independent_persistent, ControllerType: Paravirtual, ControllerNumber: 0, UnitNumber: 0 and attach it to a above PVC
		4. Wait until VM1 acquires an IP address and powers on.
		6. Verify the CNS NodeBatchAttachment CR for the PVC and VM1 shows success.
		7. Create Oracle RAC VM2 with the following settings and attach it to the same PVC: SharingMode: not set,
		DiskMode: independent_persistent, ControllerType: Paravirtual, ControllerNumber: 0, UnitNumber: 1
		8. Wait until VM2 acquires an IP address and powers on.
		9. Verify that volume attachment fails for VM2 because the multi-writer configuration is incorrect.
		10. Verify the CNS NodeBatchAttachment CR for the PVC and VM2 shows an appropriate error message.
		11. Create VM3 with the following settings and attach it to the same PVC: SharingMode: MultiWriter,
		DiskMode: persistent, ControllerType: Paravirtual, ControllerNumber: 0, UnitNumber: 2
		12. Wait until VM3 acquires an IP address and powers on.
		13. Verify that volume attachment fails for VM3 because the multi-writer configuration is incorrect.
		14. Verify the CNS NodeBatchAttachment CR for the PVC and VM3 shows an appropriate error message.
		15. Perform cleanup by deleting the Namespace.
		16. Namespace deletion should automatically remove all associated volumes and workloads.
		17. Verify that cleanup completes successfully.

	*/

	ginkgo.It("Negative-TC3", ginkgo.Label(p1, vmServiceVm, vc901, newTest, multiWriter, negative), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := MultiWriterVolumeVmConfig{
			CreatePvcCountLimit:    1,
			ParallelVolumeCreation: false,
			VmCreationCount:        3,
			ParallelVmCreation:     false,
			LateBinding:            false,
			SharingMode:            "MultiWriter",
			ControllerNumber:       0,
			UnitNumber:             0,
			DiskMode:               "independent_persistent",
			ControllerType:         "Paravirtual",
		}

		var vms []*vmopv1.VirtualMachine

		ginkgo.By("Create a WCP namespace and add clustered vmdk enabled shared vmfs storage policy")
		namespace, statuscode, err := createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{sharedVmfsStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, constants.Poll, constants.PollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, multiWriterCapableStoragePolicy,
			metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify test namespace created successfully in supervisor")
		configStatus := getTestWcpNs(vcRestSessionId, namespace)
		framework.Logf("Namespace %s is in Config Status: %s", namespace, configStatus)
		gomega.Expect(configStatus).To(gomega.Equal("RUNNING"))

		ginkgo.By("Create PVC")
		pvc, err := createPVCsOnScaleWithDifferentConfigurations(ctx, client, namespace, pvclaimlabels,
			pvcDiskSize, storageclass, rwmaccessMode, rawBlockVolumeMode,
			cfg.CreatePvcCountLimit, cfg.ParallelVolumeCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs Bound state")
		pvcs, _, err := verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deploy Oracle RAC VM1 with following setting - SharingMode: MultiWriter, " +
			"DiskMode: independent_persistent, ControllerType: Paravirtual, ControllerNumber: 0, " +
			"UnitNumber: 0")
		_, vm1, _, err := createVmServiceOnScaleWithDifferentSettings(ctx, client, vmopC, cnsopC, namespace,
			pvcs, vmClass, storageclass.Name, cfg.VmCreationCount, cfg.ParallelVmCreation,
			cfg.SharingMode, cfg.DiskMode, cfg.BusSharing,
			cfg.ControllerType, cfg.ControllerNumber, cfg.UnitNumber)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cfg = MultiWriterVolumeVmConfig{
			ControllerNumber: 0,
			UnitNumber:       1,
			DiskMode:         "independent_persistent",
			ControllerType:   "Paravirtual",
		}

		ginkgo.By("Deploy Oracle RAC VM2 with following setting - SharingMode: not set, " +
			"DiskMode: independent_persistent, ControllerType: Paravirtual, ControllerNumber: 0, " +
			"UnitNumber: 1")
		_, vm2, _, err := createVmServiceOnScaleWithDifferentSettings(ctx, client, vmopC, cnsopC, namespace,
			pvcs, vmClass, storageclass.Name, cfg.VmCreationCount, cfg.ParallelVmCreation,
			cfg.SharingMode, cfg.DiskMode, cfg.BusSharing,
			cfg.ControllerType, cfg.ControllerNumber, cfg.UnitNumber)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		cfg = MultiWriterVolumeVmConfig{
			SharingMode:      "MultiWriter",
			ControllerNumber: 0,
			UnitNumber:       2,
			DiskMode:         "persistent",
			ControllerType:   "Paravirtual",
		}

		ginkgo.By("Deploy Oracle RAC VM3 with following setting - SharingMode: not set, " +
			"DiskMode: persistent, ControllerType: Paravirtual, ControllerNumber: 0, " +
			"UnitNumber: 2")
		_, vm3, _, err := createVmServiceOnScaleWithDifferentSettings(ctx, client, vmopC, cnsopC, namespace,
			pvcs, vmClass, storageclass.Name, cfg.VmCreationCount, cfg.ParallelVmCreation,
			cfg.SharingMode, cfg.DiskMode, cfg.BusSharing,
			cfg.ControllerType, cfg.ControllerNumber, cfg.UnitNumber)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vms = append(vms, vm1...)
		vms = append(vms, vm2...)
		vms = append(vms, vm3...)

		ginkgo.By("Verify cns volume metadata verification")
		_, _, err = verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, pvcs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify CNS NodeBatchAttachment CR for PVC and VMs")
		for i := 0; i < len(vms); i++ {
			verifyCRDInSupervisor(ctx, f, vms[i].Name+"-"+pvcs[0].Name,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
			verifyIsAttachedInSupervisor(ctx, f, vms[i].Name+"-"+pvcs[0].Name, crdVersion, crdGroup)
		}

		ginkgo.By("Verify PVC attachment to Vms should fail due to incorrect multi-writer configurations passed")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vms[0],
			pvcs)).To(gomega.HaveOccurred())
	})

	/* Testcase-4
		Oracle RAC VM creation with NFS datastore

		Test Steps:
		1. Create PVC with volumeMode: Block, accessModes: "RWX", with binding mode "Immediate"
	    2. Wait for PVC to reach Bound state.
	    3. Create Oracle RAC  VM with setting like SharingMode: Multiwriter, DiskMode: independent_persistent,
		Controller Type: Paravirtual, ControllerNumber: 0, UnitNumber: 0
	    4. Wait for VM to get an IP and to be in a power-on state.
	    5. Verify volume attachment fails with an appropriate error message as NFS datastore is not supported with Oracle RAC VMs
	    6. Verify an appropriate error message is thrown.
	    7. Perform cleanup: Delete Ns. Verify cleanup happened successfully.
	*/

	ginkgo.It("Negative-TC4", ginkgo.Label(p1, vmServiceVm, vc901, newTest, multiWriter, negative), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := MultiWriterVolumeVmConfig{
			CreatePvcCountLimit:    1,
			ParallelVolumeCreation: false,
			VmCreationCount:        1,
			ParallelVmCreation:     false,
			LateBinding:            false,
			SharingMode:            "MultiWriter",
			ControllerNumber:       0,
			UnitNumber:             0,
			DiskMode:               "independent_persistent",
			ControllerType:         "Paravirtual",
		}

		// reading NFS storage policy incompatible to multi-writer
		nfsPolicy := GetAndExpectStringEnvVar(envMultiWriterNFSStoragePolicy)
		nfsStorageProfileId := e2eVSphere.GetSpbmPolicyID(nfsPolicy)

		ginkgo.By("Create a WCP namespace and add NFS shared storage policy")
		namespace, statuscode, err := createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{nfsStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, multiWriterCapableStoragePolicy,
			metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify test namespace created successfully in supervisor")
		configStatus := getTestWcpNs(vcRestSessionId, namespace)
		framework.Logf("Namespace %s is in Config Status: %s", namespace, configStatus)
		gomega.Expect(configStatus).To(gomega.Equal("RUNNING"))

		ginkgo.By("Create PVC")
		pvc, err := createPVCsOnScaleWithDifferentConfigurations(ctx, client, namespace, pvclaimlabels,
			pvcDiskSize, storageclass, rwmaccessMode, rawBlockVolumeMode,
			cfg.CreatePvcCountLimit, cfg.ParallelVolumeCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs Bound state")
		pvcs, _, err := verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, pvc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deploy Oracle RAC VM with following setting - SharingMode: MultiWriter, " +
			"DiskMode: independent_persistent, ControllerType: Paravirtual, ControllerNumber: 0, " +
			"UnitNumber: 0")
		_, vms, _, err := createVmServiceOnScaleWithDifferentSettings(ctx, client, vmopC, cnsopC, namespace,
			pvcs, vmClass, storageclass.Name, cfg.VmCreationCount, cfg.ParallelVmCreation,
			cfg.SharingMode, cfg.DiskMode, cfg.BusSharing,
			cfg.ControllerType, cfg.ControllerNumber, cfg.UnitNumber)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify cns volume metadata verification")
		_, _, err = verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, pvcs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify CNS NodeBatchAttachment CR for PVC and VM")
		verifyCRDInSupervisor(ctx, f, vms[0].Name+"-"+pvcs[0].Name,
			crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		verifyIsAttachedInSupervisor(ctx, f, vms[0].Name+"-"+pvcs[0].Name, crdVersion, crdGroup)

		ginkgo.By("Verify PVC attachment to Vms should fail due to incorrect multi-writer configurations passed")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vms[0],
			pvcs)).To(gomega.HaveOccurred())
	})

	/*
		Testcase-5
						Mixed types of VMS attached to 3 RWX PVCs

					Normal configured VM

					Oracle RAC configured VM

					WSFC configured VM

					    1. Create 3 PVCs with volumeMode: Block, accessModes: "RWX", with binding mode "Immediate"
			    2. Wait for PVCs to reach Bound state.
			    3. Create Oracle RAC VM1 with setting like - SharingMode: Multiwriter, DiskMode: independent_persistent,
				Controller Type: Paravirtual, ControllerNumber:0, ControllerNumber:0  and attach it to all 3 PVCs.
			    4. Wait for VM to get an IP and to be in a power-on state.
			    5. Verify volume attached successfully for vm1-pvc1/pvc2/pvc3
			    6. verify nodevolumeattachment CR for vm1-pvc1/pvc2/pvc3
			    Perform some IO operation on volumes from VM1
			    Create VM2 with the configuration mentioned in the testcase specification and attach it to all 3 PVCs.
			    Wait for VM to get an IP and to be in a power-on state.
			    Verify volume attached successfully for vm2-pvc1/pvc2/pvc3
			    verify nodevolumeattachment CR for vm2-pvc1/pvc2/pvc3
			    Perform some IO operation on volumes from VM2
			    Create VM3 with the configuration mentioned in the testcase specification and attach it to all 3 PVCs.
			    Wait for VM to get an IP and to be in a power-on state.
			    Verify volume attached failed for vm3-pvc1/pvc2/pvc3
			    verify nodevolumeattachment CR for vm3-pvc1/pvc2/pvc3
			    Verify an appropriate error message should get displayed for  vm3-pvc1/pvc2/pvc3
			    Perform cleanup: Delete VMs, PVCs.
			    Verify cleanup happened successfully.

	*/

	ginkgo.It("Negative-TC5", ginkgo.Label(p1, vmServiceVm, vc901, newTest, multiWriter, negative), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := MultiWriterVolumeVmConfig{
			CreatePvcCountLimit:    3,
			ParallelVolumeCreation: true,
			VmCreationCount:        1,
			ParallelVmCreation:     false,
			LateBinding:            false,
			SharingMode:            "MultiWriter",
			ControllerNumber:       0,
			UnitNumber:             0,
			DiskMode:               "independent_persistent",
			ControllerType:         "Paravirtual",
		}

		ginkgo.By("Create a WCP namespace and add multi-writer compatible storage policy")
		namespace, statuscode, err := createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{multiWriterCapableStorageProfileId}, getSvcId(vcRestSessionId, &e2eVSphere),
			[]string{}, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		defer func() {
			delTestWcpNs(vcRestSessionId, namespace)
			gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
		}()

		ginkgo.By("Read storage policy which is tagged to wcp namespace")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, multiWriterCapableStoragePolicy,
			metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify test namespace created successfully in supervisor")
		configStatus := getTestWcpNs(vcRestSessionId, namespace)
		framework.Logf("Namespace %s is in Config Status: %s", namespace, configStatus)
		gomega.Expect(configStatus).To(gomega.Equal("RUNNING"))

		ginkgo.By("Create 3 PVCs")
		pvcs, err := createPVCsOnScaleWithDifferentConfigurations(ctx, client, namespace, pvclaimlabels,
			pvcDiskSize, storageclass, rwmaccessMode, rawBlockVolumeMode,
			cfg.CreatePvcCountLimit, cfg.ParallelVolumeCreation)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs reaches Bound state")
		pvcs, _, err = verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, pvcs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deploy Oracle RAC configured VM Service VM")
		_, vms, _, err := createVmServiceOnScaleWithDifferentSettings(ctx, client, vmopC, cnsopC, namespace,
			pvcs, vmClass, storageclass.Name, cfg.VmCreationCount, cfg.ParallelVmCreation,
			cfg.SharingMode, cfg.DiskMode, cfg.BusSharing,
			cfg.ControllerType, cfg.ControllerNumber, cfg.UnitNumber)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify cns volume metadata verification")
		_, _, err = verifyPVCsBoundStateAndQueryVolumeInCNS(ctx, client, pvcs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify CNS NodeBatchAttachment CR for PVCs and VM")
		verifyCRDInSupervisor(ctx, f, vms[0].Name+"-"+pvcs[0].Name,
			crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		verifyIsAttachedInSupervisor(ctx, f, vms[0].Name+"-"+pvcs[0].Name, crdVersion, crdGroup)

		ginkgo.By("Verify both PVC attachment should fail due to incorrect multi-writer configurations passed")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vms[0],
			pvcs)).To(gomega.HaveOccurred())
	})
})
