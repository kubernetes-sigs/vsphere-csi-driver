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
	"strconv"
	"strings"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

var _ bool = ginkgo.Describe("[rwx-vmsvc-vm] RWX support with VMService Vms", func() {

	f := framework.NewDefaultFramework("rwx-vmsvc")
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
		//cnsopC                     ctlrclient.Client
		isVsanHealthServiceStopped bool
		isSPSserviceStopped        bool
		restConfig                 *restclient.Config
		snapc                      *snapclient.Clientset
		pandoraSyncWaitTime        int
		dsRef                      types.ManagedObjectReference
		labelsMap                  map[string]string
		//adminClient                clientset.Interface
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// client connection
		client = f.ClientSet
		bootstrap()

		// fetching nodes and reading storage policy name
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		// creating vc session
		vcRestSessionId = createVcSession4RestApis(ctx)

		// reading storage class name for wcp setup
		storageClassName = strings.ReplaceAll(storagePolicyName, " ", "-") // since this is a wcp setup
		storageClassName = strings.ToLower(storageClassName)

		// fetching shared datastore url
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)

		// reading datastore morf reference
		dsRef = getDsMoRefFromURL(ctx, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		// reading storage profile id of "wcpglobal_storage_profile"
		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)

		// creating/reading content library
		contentLibId, err := createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
			dsRef.Value)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}

		framework.Logf("Create a WCP namespace for the test")
		// creating wcp test namespace and setting vmclass, contlib, storage class fields in test ns
		namespace = createTestWcpNs(
			vcRestSessionId, storageProfileId, vmClass, contentLibId, getSvcId(vcRestSessionId))

		framework.Logf("Verifying storage policies usage for each storage class")
		restConfig = getRestConfigClient()
		ListStoragePolicyUsages(ctx, client, restConfig, namespace, []string{storageClassName})

		// creating vm schema
		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		/*cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())*/

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
	   Dynamic PVC creation with multiple VMServiceVMs
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

	ginkgo.It("Dynamic PVC creation with multiple"+
		" VMServiceVMs", ginkgo.Label(p0, file, wcp, vmServiceVm, vc901), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmIPs, nfsAccessPointList []string
		vmCount := 3

		ginkgo.By("Create/Get a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC")
		pvc, pvs, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, accessMode,
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

		vms := createStandaloneVmServiceVm(
			ctx, vmopC, namespace, vmClass, vmi, storageClassName, secretName, vmopv1.VirtualMachinePoweredOn, vmCount)
		defer func() {
			ginkgo.By("Deleting VM")
			for _, vm := range vms {
				deleteVmServiceVm(ctx, vmopC, namespace, vm.Name)
				crdInstanceName := pvc.Name + vm.Name
				verifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, crdCNSFileAccessConfig, crdVersion, crdGroup, false)
			}

		}()

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		vmlbsvc := createService4Vm(ctx, vmopC, namespace, vms[0].Name)
		defer func() {

			ginkgo.By("Deleting loadbalancing service for ssh with the VM")
			err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
				Name:      vmlbsvc.Name,
				Namespace: namespace,
			}})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for _, vm := range vms {
			ginkgo.By("Wait for VM to come up and get an IP")
			vmIp, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmIPs = append(vmIPs, vmIp)

			ginkgo.By("Create a CNSFileAccessConfig crd for each PVC-VM pair")
			crdInstanceName := pvc.Name + vm.Name
			err = createCnsFileAccessConfigCRD(ctx, restConfig, pvc.Name, vm.Name, namespace, crdInstanceName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			verifyCNSFileAccessConfigCRDInSupervisor(ctx, crdInstanceName, crdCNSFileAccessConfig, crdVersion, crdGroup, true)
			nfsAccessPoint, err := fetchNFSAccessPointFromCnsFileAccessConfigCRD(ctx, crdInstanceName, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			nfsAccessPointList = append(nfsAccessPointList, nfsAccessPoint)

		}

		ginkgo.By("Write IO to file volume through multiple VMService VM")
		for _, nfsAccessPoint := range nfsAccessPointList {
			err = mountRWXVolumeAndVerifyIO(vmIPs, nfsAccessPoint, "/mnt/nfs")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("verify ACL permissions on file share volumes")
		clusterName := GetAndExpectStringEnvVar(envClusterName)
		clusterRef := vcutil.GetClusterRefFromClusterName(ctx, clusterName)
		gomega.Expect(clusterRef).NotTo(gomega.BeEmpty())
		vcutil.QueryVsanFileShares(ctx, []string{volHandle}, clusterRef)

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a dynamic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, _, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc,
			volumeSnapshotClass, pvc, volHandle, diskSize, false)
		framework.Logf("Error from creating CSI snapshot with file volume: %v", err)
		gomega.Expect(err).To(gomega.HaveOccurred())
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

		ginkgo.By("Expanding the current pvc")
		currentPvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("4Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvc, err = expandPVCSize(pvc, newSize, client)
		framework.Logf("Error from expanding file volume: %v", err)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})
})
