/*
Copyright 2022 The Kubernetes Authors.
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
	"strconv"
	"strings"
	"sync"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

var _ = ginkgo.Describe("[cns-manager-suspend-api] Suspend/Resume datastore API Testing", func() {

	f := framework.NewDefaultFramework("suspend-resume-datastore")

	var (
		client              clientset.Interface
		namespace           string
		ctx                 context.Context
		cancel              context.CancelFunc
		snapc               *snapclient.Clientset
		cnshost             string
		oauth               string
		isServiceStopped    bool
		pandoraSyncWaitTime int
		serviceName         string
		scParameters        map[string]string
		datacenter          string
		vsanshareddatastore string
		nfsshared           string
		vvolshared          string
		vmfsshared          string
		policyname          string
		datastoreURL        string
		// nfslocal            string
		// vvollocal           string
		// vmfslocal           string
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		scParameters = make(map[string]string)
		namespace = getNamespaceToRunTests(f)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		isServiceStopped = false
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		cnshost = os.Getenv("CNS_HOST_URL")
		if cnshost == "" {
			ginkgo.Skip("Env CNS_HOST_URL is missing")
		}

		oauth = os.Getenv("OAUTH")
		if oauth == "" {
			ginkgo.Skip("Env OAUTH is missing")
		}

		// Set up FCD
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		datacenter = "vcqaDC"
		vsanshareddatastore = "vsanDatastore"
		policyname = "shared-ds-policy"
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		nfsshared = "nfs1-1"
		vvolshared = "pepsishared"
		vmfsshared = "sharedVmfs-0"
		// nfslocal = "nfs0-1"
		// vvollocal = "pepsi1"
		// vmfslocal = "local-0"
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if isServiceStopped {
			if serviceName == "CSI" {
				framework.Logf("Starting CSI driver")
				ignoreLabels := make(map[string]string)
				err := updateDeploymentReplicawithWait(client, 1, vSphereCSIControllerPodNamePrefix,
					csiSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Wait for the CSI Pods to be up and Running
				list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				num_csi_pods := len(list_of_pods)
				err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0,
					pollTimeout, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else if serviceName == hostdServiceName {
				framework.Logf("In afterEach function to start the hostd service on all hosts")
				hostIPs := getAllHostsIP(ctx)
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
			} else {
				vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err := invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API, And try provisioning new volumes

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		7. Provision another Block/File volume using the same storage class created in step 2
		8. Wait and verify the volume's bound state is pending and confirm error contains the right message
		9. Invoke ResumeCNSVolumeProvisioning API
		10. Wait and verify the volume gets bound
		11. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		12. Delete all the volumes created from test
		13. Delete storage class

		Invoke Suspend and Resume CNSVolumeProvisioning API, Provision volume using Storage class having datastore URL

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore by passing the datastore URL
		3. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		4. Provision a Block/File volume using the storage class created above
		5. Wait and verify the volume's bound state is pending and confirm error contains the right message
		6. Invoke ResumeCNSVolumeProvisioning API
		7. Wait and verify the volume gets bound
		8. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		9. Delete all the volumes created from test
		10. Delete storage class
	*/
	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API, And try provisioning new volumes with dsURL", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			accessmode := v1.ReadWriteOnce
			var err error
			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamDatastoreURL] = datastoreURL
			// Check if it is file volumes setups
			if rwxAccessMode {
				scParameters[scParamFsType] = nfs4FSType
				accessmode = v1.ReadWriteMany
			}

			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx,
					storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Waiting for claim to be in bound phase on non-suspended datastore")
			persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
			volumeID := persistentvolumes[0].Spec.CSI.VolumeHandle
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

			defer func() {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			// Create PVC using above SC
			ginkgo.By("Create PVC2")
			pvc2, err := createPVC(client, namespace, nil, "", storageclass, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				if pvc2 != nil {
					ginkgo.By("Delete the PVC 2")
					err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, pvc2.Namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Waiting for pvclaim 2 to be in bound phase to fail as the datastore is suspended")
			_, err = fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionShortTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred())

			ginkgo.By("Invoke resume volume provisioning on datastore")
			result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

			ginkgo.By("Waiting for claim to be in bound phase to pass")
			persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
			gomega.Expect(volumeID2).NotTo(gomega.BeEmpty())

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC2 with VolumeID: %s", volumeID2))
			queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult2.Volumes) > 0).To(gomega.BeTrue())

			if !rwxAccessMode {
				datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumes := range datastore.ContainerVolumes {
					if volumes.FcdId == volumeID2 {
						framework.Logf("Found the FCD ID %s, on the node %s", volumes.FcdId,
							volumes.AttachmentDetails.Vm)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)

						jobId, _, err := migrateVolumes(cnshost, oauth, datacenter, vsanshareddatastore,
							nfsshared, volumes.FcdId, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("Current status of the migration task is %s", jobResult.JobStatus.OverallPhase)
						framework.Logf("Migrate of volume status is %s", jobResult.JobStatus.VolumeMigrationJobStatus.
							VolumeMigrationTasks[0].Phase)

						jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

						jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {
						framework.Logf("Now looking at the FCD %s", volumes.FcdId)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)
					}
				}
			}
			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC2 with VolumeID: %s", volumeID2))
			queryResult3, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult3.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API for one of the datastore and
			try provisioning volumes on the other datastore
		1. Pre-Setup
		2. Create a storage class to provision volumes (on vVol, vsanDatastore, vmfs,nfs)
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Invoke SuspendCNSVolumeProvisioning API on vsanDatastore, vmfs, nfs datastores
		7. Provision another Block/File volume to pick the available vVol datastore
		8. Wait and verify the volume gets bound
		9. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		10. Invoke ResumeCNSVolumeProvisioning API
		11. Delete all the volumes from test
		12. Verify all test created volumes are deleted
	*/
	ginkgo.It("Invoke suspend volume provisioning api on datastore and provision on another datastore", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		accessmode := v1.ReadWriteOnce

		if vanillaCluster {
			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname
			// Check if it is file volumes setups
			if rwxAccessMode {
				scParameters[scParamFsType] = nfs4FSType
				accessmode = v1.ReadWriteMany
				ginkgo.Skip("Test is not supported on non File-Vanilla setups")
			}

			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			defer func() {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Waiting for claim to be in bound phase to pass")
			persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := persistentvolumes[0].Spec.CSI.VolumeHandle
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err = suspendvolumeprovisioning(cnshost, oauth, datacenter, nfsshared, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, nfsshared, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err = suspendvolumeprovisioning(cnshost, oauth, datacenter, vmfsshared, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vmfsshared, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			// Create PVC using above SC
			ginkgo.By("Create pvc 2")
			pvc2, err := createPVC(client, namespace, nil, "", storageclass, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for claim to be in bound phase to pass")
			_, err = fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pv2 := getPvFromClaim(client, pvc2.Namespace, pvc2.Name)
			volumeID2 := pv2.Spec.CSI.VolumeHandle

			defer func() {
				if pvc2 != nil {
					ginkgo.By("Delete the PVC 2")
					err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, pvc2.Namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID2)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC2 with VolumeID: %s", volumeID2))
			queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult2.Volumes) > 0).To(gomega.BeTrue())
			if !rwxAccessMode {
				datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vvolshared)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumes := range datastore.ContainerVolumes {

					if volumes.FcdId == volumeID2 {
						framework.Logf("Found the FCD ID %s, on the node %s", volumes.FcdId,
							volumes.AttachmentDetails.Vm)
						framework.Logf("Source Datastore %s, and Datacenter is %s", datastore.Datastore,
							datastore.Datacenter)

						jobId, _, err := migrateVolumes(cnshost, oauth, datacenter, vvolshared,
							nfsshared, volumes.FcdId, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("Current status of the migration task is %s",
							jobResult.JobStatus.OverallPhase)
						framework.Logf("Migrate of volume status is %s",
							jobResult.JobStatus.VolumeMigrationJobStatus.VolumeMigrationTasks[0].Phase)

						jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

						jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {
						framework.Logf("Now looking at the FCD %s", volumes.FcdId)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)
					}
				}
			}
			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC2 with VolumeID: %s", volumeID2))
			queryResult3, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult3.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke SuspendCNSVolumeProvisioning API, And try deleting existing volumes

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		7. Provision another Block/File volume using the same storage class created in step 2
		8. Wait and verify the volume's bound state is pending and confirm error contains the right message
		9. Delete all the volumes created from test
		10. Delete storage class
		11. Confirm all volumes are deleted successfully
		12. Invoke ResumeCNSVolumeProvisioning API
	*/
	ginkgo.It("Invoke SuspendCNSVolumeProvisioning API, And try deleting existing volumes", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		accessmode := v1.ReadWriteOnce

		if vanillaCluster {
			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamDatastoreURL] = datastoreURL
			// Check if it is file volumes setups
			if rwxAccessMode {
				scParameters[scParamFsType] = nfs4FSType
				accessmode = v1.ReadWriteMany
			}

			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for claim to be in bound phase to pass")
			persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := persistentvolumes[0].Spec.CSI.VolumeHandle
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			defer func() {
				if pvclaim != nil {
					err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			// Create PVC using above SC
			pvc2, err := createPVC(client, namespace, nil, "", storageclass, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				if pvc2 != nil {
					ginkgo.By("Delete the PVC")
					err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, pvc2.Namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Waiting for claim to be in bound phase to fail")
			_, err = fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionShortTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred())

		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API, And try provisioning new volumes
			through StatefulSet application with podManagementPolicy=OrderedReady

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		4. Create an statefulset application with 5 replica using SC created above
		5. Wait and verify the volume's bound state is pending and confirm error contains the right message
		6. Invoke ResumeCNSVolumeProvisioning API
		7. Wait and verify the volume gets bound
		8. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		9. Delete STS application
		10. Delete all the volumes created from test
		11. Delete storage class
	*/
	ginkgo.It("Suspend/Resume volumes provisiong, test via STS podManagementPolicy=OrderedReady", func() {
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "nginx-sc-default-" + curtimestring + val
		accessmode := v1.ReadWriteOnce

		if vanillaCluster {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			scParameters[scParamDatastoreURL] = datastoreURL
			// Check if it is file volumes setups
			if rwxAccessMode {
				scParameters[scParamFsType] = nfs4FSType
				accessmode = v1.ReadWriteMany
			}

			ginkgo.By("Creating StorageClass for Statefulset")
			scSpec := getVSphereStorageClassSpec(scName, scParameters, nil, "", "", false)
			sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Creating service")
			service := CreateService(namespace, client)

			defer func() {
				deleteService(namespace, client, service)
			}()

			ginkgo.By("Creating statefulset")
			statefulset := GetStatefulSetFromManifest(namespace)
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Annotations["volume.beta.kubernetes.io/storage-class"] = scName
			statefulset.Spec.PodManagementPolicy = apps.OrderedReadyPodManagement
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.AccessModes[0] =
				accessmode
			replicas := *(statefulset.Spec.Replicas)

			_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
			framework.ExpectNoError(err)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
				fss.DeleteAllStatefulSets(client, namespace)
			}()

			// Adding an explicit wait here for the volume provision to kick start
			time.Sleep(30 * time.Second)

			ginkgo.By("Get all PVC created on namespace")
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				ginkgo.By("Waiting for claim to be in bound phase to fail")
				_, err = fpv.WaitForPVClaimBoundPhase(client,
					[]*v1.PersistentVolumeClaim{&claim}, framework.ClaimProvisionShortTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred())
			}

			ginkgo.By("Invoke resume volume provisioning on datastore")
			result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

			// Waiting for pods status to be Ready
			fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			var fcdIds [3]string
			// Get the list of Volumes attached to Pods before scale down
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for index, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						// Verify the attached volume match the one in CNS cache
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						fcdIds[index] = pv.Spec.CSI.VolumeHandle
					}
				}
			}

			if !rwxAccessMode {
				jobId, _, err := migrateVolumes(cnshost, oauth, datacenter, vsanshareddatastore, vmfsshared, "", false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("Current status of the migration task is %s", jobResult.JobStatus.OverallPhase)
				framework.Logf("Migrate of volume status is %s",
					jobResult.JobStatus.VolumeMigrationJobStatus.VolumeMigrationTasks[0].Phase)

				jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

				jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API, And try provisioning new volumes
			through StatefulSet application with podManagementPolicy=Parallel

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		4. Create an statefulset application with 5 replica using SC created above
		5. Wait and verify the volume's bound state is pending and confirm error contains the right message
		6. Invoke ResumeCNSVolumeProvisioning API
		7. Wait and verify the volume gets bound
		8. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		9. Delete STS application
		10. Delete all the volumes created from test
		11. Delete storage class
	*/
	ginkgo.It("Suspend/Resume volumes provisiong, test via STS podManagementPolicy=Parallel", func() {
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "nginx-sc-default-" + curtimestring + val
		accessmode := v1.ReadWriteOnce

		if vanillaCluster {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			scParameters[scParamDatastoreURL] = datastoreURL
			// Check if it is file volumes setups
			if rwxAccessMode {
				scParameters[scParamFsType] = nfs4FSType
				accessmode = v1.ReadWriteMany
			}

			ginkgo.By("Creating StorageClass for Statefulset")
			scSpec := getVSphereStorageClassSpec(scName, scParameters, nil, "", "", false)
			sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Creating service")
			service := CreateService(namespace, client)

			defer func() {
				deleteService(namespace, client, service)
			}()

			ginkgo.By("Creating statefulset")
			statefulset := GetStatefulSetFromManifest(namespace)
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Annotations["volume.beta.kubernetes.io/storage-class"] = scName
			statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.AccessModes[0] =
				accessmode
			replicas := *(statefulset.Spec.Replicas)

			_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
			framework.ExpectNoError(err)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
				fss.DeleteAllStatefulSets(client, namespace)
			}()

			// Adding an explicit wait here for the volume provision to kick start
			time.Sleep(30 * time.Second)

			ginkgo.By("Get all PVC created on namespace")
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				ginkgo.By("Waiting for claim to be in bound phase to fail")
				_, err = fpv.WaitForPVClaimBoundPhase(client,
					[]*v1.PersistentVolumeClaim{&claim}, framework.ClaimProvisionShortTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred())
			}

			ginkgo.By("Invoke resume volume provisioning on datastore")
			result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

			// Waiting for pods status to be Ready
			fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			var fcdIds [3]string
			// Get the list of Volumes attached to Pods before scale down
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for index, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace,
							volumespec.PersistentVolumeClaim.ClaimName)
						// Verify the attached volume match the one in CNS cache
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						fcdIds[index] = pv.Spec.CSI.VolumeHandle
					}
				}
			}
			if !rwxAccessMode {
				jobId, _, err := migrateVolumes(cnshost, oauth, datacenter, vsanshareddatastore, vmfsshared, "", false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("Current status of the migration task is %s", jobResult.JobStatus.OverallPhase)
				framework.Logf("Migrate of volume status is %s",
					jobResult.JobStatus.VolumeMigrationJobStatus.VolumeMigrationTasks[0].Phase)

				jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

				jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API, Static volume provision
		1. Pre-Setup
		2. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		3. Create FCD and wait for fcd to allow syncing with pandora
		4. Create PV Spec with volumeID set to FCDID created above
		5. Create PVC with the storage request set to PV's storage capacity
		6. Wait and verify the volume is bound
		7. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Invoke ResumeCNSVolumeProvisioning API
		9. Delete all the volumes created from test
		10. Delete storage class
	*/
	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API, Static volume provision", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			var err error
			deleteFCDRequired := false
			var defaultDatastore *object.Datastore
			var defaultDatacenter *object.Datacenter
			var datacenters []string
			datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finder := find.NewFinder(e2eVSphere.Client.Client, false)
			cfg, err := getConfig()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			dcList := strings.Split(cfg.Global.Datacenters, ",")
			for _, dc := range dcList {
				dcName := strings.TrimSpace(dc)
				if dcName != "" {
					datacenters = append(datacenters, dcName)
				}
			}

			for _, dc := range datacenters {
				defaultDatacenter, err = finder.Datacenter(ctx, dc)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				finder.SetDatacenter(defaultDatacenter)
				defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Creating FCD Disk")
			fcdID, err := e2eVSphere.createFCD(ctx, "BasicStaticFCD", diskSizeInMb, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			deleteFCDRequired = true
			framework.Logf("FCD ID is : %s", fcdID)

			defer func() {
				if deleteFCDRequired && fcdID != "" && defaultDatastore != nil {
					ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					err := e2eVSphere.deleteFCD(ctx, fcdID, defaultDatastore.Reference())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
				pandoraSyncWaitTime, fcdID))
			time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

			// Creating label for PV. PVC will use this label as Selector to find PV
			staticPVLabels := make(map[string]string)
			staticPVLabels["fcd-id"] = fcdID

			ginkgo.By("Creating the PV")
			pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimDelete, staticPVLabels)
			pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
			if err != nil {
				return
			}

			defer func() {
				ginkgo.By("Verify PV should be deleted automatically")
				framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeout))
			}()

			err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := pv.Spec.CSI.VolumeHandle

			ginkgo.By("Creating the PVC")
			pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
			pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				ginkgo.By("Deleting the PV Claim")
				framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace),
					"Failed to delete PVC ", pvc.Name)
			}()

			// Wait for PV and PVC to Bind
			framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(),
				namespace, pv, pvc))

			// Set deleteFCDRequired to false.
			// After PV, PVC is in the bind state, Deleting PVC should delete container volume.
			// So no need to delete FCD directly using vSphere API call.
			deleteFCDRequired = false

			ginkgo.By("Verifying CNS entry is present in cache")
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By("Creating pod to attach PV to the node")
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc},
				false, execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				// Delete POD
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			var vmUUID string
			nodeName := pod.Spec.NodeName

			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			if !rwxAccessMode {
				datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				for _, volumes := range datastore.OtherVolumes {
					framework.Logf("Now looking at the FCD %s", volumes.FcdId)
					framework.Logf("Source Datastore %s, and Datacenter is %s", datastore.Datastore,
						datastore.Datacenter)
					if volumes.FcdId == fcdID {
						framework.Logf("Found the FCD ID %s, on the node %s", volumes.FcdId,
							volumes.AttachmentDetails.Vm)
						framework.Logf("Source Datastore %s, and Datacenter is %s", datastore.Datastore,
							datastore.Datacenter)

						jobId, _, err := migrateVolumes(cnshost, oauth, datacenter, vsanshareddatastore,
							vmfsshared, volumes.FcdId, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						ginkgo.By("Verify the volume is accessible and Read/write is possible")
						cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"cat /mnt/volume1/Pod1.html "}
						output := framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

						wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
						framework.RunKubectlOrDie(namespace, wrtiecmd...)
						output = framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output,
							"Hello message from test into Pod1")).NotTo(gomega.BeFalse())

						jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("Current status of the migration task is %s", jobResult.JobStatus.OverallPhase)
						framework.Logf("Migrate of volume status is %s",
							jobResult.JobStatus.VolumeMigrationJobStatus.VolumeMigrationTasks[0].Phase)

						jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

						jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API, Verify the volumes
			are accessible by pods and are able to write into it
		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create a pod using the volume created above
		7. Write a text message into the volume
		8. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		9. Continue writing into the volume with random text message
		10. Verify the volume pods are still stable and text written into volume is upto date
		11. Invoke ResumeCNSVolumeProvisioning API
		12. Delete all the volumes created from test
		13. Delete storage class
	*/
	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API, Verify the volumes"+
		" are accessible by pods and are able to write into it", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error
			accessmode := v1.ReadWriteOnce

			scParameters[scParamDatastoreURL] = datastoreURL
			// Check if it is file volumes setups
			if rwxAccessMode {
				scParameters[scParamFsType] = nfs4FSType
				accessmode = v1.ReadWriteMany
			}
			ginkgo.By("Creating Storage Class and PVC")
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", true, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Waiting for claim to be in bound phase")
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
			volumeID := pv.Spec.CSI.VolumeHandle

			defer func() {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if volumeID != "" {
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By("Creating pod to attach PV to the node")
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
				false, execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				// Delete POD
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			var vmUUID string
			nodeName := pod.Spec.NodeName

			if vanillaCluster {
				vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
			}

			if !rwxAccessMode {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
			}
			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Verify the volume is accessible and Read/write is possible")
			cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat /mnt/volume1/Pod1.html "}
			output := framework.RunKubectlOrDie(namespace, cmd...)
			gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

			wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
			framework.RunKubectlOrDie(namespace, wrtiecmd...)
			output = framework.RunKubectlOrDie(namespace, cmd...)
			gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

			if !rwxAccessMode {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
			}
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API, Verify new volume provision post resume is successful

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		4. Provision a Block/File volume using the storage class created above
		5. Wait and verify the volume's bound state is pending and confirm error contains the right message
		6. Invoke ResumeCNSVolumeProvisioning API
		7. Wait and verify the volume gets bound
		8. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		9. Create new volumes freshly post resume API is invoked using the same storage class created above
		10. Wait and verify the volume gets bound
		11. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		12. Delete all the volumes created from test
		13. Delete storage class
	*/
	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API, Verify new volume "+
		"provision post resume is successful", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error
			accessmode := v1.ReadWriteOnce

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamDatastoreURL] = datastoreURL
			// Check if it is file volumes setups
			// if rwxAccessMode {
			// 	scParameters[scParamFsType] = nfs4FSType
			// 	accessmode = v1.ReadWriteMany
			// }
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "",
				nil, "", true, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			defer func() {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Waiting for claim to be in bound phase to fail")
			_, err = fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionShortTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred())

			ginkgo.By("Invoke resume volume provisioning on datastore")
			result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

			ginkgo.By("Waiting for claim to be in bound phase to pass")
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionShortTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
			volumeID := pv.Spec.CSI.VolumeHandle

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			// Create PVC using above SC
			pvc2, err := createPVC(client, namespace, nil, "", storageclass, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				if pvc2 != nil {
					ginkgo.By("Delete the PVC")
					err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, pvc2.Namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Waiting for claim to be in bound phase to pass")
			persistentvolumes1, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID2 := persistentvolumes1[0].Spec.CSI.VolumeHandle

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC2 with VolumeID: %s", volumeID2))
			queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult2.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By("Creating pod to attach PV to the node")
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc2},
				false, execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				// Delete POD
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			var vmUUID string
			nodeName := pod.Spec.NodeName

			if vanillaCluster {
				vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
			}

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID2, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID2, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			if !rwxAccessMode {
				datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumes := range datastore.ContainerVolumes {

					if volumes.FcdId == volumeID2 {
						framework.Logf("Found the FCD ID %s, on the node %s",
							volumes.FcdId, volumes.AttachmentDetails.Vm)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)

						jobId, _, err := migrateVolumes(cnshost, oauth, datacenter, vsanshareddatastore, nfsshared,
							volumes.FcdId, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						ginkgo.By("Verify the volume is accessible and Read/write is possible")
						cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"cat /mnt/volume1/Pod1.html "}
						output := framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

						wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
						framework.RunKubectlOrDie(namespace, wrtiecmd...)
						output = framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output,
							"Hello message from test into Pod1")).NotTo(gomega.BeFalse())

						jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("Current status of the migration task is %s",
							jobResult.JobStatus.OverallPhase)
						framework.Logf("Migrate of volume status is %s",
							jobResult.JobStatus.VolumeMigrationJobStatus.VolumeMigrationTasks[0].Phase)

						jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

						jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {
						framework.Logf("Now looking at the FCD %s", volumes.FcdId)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)
					}
				}
			}
			// Modify PVC spec to trigger volume expansion
			// We expand the PVC while no pod is using it to ensure offline expansion
			currentPvcSize := pvc2.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("4Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			claims, err := expandPVCSize(pvc2, newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(claims).NotTo(gomega.BeNil())

			ginkgo.By("Waiting for file system resize to finish")
			claims, err = waitForFSResize(pvc2, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pvcConditions := claims.Status.Conditions
			expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID2, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID2, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API, Verify attach/detach volume

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait and verify the volume gets bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		7. Create new pod and attach to pvc created above
		8. Verify pod is running and volume is attached
		9. Delete pod and verify the detach works
		10. Delete all the volumes created from test
		11. Delete storage class
		12. Invoke ResumeCNSVolumeProvisioning API
	*/
	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API, Verify attach/detach volume", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error
			accessmode := v1.ReadWriteOnce

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamDatastoreURL] = datastoreURL
			// Check if it is file volumes setups
			// if rwxAccessMode {
			// 	scParameters[scParamFsType] = nfs4FSType
			// 	accessmode = v1.ReadWriteMany
			// }
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "",
				nil, "", true, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			defer func() {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Waiting for claim to be in bound phase to pass")
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionShortTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
			volumeID := pv.Spec.CSI.VolumeHandle

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Creating pod to attach PV to the node")
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
				false, execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				// Delete POD
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			var vmUUID string
			nodeName := pod.Spec.NodeName

			if vanillaCluster {
				vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
			}

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			if !rwxAccessMode {
				datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumes := range datastore.ContainerVolumes {

					if volumes.FcdId == volumeID {
						framework.Logf("Found the FCD ID %s, on the node %s",
							volumes.FcdId, volumes.AttachmentDetails.Vm)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)

						jobId, _, err := migrateVolumes(cnshost, oauth, datacenter, vsanshareddatastore, nfsshared,
							volumes.FcdId, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						ginkgo.By("Verify the volume is accessible and Read/write is possible")
						cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"cat /mnt/volume1/Pod1.html "}
						output := framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

						wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
						framework.RunKubectlOrDie(namespace, wrtiecmd...)
						output = framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output,
							"Hello message from test into Pod1")).NotTo(gomega.BeFalse())

						jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("Current status of the migration task is %s",
							jobResult.JobStatus.OverallPhase)
						framework.Logf("Migrate of volume status is %s",
							jobResult.JobStatus.VolumeMigrationJobStatus.VolumeMigrationTasks[0].Phase)

						jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

						jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {
						framework.Logf("Now looking at the FCD %s", volumes.FcdId)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)
					}
				}
			}
			// Modify PVC spec to trigger volume expansion
			// We expand the PVC while no pod is using it to ensure offline expansion
			currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("4Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			claims, err := expandPVCSize(pvclaim, newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(claims).NotTo(gomega.BeNil())

			ginkgo.By("Waiting for file system resize to finish")
			claims, err = waitForFSResize(pvclaim, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pvcConditions := claims.Status.Conditions
			expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API, On non-existing Datastore

		1. Pre-Setup
		2. Invoke SuspendCNSVolumeProvisioning API on an non-existing or deleted datastore
		3. Check the API invocation fails
		4. Invoke ResumeCNSVolumeProvisioning API on an non-existing or deleted datastore
		5. Check the API invocation fails
	*/
	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API, On non-existing Datastore", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("Invoke suspend volume provisioning on datastore")
			_, statusCode, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, "dummy-value", true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(statusCode == 500).To(gomega.BeTrue())

			ginkgo.By("Invoke resume volume provisioning on datastore")
			_, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, "dummy-value", true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(statusCode == 500).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API multiple times or concurrently

		1. Pre-Setup
		2. Invoke SuspendCNSVolumeProvisioning API on vsanDatastore
		3. Invoke SuspendCNSVolumeProvisioning API on vsanDatastore again
		4. API is expected to respond positively
		5. Invoke ResumeCNSVolumeProvisioning API on vsanDatastore
		6. Invoke ResumeCNSVolumeProvisioning API on vsanDatastore again
		7. API is expected to respond positively
	*/
	ginkgo.It("Invoke suspend/resume volume provisioning api, invoke API multiple times", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error
			accessmode := v1.ReadWriteOnce

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err = suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err = suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err = suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err = suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamDatastoreURL] = datastoreURL
			// Check if it is file volumes setups
			if rwxAccessMode {
				scParameters[scParamFsType] = nfs4FSType
				accessmode = v1.ReadWriteMany
			}

			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			defer func() {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Waiting for claim to be in bound phase to fail")
			_, err = fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionShortTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred())

			ginkgo.By("Invoke resume volume provisioning on datastore")
			result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

			ginkgo.By("Waiting for claim to be in bound phase to pass")
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
			volumeID := pv.Spec.CSI.VolumeHandle

			if !rwxAccessMode {
				datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumes := range datastore.ContainerVolumes {

					if volumes.FcdId == volumeID {
						framework.Logf("Found the FCD ID %s, on the node %s", volumes.FcdId, volumes.AttachmentDetails.Vm)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)

						jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
							vsanshareddatastore, nfsshared, volumes.FcdId, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("Current status of the migration task is %s", jobResult.JobStatus.OverallPhase)
						framework.Logf("Migrate of volume status is %s",
							jobResult.JobStatus.VolumeMigrationJobStatus.VolumeMigrationTasks[0].Phase)

						jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

						jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {
						framework.Logf("Now looking at the FCD %s", volumes.FcdId)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)
					}
				}
			}
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API while the vsan-health service is down

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Bring down vsan-health service
		7. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore and check the status
		8. Bring back the vsan-health service
		9. Provision another Block/File volume using the same storage class created in step 2
		10. Wait and verify the volume's bound state is pending and confirm error contains the right message
		11. Bring down vsan-health service
		12. Invoke ResumeCNSVolumeProvisioning API
		13. Bring back the vsan-health service
		14. Wait and verify the volume gets bound
		15. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		16. Delete all the volumes created from test
		17. Delete storage class
	*/
	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API while the vsan-health service is down", func() {
		serviceName = vsanhealthServiceName
		suspendResumeVolumeWhilevsanHealthServiceIsDown(serviceName, namespace, client, isServiceStopped,
			scParameters, cnshost, oauth, datacenter, vsanshareddatastore, datastoreURL, vmfsshared)
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API while the hostd service is down

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Bring down hostd service on all of the hosts
		7. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore and check the status
		8. Bring up the hostd service on all the hosts
		9. Provision another Block/File volume using the same storage class created in step 2
		10. Wait and verify the volume's bound state is pending and confirm error contains the right message
		11. Bring down hostd service on all of the hosts
		12. Invoke ResumeCNSVolumeProvisioning API
		13. Bring back the hostd service on all of the hosts
		14. Wait and verify the volume gets bound
		15. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		16. Delete all the volumes created from test
		17. Delete storage class
	*/

	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API while the hostd service is down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error
			accessmode := v1.ReadWriteOnce

			scParameters[scParamDatastoreURL] = datastoreURL
			// Check if it is file volumes setups
			if rwxAccessMode {
				scParameters[scParamFsType] = nfs4FSType
				accessmode = v1.ReadWriteMany
			}

			ginkgo.By("Creating Storage Class and PVC")
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Waiting for claim to be in bound phase pass")
			persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionShortTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := persistentvolumes[0].Spec.CSI.VolumeHandle
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

			defer func() {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if volumeID != "" {
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs := getAllHostsIP(ctx)
			isServiceStopped = true

			var wg sync.WaitGroup
			wg.Add(len(hostIPs))

			for _, hostIP := range hostIPs {
				go stopHostD(ctx, hostIP, &wg)
			}
			wg.Wait()

			defer func() {
				framework.Logf("In defer function to start the hostd service on all hosts")
				if isServiceStopped {
					for _, hostIP := range hostIPs {
						startHostDOnHost(ctx, hostIP)
					}
					isServiceStopped = false
				}
			}()

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs = getAllHostsIP(ctx)
			isServiceStopped = true
			if isServiceStopped {
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
				isServiceStopped = false
			}

			//Reestablish the connections
			bootstrap()

			// Create PVC using above SC
			pvc2, err := createPVC(client, namespace, nil, "", storageclass, accessmode)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				if pvc2 != nil {
					ginkgo.By("Delete the PVC")
					err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, pvc2.Namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Waiting for claim to be in bound phase to fail")
			_, err = fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionShortTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred())

			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs = getAllHostsIP(ctx)
			isServiceStopped = true

			wg.Add(len(hostIPs))

			for _, hostIP := range hostIPs {
				go stopHostD(ctx, hostIP, &wg)
			}
			wg.Wait()

			defer func() {
				framework.Logf("In defer function to start the hostd service on all hosts")
				if isServiceStopped {
					for _, hostIP := range hostIPs {
						startHostDOnHost(ctx, hostIP)
					}
					isServiceStopped = false
				}
			}()

			//Reestablish the connections
			bootstrap()

			ginkgo.By("Invoke resume volume provisioning on datastore")
			result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs = getAllHostsIP(ctx)
			isServiceStopped = true
			if isServiceStopped {
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
				isServiceStopped = false
			}

			ginkgo.By("Waiting for claim to be in bound phase pass")
			persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
			gomega.Expect(volumeID2).NotTo(gomega.BeEmpty())

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC2 with VolumeID: %s", volumeID2))
			queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult2.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API while the SPS service is down

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Bring down SPS service
		7. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore and check the status
		8. Bring up the SPS service
		9. Provision another Block/File volume using the same storage class created in step 2
		10. Wait and verify the volume's bound state is pending and confirm error contains the right message
		11. Bring down SPS service
		12. Invoke ResumeCNSVolumeProvisioning API
		13. Bring back the SPS service
		14. Wait and verify the volume gets bound
		15. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		16. Delete all the volumes created from test
		17. Delete storage class
	*/

	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API while the SPS service is down", func() {
		serviceName = spsServiceName
		suspendResumeVolumeWhileServiceIsDown(serviceName, namespace, client, isServiceStopped, scParameters,
			cnshost, oauth, datacenter, vsanshareddatastore, datastoreURL, vmfsshared)
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API while the csi-controller is down

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Bring down csi-controller from cluster
		7. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore and check the status
		8. Bring up the csi-controller service
		9. Provision another Block/File volume using the same storage class created in step 2
		10. Wait and verify the volume's bound state is pending and confirm error contains the right message
		11. Bring down csi-controller service
		12. Invoke ResumeCNSVolumeProvisioning API
		13. Bring back the csi-controller service
		14. Wait and verify the volume gets bound
		15. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		16. Delete all the volumes created from test
		17. Delete storage class
	*/

	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API while the csi-controller is down", func() {
		serviceName = "CSI"
		suspendResumeVolumeWhileServiceIsDown(serviceName, namespace, client, isServiceStopped, scParameters,
			cnshost, oauth, datacenter, vsanshareddatastore, datastoreURL, vmfsshared)
	})

	/*
		Invoke Suspend and Resume CNSVolumeProvisioning API while the vpxd is down

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Bring down csi-controller from cluster
		7. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore and check the status
		8. Bring up the csi-controller service
		9. Provision another Block/File volume using the same storage class created in step 2
		10. Wait and verify the volume's bound state is pending and confirm error contains the right message
		11. Bring down csi-controller service
		12. Invoke ResumeCNSVolumeProvisioning API
		13. Bring back the csi-controller service
		14. Wait and verify the volume gets bound
		15. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		16. Delete all the volumes created from test
		17. Delete storage class
	*/

	ginkgo.It("Invoke Suspend and Resume CNSVolumeProvisioning API while the vpxd is down", func() {
		serviceName = vpxdServiceName
		suspendResumeVolumeWhileVpxdServiceIsDown(serviceName, namespace, client, isServiceStopped, scParameters,
			cnshost, oauth, datacenter, vsanshareddatastore, datastoreURL, nfsshared)
	})

	/*
		Post SuspendCNSVolumeProvisioning API, Perform Online resize

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create POD using above created PVC
		7. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		8. Modify size of PVC to trigger online volume expansion
		9. Verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		10. Verify the resized PVC by doing CNS query
		11. Make sure data is intact on the PV mounted on the pod
		12. Make sure file system has increased
		13. Invoke ResumeCNSVolumeProvisioning API
		14. Delete pod
		15. Delete all the volumes created from test
		16. Delete storage class
	*/
	ginkgo.It("Post SuspendCNSVolumeProvisioning API - Perform Online resize", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamDatastoreURL] = datastoreURL
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", true, v1.ReadWriteOnce)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Waiting for claim to be in bound phase")
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
			volumeID := pv.Spec.CSI.VolumeHandle

			defer func() {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if volumeID != "" {
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Creating pod to attach PV to the node")
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
				false, execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				// Delete POD
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			var vmUUID string
			nodeName := pod.Spec.NodeName

			if vanillaCluster {
				vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
			}

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			if !rwxAccessMode {
				datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				for _, volumes := range datastore.ContainerVolumes {
					if volumes.FcdId == volumeID {
						framework.Logf("Found the FCD ID %s, on the node %s",
							volumes.FcdId, volumes.AttachmentDetails.Vm)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)

						jobId, _, err := migrateVolumes(cnshost, oauth, datacenter, vsanshareddatastore, nfsshared,
							volumes.FcdId, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						ginkgo.By("Verify the volume is accessible and Read/write is possible")
						cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"cat /mnt/volume1/Pod1.html "}
						output := framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

						wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
						framework.RunKubectlOrDie(namespace, wrtiecmd...)
						output = framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output,
							"Hello message from test into Pod1")).NotTo(gomega.BeFalse())

						jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("Current status of the migration task is %s",
							jobResult.JobStatus.OverallPhase)
						framework.Logf("Migrate of volume status is %s",
							jobResult.JobStatus.VolumeMigrationJobStatus.VolumeMigrationTasks[0].Phase)

						jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

						jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {
						framework.Logf("Now looking at the FCD %s", volumes.FcdId)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)
					}
				}
			}
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			// Modify PVC spec to trigger volume expansion
			// We expand the PVC while no pod is using it to ensure offline expansion
			currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("4Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			claims, err := expandPVCSize(pvclaim, newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(claims).NotTo(gomega.BeNil())

			ginkgo.By("Waiting for file system resize to finish")
			claims, err = waitForFSResize(pvclaim, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pvcConditions := claims.Status.Conditions
			expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

			ginkgo.By("Verify the volume is accessible and Read/write is possible")
			cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat /mnt/volume1/Pod1.html "}
			output := framework.RunKubectlOrDie(namespace, cmd...)
			if !rwxAccessMode {
				gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
			} else {
				gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())
			}

			wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"echo 'Hello message-2 from test into Pod1' > /mnt/volume1/Pod1.html"}
			framework.RunKubectlOrDie(namespace, wrtiecmd...)
			output = framework.RunKubectlOrDie(namespace, cmd...)
			gomega.Expect(strings.Contains(output, "Hello message-2 from test into Pod1")).NotTo(gomega.BeFalse())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Post SuspendCNSVolumeProvisioning API - Perform Offline resize

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Modify size of PVC to trigger offline volume expansion
		7. Wait for some time and verify that "FilesystemResizePending"  on PVC
		8. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		9. Create POD using the above PVC
		10. Verify "FilesystemResizePending" is removed from PVC
		11. Query CNS volume and make sure new size is updated
		12. Verify volume is mounted on POD
		13. Verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		14. Verify the resized PVC by doing CNS query
		15. Make sure file system has increased
		16. Invoke ResumeCNSVolumeProvisioning API
		17. Delete pod
		18. Delete all the volumes created from test
		19. Delete storage class
	*/

	ginkgo.It("Post SuspendCNSVolumeProvisioning API - Perform Offline resize", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamDatastoreURL] = datastoreURL

			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", true, v1.ReadWriteOnce)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Waiting for claim to be in bound phase")
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
			volumeID := pv.Spec.CSI.VolumeHandle

			defer func() {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if volumeID != "" {
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By("Expanding current pvc")
			currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("1Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			pvclaim, err = expandPVCSize(pvclaim, newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvclaim).NotTo(gomega.BeNil())

			pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			if pvcSize.Cmp(newSize) != 0 {
				framework.Failf("error updating pvc size %q", pvclaim.Name)
			}

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			ginkgo.By("Waiting for controller volume resize to finish")
			err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Checking for conditions on pvc")
			pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace,
				pvclaim.Name, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if len(queryResult.Volumes) == 0 {
				err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying disk size requested in volume expansion is honored")
			newSizeInMb := int64(3072)
			if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb !=
				newSizeInMb {
				err = fmt.Errorf("got wrong disk size after volume expansion")
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Post SuspendCNSVolumeProvisioning API - Perform snapshot operation

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Modify size of PVC to trigger offline volume expansion
		7. wait for some time and verify that "FilesystemResizePending"  on PVC
		8. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		9. Take multiple snapshots for the volume
		10. Verify snapshot operation is success
		11. Invoke ResumeCNSVolumeProvisioning API
		12. Delete snapshots for the volume
		13. Delete all the volumes created from test
		14. Delete storage class
	*/
	ginkgo.It("Post SuspendCNSVolumeProvisioning API - Perform snapshot operation", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error
			var snapshotContentCreated = false
			var snapshotCreated = false

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamDatastoreURL] = datastoreURL

			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, v1.ReadWriteOnce)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			defer func() {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Waiting for claim to be in bound phase")
			pvc, err := fpv.WaitForPVClaimBoundPhase(client,
				[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvc).NotTo(gomega.BeEmpty())
			pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
			volumeID := pv.Spec.CSI.VolumeHandle
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By("Creating pod to attach PV to the node")
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
				false, execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				// Delete POD
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			var vmUUID string
			nodeName := pod.Spec.NodeName

			if vanillaCluster {
				vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
			}

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			ginkgo.By("Invoke suspend volume provisioning on datastore")
			result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter,
				vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

			defer func() {
				ginkgo.By("Invoke resume volume provisioning on datastore")
				result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter,
					vsanshareddatastore, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
			}()

			//Get snapshot client using the rest config
			restConfig = getRestConfigClient()
			snapc, err = snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create volume snapshot class")
			volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
				getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				framework.Logf("Deleting volume snapshot class")
				err := snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx,
					volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Create a volume snapshot")
			volumeSnapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Volume snapshot name is : %s", volumeSnapshot.Name)

			defer func() {
				if snapshotContentCreated {
					framework.Logf("Deleting volume snapshot content")
					err := snapc.SnapshotV1().VolumeSnapshotContents().Delete(ctx,
						*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.DeleteOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					err := snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
						volumeSnapshot.Name, metav1.DeleteOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			ginkgo.By("Verify volume snapshot is created")
			volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace,
				volumeSnapshot.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotCreated = true
			gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotContentCreated = true
			gomega.Expect(*snapshotContent.Status.ReadyToUse).To(gomega.BeTrue())

			framework.Logf("Get volume snapshot ID from snapshot handle")
			snapshothandle := *snapshotContent.Status.SnapshotHandle
			snapshotId := strings.Split(snapshothandle, "+")[1]

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = verifySnapshotIsCreatedInCNS(volumeID, snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if !rwxAccessMode {
				datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumes := range datastore.ContainerVolumes {

					if volumes.FcdId == volumeID {
						framework.Logf("Found the FCD ID %s, on the node %s", volumes.FcdId, volumes.AttachmentDetails.Vm)
						framework.Logf("Source Datastore %s, and Datacenter is %s",
							datastore.Datastore, datastore.Datacenter)

						jobId, _, err := migrateVolumes(cnshost, oauth, datacenter, vsanshareddatastore,
							vvolshared, volumes.FcdId, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						ginkgo.By("Verify the volume is accessible and Read/write is possible")
						cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"cat /mnt/volume1/Pod1.html "}
						output := framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

						wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
						framework.RunKubectlOrDie(namespace, wrtiecmd...)
						output = framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

						jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("Current status of the migration task is %s", jobResult.JobStatus.OverallPhase)
						framework.Logf("Migrate of volume status is %s",
							jobResult.JobStatus.VolumeMigrationJobStatus.VolumeMigrationTasks[0].Phase)

						jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

						jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						wrtiecmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
							"echo 'new message from test into Pod1' > /mnt/volume1/Pod1.html"}
						framework.RunKubectlOrDie(namespace, wrtiecmd...)

						ginkgo.By("Verify the volume is accessible and Read/write is possible")
						output = framework.RunKubectlOrDie(namespace, cmd...)
						gomega.Expect(strings.Contains(output, "new message from test into Pod1")).NotTo(gomega.BeFalse())

						ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
						isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

					} else {
						framework.Logf("Now looking at the FCD %s", volumes.FcdId)
						framework.Logf("Source Datastore %s, and Datacenter is %s", datastore.Datastore, datastore.Datacenter)
					}
				}
			}

			ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
			err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx, volumeSnapshot.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotCreated = false

			framework.Logf("Wait till the volume snapshot is deleted")
			err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx,
				*volumeSnapshot.Status.BoundVolumeSnapshotContentName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotContentCreated = false

			ginkgo.By("Verify snapshot entry is deleted from CNS")
			err = verifySnapshotIsDeletedInCNS(volumeID, snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

})

func suspendResumeVolumeWhileServiceIsDown(serviceName string, namespace string,
	client clientset.Interface, isServiceStopped bool,
	scParameters map[string]string, cnshost string, oauth string, datacenter string,
	vsanshareddatastore string, datastoreURL string, vmfsshared string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for Vanilla setup")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		ignoreLabels := make(map[string]string)
		accessmode := v1.ReadWriteOnce

		scParameters[scParamDatastoreURL] = datastoreURL
		// Check if it is file volumes setups
		if rwxAccessMode {
			scParameters[scParamFsType] = nfs4FSType
			accessmode = v1.ReadWriteMany
		}

		ginkgo.By("Creating Storage Class and PVC")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
			scParameters, "", nil, "", false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for claim to be in bound phase")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(persistentvolumes).NotTo(gomega.BeEmpty())
		volumeID := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if volumeID != "" {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

		if serviceName == "CSI" {
			ginkgo.By("Stopping CSI driver")
			// TODO: Stop printing csi logs on the console
			collectPodLogs(ctx, client, csiSystemNamespace)
			err = updateDeploymentReplicawithWait(client, 0, vSphereCSIControllerPodNamePrefix,
				csiSystemNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = true

			defer func() {
				if isServiceStopped {
					framework.Logf("Starting CSI driver")
					err = updateDeploymentReplicawithWait(client, 1, vSphereCSIControllerPodNamePrefix,
						csiSystemNamespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					// Wait for the CSI Pods to be up and Running
					list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					num_csi_pods := len(list_of_pods)
					err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0,
						pollTimeout, ignoreLabels)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isServiceStopped = false
				}
			}()
		} else if serviceName == hostdServiceName {
			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs := getAllHostsIP(ctx)
			isServiceStopped = true

			var wg sync.WaitGroup
			wg.Add(len(hostIPs))

			for _, hostIP := range hostIPs {
				go stopHostD(ctx, hostIP, &wg)
			}
			wg.Wait()

			defer func() {
				framework.Logf("In defer function to start the hostd service on all hosts")
				if isServiceStopped {
					for _, hostIP := range hostIPs {
						startHostDOnHost(ctx, hostIP)
					}
					isServiceStopped = false
				}
			}()

		} else {
			ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			err = invokeVCenterServiceControl(stopOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = true
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcStoppedMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				if isServiceStopped {
					ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
					err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isServiceStopped = false
				}
			}()
		}

		ginkgo.By("Invoke suspend volume provisioning on datastore")
		result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

		defer func() {
			ginkgo.By("Invoke resume volume provisioning on datastore")
			result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
		}()

		if serviceName == "CSI" {
			if isServiceStopped {
				framework.Logf("Starting CSI driver")
				err = updateDeploymentReplicawithWait(client, 1, vSphereCSIControllerPodNamePrefix,
					csiSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// Wait for the CSI Pods to be up and Running
				list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				num_csi_pods := len(list_of_pods)
				err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0,
					pollTimeout, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}

		} else if serviceName == hostdServiceName {
			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs := getAllHostsIP(ctx)
			isServiceStopped = true
			if isServiceStopped {
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
				isServiceStopped = false
			}
		} else {
			if isServiceStopped {
				vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}
		}

		// Create PVC using above SC
		pvc2, err := createPVC(client, namespace, nil, "", storageclass, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if pvc2 != nil {
				ginkgo.By("Delete the PVC")
				err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, pvc2.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Waiting for claim to be in bound phase to fail")
		_, err = fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionShortTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())

		if serviceName == "CSI" {
			ginkgo.By("Stopping CSI driver")
			// TODO: Stop printing csi logs on the console
			collectPodLogs(ctx, client, csiSystemNamespace)
			err = updateDeploymentReplicawithWait(client, 0, vSphereCSIControllerPodNamePrefix,
				csiSystemNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = true

			defer func() {
				if isServiceStopped {
					framework.Logf("Starting CSI driver")
					err = updateDeploymentReplicawithWait(client, 1, vSphereCSIControllerPodNamePrefix,
						csiSystemNamespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					// Wait for the CSI Pods to be up and Running
					list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					num_csi_pods := len(list_of_pods)
					err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0,
						pollTimeout, ignoreLabels)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isServiceStopped = false
				}
			}()
		} else if serviceName == hostdServiceName {
			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs := getAllHostsIP(ctx)
			isServiceStopped = true

			var wg sync.WaitGroup
			wg.Add(len(hostIPs))

			for _, hostIP := range hostIPs {
				go stopHostD(ctx, hostIP, &wg)
			}
			wg.Wait()

			defer func() {
				framework.Logf("In defer function to start the hostd service on all hosts")
				if isServiceStopped {
					for _, hostIP := range hostIPs {
						startHostDOnHost(ctx, hostIP)
					}
					isServiceStopped = false
				}
			}()
		} else {
			ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			err = invokeVCenterServiceControl(stopOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = true
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcStoppedMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				if isServiceStopped {
					ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
					err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isServiceStopped = false
				}
			}()
		}

		ginkgo.By("Invoke resume volume provisioning on datastore")
		result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

		if serviceName == "CSI" {
			if isServiceStopped {
				framework.Logf("Starting CSI driver")
				err = updateDeploymentReplicawithWait(client, 1, vSphereCSIControllerPodNamePrefix,
					csiSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// Wait for the CSI Pods to be up and Running
				list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				num_csi_pods := len(list_of_pods)
				err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0,
					pollTimeout, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}

		} else if serviceName == hostdServiceName {
			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs := getAllHostsIP(ctx)
			isServiceStopped = true
			if isServiceStopped {
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
				isServiceStopped = false
			}
		} else {
			if isServiceStopped {
				vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}
		}

		ginkgo.By("Waiting for claim to be in bound phase")
		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volumeID2).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC2 with VolumeID: %s", volumeID2))
		queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult2.Volumes) > 0).To(gomega.BeTrue())

	} else {
		ginkgo.Skip("Test is not supported on non Vanilla setups")
	}
}

func suspendResumeVolumeWhileVpxdServiceIsDown(serviceName string, namespace string,
	client clientset.Interface, isServiceStopped bool, scParameters map[string]string, cnshost string,
	oauth string, datacenter string, vsanshareddatastore string, datastoreURL string, nfsshared string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for Vanilla setup")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		accessmode := v1.ReadWriteOnce

		scParameters[scParamDatastoreURL] = datastoreURL
		// Check if it is file volumes setups
		if rwxAccessMode {
			scParameters[scParamFsType] = nfs4FSType
			accessmode = v1.ReadWriteMany
		}

		ginkgo.By("Creating Storage Class and PVC")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
			scParameters, "", nil, "", false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for claim to be in bound phase")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(persistentvolumes).NotTo(gomega.BeEmpty())
		volumeID := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = true
		err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}
		}()

		ginkgo.By("Invoke suspend volume provisioning on datastore")
		_, statusCode, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statusCode == 500).To(gomega.BeTrue())

		defer func() {
			ginkgo.By("Invoke resume volume provisioning on datastore")
			result, _, err := resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
		}()

		if isServiceStopped {
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
			err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = false
		}

		// Create PVC using above SC
		pvc2, err := createPVC(client, namespace, nil, "", storageclass, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if pvc2 != nil {
				ginkgo.By("Delete the PVC")
				err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, pvc2.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Waiting for claim to be in bound phase to pass")
		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volumeID2).NotTo(gomega.BeEmpty())

		//Reestablish the connections
		bootstrap()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC2 with VolumeID: %s", volumeID2))
		queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult2.Volumes) > 0).To(gomega.BeTrue())
	} else {
		ginkgo.Skip("Test is not supported on non Vanilla setups")
	}
}

func suspendResumeVolumeWhilevsanHealthServiceIsDown(serviceName string, namespace string,
	client clientset.Interface, isServiceStopped bool, scParameters map[string]string,
	cnshost string, oauth string, datacenter string, vsanshareddatastore string,
	datastoreURL string, vvolshared string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for Vanilla setup")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		accessmode := v1.ReadWriteOnce

		scParameters[scParamDatastoreURL] = datastoreURL
		// Check if it is file volumes setups
		if rwxAccessMode {
			scParameters[scParamFsType] = nfs4FSType
			accessmode = v1.ReadWriteMany
		}

		ginkgo.By("Creating Storage Class and PVC")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
			scParameters, "", nil, "", false, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for claim to be in bound phase")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(persistentvolumes).NotTo(gomega.BeEmpty())
		volumeID := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if volumeID != "" {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = true
		err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}
		}()

		ginkgo.By("Invoke suspend volume provisioning on datastore")
		result, _, err := suspendvolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully suspended"))

		defer func() {
			ginkgo.By("Invoke resume volume provisioning on datastore")
			result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))
		}()

		if isServiceStopped {
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
			err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = false
		}

		// Create PVC using above SC
		pvc2, err := createPVC(client, namespace, nil, "", storageclass, accessmode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if pvc2 != nil {
				ginkgo.By("Delete the PVC")
				err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, pvc2.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Waiting for claim to be in bound phase to fail")
		_, err = fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionShortTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = true
		err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}
		}()

		ginkgo.By("Invoke resume volume provisioning on datastore")
		result, _, err = resumevolumeprovisioning(cnshost, oauth, datacenter, vsanshareddatastore, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result.Message).To(gomega.ContainSubstring("Successfully resumed"))

		if isServiceStopped {
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
			err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = false
		}

		ginkgo.By("Waiting for claim to be in bound phase to pass")
		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volumeID2).NotTo(gomega.BeEmpty())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC2 with VolumeID: %s", volumeID2))
		queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult2.Volumes) > 0).To(gomega.BeTrue())
	} else {
		ginkgo.Skip("Test is not supported on non Vanilla setups")
	}
}
