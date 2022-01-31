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

var _ = ginkgo.Describe("[cns-manager-migrate-api] Migrate Volume API Testing", func() {

	f := framework.NewDefaultFramework("migrate-volume")

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
		nfsshared = "nfs1-1"
		vvolshared = "pepsishared"
		vmfsshared = "sharedVmfs-0"

		// Create a tag based shared-ds-policy including the datastores vsan, vvol, vmfs, nfs
		policyname = "shared-ds-policy"

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

	// /*
	// 	Basic usecase (Attached PV), migrate dynamically created volume

	// 	1. Pre-Setup
	// 	2. Create a storage class to provision volumes on shared vSanDatastore
	// 	3. Provision a Block volume using the storage class created above of size 50GB
	// 	4. Wait for the volume to be bound
	// 	5. Verify volumes are created on CNS by using CNSQuery API
	// 		and also check metadata is pushed to CNS
	// 	6. Create a Pod using the volume created above
	// 	7. Wait for the Pod to be up and running
	// 	8. Identify the hosts which are hosting the volumes
	// 	9. Invoke MigrateVolumes API for the above created volume (vsan → vmfs)
	// 	10. Continue writing into the volume while the migration is in progress
	// 	11. Wait and verify the volume migration is success by calling getJobStatus
	// 	12. Verify volume's metadata is intact and volume's health reflects accessible
	// 	13. Delete pod
	// 	14. Delete all the volumes created from test
	// 	15. Delete storage class
	// */
	// ginkgo.It("Test-cns", func() {
	// 	ctx, cancel = context.WithCancel(context.Background())
	// 	defer cancel()
	// 	if vanillaCluster {
	// 		ginkgo.By("CNS_TEST: Running for Vanilla setup")
	// 		var err error

	// 		var defaultDatastore *object.Datastore
	// 		var defaultDatacenter *object.Datacenter

	// 		var datacenters []string
	// 		datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
	// 		ctx, cancel := context.WithCancel(context.Background())
	// 		defer cancel()
	// 		finder := find.NewFinder(e2eVSphere.Client.Client, false)
	// 		cfg, err := getConfig()
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 		dcList := strings.Split(cfg.Global.Datacenters, ",")
	// 		for _, dc := range dcList {
	// 			dcName := strings.TrimSpace(dc)
	// 			if dcName != "" {
	// 				datacenters = append(datacenters, dcName)
	// 			}
	// 		}

	// 		for _, dc := range datacenters {
	// 			defaultDatacenter, err = finder.Datacenter(ctx, dc)
	// 			gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 			finder.SetDatacenter(defaultDatacenter)
	// 			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
	// 			gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 		}

	// 		ginkgo.By("Deleting an FCD")
	// 		err = e2eVSphere.deleteFCD(ctx, "545b6008-a93d-4ccf-9f2e-96e8c05bfc90", defaultDatastore.Reference())
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// 		err = e2eVSphere.deleteFCD(ctx, "798bc565-c88f-4db5-a2cc-79c656f07714", defaultDatastore.Reference())
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// 		err = e2eVSphere.deleteFCD(ctx, "9218eee5-6e26-40e8-8eaf-a70d23a86915", defaultDatastore.Reference())
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// 		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s",
	//"545b6008-a93d-4ccf-9f2e-96e8c05bfc90"))
	// 		queryResult, err := e2eVSphere.queryCNSVolumeWithResult("545b6008-a93d-4ccf-9f2e-96e8c05bfc90")
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
	// 		framework.Logf("Quyery results +%v", queryResult)

	// 		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s",
	//"798bc565-c88f-4db5-a2cc-79c656f07714"))
	// 		queryResult, err = e2eVSphere.queryCNSVolumeWithResult("798bc565-c88f-4db5-a2cc-79c656f07714")
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
	// 		framework.Logf("Quyery results +%v", queryResult)

	// 		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s",
	//"9218eee5-6e26-40e8-8eaf-a70d23a86915"))
	// 		queryResult, err = e2eVSphere.queryCNSVolumeWithResult("9218eee5-6e26-40e8-8eaf-a70d23a86915")
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
	// 		framework.Logf("Quyery results +%v", queryResult)

	// 		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s",
	//"965ecf02-075c-40e6-997e-9f076d53c677"))
	// 		queryResult, err = e2eVSphere.queryCNSVolumeWithResult("965ecf02-075c-40e6-997e-9f076d53c677")
	// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// 		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
	// 		framework.Logf("Quyery results +%v", queryResult)

	// 	} else {
	// 		ginkgo.Skip("Test is not supported on non Vanilla setups")
	// 	}
	// })

	/*
		Basic usecase (Attached PV), migrate dynamically created volume

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 50GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API
			and also check metadata is pushed to CNS
		6. Create a Pod using the volume created above
		7. Wait for the Pod to be up and running
		8. Identify the hosts which are hosting the volumes
		9. Invoke MigrateVolumes API for the above created volume (vsan → vmfs)
		10. Continue writing into the volume while the migration is in progress
		11. Wait and verify the volume migration is success by calling getJobStatus
		12. Verify volume's metadata is intact and volume's health reflects accessible
		13. Delete pod
		14. Delete all the volumes created from test
		15. Delete storage class
	*/
	ginkgo.It("Migrate dynamically created attached volume", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "50Gi", nil, "", false, v1.ReadWriteOnce)
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
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			// Create a Pod to use this PVC, and verify volume has been attached
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
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {

				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s", volumes.FcdId,
						volumes.AttachmentDetails.Vm)
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

					wrtiecmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
						"echo 'new message from test into Pod1' > /mnt/volume1/Pod1.html"}
					framework.RunKubectlOrDie(namespace, wrtiecmd...)

					ginkgo.By("Verify the volume is accessible and Read/write is possible")
					output = framework.RunKubectlOrDie(namespace, cmd...)
					gomega.Expect(strings.Contains(output,
						"new message from test into Pod1")).NotTo(gomega.BeFalse())

				} else {
					framework.Logf("Now looking at the FCD %s", volumes.FcdId)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)
				}
			}

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Basic usecase (Detached PV), migrate dynamically created volume

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore of size 100GB
		3. Provision a Block volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Invoke MigrateVolumes API for the above created volume (vsan → vmfs)
		7. Wait and verify the volume migration is success by calling getJobStatus
		8. Verify volume's metadata is intact and volume's health reflects accessible
		9. Delete all the volumes created from test
		10. Delete storage class
	*/
	ginkgo.It("Migrate dynamically provisioned detached volume", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "100Gi", nil, "", false, v1.ReadWriteOnce)
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
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {

				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s", volumes.FcdId,
						volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)

					jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, nfsshared, volumes.FcdId, false)
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
			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Basic usecase - migrate STS created volumes

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Create an statefulset application with 15 replica using SC created above and volume of size 25GB
		4. Wait for the volumes to be bound
		5. Wait for all the pods to be up and running
		6. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		7. Identify the hosts which are hosting the volumes
		8. Invoke MigrateVolumes API for the above created volume (vsan → nfs)
		9. Wait and verify the volume migration is success by calling getJobStatus
		10. Verify volume's metadata is intact and volume's health reflects accessible
		11. Verify the pods can still use the volume
		12. Delete STS application
		13. Delete all the volumes created from test
		14. Delete storage class
	*/
	ginkgo.It("Migrate STS created volumes", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			curtime := time.Now().Unix()
			randomValue := rand.Int()
			val := strconv.FormatInt(int64(randomValue), 10)
			val = string(val[1:3])
			curtimestring := strconv.FormatInt(curtime, 10)
			scName := "nginx-sc-default-" + curtimestring + val
			datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
			scParameters[scParamDatastoreURL] = datastoreURL

			ginkgo.By("Creating StorageClass for Statefulset")
			scSpec := getVSphereStorageClassSpec(scName, scParameters, nil, "", "", false)
			sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Creating service")
			service := CreateService(namespace, client)

			defer func() {
				deleteService(namespace, client, service)
			}()

			ginkgo.By("Creating statefulset")
			statefulset := GetStatefulSetFromManifest(namespace)
			*statefulset.Spec.Replicas = 15
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Annotations["volume.beta.kubernetes.io/storage-class"] = sc.Name
			currentSize := statefulset.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage()
			newSize := currentSize.DeepCopy()
			newSize.Add(resource.MustParse("25Gi"))
			statefulset.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[v1.ResourceStorage] = newSize
			CreateStatefulSet(namespace, statefulset, client)

			defer func() {
				ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
				fss.DeleteAllStatefulSets(client, namespace)
			}()

			replicas := *(statefulset.Spec.Replicas)
			// Waiting for pods status to be Ready
			fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			var fcdIds [15]string

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

			jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
				vsanshareddatastore, vmfsshared, "", false)
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

			replicas = *(statefulset.Spec.Replicas)
			// Waiting for pods status to be Ready
			fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
			ssPodsBeforeScaleDown = fss.GetPodList(client, statefulset)
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Basic usecase - Migrate Statically created volumes

		1. Pre-Setup
		2. Create FCD and wait for fcd to allow syncing with pandora
		3. Create PV Spec with volumeID set to FCDID created above
		4. Create PVC with the storage request set to PV's storage capacity
		5. Wait and verify the volume gets bound
		6. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		7. Create a Pod using the volume created above
		8. Wait for the Pod to be up and running
		9. Identify the hosts which are hosting the volumes
		10. Invoke MigrateVolumes API for the above created volume (vsan → vVol)
		11. Wait and verify the volume migration is success by calling getJobStatus
		12. Verify volume's metadata is intact and volume's health reflects accessible
		13. Verify the pods can still use the volume
		14. Delete all the volumes created from test
		15. Delete storage class
	*/
	ginkgo.It("Migrate Statically created volumes", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
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

			ginkgo.By("Creating FCD Disk")
			fcdID, err := e2eVSphere.createFCD(ctx, "BasicStaticFCD", int64(204800), defaultDatastore.Reference())
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

			// Creating label for PV.
			// PVC will use this label as Selector to find PV
			staticPVLabels := make(map[string]string)
			staticPVLabels["fcd-id"] = fcdID

			ginkgo.By("Creating the PV")
			pv := getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimDelete, staticPVLabels)
			pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
			_, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

			if vanillaCluster {
				vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
			}

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			for _, volumes := range datastore.OtherVolumes {
				framework.Logf("Now looking at the FCD %s", volumes.FcdId)
				framework.Logf("Source Datastore %s, and Datacenter is %s",
					datastore.Datastore, datastore.Datacenter)
				if volumes.FcdId == fcdID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)

					jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, vmfsshared, volumes.FcdId, false)
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
				}
			}

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			ginkgo.By("Verifying CNS entry is present in cache")
			_, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Migrate volumes having snapshots

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 50GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Take multiple snapshots for the volumes (for details ref: Snapshot TDS)
		7. Identify the hosts which are hosting the volumes
		8. Invoke MigrateVolumes API for the above created volume (vsan → vVol)
		9. Wait and verify the volume migration is success by calling getJobStatus
		10. Verify volume's metadata is intact and volume's health reflects accessible
		11. Delete all the volumes created from test
		12. Delete storage class
	*/
	ginkgo.It("Migrate volumes having snapshots", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error
			var snapshotContentCreated = false
			var snapshotCreated = false

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname

			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "50Gi", nil, "", false, v1.ReadWriteOnce)
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
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

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
				getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, pvclaim.Name),
				metav1.CreateOptions{})
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
			volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			snapshotCreated = true
			gomega.Expect(volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse("50Gi"))).To(gomega.BeZero())

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

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {

				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)

					jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, vvolshared, volumes.FcdId, false)
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

					wrtiecmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
						"echo 'new message from test into Pod1' > /mnt/volume1/Pod1.html"}
					framework.RunKubectlOrDie(namespace, wrtiecmd...)

					ginkgo.By("Verify the volume is accessible and Read/write is possible")
					output = framework.RunKubectlOrDie(namespace, cmd...)
					gomega.Expect(strings.Contains(output,
						"new message from test into Pod1")).NotTo(gomega.BeFalse())

				} else {
					framework.Logf("Now looking at the FCD %s", volumes.FcdId)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)
				}
			}

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			ginkgo.By("Delete volume snapshot and verify the snapshot content is deleted")
			err = snapc.SnapshotV1().VolumeSnapshots(namespace).Delete(ctx,
				volumeSnapshot.Name, metav1.DeleteOptions{})
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

	/*
		Migrate volumes exercised by online resize

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 250GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create POD using above created PVC
		7. Modify size of PVC to trigger online volume expansion to size 500GB
		8. Verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		9. Verify the resized PVC by doing CNS query
		10. Make sure data is intact on the PV mounted on the pod
		11. Make sure file system has increased
		12. Identify the hosts which are hosting the volumes
		13. Invoke MigrateVolumes API for the above created volume (vsan → nfs)
		14. Wait and verify the volume migration is success by calling getJobStatus
		15. Verify volume's metadata is intact and volume's health reflects accessible
		16. Delete the Pod
		17. Delete all the volumes created from test
		18. Delete storage class
	*/
	ginkgo.It("Migrate online resize exerised volume", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname

			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "250Gi", nil, "", true, v1.ReadWriteOnce)
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
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			// Modify PVC spec to trigger volume expansion
			// We expand the PVC while no pod is using it to ensure offline expansion
			currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("500Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			claims, err := expandPVCSize(pvclaim, newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(claims).NotTo(gomega.BeNil())

			ginkgo.By("Waiting for file system resize to finish")
			claims, err = waitForFSResize(pvclaim, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pvcConditions := claims.Status.Conditions
			expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {

				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)

					jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, nfsshared, volumes.FcdId, false)
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

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Migrate volumes exercised by offline resize

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 250GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Modify size of PVC to trigger offline volume expansion to size 500GB
		7. wait for some time and verify that "FilesystemResizePending"  on PVC
		8. Create POD using the above PVC
		9. Verify "FilesystemResizePending" is removed from PVC
		10. Query CNS volume and make sure new size is updated
		11. Verify volume is mounted on POD
		12. Verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		13. Verify the resized PVC by doing CNS query
		14. Make sure data is intact on the PV mounted on the pod
		15. Make sure file system has increased
		16. Identify the hosts which are hosting the volumes
		17. Invoke MigrateVolumes API for the above created volume (vsan → vmfs)
		18. Wait and verify the volume migration is success by calling getJobStatus
		19. Verify volume's metadata is intact and volume's health reflects accessible
		20. Delete the Pod
		21. Delete all the volumes created from test
		22. Delete storage class
	*/
	ginkgo.It("Migrate volumes exercised by offline resize", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname

			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "250Gi", nil, "", true, v1.ReadWriteOnce)
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

			ginkgo.By("Expanding current pvc")
			currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("500Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			pvclaim, err = expandPVCSize(pvclaim, newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvclaim).NotTo(gomega.BeNil())

			pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			if pvcSize.Cmp(newSize) != 0 {
				framework.Failf("error updating pvc size %q", pvclaim.Name)
			}

			ginkgo.By("Waiting for controller volume resize to finish")
			err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Checking for conditions on pvc")
			pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client,
				namespace, pvclaim.Name, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if len(queryResult.Volumes) == 0 {
				err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verifying disk size requested in volume expansion is honored")
			newSizeInMb := int64(768000)
			if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb !=
				newSizeInMb {
				err = fmt.Errorf("got wrong disk size after volume expansion +%v ",
					queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {

				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)

					jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, nfsshared, volumes.FcdId, false)
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
			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Migration of volumes having SC with datastore url

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore by passing the datastore URL
		3. Provision a Block volume using the storage class created above of size 300GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Identify the hosts which are hosting the volumes
		7. Invoke MigrateVolumes API for the above created volume (vsan → vVol)
		8. Wait and verify the volume migration succeeds by calling getJobStatus
		9. Verify volume's metadata is intact and volume's health reflects accessible
		10. Delete all the volumes created from test
		11. Delete storage class
	*/
	ginkgo.It("Migration of volumes having SC with datastore url", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error
			scParameters := make(map[string]string)
			datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
			scParameters[scParamDatastoreURL] = datastoreURL

			ginkgo.By("Creating Storage Class and PVC")
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "300Gi", nil, "", false, v1.ReadWriteOnce)
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

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {
				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)

					jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, vvolshared, volumes.FcdId, false)
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

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Perform online volume resize during the migration

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 100GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create a Pod using the volume created above
		7. Wait for the Pod to be up and running
		8. Identify the hosts which are hosting the volumes
		9. Invoke MigrateVolumes API for the above created volume (vsan → vmfs)
		10. While the migration is in progress perform modify size of PVC
			to trigger online volume expansion to size 250GB
		11. Verify the PVC status will change and stays in "FilesystemResizePending"
			until the migrate operation is complete
		12. Wait and verify the volume migration is success first before the online resize
		13. Verify volume's metadata is intact and volume's health reflects accessible
		14. Now Verify the PVC status will change "FilesystemResizePending" & wait till the status is removed
		15. Verify the resized PVC by doing CNS query
		16. Wait and verify volume size on file system has increased
		17. Delete pod
		18. Delete all the volumes created from test
		19. Delete storage class
	*/
	ginkgo.It("Perform online volume resize during the migration", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname

			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "100Gi", nil, "", true, v1.ReadWriteOnce)
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
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
				vsanshareddatastore, vvolshared, volumeID, false)
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
			framework.Logf("Migrate of volume status is %s", jobResult.JobStatus.VolumeMigrationJobStatus)

			jobResult, err = waitForMigrationToBePhase(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Modify PVC spec to trigger volume expansion
			// We expand the PVC while no pod is using it to ensure offline expansion
			currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("25Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			claims, err := expandPVCSize(pvclaim, newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(claims).NotTo(gomega.BeNil())

			jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

			jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for file system resize to finish")
			claims, err = waitForFSResize(pvclaim, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pvcConditions := claims.Status.Conditions
			expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Perform migration API on volume under size expansion

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 100GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create a Pod using the volume created above
		7. Wait for the Pod to be up and running
		8. Identify the hosts which are hosting the volumes
		9. Perform modify size of PVC to trigger online volume expansion to size 250GB
		10. Invoke MigrateVolumes API for the above created volume (vsan → vmfs)
		11. Wait and verify the status of volume migration is not started until the resize is complete
		12. Verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		13. Verify the resized PVC by doing CNS query
		14. Wait and verify volume size on file system has increased
		15. Wait and verify the volume migration operations starts after the resize and succeeds
		16. Delete pod
		17. Delete all the volumes created from test
		18. Delete storage class
	*/

	ginkgo.It("Perform migration API on volume under size expansion", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			//scParameters[scParamStoragePolicyName] = policyname
			scParameters[scParamStoragePolicyName] = policyname

			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "100Gi", nil, "", true, v1.ReadWriteOnce)
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
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			// Modify PVC spec to trigger volume expansion
			// We expand the PVC while no pod is using it to ensure offline expansion
			currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("250Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			claims, err := expandPVCSize(pvclaim, newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(claims).NotTo(gomega.BeNil())

			jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
				vsanshareddatastore, vmfsshared, volumeID, false)
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
			framework.Logf("Migrate of volume status is %s", jobResult.JobStatus.VolumeMigrationJobStatus)

			jobResult, err = waitForMigrationToBePhase(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for file system resize to finish")
			claims, err = waitForFSResize(pvclaim, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pvcConditions := claims.Status.Conditions
			expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

			jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

			jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Perform attach/detach while the migration is in progress
	*/
	ginkgo.It("Perform attach/detach while the migration is in progress", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname

			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "10Gi", nil, "", true, v1.ReadWriteOnce)
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

			ginkgo.By("Migrate the volume and perform pod creation")
			jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
				vsanshareddatastore, vvolshared, volumeID, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Creating pod to attach PV to the node")
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
				false, execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isPodCreated := true

			defer func() {
				if isPodCreated {
					// Delete POD
					ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
					err = fpod.DeletePodWithWait(client, pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()

			var vmUUID string
			nodeName := pod.Spec.NodeName
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)

			jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Current status of the migration task is %s", jobResult.JobStatus.OverallPhase)
			framework.Logf("Migrate of volume status is %s", jobResult.JobStatus.VolumeMigrationJobStatus)

			jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

			jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

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

			ginkgo.By("Migrate the volume and perform pod deletion")
			jobId, _, err = migrateVolumes(cnshost, oauth, datacenter, vvolshared,
				vsanshareddatastore, volumeID, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isPodCreated = false

			jobResult, err = getJobStatus(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Current status of the migration task is %s", jobResult.JobStatus.OverallPhase)
			framework.Logf("Migrate of volume status is %s", jobResult.JobStatus.VolumeMigrationJobStatus)

			jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Success"))

			jobResult, err = waitForMigrationToComplete(cnshost, oauth, jobId.JobId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is detached from the node")
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Migrate dynamically created volume while vsan-health service is down

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 100GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create a Pod using the volume created above
		7. Wait for the Pod to be up and running
		8. Identify the hosts which are hosting the volumes
		9. Bring down the vsan-health Service
		10. Invoke MigrateVolumes API for the above created volume (vsan → vvol)
		11. Wait and verify the volume migration is pending/fails by calling getJobStatus
		12. Bring back the vsan-health service
		13. Wait and verify the volume migration is success
		14. Verify volume's metadata is intact and volume's health reflects accessible
		15. Delete pod
		16. Delete all the volumes created from test
		17. Delete storage class
	*/

	ginkgo.It("Migrate dynamically created volume while vsan-health service is down", func() {
		serviceName = vsanhealthServiceName
		migrateVolumeWhilevsanHealthServiceIsDown(serviceName, namespace, client, isServiceStopped, scParameters,
			cnshost, oauth, datacenter, vsanshareddatastore, policyname, vmfsshared)
	})

	/*
		Migrate dynamically created volume while sps service is down

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 100GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create a Pod using the volume created above
		7. Wait for the Pod to be up and running
		8. Identify the hosts which are hosting the volumes
		9. Bring down the SPS Service
		10. Invoke MigrateVolumes API for the above created volume (vsan → nfs)
		11. Wait and verify the volume migration is success by calling getJobStatus
		12. Bring back the SPS service
		13. Verify volume's metadata is intact and volume's health reflects accessible
		14. Delete pod
		15. Delete all the volumes created from test
		16. Delete storage class
	*/
	ginkgo.It("Migrate dynamically created volume while sps service is down", func() {
		serviceName = spsServiceName
		migrateVolumeWhileServiceIsDown(serviceName, namespace, client, isServiceStopped,
			scParameters, cnshost, oauth, datacenter, vsanshareddatastore, policyname, vmfsshared)
	})

	/*
		Migrate dynamically created volume while hostd service is down

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 100GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create a Pod using the volume created above
		7. Wait for the Pod to be up and running
		8. Identify the hosts which are hosting the volumes
		9. Bring down the hostd Service on all the hosts
		10. Invoke MigrateVolumes API for the above created volume (vsan → vvol)
		11. Wait and verify the volume migration is success by calling getJobStatus
		12. Bring back the hostd service on all the hosts
		13. Verify volume's metadata is intact and volume's health reflects accessible
		14. Delete pod
		15. Delete all the volumes created from test
		16. Delete storage class
	*/
	ginkgo.It("Migrate dynamically created volume while hostd service is down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error
			scParameters[scParamStoragePolicyName] = policyname

			ginkgo.By("Creating Storage Class and PVC")
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, v1.ReadWriteOnce)
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

			jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
				vsanshareddatastore, nfsshared, volumeID, false)
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

			ginkgo.By("Fetch IPs for the all the hosts in the cluster")
			hostIPs = getAllHostsIP(ctx)
			isServiceStopped = true
			if isServiceStopped {
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
				isServiceStopped = false
			}

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Migrate dynamically created volume while vpxd service is down

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 100GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create a Pod using the volume created above
		7. Wait for the Pod to be up and running
		8. Identify the hosts which are hosting the volumes
		9. Bring down the vpxd Service
		10. Invoke MigrateVolumes API for the above created volume (vsan → vvol)
		11. Wait and verify the volume migration is fails by calling getJobStatus
		12. Bring back the vpxd Service
		13. Verify volume's metadata is intact and volume's health reflects accessible on the source datastore
		14. Delete pod
		15. Delete all the volumes created from test
		16. Delete storage class
	*/
	ginkgo.It("Migrate dynamically created volume while vpxd service is down", func() {
		serviceName = vpxdServiceName
		migrateVolumeWhileVpxdServiceIsDown(serviceName, namespace, client, isServiceStopped,
			scParameters, cnshost, oauth, datacenter, vsanshareddatastore, policyname, nfsshared)
	})

	/*
		Migrate dynamically created volume while csi-controller is down

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 100GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create a Pod using the volume created above
		7. Wait for the Pod to be up and running
		8. Identify the hosts which are hosting the volumes
		9. Bring down the csi-controller on cluster
		10. Invoke MigrateVolumes API for the above created volume (vsan → vvol)
		11. Wait and verify the volume migration is success by calling getJobStatus
		12. Bring back the csi-controller on cluster
		13. Verify volume's metadata is intact and volume's health reflects accessible
		14. Delete pod
		15. Delete all the volumes created from test
		16. Delete storage class
	*/
	ginkgo.It("Migrate dynamically created volume while csi-controller is down", func() {
		serviceName = "CSI"
		migrateVolumeWhileServiceIsDown(serviceName, namespace, client, isServiceStopped,
			scParameters, cnshost, oauth, datacenter, vsanshareddatastore, policyname, vmfsshared)
	})

	/*
		Migrate volume into an non-existing datastore

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Invoke MigrateVolumes API for the above created volume into an non-existing datastore
		7. Wait and verify the volume migration fails
		8. Verify volume's metadata is still intact and volume's health reflects accessible
		9. Delete all the volumes created from test
		10. Delete storage class
	*/
	ginkgo.It("Migrate volume into an non-existing datastore", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, v1.ReadWriteOnce)
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

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {

				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)
					jobId, _, _ := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, "invalid", volumes.FcdId, false)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					jobResult, err := getJobStatus(cnshost, oauth, jobId.JobId)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.Logf("Current status of the migration task is %s",
						jobResult.JobStatus.OverallPhase)
					framework.Logf("Migrate of volume status is %s",
						jobResult.JobStatus.VolumeMigrationJobStatus.VolumeMigrationTasks[0].Phase)

					jobResult, err = waitforjob(cnshost, oauth, jobId.JobId)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(jobResult.JobStatus.OverallPhase).To(gomega.Equal("Error"))
				} else {
					framework.Logf("Now looking at the FCD %s", volumes.FcdId)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)
				}
			}

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke migration from an non-existing datastore

		1. Pre-Setup
		2. Invoke MigrateVolumes API from source datastore as non-existing
			datastore and target datastore as a valid vsanDatastore
		3. Wait and verify the migration operation fails by calling getJobStatus
	*/
	ginkgo.It("Invoke migration from an non-existing datastore", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, v1.ReadWriteOnce)
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

			_, statusCode, _ := migrateVolumes(cnshost, oauth, datacenter,
				"invalid", nfsshared, "", true)
			gomega.Expect(statusCode == 500).To(gomega.BeTrue(),
				"Migrate Volume API call should fail, but passing")

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Invoke migration with source and target datastore as same (vsanDatastore)

		1. Pre-Setup
		2. Invoke MigrateVolumes API from source datastore and target datastore as same (vsanDatastore)
		3. Wait and verify the migration operation fails by calling getJobStatus
	*/
	ginkgo.It("Invoke migration with source and target datastore as same (vsanDatastore)", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, v1.ReadWriteOnce)
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

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {
				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)
					_, statusCode, _ := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, vsanshareddatastore, volumes.FcdId, true)
					gomega.Expect(statusCode == 500).To(gomega.BeTrue(),
						"Migrate Volume API call should fail, but passing")
				} else {
					framework.Logf("Now looking at the FCD %s", volumes.FcdId)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)
				}
			}

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Migrate Volume back to origin datastore or Migrate a single volumes multiple times

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above of size 50GB
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create a Pod using the volume created above
		7. Wait for the Pod to be up and running
		8. Identify the hosts which are hosting the volumes
		9. Invoke MigrateVolumes API for the above created volume (vsan → vmfs)
		10. Wait and verify the volume migration is success by calling getJobStatus
		11. Verify volume's metadata is intact and volume's health reflects accessible
		12. Invoke MigrateVolumes API again back to its origin (vmfs → vsan)
		13. Wait and verify the volume migration is success by calling getJobStatus
		14. Verify volume's metadata is intact and volume's health reflects accessible
		15. Delete pod
		16. Delete all the volumes created from test
		17. Delete storage class
	*/
	ginkgo.It("Migrate Volume back to origin datastore or Migrate a single volumes multiple times", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname
			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "50Gi", nil, "", false, v1.ReadWriteOnce)
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

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {

				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)

					jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, vmfsshared, volumes.FcdId, false)
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

					jobId, _, err = migrateVolumes(cnshost, oauth, datacenter, vmfsshared,
						vsanshareddatastore, volumes.FcdId, false)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					jobResult, err = getJobStatus(cnshost, oauth, jobId.JobId)
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

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Post MigrateVolumes API - Perform Online resize

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Create a Pod using the volume created above
		7. Wait for the Pod to be up and running
		8. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		9. Identify the hosts which are hosting the volumes
		10. Invoke MigrateVolumes API for the above created volume (vsan → vmfs)
		11. Wait and verify the volume migration is success by calling getJobStatus
		12. Verify volume's metadata is intact and volume's health reflects accessible
		13. Invoke ResumeCNSVolumeProvisioning API on shared vSAN datastore
		14. Modify size of PVC to trigger online volume expansion
		15. Verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		16. Verify the resized PVC by doing CNS query
		17. Make sure data is intact on the PV mounted on the pod
		18. Make sure file system has increased
		19. Delete pod
		20. Verify the detach volume is success from the pod
		21. Delete all the volumes created from test
		22. Delete storage class
	*/
	ginkgo.It("Post MigrateVolumes API - Perform Online resize", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname
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

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {

				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)

					jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, nfsshared, volumes.FcdId, false)
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

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Post MigrateVolumes API - Perform Offline resize

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore
		3. Provision a Block/File volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Invoke SuspendCNSVolumeProvisioning API on shared vSAN datastore
		7. Identify the hosts which are hosting the volumes
		8. Invoke MigrateVolumes API for the above created volume (vsan → vmfs)
		9. Wait and verify the volume migration is success by calling getJobStatus
		10. Invoke ResumeCNSVolumeProvisioning API on shared vSAN datastore
		11. Verify volume's metadata is intact and volume's health reflects accessible
		12. Modify size of PVC to trigger offline volume expansion
		13. wait for some time and verify that "FilesystemResizePending"  on PVC
		14. Create POD using the above PVC
		15. Verify "FilesystemResizePending" is removed from PVC
		16. Query CNS volume and make sure new size is updated
		17. Verify volume is mounted on POD
		18. Verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		19. Verify the resized PVC by doing CNS query
		20. Make sure file system has increased
		21. Delete pod
		22. Delete all the volumes created from test
		23. Delete storage class
	*/
	ginkgo.It("Post MigrateVolumes API - Perform Offline resize", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname
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

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {

				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)

					jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, vvolshared, volumes.FcdId, false)
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

			ginkgo.By("Waiting for controller volume resize to finish")
			err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Checking for conditions on pvc")
			pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client,
				namespace, pvclaim.Name, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
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

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})

	/*
		Post MigrateVolumes API - Attach/Detach volume with new Pod

		1. Pre-Setup
		2. Create a storage class to provision volumes on shared vSanDatastore of size 100GB
		3. Provision a Block volume using the storage class created above
		4. Wait for the volume to be bound
		5. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		6. Invoke MigrateVolumes API for the above created volume (vsan → vmfs)
		7. Wait and verify the volume migration is success by calling getJobStatus
		8. Verify volume's metadata is intact and volume's health reflects accessible
		9. Create a Pod using the volume created above
		10. Wait for the Pod to be up and running
		11. Identify the hosts which are hosting the volumes
		12. Invoke MigrateVolumes API again back to its origin datastore (vmfs → vsan)
		13. Verify volume's metadata is intact and volume's health reflects accessible
		14. Delete Pod
		15. Wait and verify the volume is detached
		16. Delete all the volumes created from test
		17. Delete storage class
	*/
	ginkgo.It("Post MigrateVolumes API - Attach/Detach volume with new Pod", func() {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for Vanilla setup")
			var storageclass *storagev1.StorageClass
			var pvclaim *v1.PersistentVolumeClaim
			var err error

			ginkgo.By("Creating Storage Class and PVC")
			scParameters[scParamStoragePolicyName] = policyname

			storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
				scParameters, "", nil, "", false, v1.ReadWriteOnce)
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

			datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumes := range datastore.ContainerVolumes {

				if volumes.FcdId == volumeID {
					framework.Logf("Found the FCD ID %s, on the node %s",
						volumes.FcdId, volumes.AttachmentDetails.Vm)
					framework.Logf("Source Datastore %s, and Datacenter is %s",
						datastore.Datastore, datastore.Datacenter)

					jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
						vsanshareddatastore, vvolshared, volumes.FcdId, false)
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
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

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

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

		} else {
			ginkgo.Skip("Test is not supported on non Vanilla setups")
		}
	})
})

func migrateVolumeWhileServiceIsDown(serviceName string, namespace string,
	client clientset.Interface, isServiceStopped bool, scParameters map[string]string,
	cnshost string, oauth string, datacenter string, vsanshareddatastore string,
	policyname string, vmfsshared string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for Vanilla setup")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		scParameters[scParamStoragePolicyName] = policyname

		ginkgo.By("Creating Storage Class and PVC")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
			scParameters, "", nil, "", false, v1.ReadWriteOnce)
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

		ignoreLabels := make(map[string]string)

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

		datastore, err := getFCDPerDatastore(cnshost, oauth, datacenter, vsanshareddatastore)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumes := range datastore.ContainerVolumes {
			if volumes.FcdId == volumeID {
				framework.Logf("Found the FCD ID %s, on the node %s",
					volumes.FcdId, volumes.AttachmentDetails.Vm)
				framework.Logf("Source Datastore %s, and Datacenter is %s",
					datastore.Datastore, datastore.Datacenter)

				jobId, _, err := migrateVolumes(cnshost, oauth, datacenter,
					vsanshareddatastore, vmfsshared, volumes.FcdId, false)
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

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
	} else {
		ginkgo.Skip("Test is not supported on non Vanilla setups")
	}
}

func migrateVolumeWhileVpxdServiceIsDown(serviceName string, namespace string, client clientset.Interface,
	isServiceStopped bool, scParameters map[string]string, cnshost string, oauth string,
	datacenter string, vsanshareddatastore string, policyname string, nfsshared string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for Vanilla setup")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		scParameters[scParamStoragePolicyName] = policyname

		ginkgo.By("Creating Storage Class and PVC")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
			scParameters, "", nil, "", false, v1.ReadWriteOnce)
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

		_, statusCode, err := migrateVolumes(cnshost, oauth, datacenter,
			vsanshareddatastore, nfsshared, volumeID, true)
		gomega.Expect(statusCode == 500).To(gomega.BeTrue(),
			"Migrate Volume API call should fail, but passing")

		if isServiceStopped {
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
			err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = false
		}
	} else {
		ginkgo.Skip("Test is not supported on non Vanilla setups")
	}
}

func migrateVolumeWhilevsanHealthServiceIsDown(serviceName string, namespace string,
	client clientset.Interface, isServiceStopped bool, scParameters map[string]string,
	cnshost string, oauth string, datacenter string, vsanshareddatastore string,
	policyname string, vvolshared string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for Vanilla setup")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		scParameters[scParamStoragePolicyName] = policyname

		ginkgo.By("Creating Storage Class and PVC")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil,
			scParameters, "", nil, "", false, v1.ReadWriteOnce)
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

		_, statusCode, err := migrateVolumes(cnshost, oauth, datacenter,
			vsanshareddatastore, vvolshared, volumeID, true)
		gomega.Expect(statusCode == 503).To(gomega.BeTrue(),
			"Migrate Volume API call should fail, but passing")

		if isServiceStopped {
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
			err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isServiceStopped = false
		}

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s", volumeID))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
	} else {
		ginkgo.Skip("Test is not supported on non Vanilla setups")
	}
}
