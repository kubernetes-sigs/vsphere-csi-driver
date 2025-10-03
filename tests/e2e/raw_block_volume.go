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

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	statefulset_volname         string = "block-vol"
	statefulset_devicePath      string = "/dev/testblk"
	pod_devicePathPrefix        string = "/mnt/volume"
	volsizeInMiBBeforeExpansion int64  = 2048
)

var _ = ginkgo.Describe("raw block volume support", func() {

	f := framework.NewDefaultFramework("e2e-raw-block-volume")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		namespace              string
		client                 clientset.Interface
		defaultDatacenter      *object.Datacenter
		datastoreURL           string
		scParameters           map[string]string
		storageClassName       string
		storagePolicyName      string
		svcPVCName             string
		rawBlockVolumeMode     = corev1.PersistentVolumeBlock
		pandoraSyncWaitTime    int
		deleteFCDRequired      bool
		fcdID                  string
		pv                     *corev1.PersistentVolume
		pvc                    *corev1.PersistentVolumeClaim
		snapc                  *snapclient.Clientset
		restConfig             *restclient.Config
		guestClusterRestConfig *restclient.Config
		adminClient            clientset.Interface
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		namespace = getNamespaceToRunTests(f)
		client = f.ClientSet
		var err error
		bootstrap()

		var nodeList *corev1.NodeList
		runningAsDevopsUser := GetBoolEnvVarOrDefault("IS_DEVOPS_USER", false)
		adminClient, client = initializeClusterClientsByUserRoles(client)
		if guestCluster && runningAsDevopsUser {

			saName := namespace + "sa"
			client, err = createScopedClient(ctx, client, namespace, saName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}
		nodeList, err = fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)

		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		//Get snapshot client using the rest config
		if !guestCluster {
			restConfig = getRestConfigClient()
			snapc, err = snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			guestClusterRestConfig = getRestConfigClientForGuestCluster(guestClusterRestConfig)
			snapc, err = snapclient.NewForConfig(guestClusterRestConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		sc, err := adminClient.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		framework.Logf("err: %v", err)
		if err == nil && sc != nil {
			gomega.Expect(adminClient.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		deleteFCDRequired = false

		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
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
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By(fmt.Sprintf("Deleting all statefulsets and/or deployments in namespace: %v", namespace))
		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if supervisorCluster {
			dumpSvcNsEventsOnTestFailure(client, namespace)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
			dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}
		if deleteFCDRequired {
			ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
			err := e2eVSphere.deleteFCD(ctx, fcdID, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

	/*
		Test statefulset scaleup/scaledown operations with raw block volume
		Steps
		1. Create a storage class.
		2. Create nginx service.
		3. Create nginx statefulsets with 3 replicas and using raw block volume.
		4. Wait until all Pods are ready and PVCs are bounded with PV.
		5. Scale down statefulsets to 2 replicas.
		6. Scale up statefulsets to 3 replicas.
		7. Scale down statefulsets to 0 replicas and delete all pods.
		8. Delete all PVCs from the tests namespace.
		9. Delete the storage class.
	*/
	ginkgo.It("[ef-vanilla-block][cf-vks][csi-block-vanilla][csi-block-vanilla-parallelized][csi-guest]"+
		"Statefulset testing with raw block volume and default podManagementPolicy", ginkgo.Label(p0, block, vanilla,
		tkg, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		storageClassName = defaultNginxStorageClassName
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			storageClassName = defaultNginxStorageClassName
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := adminClient.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := adminClient.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset with raw block volume")
		scName := defaultNginxStorageClassName
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Spec.Template.Spec.Containers[len(statefulset.Spec.Template.Spec.Containers)-1].VolumeMounts = nil
		statefulset.Spec.Template.Spec.Containers[len(statefulset.Spec.Template.Spec.Containers)-1].
			VolumeDevices = []corev1.VolumeDevice{
			{
				Name:       statefulset_volname,
				DevicePath: statefulset_devicePath,
			},
		}
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &scName
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].ObjectMeta.Name =
			statefulset_volname
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.VolumeMode =
			&rawBlockVolumeMode
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			deleteAllStsAndPodsPVCsInNamespace(ctx, client, namespace)
		}()

		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		// Check if raw device available inside all pods of statefulset
		gomega.Expect(CheckDevice(ctx, client, statefulset, statefulset_devicePath)).NotTo(gomega.HaveOccurred())

		ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Get the list of Volumes attached to Pods before scale down
		var volumesBeforeScaleDown []string
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					volumeID := pv.Spec.CSI.VolumeHandle
					if guestCluster {
						volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
					}
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, volumeID)
					// Verify the attached volume match the one in CNS cache
					queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas-1))
		_, scaledownErr := fss.Scale(ctx, client, statefulset, replicas-1)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas-1)
		ssPodsAfterScaleDown, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas-1)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// After scale down, verify vSphere volumes are detached from deleted pods
		ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			if err != nil {
				gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						volumeID := pv.Spec.CSI.VolumeHandle
						if guestCluster {
							volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
						}
						isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
							client, volumeID, sspod.Spec.NodeName)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
							fmt.Sprintf("Volume %q is not detached from the node %q",
								volumeID, sspod.Spec.NodeName))
					}
				}
			}
		}

		// After scale down, verify the attached volumes match those in CNS Cache
		for _, sspod := range ssPodsAfterScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					volumeID := pv.Spec.CSI.VolumeHandle
					if guestCluster {
						volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
					}
					queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)

		ssPodsAfterScaleUp, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitTimeoutForPodReadyInNamespace(ctx, client, sspod.Name, statefulset.Namespace, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					volumeID := pv.Spec.CSI.VolumeHandle
					if guestCluster {
						volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
					}
					ginkgo.By("Verify scale up operation should not introduced new volume")
					gomega.Expect(isValuePresentInTheList(volumesBeforeScaleDown, volumeID)).To(gomega.BeTrue())
					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						volumeID, sspod.Spec.NodeName))
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var vmUUID string
					if vanillaCluster {
						vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
					} else if guestCluster {
						vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
					ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
					queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
				}
			}
		}

		replicas = 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr = fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		ssPodsAfterScaleDown, err = fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
	})

	/*
		Test dynamic volume provisioning with raw block volume
		Steps
		1. Create a PVC.
		2. Create pod and wait for pod to become ready.
		3. Verify volume is attached.
		4. Write some test data to raw block device inside pod.
		5. Verify the data written on the volume correctly.
		6. Delete pod.
		7. Create a new pod using the previously created volume and wait for pod to
		    become ready.
		8. Verify previously written data using a read on volume.
		9. Write some new test data and verify it.
		10. Delete pod.
		11. Wait for volume to be detached.
	*/
	ginkgo.It("[ef-vanilla-block][cf-vks][csi-block-vanilla] [csi-guest] [csi-block-vanilla-parallelized]"+
		"Should create and delete pod with the same raw block volume", ginkgo.Label(p0, block, vanilla, tkg,
		vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating Storage Class and PVC")
		// Decide which test setup is available to run.
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
		}
		sc, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := adminClient.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating raw block PVC")
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", sc, nil, "")
		pvcspec.Spec.VolumeMode = &rawBlockVolumeMode
		pvc, err = fpv.CreatePVC(ctx, client, namespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := WaitForPVClaimBoundPhase(ctx, client, []*corev1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName = volumeID
			volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.WaitForPersistentVolumeDeleted(ctx, adminClient, pv.Name, poll, pollTimeoutShort)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(ctx, client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			volumeID, pod.Spec.NodeName))
		var vmUUID string
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		}
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		// Write and read some data on raw block volume inside the pod.
		// Use same devicePath for raw block volume here as used inside podSpec by createPod().
		// Refer setVolumes() for more information on naming of devicePath.
		volumeIndex := 1
		devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 1mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=1").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyIOOnRawBlockVolume(namespace, pod.Name, devicePath, testdataFile, 0, 1)

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			volumeID, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volumeID, pod.Spec.NodeName))
		if guestCluster {
			ginkgo.By("Waiting for CnsNodeVMAttachment controller to reconcile resource")
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}

		ginkgo.By("Creating a new pod using the same volume")
		pod, err = createPod(ctx, client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			volumeID, pod.Spec.NodeName))

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		}
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Verify previously written data. Later perform another write and verify it.
		ginkgo.By(fmt.Sprintf("Verify previously written data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataFromRawBlockVolume(namespace, pod.Name, devicePath, testdataFile, 0, 1)
		ginkgo.By(fmt.Sprintf("Writing new 1mb test data in file %v", testdataFile))
		op, err = exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=1").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Write and read new data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyIOOnRawBlockVolume(namespace, pod.Name, devicePath, testdataFile, 0, 1)

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client,
			volumeID, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volumeID, pod.Spec.NodeName))
		if guestCluster {
			ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
			time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}
	})

	/*
	   Test static volume provisioning with raw block volume
	   Steps:
	   1. Create FCD and wait for fcd to allow syncing with pandora.
	   2. Create PV Spec with volumeID set to FCDID created in Step-1, and
	      PersistentVolumeReclaimPolicy is set to Delete.
	   3. Create PVC with the storage request set to PV's storage capacity.
	   4. Wait for PV and PVC to bound.
	   5. Create a POD.
	   6. Verify volume is attached to the node and volume is accessible in the pod.
	   7. Verify container volume metadata is present in CNS cache.
	   8. Delete POD.
	   9. Verify volume is detached from the node.
	   10. Delete PVC.
	   11. Verify PV is deleted automatically.
	*/
	ginkgo.It("[ef-vanilla-block][csi-block-vanilla] [csi-block-vanilla-parallelized] Verify basic static "+
		"provisioning workflow with raw block volume", ginkgo.Label(p0, block, vanilla, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Creating FCD Disk")
		fcdID, err := e2eVSphere.createFCD(ctx, "BasicStaticFCD", diskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		deleteFCDRequired = true

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		// Creating label for PV.
		// PVC will use this label as Selector to find PV.
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID

		ginkgo.By("Creating raw block PV")
		pv = getPersistentVolumeSpec(fcdID, corev1.PersistentVolumeReclaimDelete, staticPVLabels, "")
		pv.Spec.VolumeMode = &rawBlockVolumeMode
		pv, err = adminClient.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := fpv.DeletePersistentVolume(ctx, adminClient, pv.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating raw block PVC")
		pvc = getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc.Spec.VolumeMode = &rawBlockVolumeMode
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv, pvc))
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.WaitForPersistentVolumeDeleted(ctx, adminClient, pv.Name, poll, pollTimeoutShort)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		// Set deleteFCDRequired to false.
		// After PV, PVC is in the bind state, Deleting PVC should delete
		// container volume. So no need to delete FCD directly using vSphere
		// API call.
		deleteFCDRequired = false

		ginkgo.By("Verifying CNS entry is present in cache")
		_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the Pod")
		var pvclaims []*corev1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")

		// Write and read some data on raw block volume inside the pod.
		// Use same devicePath for raw block volume here as used inside podSpec by createPod().
		// Refer setVolumes() for more information on naming of devicePath.
		volumeIndex := 1
		devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 1mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=1").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyIOOnRawBlockVolume(namespace, pod.Name, devicePath, testdataFile, 0, 1)

		ginkgo.By("Verify container volume metadata is present in CNS cache")
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolume with VolumeID: %s", pv.Spec.CSI.VolumeHandle))
		_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := []types.KeyValue{{Key: "fcd-id", Value: fcdID}}
		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
			pvc.Name, pv.ObjectMeta.Name, pod.Name, labels...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the Pod")
		framework.ExpectNoError(fpod.DeletePodWithWait(ctx, client, pod), "Failed to delete pod", pod.Name)

		ginkgo.By(fmt.Sprintf("Verify volume %q is detached from the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")
	})

	/*
		Test online volume expansion on dynamic raw block volume
		Steps:
		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create raw block PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		8. Verify the resized PVC by doing CNS query
		9. Make sure data is intact on the PV mounted on the pod
		10.  Make sure file system has increased

	*/
	ginkgo.It("[ef-vanilla-block][cf-vks][csi-block-vanilla][csi-block-vanilla-parallelized][csi-guest]"+
		"Verify online volume expansion on dynamic raw block volume", ginkgo.Label(p0,
		block, vanilla, tkg,
		vc70), func() {
		ginkgo.By("Invoking Test for online Volume Expansion on raw block volume")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create StorageClass with allowVolumeExpansion set to true")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamDatastoreURL] = sharedVSANDatastoreURL
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
		}
		scParameters[scParamFsType] = ext4FSType
		sc, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := adminClient.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating raw block PVC")
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", sc, nil, "")
		pvcspec.Spec.VolumeMode = &rawBlockVolumeMode
		diskSize := resource.MustParse(diskSize4GB)
		pvcspec.Spec.Resources.Requests[corev1.ResourceStorage] = diskSize

		pvc, err = fpv.CreatePVC(ctx, client, namespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := WaitForPVClaimBoundPhase(ctx, client, []*corev1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv = pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName = volumeID
			volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.WaitForPersistentVolumeDeleted(ctx, adminClient, pv.Name, poll, pollTimeoutShort)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Pod using the above raw block PVC")
		var pvclaims []*corev1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			volumeID, pod.Spec.NodeName))
		var vmUUID string
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		}
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		// Get the size of block device and verify if device is accessible by performing write and read.
		// Write and read some data on raw block volume inside the pod.
		// Use same devicePath for raw block volume here as used inside podSpec by createPod().
		// Refer setVolumes() for more information on naming of devicePath.
		volumeIndex := 1
		devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		ginkgo.By(fmt.Sprintf("Check size for block device at devicePath %v before expansion", devicePath))
		originalBlockDevSize, err := getBlockDevSizeInBytes(f, namespace, pod, devicePath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 1mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=1").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyIOOnRawBlockVolume(namespace, pod.Name, devicePath, testdataFile, 0, 1)

		defer func() {
			// Delete Pod.
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				volumeID, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volumeID, pod.Spec.NodeName))
			if guestCluster {
				ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
				time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
				verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
					crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
			}
		}()

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvc, pod)

		ginkgo.By("Wait for block device size to be updated inside pod after expansion")
		isPvcExpandedInsidePod := false
		timeOut := 0
		for !isPvcExpandedInsidePod && timeOut < 600 {
			blockDevSize, err := getBlockDevSizeInBytes(f, namespace, pod, devicePath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if blockDevSize > originalBlockDevSize {
				ginkgo.By("Volume size updated inside pod successfully")
				isPvcExpandedInsidePod = true
			} else {
				ginkgo.By(fmt.Sprintf("updating volume size for %q. Resulting volume size is %d", pvc.Name, blockDevSize))
				time.Sleep(30 * time.Second)
				timeOut += 30
			}
		}

		// Verify original data on raw block volume after expansion
		ginkgo.By(fmt.Sprintf("Verify previously written data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataFromRawBlockVolume(namespace, pod.Name, devicePath, testdataFile, 0, 1)
		// Write data on expanded space to verify the expansion is successful and accessible.
		ginkgo.By(fmt.Sprintf("Writing new 1mb test data in file %v", testdataFile))
		op, err = exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=1").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Write testdata of 1MB size from offset=%v on raw volume at path %v inside pod %v",
			volsizeInMiBBeforeExpansion, pod.Spec.Containers[0].VolumeDevices[0].DevicePath, pod.Name))
		verifyIOOnRawBlockVolume(namespace, pod.Name, devicePath, testdataFile,
			volsizeInMiBBeforeExpansion, 1)
	})

	/*
	   Test to verify volume expansion is supported if allowVolumeExpansion
	   is true in StorageClass, PVC is created and offline and not attached
	   to a Pod before the expansion.
	   Steps
	   1. Create StorageClass with allowVolumeExpansion set to true.
	   2. Create raw block PVC which uses the StorageClass created in step 1.
	   3. Wait for PV to be provisioned.
	   4. Wait for PVC's status to become Bound.
	   5. Create pod using PVC on specific node.
	   6. Wait for Disk to be attached to the node.
	   7. Write some data to raw block PVC.
	   8. Detach the volume.
	   9. Modify PVC's size to trigger offline volume expansion.
	   10. Create pod again using PVC on specific node.
	   11. Wait for Disk to be attached to the node.
	   12. Verify data written on PVC before expansion.
	   13. Delete pod and Wait for Volume Disk to be detached from the Node.
	   14. Delete PVC, PV and Storage Class.
	*/
	ginkgo.It("[ef-vanilla-block][csi-block-vanilla] [csi-block-vanilla-parallelized] [csi-guest]"+
		"[ef-vks][ef-vks-n1][ef-vks-n2] Verify offline volume expansion with raw block volume", ginkgo.Label(p0,
		block, vanilla, tkg, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Invoking Test for Offline Volume Expansion")
		// Create Storage class and PVC
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType

		// Create a StorageClass that sets allowVolumeExpansion to true
		ginkgo.By("Creating Storage Class with allowVolumeExpansion = true")
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
		}
		sc, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := adminClient.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating raw block PVC")
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", sc, nil, "")
		pvcspec.Spec.VolumeMode = &rawBlockVolumeMode
		pvc, err = fpv.CreatePVC(ctx, client, namespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		// Waiting for PVC to be bound
		var pvclaims []*corev1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs, err := WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		svcPVCName := pv.Spec.CSI.VolumeHandle
		if guestCluster {
			volumeID = getVolumeIDFromSupervisorCluster(volumeID)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.WaitForPersistentVolumeDeleted(ctx, adminClient, pv.Name, poll, pollTimeoutShort)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(ctx, client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var vmUUID string
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, pod.Spec.NodeName))
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Get the size of block device and verify if device is accessible by performing write and read.
		volumeIndex := 1
		devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		ginkgo.By(fmt.Sprintf("Check size for block device at devicePath %v before expansion", devicePath))
		originalBlockDevSize, err := getBlockDevSizeInBytes(f, namespace, pod, devicePath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 1mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=1").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyIOOnRawBlockVolume(namespace, pod.Name, devicePath, testdataFile, 0, 1)

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
			client, volumeID, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volumeID, pod.Spec.NodeName))
		if guestCluster {
			ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
			time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}

		// Modify PVC spec to trigger volume expansion
		// We expand the PVC while no pod is using it to ensure offline expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvc, err = expandPVCSize(pvc, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc).NotTo(gomega.BeNil())

		pvcSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvc.Name)
		}
		if guestCluster {
			ginkgo.By("Checking for PVC request size change on SVC PVC")
			b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(b).To(gomega.BeTrue())
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvc, adminClient, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if guestCluster {
			ginkgo.By("Checking for resize on SVC PV")
			verifyPVSizeinSupervisor(svcPVCName, newSize)
		}

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(3072)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a new Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating a new pod to attach PV again to the node")
		pod, err = createPod(ctx, client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume after expansion: %s is attached to the node: %s",
			volumeID, pod.Spec.NodeName))
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		pvcConditions := pvc.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify block device size inside pod after expansion")
		blockDevSize, err := getBlockDevSizeInBytes(f, namespace, pod, devicePath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if blockDevSize <= originalBlockDevSize {
			framework.Failf("error updating volume size for %q. Resulting volume size is %d", pvc.Name, blockDevSize)
		}
		ginkgo.By("Resized volume attached successfully")

		// Verify original data on raw block volume after expansion
		ginkgo.By(fmt.Sprintf("Verify previously written data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataFromRawBlockVolume(namespace, pod.Name, devicePath, testdataFile, 0, 1)
		// Write data on expanded space to verify the expansion is successful and accessible.
		ginkgo.By(fmt.Sprintf("Writing new 1mb test data in file %v", testdataFile))
		op, err = exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=1").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Write testdata of 1MB size from offset=%v on raw volume at path %v inside pod %v",
			volsizeInMiBBeforeExpansion, pod.Spec.Containers[0].VolumeDevices[0].DevicePath, pod.Name))
		verifyIOOnRawBlockVolume(namespace, pod.Name, devicePath, testdataFile,
			volsizeInMiBBeforeExpansion, 1)

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the new pod %s in namespace %s after expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client, volumeID, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volumeID, pod.Spec.NodeName))
		if guestCluster {
			ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
			time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}
	})

	/*
		Test snapshot restore operation with raw block volume
		Steps:
		1. Create a storage class (eg: vsan default) and create a pvc using this sc
		2. Write some data on source volume
		3. Create a VolumeSnapshot class with snapshotter as vsphere-csi-driver and set deletionPolicy to Delete
		4. Create a volume-snapshot with labels, using the above snapshot-class and pvc (from step-1) as source
		5. Ensure the snapshot is created, verify using get VolumeSnapshot
		6. Also verify that VolumeSnapshotContent is auto-created
		7. Verify the references to pvc and volume-snapshot on this object
		8. Verify that the VolumeSnapshot has ready-to-use set to True
		9. Verify that the Restore Size set on the snapshot is same as that of the source volume size
		10. Query the snapshot from CNS side using volume id - should pass and return the snapshot entry
		11. Restore the snapshot to another pvc
		12. Verify previous data written on source volume is present on restored volume
		13. Delete the above snapshot from k8s side using kubectl delete, run a get and ensure it is removed
		14. Also ensure that the VolumeSnapshotContent is deleted along with the
		    volume snapshot as the policy is delete
		15. Query the snapshot from CNS side - should return 0 entries
		16. Cleanup: Delete PVC, SC (validate they are removed)
	*/
	ginkgo.It("[cf-vks] [block-vanilla-snapshot] [cf-vanilla-block][tkg-snapshot][ef-vks-snapshot] Verify snapshot "+
		"dynamic provisioning workflow with raw block volume", ginkgo.Label(p0, block,
		vanilla, tkg, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class")
		sc, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := adminClient.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating source raw block PVC")
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", sc, nil, "")
		pvcspec.Spec.VolumeMode = &rawBlockVolumeMode
		pvc1, err := fpv.CreatePVC(ctx, client, namespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		ginkgo.By("Expect source volume claim to provision volume successfully")
		pvs, err := WaitForPVClaimBoundPhase(ctx, client, []*corev1.PersistentVolumeClaim{pvc1},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		if guestCluster {
			volumeID = getVolumeIDFromSupervisorCluster(volumeID)
		}

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volumeID))

		ginkgo.By("Creating pod to attach source PV to the node")
		pod1, err := createPod(ctx, client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc1},
			false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var vmUUID string
		nodeName := pod1.Spec.NodeName
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod1.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod1.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID, nodeName))
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Write and read some data on raw block volume inside the pod.
		// Use same devicePath for raw block volume here as used inside podSpec by createPod().
		// Refer setVolumes() for more information on naming of devicePath.
		volumeIndex := 1
		devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		rand.New(rand.NewSource(time.Now().Unix()))
		testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
		ginkgo.By(fmt.Sprintf("Creating a 1mb test data file %v", testdataFile))
		op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=1").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			op, err = exec.Command("rm", "-f", testdataFile).Output()
			fmt.Println(op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		ginkgo.By(fmt.Sprintf("Write and read data on source raw volume attached to: %v at path %v", pod1.Name,
			pod1.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyIOOnRawBlockVolume(namespace, pod1.Name, devicePath, testdataFile, 0, 1)

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
			pvc1, volumeID, diskSize, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
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

		// Restore volumeSnapshot to another PVC
		ginkgo.By("Restore volume snapshot to another raw block PVC")
		restorePvcSpec := getPersistentVolumeClaimSpecWithDatasource(namespace, diskSize, sc, nil,
			corev1.ReadWriteOnce, volumeSnapshot.Name, snapshotapigroup)
		restorePvcSpec.Spec.VolumeMode = &rawBlockVolumeMode
		restoredPvc, err := fpv.CreatePVC(ctx, client, namespace, restorePvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		restoredPvs, err := WaitForPVClaimBoundPhase(ctx, client,
			[]*corev1.PersistentVolumeClaim{restoredPvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID2 := restoredPvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volumeID2).NotTo(gomega.BeEmpty())
		if guestCluster {
			volumeID2 = getVolumeIDFromSupervisorCluster(volumeID2)
		}
		defer func() {
			ginkgo.By("Deleting the restored PVC")
			err := fpv.DeletePersistentVolumeClaim(ctx, client, restoredPvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Wait for the restored PVC to disappear in CNS")
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach restored PVC to the node")
		pod2, err := createPod(ctx, client, namespace, nil, []*corev1.PersistentVolumeClaim{restoredPvc}, false,
			"")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		nodeName = pod2.Spec.NodeName
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod2.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod2.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID2, nodeName))
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID2, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volumeID2, nodeName))
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volumeID2, nodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volumeID2, nodeName))
			if guestCluster {
				ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
				time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
				verifyCRDInSupervisorWithWait(ctx, f, pod2.Spec.NodeName+"-"+svcPVCName,
					crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
			}
		}()

		// Verify previously written data. Later perform another write and verify it.
		ginkgo.By(fmt.Sprintf("Verify previously written data on restored volume attached to: %v at path %v", pod2.Name,
			pod2.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataFromRawBlockVolume(namespace, pod2.Name, devicePath, testdataFile, 0, 1)
		ginkgo.By(fmt.Sprintf("Writing new 1mb test data in file %v", testdataFile))
		op, err = exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
			"bs=1M", "count=1").Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Write and read data on restored volume attached to: %v at path %v", pod2.Name,
			pod2.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyIOOnRawBlockVolume(namespace, pod2.Name, devicePath, testdataFile, 0, 1)

		ginkgo.By("Delete dyanmic volume snapshot")
		snapshotCreated, snapshotContentCreated, err = deleteVolumeSnapshot(ctx, snapc, namespace,
			volumeSnapshot, pandoraSyncWaitTime, volumeID, snapshotId, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
