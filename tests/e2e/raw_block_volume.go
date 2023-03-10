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
	"os"
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	statefulset_volname    string = "block-vol"
	statefulset_devicePath string = "/dev/testblk"
	pod_devicePathPrefix   string = "/mnt/volume"
)

var _ = ginkgo.Describe("raw block volume support", func() {

	f := framework.NewDefaultFramework("e2e-vsphere-statefulset")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		namespace           string
		client              clientset.Interface
		defaultDatacenter   *object.Datacenter
		datastoreURL        string
		scParameters        map[string]string
		storageClassName    string
		storagePolicyName   string
		svcPVCName          string
		rawBlockVolumeMode  = corev1.PersistentVolumeBlock
		pandoraSyncWaitTime int
		deleteFCDRequired   bool
		fcdID               string
		pv                  *corev1.PersistentVolume
		pvc                 *corev1.PersistentVolumeClaim
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		namespace = getNamespaceToRunTests(f)
		client = f.ClientSet
		bootstrap()
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
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

		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		}
		if deleteFCDRequired {
			ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
			err := e2eVSphere.deleteFCD(ctx, fcdID, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
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
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] [csi-guest]"+
		"Statefulset testing with raw block volume and default podManagementPolicy", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Set Resource quota for GC")
		if vanillaCluster {
			storageClassName = defaultNginxStorageClassName
			scParameters = nil
		} else if guestCluster {
			storageClassName = defaultNginxStorageClassName
			scParameters[svStorageClassName] = storagePolicyName
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}

		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
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

		ginkgo.By("Creating statefulset with raw block volume")
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
			Annotations["volume.beta.kubernetes.io/storage-class"] = defaultNginxStorageClassName
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].ObjectMeta.Name =
			statefulset_volname
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.VolumeMode =
			&rawBlockVolumeMode
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		// Check if raw device available inside all pods of statefulset
		gomega.Expect(CheckDevice(client, statefulset, statefulset_devicePath)).NotTo(gomega.HaveOccurred())

		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
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
		_, scaledownErr := fss.Scale(client, statefulset, replicas-1)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas-1)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
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
		_, scaleupErr := fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)

		ssPodsAfterScaleUp := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitTimeoutForPodReadyInNamespace(client, sspod.Name, statefulset.Namespace, pollTimeout)
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
					gomega.Expect(contains(volumesBeforeScaleDown, volumeID)).To(gomega.BeTrue())
					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						volumeID, sspod.Spec.NodeName))
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					vmUUID := getNodeUUID(ctx, client, sspod.Spec.NodeName)
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
		_, scaledownErr = fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		ssPodsAfterScaleDown = fss.GetPodList(client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
	})

	/*
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
	ginkgo.It("[csi-block-vanilla] [csi-guest] [csi-block-vanilla-parallelized] "+
		"Should create and delete pod with the same raw block volume", func() {
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
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating raw block PVC")
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", sc, nil, "")
		pvcspec.Spec.VolumeMode = &rawBlockVolumeMode
		pvc, err = fpv.CreatePVC(client, namespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*corev1.PersistentVolumeClaim{pvc},
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
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
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
		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataOnRawBlockVolume(namespace, pod.Name, devicePath, "This is first write..")

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		if guestCluster {
			ginkgo.By("Waiting for CnsNodeVMAttachment controller to reconcile resource")
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}

		ginkgo.By("Creating a new pod using the same volume")
		pod, err = createPod(client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

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
		readDataFromRawBlockVolume(namespace, pod.Name, devicePath, "This is first write..")
		ginkgo.By(fmt.Sprintf("Write and read new data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataOnRawBlockVolume(namespace, pod.Name, devicePath, "This is second write..")

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		if guestCluster {
			ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
			time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}
	})

	/*
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
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] Verify basic static provisioning workflow"+
		" with raw block volume", func() {
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
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := fpv.DeletePersistentVolume(client, pv.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating raw block PVC")
		pvc = getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc.Spec.VolumeMode = &rawBlockVolumeMode
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, pv, pvc))
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort)
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
		pod, err := createPod(client, namespace, nil, pvclaims, false, "")
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
		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataOnRawBlockVolume(namespace, pod.Name, devicePath, "This is first write..")

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
		framework.ExpectNoError(fpod.DeletePodWithWait(client, pod), "Failed to delete pod", pod.Name)

		ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", pod.Spec.NodeName))
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")
	})
})
