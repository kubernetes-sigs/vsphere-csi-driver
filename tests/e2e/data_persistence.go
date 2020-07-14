/*
Copyright 2019 The Kubernetes Authors.

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
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

/*
	Steps

	1. Create a PVC.
	2. Create pod and wait for pod to become ready.
	3. Verify volume is attached.
	4. Create empty file on the volume to verify volume is writable.
	5. Verify newly created file and previously created files exist on the volume.
	6. Delete pod.
	7. Create a new pod using the previously created volume and wait for pod to become ready.
	8. Create another empty file on the volume .
	9. Verify newly created file and previously created files exist on the volume.
	10. Delete pod.
	11. Wait for volume to be detached.

*/

var _ = ginkgo.Describe("Data Persistence", func() {
	f := framework.NewDefaultFramework("e2e-data-persistence")
	var (
		client            clientset.Interface
		defaultDatacenter *object.Datacenter
		defaultDatastore  *object.Datastore
		namespace         string
		scParameters      map[string]string
		storagePolicyName string
		svcPVCName        string // PVC Name in the Supervisor Cluster
		datastoreURL      string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
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
	})

	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-guest] Should create and delete pod with the same volume source", func() {
		var sc *storagev1.StorageClass
		var pvc *v1.PersistentVolumeClaim
		var err error
		ginkgo.By("Creating Storage Class and PVC")
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, nil, "", nil, "", false, "")
		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			ginkgo.By(fmt.Sprintf("storagePolicyName: %s", storagePolicyName))
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storagePolicyName)
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "", storagePolicyName)
		} else {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
			sc, pvc, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(sc.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := framework.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster
			svcPVCName = volumeID
			volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		defer func() {
			err := framework.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		var vmUUID string
		var exists bool
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			vmUUID = getNodeUUID(client, pod.Spec.NodeName)
		} else if supervisorCluster {
			annotations := pod.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
			_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		}
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		var volumeFiles []string
		// Create an empty file on the mounted volumes on the pod
		ginkgo.By(fmt.Sprintf("Creating an empty file on the volume mounted on: %v", pod.Name))
		newEmptyFileName := fmt.Sprintf("/mnt/volume1/%v_file_A.txt", namespace)
		volumeFiles = append(volumeFiles, newEmptyFileName)
		createAndVerifyFilesOnVolume(namespace, pod.Name, []string{newEmptyFileName}, volumeFiles)
		ginkgo.By("Deleting the pod")
		err = framework.DeletePodWithWait(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if supervisorCluster {
			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", pv.Spec.CSI.VolumeHandle, vmUUID))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", vmUUID, pv.Spec.CSI.VolumeHandle))
		} else {
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			if guestCluster {
				ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
				time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
				verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
			}
		}

		ginkgo.By("Creating a new pod using the same volume")
		pod, err = framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		if vanillaCluster {
			vmUUID = getNodeUUID(client, pod.Spec.NodeName)
		} else if supervisorCluster {
			annotations := pod.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
			_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		}
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Create another empty file on the mounted volume on the pod and
		// verify newly and previously created files present on the volume mounted on the pod
		ginkgo.By(fmt.Sprintf("Creating a second empty file on the same volume mounted on: %v", pod.Name))
		newEmptyFileName = fmt.Sprintf("/mnt/volume1/%v_file_B.txt", namespace)
		volumeFiles = append(volumeFiles, newEmptyFileName)
		createAndVerifyFilesOnVolume(namespace, pod.Name, []string{newEmptyFileName}, volumeFiles)
		ginkgo.By("Deleting the pod")
		err = framework.DeletePodWithWait(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if supervisorCluster {
			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", pv.Spec.CSI.VolumeHandle, vmUUID))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", vmUUID, pv.Spec.CSI.VolumeHandle))
		} else {
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			if guestCluster {
				ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
				time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
				verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
			}
		}
	})

	/*
		Data Persistence in case of Static Volume Provisioning on SVC

		Steps

		1. Create FCD with valid storage policy.
		2. Create Resource quota
		3. Create CNS register volume with above created FCD
		4. verify PV, PVC got created , check the bidirectional referance.
		5. Create POD, with above created PVC
		6. Verify volume is attached to the node and volume is accessible in the pod.
		7. Create an empty file on the mounted volumes on the pod
		7. Delete POD.
		8. Create a new pod using the previously created volume and wait for pod to become ready.
		9. Create an empty file again and Verify newly created file and previously created files exist on the volume.
		10. Delete Newly created POD.
		11. Delete PVC
		12. Verify PV is deleted automatically.
		13. Verify Volume id deleted automatically.
		14. Verify CRD deleted automatically
		15. Delete resource quota
	*/
	ginkgo.It("[csi-supervisor] Data Persistence in case of Static Volume Provisioning on SVC", func() {
		var pvc *v1.PersistentVolumeClaim
		var err error
		var vmUUID string
		var exists bool
		var volumeFiles []string

		ctx, cancel := context.WithCancel(context.Background())
		log := logger.GetLogger(ctx)
		defer cancel()

		// Get a config to talk to the apiserver
		k8senv := GetAndExpectStringEnvVar("KUBECONFIG")
		restConfig, err := clientcmd.BuildConfigFromFlags("", k8senv)
		if err != nil {
			return
		}

		curtime := time.Now().Unix()
		curtimeinstring := strconv.FormatInt(curtime, 10)
		pvcName := "cns-pvc-" + curtimeinstring

		ginkgo.By("Get the Profile ID")
		ginkgo.By(fmt.Sprintf("storagePolicyName: %s", storagePolicyName))
		profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
		log.Infof(" Profile ID :%s", profileID)
		scParameters := make(map[string]string)
		scParameters["storagePolicyID"] = profileID
		client.StorageV1().StorageClasses().Delete(storagePolicyName, nil)
		storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, storagePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		storageclass, err = client.StorageV1().StorageClasses().Get(storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		log.Infof(" storageclass Name :%s", storageclass.GetName())

		defer func() {
			log.Infof(" Delete storage class ")
			err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create FCD with valid storage policy.")
		fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx, "staticfcd"+curtimeinstring, profileID, diskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora", 90, fcdID))
		time.Sleep(time.Duration(90) * time.Second)

		ginkgo.By("Create resource quota")
		createResourceQuota(client, namespace, rqLimit, storagePolicyName)

		ginkgo.By(" Import above created FCD ")
		cnsRegisterVolume := getCNSRegisterVolummeSpec(ctx, namespace, fcdID, pvcName, v1.ReadWriteOnce)
		err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(time.Duration(60) * time.Second)
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		log.Infof(" cnsRegisterVolumeName : %s", cnsRegisterVolumeName)

		ginkgo.By(" verify created PV, PVC and check the bidirectional referance")
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(pvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := getPvFromClaim(client, namespace, pvcName)
		verifyBidirectionalReferenceOfPVandPVC(ctx, client, pvc, pv, fcdID)

		ginkgo.By("Creating pod")
		pod, err := framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		volumeID := pv.Spec.CSI.VolumeHandle
		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		ginkgo.By(fmt.Sprintf("Creating an empty file on the volume mounted on: %v", pod.Name))
		newEmptyFileName := fmt.Sprintf("/mnt/volume1/%v_file_A.txt", namespace)
		volumeFiles = append(volumeFiles, newEmptyFileName)
		createAndVerifyFilesOnVolume(namespace, pod.Name, []string{newEmptyFileName}, volumeFiles)

		ginkgo.By("Deleting the pod")
		framework.DeletePodWithWait(f, client, pod)

		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", pv.Spec.CSI.VolumeHandle, vmUUID))
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", vmUUID, pv.Spec.CSI.VolumeHandle))

		ginkgo.By("Creating a new pod using the same volume")
		pod, err = framework.CreatePod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		annotations = pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Create another empty file on the mounted volume on the pod and verify newly and previously created files present on the volume mounted on the pod")
		ginkgo.By(fmt.Sprintf("Creating a second empty file on the same volume mounted on: %v", pod.Name))
		newEmptyFileName = fmt.Sprintf("/mnt/volume1/%v_file_B.txt", namespace)
		volumeFiles = append(volumeFiles, newEmptyFileName)
		createAndVerifyFilesOnVolume(namespace, pod.Name, []string{newEmptyFileName}, volumeFiles)

		ginkgo.By("Deleting the pod")
		framework.DeletePodWithWait(f, client, pod)

		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", pv.Spec.CSI.VolumeHandle, vmUUID))
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", vmUUID, pv.Spec.CSI.VolumeHandle))

		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(framework.DeletePersistentVolumeClaim(client, pvc.Name, namespace), "Failed to delete PVC ", pvc.Name)
		pvc = nil

		ginkgo.By("Verify PV should be deleted automatically")
		framework.ExpectNoError(framework.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort))
		pv = nil

		ginkgo.By("Verify CRD should be deleted automatically")
		time.Sleep(time.Duration(40) * time.Second)
		flag := queryCNSRegisterVolume(ctx, restConfig, cnsRegisterVolumeName, namespace)
		gomega.Expect(flag).NotTo(gomega.BeTrue())

		ginkgo.By("Delete Resource quota")
		deleteResourceQuota(client, namespace)
	})
})

func createAndVerifyFilesOnVolume(namespace string, podname string, newEmptyfilesToCreate []string, filesToCheck []string) {
	createEmptyFilesOnVSphereVolume(namespace, podname, newEmptyfilesToCreate)
	ginkgo.By(fmt.Sprintf("Verify files exist on volume mounted on: %v", podname))
	verifyFilesExistOnVSphereVolume(namespace, podname, filesToCheck...)
}
