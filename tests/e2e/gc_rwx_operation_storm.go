/*
Copyright 2021 The Kubernetes Authors.

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

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwm-csi-tkg] File Volume Operation storm Test", func() {
	f := framework.NewDefaultFramework("rwx-tkg-operation-storm")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		volHealthCheck    bool
		volumeOpsScale    int
		pvclaims          []*v1.PersistentVolumeClaim
		podArray          []*v1.Pod
		scParameters      map[string]string
		storagePolicyName string
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		// TODO: Read value from command line
		volHealthCheck = false
		volumeOpsScale = 5
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, rqLimit)
		pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)
		podArray = make([]*v1.Pod, volumeOpsScale)
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
	})

	/*
		Concurrency Testing - Create multiple Pods concurrently using a single volume
		1. Create SC
		2. Create One PVC with "ReadWriteMany" using the SC from above in GC
		3. Wait for PVC to be Bound in GC
		4. Verify if the mapping PVC are bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata CRD is created
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Create a Pod and Create a file
		9. Create Multiple Pods concurrently using PVC created above at a mount path specified in PodSpec
		10. Verify CnsFileAccessConfig CRD are created
		11. Verify all the pods are in the Running phase
		12. From any one of the Pod try to create a file1.txt at the mount path.
		13. From the remaining pods keep writing into the file at a definite time interval.
		14. Check if the creation and write is successful for all the pods - Verify for data loss
		15. Delete all the pods concurrently
		16. Verify CnsFileAccessConfig CRD are deleted
		17. Verify if all the pods are successfully deleted
		18. Delete the PVC
		19. Verify if PVC and PV are deleted in the SV cluster and GC
		20. Verify CnsVolumeMetadata CRD is deleted
		21. Verify volumes are deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("Verify multiple Pods concurrently using a one file volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvcNameInSV := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		defer func() {
			if pvclaim != nil {
				err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				//Add a check to validate CnsVolumeMetadata crd
				verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
			}
		}()

		pvcUID := string(pvclaim.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", fcdIDInCNS)
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdIDInCNS)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		framework.Logf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints)

		ginkgo.By("Verifying volume type specified in PVC is honored")
		gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(),
			"Volume type is not FILE")
		ginkgo.By("Verifying volume size is honored")
		newSizeInMb := int64(2048)
		gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
			CapacityInMb == newSizeInMb).To(gomega.BeTrue(), "Volume Capaticy is not matching")

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+pvcNameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, persistentvolumes[0], pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		var pods []*v1.Pod
		for i := 2; i <= volumeOpsScale; i++ {
			ginkgo.By("Create Pods concurrently, without waiting them to be running here..")
			executeCommand := "echo 'Hi' && while true ; do sleep 2 ; done"
			newPod := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, executeCommand)

			newPod, err = client.CoreV1().Pods(namespace).Create(ctx, newPod, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				// Delete POD
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", newPod.Name, namespace))
				err = fpod.DeletePodWithWait(client, newPod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
			pods = append(pods, newPod)
		}

		defer func() {
			for _, deletePod := range pods {
				// Delete POD
				ginkgo.By(fmt.Sprintf("Deleting the pods %s in namespace %s", deletePod.Name, namespace))
				err = fpod.DeletePodWithWait(client, deletePod)
				if !apierrors.IsNotFound(err) {
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		gcClusterID := strings.Replace(pvcNameInSV, pvcUID, "", -1)
		framework.Logf("gcClusterId " + gcClusterID)

		pvUID := string(persistentvolumes[0].UID)
		framework.Logf("PV uuid " + pvUID)

		for _, multipod := range pods {
			ginkgo.By(fmt.Sprintf("Wait for pod %s to be up and running", multipod.Name))
			err = fpod.WaitForPodNameRunningInNamespace(client, multipod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			tempPod, err := client.CoreV1().Pods(namespace).Get(ctx, multipod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			podUID := string(tempPod.UID)
			framework.Logf("Pod uuid : " + podUID)

			//Add a check to validate CnsVolumeMetadata crd
			verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)
			verifyCRDInSupervisorWithWait(ctx, f, gcClusterID+pvUID, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)
			verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID,
				crdCNSVolumeMetadatas, crdVersion, crdGroup, true, gcClusterID+pvUID, false, nil, false)
			verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+podUID, crdCNSVolumeMetadatas,
				crdVersion, crdGroup, true, persistentvolumes[0].Spec.CSI.VolumeHandle, false, nil, false)

			ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD %s is created or not for Pod %s",
				tempPod.Spec.NodeName+"-"+pvcNameInSV, multipod.Name))
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, tempPod.Spec.NodeName+"-"+pvcNameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, true)
		}

		//TODO: Write into the file Synchronously
		ginkgo.By("Verify the volume is accessible by all the pods for Read/write")
		for i, writepod := range pods {
			message := "Hello message from Pod" + strconv.Itoa(i+2)
			cmd = []string{"exec", writepod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"echo '" + message + "'>> /mnt/volume1/Pod1.html"}
			_, err = framework.RunKubectl(namespace, cmd...)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		cmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output, err = framework.RunKubectl(namespace, cmd...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Output from the Pod1.html is %s", output)
		gomega.Expect(strings.Contains(output, "Hello message from Pod")).NotTo(gomega.BeFalse())

		ginkgo.By("Delete all the pods concurrently")
		for _, podToDelete := range pods {
			err = client.CoreV1().Pods(namespace).Delete(ctx, podToDelete.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, multiPod := range pods {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", multiPod.Name, namespace))
			err = fpod.DeletePodWithWait(client, multiPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				multiPod.Spec.NodeName+"-"+pvcNameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, multiPod.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, multiPod.Spec.NodeName+"-"+pvcNameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}
	})

	/*
		Concurrency Testing - Create multiple Pods concurrently using a Multiple volumes
		1. Create a SC
		2. Create 10 PVCs with "ReadWriteMany" using the SC from above in GC
		3. Wait for PVCs to be Bound in GC
		4. Verify if the mapping PVCs are bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata CRD are created
		6. Verify health status of PVCs
		7. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Create Multiple Pods concurrently using multiple PVCs created above at a mount path specified in PodSpec
		9. Verify CnsFileAccessConfig CRD are created
		10. Verify all the pods are in the Running phase
		11. Verify ACL net permission set by calling CNSQuery for the file volume
		12. Create a podN-file at the mount path and try writing into the file. Check if the creation and
			write is successful for all the pods
		13. Delete all the pods concurrently
		14. Verify CnsFileAccessConfig CRD are deleted
		15. Verify if all the pods are successfully deleted
		16. Delete the PVCs
		17. Verify if PVCs and PVs are deleted in the SV cluster and GC
		18. Verify CnsVolumeMetadata CRD is deleted
		19. Verify volumes are deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("Verify multiple Pods concurrently using a multiple file volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var err error
		defaultDatastore = getDefaultDatastore(ctx)
		var persistentvolumes = make([]*v1.PersistentVolume, volumeOpsScale)
		var fileAccessCRD []string

		ginkgo.By("CNS_TEST: Running for GC setup")
		ginkgo.By("Create storage class")
		scParameters[svStorageClassName] = storagePolicyName
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		storageclass, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating PVCs using the Storage Class")
		for count := 0; count < volumeOpsScale; count++ {
			framework.Logf("Creating PVC index %s", strconv.Itoa(count))
			pvclaims[count], err = fpv.CreatePVC(client, namespace,
				getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, v1.ReadWriteMany))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			for count := 0; count < volumeOpsScale; count++ {
				if pvclaims[count].Name != "" {
					framework.Logf("Inside Defer function now for PVC index %s", strconv.Itoa(count))
					err = fpv.DeletePersistentVolumeClaim(client, pvclaims[count].Name, pvclaims[count].Namespace)
					if !apierrors.IsNotFound(err) {
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
		}()

		for index, claim := range pvclaims {
			framework.Logf("Waiting for all claims %s to be in bound state - PVC number %s", claim.Name, index)
			pv, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{claim},
				framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			persistentvolumes[index] = pv[0]

			pvcNameInSV := pv[0].Spec.CSI.VolumeHandle
			gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
			fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
			gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

			// Verify using CNS Query API if VolumeID retrieved from PV is present.
			framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", fcdIDInCNS)
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdIDInCNS)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
			framework.Logf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
				queryResult.Volumes[0].Name,
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
				queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints)

			ginkgo.By("Verifying volume type specified in PVC is honored")
			gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(),
				"Volume type is not FILE")
			ginkgo.By("Verifying volume size is honored")
			newSizeInMb := int64(2048)
			gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
				CapacityInMb == newSizeInMb).To(gomega.BeTrue(), "Volume Capaticy is not matching")

			//Add a check to validate CnsVolumeMetadata crd
			verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)
		}

		if volHealthCheck {
			for index, claim := range pvclaims {
				framework.Logf("poll for health status annotation for volume number = %s", index)
				err = pvcHealthAnnotationWatcher(ctx, client, claim, healthStatusAccessible)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		podCount := 0
		for podCount < volumeOpsScale {
			ginkgo.By("Create Pods concurrently, without waiting them to be running here..")
			executeCommand := "echo 'Hi' && while true ; do sleep 2 ; done"
			newPod := fpod.MakePod(namespace, nil, pvclaims, false, executeCommand)
			newPod, err = client.CoreV1().Pods(namespace).Create(ctx, newPod, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			podArray[podCount] = newPod
			podCount++
		}

		defer func() {
			for index := range podArray {
				// Delete POD
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", podArray[index].Name,
					podArray[index].Namespace))
				err = fpod.DeletePodWithWait(client, podArray[index])
				if !apierrors.IsNotFound(err) {
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		for index := range podArray {
			ginkgo.By("Wait for pod to be up and running")
			err = fpod.WaitForPodNameRunningInNamespace(client, podArray[index].Name, podArray[index].Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			tempPod, err := client.CoreV1().Pods(namespace).Get(ctx, podArray[index].Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			podArray[index] = tempPod

			for volIndex := range pvclaims {
				ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Created or not for Pod")
				pvcNameInSV := persistentvolumes[volIndex].Spec.CSI.VolumeHandle
				gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
				fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
				gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

				framework.Logf("Searching for file access config crd %s", tempPod.Spec.NodeName+"-"+pvcNameInSV)
				verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, tempPod.Spec.NodeName+"-"+pvcNameInSV,
					crdCNSFileAccessConfig, crdVersion, crdGroup, true)
				fileAccessCRD = append(fileAccessCRD, tempPod.Spec.NodeName+"-"+pvcNameInSV)
			}
		}

		for index := range podArray {
			ginkgo.By("Verify the volume is accessible by all the pods for Read/write")
			message := "Hello message from Pod" + strconv.Itoa(index+1)

			for volIndex := range pvclaims {
				volumePath := "'>> /mnt/volume" + strconv.Itoa(volIndex+1) + "/File.html"
				cmd := []string{"exec", podArray[index].Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
					"echo '" + message + volumePath}
				_, err = framework.RunKubectl(namespace, cmd...)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		//Make a random check on the volumes to verify the data
		for _, pod := range podArray {
			for volIndex := range pvclaims {
				volumePath := "cat /mnt/volume" + strconv.Itoa(volIndex+1) + "/File.html"
				cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c", volumePath}
				output, err := framework.RunKubectl(namespace, cmd...)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("Output from the File.html is %s", output)
				gomega.Expect(strings.Contains(output, "Hello message from Pod")).NotTo(gomega.BeFalse())
			}
		}

		ginkgo.By("Delete all the pods concurrently")
		for _, temppod := range podArray {
			framework.Logf("Deleting pod %s", temppod.Name)
			err = client.CoreV1().Pods(namespace).Delete(ctx, temppod.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, pod := range podArray {
			framework.Logf("Checking if the pod %s is completely deleted or not", pod.Name)
			err := fpod.WaitForPodNotFoundInNamespace(client, pod.Name, pod.Namespace, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for index := range fileAccessCRD {
			framework.Logf("Checking if the CRD %s is completely deleted or not", fileAccessCRD[index])
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, fileAccessCRD[index],
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})
})
