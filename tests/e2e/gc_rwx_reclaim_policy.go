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

// Test to verify RWX volume provision with reclaim policy set and modified
var _ = ginkgo.Describe("File Volume Test for Reclaim Policy", func() {
	f := framework.NewDefaultFramework("rwx-tkg-reclaim")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		scParameters      map[string]string
		storagePolicyName string
		volHealthCheck    bool
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		// TODO: Read value from command line
		volHealthCheck = false
		namespace = getNamespaceToRunTests(f)
		svcClient, svNamespace := getSvcClientAndNamespace()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		setResourceQuota(svcClient, svNamespace, rqLimit)
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
		Verify static volume provisioning works
		1. Create a Storage class
		2. Create a PVC with "ReadWriteMany" access mode using the SC from above in GC
		3. Wait for PVC to be Bound in GC
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata CRD is created
		6. Patch the PV with reclaim policy to Retain in SV
		7. kubectl patch pv <pv-name> -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
		8. Verify health status of PVC
		9. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		10. Create a Pod1 using PVC created above at a mount path specified in PodSpec
		11. Verify CnsFileAccessConfig CRD is created
		12. Verify Pod1 is in the Running phase
		13. Create a file (file1.txt) at the mount path. Check if the creation is successful
		14. Delete Pod1
		15. Verify CnsFileAccessConfig CRD is deleted
		16. Verify if the Pod is successfully deleted
		17. Delete PVC in GC
		18. Verify if PVC and PV still persists in the SV cluster
		19. Verify PVC name is removed from PV entry on CNS
		20. Create a PV with reclaim policy=delete & access mode ReadWriteMany in GC
		    using the bound PVC in the SV cluster as the volume id PV1
		21. Create PVC2 with "ReadWriteMany" access mode which gets bounds to PV2 (reclaim policy=delete)
		22. Verify CnsVolumeMetadata CRD is created
		23. Verify health status of PVC
		24. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		25. Create Pod2 using PVC2 created above
		26. Verify CnsFileAccessConfig CRD is created
		27. Verify Pod2 is in the Running phase
		28. Verify ACL net permission set by calling CNSQuery for the file volume
		29. Read the file (file1.txt) created by Pod1 from Pod2. Check if reading is successful
		30. Create a new file (file2.txt) at the mount path. Check if the creation is successful
		31. Delete Pod2
		32. Verify CnsFileAccessConfig CRD is deleted
		33. Verify if Pod2 is successfully deleted
		34. Delete PVC2 and PVs from GC
		35. Verify if PVCs and PVs are deleted in the SV cluster and GC
		36. Verify CnsVolumeMetadata CRD is deleted
		37. Check if the VolumeID is deleted from CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify Reclaim Policy Retain with file volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var err error
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcUID := string(pvclaim.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)

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
			}
		}()

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

		// Changing the reclaim policy of the pv to retain.
		ginkgo.By("Changing the volume reclaim policy")
		persistentvolumes[0].Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
		pv, err = client.CoreV1().PersistentVolumes().Update(ctx, persistentvolumes[0],
			metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod.Spec.NodeName+"-"+pvcNameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, pv, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		writeCmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, writeCmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod.Spec.NodeName+"-"+pvcNameInSV))
		err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
			crdVersion, crdGroup, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, false)

		ginkgo.By("Deleting the PVC")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil

		ginkgo.By("Verifying if volume still exists in the Supervisor Cluster")
		fcdIDInCNS = getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		// Creating label for PV
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)

		ginkgo.By("Creating the PV in guest cluster")
		pv2 := getPersistentVolumeSpecForRWX(pvcNameInSV, v1.PersistentVolumeReclaimDelete,
			staticPVLabels, diskSize, "", v1.ReadWriteMany)
		pv2, err = client.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if pv != nil {
				ginkgo.By("Deleting the PV1")
				err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if pv2 != nil {
				ginkgo.By("Deleting the PV2")
				err = client.CoreV1().PersistentVolumes().Delete(ctx, pv2.Name, *metav1.NewDeleteOptions(0))
				if !apierrors.IsNotFound(err) {
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		ginkgo.By("Creating the PVC in guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespace, staticPVLabels, pv2.Name, diskSize)
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PV1")
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pv = nil

			ginkgo.By("Deleting the PVC2")
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			//Add a check to validate CnsVolumeMetadata crd
			verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
		}()

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client,
			framework.NewTimeoutContextWithDefaults(), namespace, pv2, pvc2))

		// Create a Pod to use this PVC
		ginkgo.By("Creating pod2 to attach PV to the node")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc2}, false, execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodNameRunningInNamespace(client, pod2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod2.Spec.NodeName+"-"+pvcNameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod2.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvcNameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		gcClusterID := strings.Replace(pvcNameInSV, pvcUID, "", -1)
		framework.Logf("gcClusterId " + gcClusterID)

		pvUID := string(pv.UID)
		framework.Logf("PV uuid " + pvUID)

		pv2UID := string(pv2.UID)
		framework.Logf("PV2 uuid " + pv2UID)

		pod2UID := string(pod2.UID)
		framework.Logf("Pod uuid : " + pod2UID)

		//Add a check to validate CnsVolumeMetadata crd
		verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)
		verifyCRDInSupervisorWithWait(ctx, f, gcClusterID+pv2UID, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pv2UID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv2.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pod2UID, crdCNSVolumeMetadatas,
			crdVersion, crdGroup, true, pv2.Spec.CSI.VolumeHandle, false, nil, false)

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible from pod2")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output = framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		writeCmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, writeCmd2...)
		output = framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2")).NotTo(gomega.BeFalse())
	})

	/*
		Verify static volume provisioning fails
		1. Create a Storage class
		2. Create a PVC-1 with "ReadWriteOnce" using the SC from in the GC
		3. Wait for PVC-1 to be Bound in the GC
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata CRD is created
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Create a PV-2 with "ReadWriteMany" access mode in GC using the bound PVC-1 as the volumeId in the SV cluster
		9. Create PVC2 with "ReadWriteMany" access mode which gets bounds to PV-2 (reclaim policy=delete)
		10. Verify CnsVolumeMetadata CRD is created
		11. Verify health status of PVC
		12. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		13. Create Pod using PVC-2 created above
		14. Verify Pod creation fails
		15. Verify the error message returned on failure is correct
		16. Delete PVC and PVs from GC
		17. Verify if PVC and PVs are deleted in the SV cluster and GC
		18. Verify CnsVolumeMetadata CRD is deleted
		19. Check if the VolumeID is deleted from CNS by using CNSQuery API
	*/
	ginkgo.It("Verify ReadWriteOnce PVC is not usable as ReadWriteMany", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteOnce)
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
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, persistentvolumes[0], nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Creating label for PV
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)

		ginkgo.By("Creating the PV in guest cluster")
		pv2 := getPersistentVolumeSpecForRWX(pvcNameInSV, v1.PersistentVolumeReclaimDelete,
			staticPVLabels, diskSize, "", v1.ReadWriteMany)
		pv2, err = client.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PV2")
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating the PVC in guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespace, staticPVLabels, pv2.Name, diskSize)
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PVC2")
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client,
			framework.NewTimeoutContextWithDefaults(), namespace, pv2, pvc2))

		// Create a Pod to use this PVC and expect an error
		ginkgo.By("Creating pod to attach PVC")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc2}, false, execRWXCommandPod2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod.Spec.NodeName+"-"+pvcNameInSV))
		err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
			crdVersion, crdGroup, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, false)

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod in defer function %s in namespace %s", pod.Name, pod.Namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		Verify static volume provisioning works using the volume ID from PVC created in SVC
		1. Create a Storage class
		2. Create a PVC with "ReadWriteMany" using the SC from in the SVC
		3. Wait for PVC to be Bound in the SVC
		4. Verify CnsVolumeMetadata CRD is created
		5. Verify health status of PVC
		6. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		7. Create a PV-2 with reclaim policy=delete in GC using the bound PVC's volumeId in the SV cluster
		8. 	Create a PVC-2 with "ReadWriteMany"  access mode in GC using the above PV-2
		9. Verify the PVC-2 is bound in GC
		10. Verify CnsVolumeMetadata CRD is created
		11. Verify health status of PVC-2
		12. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		13. Create a Pod1 using PVC-2 created above at a mount path specified in PodSpec
		14. Verify CnsFileAccessConfig CRD is created
		15. Verify Pod1 is in the Running phase
		16. Create a file (file1.txt) at the mount path. Check if the creation is successful
		17. Delete Pod1
		18. Verify CnsFileAccessConfig CRD is deleted
		19. Verify if the Pod is successfully deleted
		20. Delete PVCs in GC
		21. Verify if PVCs and PVs are deleted in the SV cluster and GC
		22. Verify CnsVolumeMetadata CRD is deleted
		23. Check if the VolumeID is deleted from CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify ReadWriteMany PVC is usable by another PVC as static provision", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcUID := string(pvclaim.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)

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
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcdIDInCNS)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			//Add a check to validate CnsVolumeMetadata crd
			verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
		}()

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

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, persistentvolumes[0], nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Creating label for PV
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)

		ginkgo.By("Creating the PV in guest cluster")
		pv2 := getPersistentVolumeSpecForRWX(pvcNameInSV, v1.PersistentVolumeReclaimDelete,
			staticPVLabels, diskSize, "", v1.ReadWriteMany)
		pv2, err = client.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PV2")
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating the PVC in guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespace, staticPVLabels, pv2.Name, diskSize)
		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PVC2")
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client,
			framework.NewTimeoutContextWithDefaults(), namespace, pv2, pvc2))

		// Create a Pod to use this PVC
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc2}, false, execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod.Spec.NodeName+"-"+pvcNameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		gcClusterID := strings.Replace(pvcNameInSV, pvcUID, "", -1)
		framework.Logf("gcClusterId " + gcClusterID)

		pvUID := string(persistentvolumes[0].UID)
		framework.Logf("PV uuid " + pvUID)

		pv2UID := string(pv2.UID)
		framework.Logf("PV2 uuid " + pv2UID)

		podUID := string(pod.UID)
		framework.Logf("Pod uuid : " + podUID)

		//Add a check to validate CnsVolumeMetadata crd
		verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)
		verifyCRDInSupervisorWithWait(ctx, f, gcClusterID+pv2UID, crdCNSVolumeMetadatas, crdVersion, crdGroup, true)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pv2UID,
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, pv2.Spec.CSI.VolumeHandle, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+podUID, crdCNSVolumeMetadatas,
			crdVersion, crdGroup, true, pv2.Spec.CSI.VolumeHandle, false, nil, false)

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod2.html "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod2")).NotTo(gomega.BeFalse())

		writeCmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2' > /mnt/volume1/Pod2.html"}
		framework.RunKubectlOrDie(namespace, writeCmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2")).NotTo(gomega.BeFalse())
	})
})
