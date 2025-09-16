/*
Copyright 2020 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"

	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-file-vanilla] Verify Two Pods can read write files "+
	"when created with same PVC (dynamically provisioned) with access mode ReadWriteMany", func() {
	f := framework.NewDefaultFramework("file-volume-basic")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string
	)
	const (
		filePath1  = "/mnt/volume1/file1.txt"
		filePath2  = "/mnt/volume1/file2.txt"
		accessMode = v1.ReadWriteMany
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	/*
		Verify Two Pods can read the files written by each other, when both have same pvc mounted

			1. Create StorageClass with fsType as "nfs4"
			2. Create a PVC with "ReadWriteMany" using the SC from above
			3. Wait for PVC to be Bound
			4. Get the VolumeID from PV
			5. Verify using CNS Query API if VolumeID retrieved from PV is present. Also verify
				Name, Capacity, VolumeType, Health matches
			6. Verify if VolumeID is created on one of the VSAN datastores from list of datacenters provided in vsphere.conf
			7. Create Pod1 using PVC created above at a mount path specified in PodSpec
			8. Create a file (file1.txt) at the mount path. Check if the creation is successful
			9. Create Pod2 using PVC created above, Wait for Pod2 to be Running
			10. Read the file (file1.txt) created in Step 8 from Pod2. Check if reading is successful
			11. Create a new file (file2.txt) at the mount path from Pod2. Check if the creation is successful
			12. Read the file (file2.txt) at the mount path from Pod1.
		Cleanup:
			1. Delete all the Pods, pvcs and storage class and verify the deletion
	*/
	ginkgo.It("[csi-file-vanilla] Verify Two Pods can read the files written by each other, "+
		"when both have same pvc mounted", ginkgo.Label(p0, file, vanilla, vc70), func() {
		invokeTestForCreateFileVolumeAndMount(f, client,
			namespace, accessMode, filePath1, filePath2, false, false, false)
	})

	/*
		Verify Pod can read the files written by other Pod, which is deleted, when both have same pvc mounted

			1. Create StorageClass with fsType as "nfs4"
			2. Create a PVC with "ReadWriteMany" using the SC from above
			3. Wait for PVC to be Bound
		    4. Get the VolumeID from PV
		    5. Verify using CNS Query API if VolumeID retrieved from PV is present. Also verify
			    Name, Capacity, VolumeType, Health matches
			6. Verify if VolumeID is created on one of the VSAN datastores from list of datacenters provided in vsphere.conf
			7. Create Pod1 using PVC created above at a mount path specified in PodSpec
			8. Create a file (file1.txt) at the mount path. Check if the creation is successful
			9. Delete the Pod Pod1
			10. Create Pod2 using PVC created above, Wait for Pod2 to be Running
			11. Read the file (file1.txt) created in Step 8 from Pod2. Check if reading is successful
			12. Create a new file (file2.txt) at the mount path from Pod2. Check if the creation is successful
		Cleanup:
			1. Delete all the Pods, pvcs and storage class and verify the deletion
	*/
	ginkgo.It("[csi-file-vanilla] Verify Pod can read the files written by other Pod, "+
		"which is deleted, when both have same pvc mounted", ginkgo.Label(p0, file, vanilla, vc70), func() {
		invokeTestForCreateFileVolumeAndMount(f, client, namespace, accessMode, filePath1, filePath2,
			true, false, false)
	})

	/*
		Verify Pod can read the files written by other Pod, which is deleted, when both have same pvc mounted

		    1. Create StorageClass with fsType as "nfs4"
			2. Create a PVC with "ReadWriteMany" using the SC from above
			3. Wait for PVC to be Bound
			4. Get the VolumeID from PV
			5. Verify using CNS Query API if VolumeID retrieved from PV is present. Also verify
				Name, Capacity, VolumeType, Health matches
			6. Verify if VolumeID is created on one of the VSAN datastores from list of datacenters provided in vsphere.conf
			7. Create Pod1 using PVC created above at a mount path specified in PodSpec
			8. Create a file (file1.txt) at the mount path. Check if the creation is successful
			9. Create Pod2 using normal user and PVC created above, Wait for Pod2 to be Running
			10. Read the file (file1.txt) created in Step 8 from Pod2. Check if reading is successful
			11. Create a new file (file2.txt) at the mount path from Pod2. Check if the creation is successful
		Cleanup:
			1. Delete all the Pods, pvcs and storage class and verify the deletion
	*/
	ginkgo.It("[csi-file-vanilla] Verify Pod can read the files written by other Pod created as root user, "+
		"when both have same pvc mounted", ginkgo.Label(p0, file, vanilla, vc70), func() {
		invokeTestForCreateFileVolumeAndMount(f, client, namespace, accessMode, filePath1, filePath2, false, true, false)
	})

	/*
		Verify Pod can read the files written by other Pod, which is deleted, when both have same pvc mounted

			1. Create StorageClass with fsType as "nfs4"
			2. Create a PVC1 with "ReadWriteMany" using the SC from above
		    3. Wait for PVC1 to be Bound
			4. Get the VolumeID from PV
		    5. Verify using CNS Query API if VolumeID retrieved from PV is present. Also verify
				Name, Capacity, VolumeType, Health matches
			6. Verify if VolumeID is created on one of the VSAN datastores from list of datacenters provided in vsphere.conf
		    7. Create Pod1 using PVC1 created above at a mount path specified in PodSpec
			8. Create a file (file1.txt) at the mount path. Check if the creation is successful
			9. Delete the Pod and create a PVC2 with same file share created with PVC1
			10. Create Pod2 with PVC created above, Wait for Pod2 to be Running
		   	11. Read the file (file1.txt) created in Step 8 from Pod2. Check if reading is successful
			12. Create a new file (file2.txt) at the mount path from Pod2. Check if the creation is successful
		Cleanup:
			1. Delete all the Pods, pvcs and storage class and verify the deletion
	*/
	ginkgo.It("[csi-file-vanilla] Verify Pod can read the files written by other Pod, "+
		"which is deleted, when the Pod has pvc statically provisoned on same vsan file share", ginkgo.Label(p0,
		file, vanilla, vc70), func() {
		invokeTestForCreateFileVolumeAndMount(f, client, namespace, accessMode, filePath1, filePath2, true, false, true)
	})

	/*
		Verify provisioning of PVC fails for RWX PVC with xfs fstype
		    1.Create StorageClass with fsType as "xfs"
		    2.Create a PVC with "ReadWriteMany" using the SC from above
		    3.Verify for pvc to not come to Bound state as provisioning of pvc fails with xfs fstype
		Cleanup:
		    1.Delete all the pvcs and storage class and verify the deletion
	*/
	ginkgo.It("[csi-file-vanilla] Verify provisioning of PVC fails for RWX PVC with xfs fstype", ginkgo.Label(p1,
		negative, file, vanilla, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create Storage class and PVC
		ginkgo.By("Creating Storage Class and PVC with xfs fstype")
		scParameters := map[string]string{}
		scParameters[scParamFsType] = xfsFSType

		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var pvclaims []*v1.PersistentVolumeClaim
		pvclaim, err := createPVC(ctx, client, namespace, nil, "", sc, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims = append(pvclaims, pvclaim)
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		framework.Logf("Error from provisioning of pvc is: %v", err)
		gomega.Expect(err).To(gomega.HaveOccurred())

		// clean up for pvc
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

	})

	/*
		Verify mounting of volume for RWX PVC with ext4 fstype passes
		    1.Create StorageClass with fsType as "ext4"
		    2.Create a PVC with "ReadWriteMany" using the SC from above
		    3.Wait for PVC to be Bound
		    4.Get the VolumeID from PV
		    5.Verify using CNS Query API if VolumeID retrieved from PV is present. Also verify
			Name, Capacity, VolumeType, Health matches
		    6.Verify if VolumeID is created on one of the VSAN datastores from list of datacenters provided in vsphere.conf
		    7.Create Pod using PVC created above at a mount path specified in PodSpec
		    8.Verify if pod comes to Running state as CSI driver ignores the ext4 and default to nfs4 fstype
			while mounting the file volume
		Cleanup:
		    1.Delete all the Pods, pvcs and storage class and verify the deletion
	*/
	ginkgo.It("[csi-file-vanilla] Verify mounting of volume for RWX PVC with ext4 fstype passes", ginkgo.Label(p0,
		file, vanilla, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create Storage class and PVC
		ginkgo.By("Creating Storage Class and PVC with xfs fstype")
		scParameters := map[string]string{}
		scParameters[scParamFsType] = ext4FSType

		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var pvclaims []*v1.PersistentVolumeClaim
		pvclaim, err := createPVC(ctx, client, namespace, nil, "", sc, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle

		// clean up for pvc
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pv := persistentvolumes[0]
			err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeHandle))
		}()

		// Verify various properties Capacity, VolumeType, datastore and datacenter of volume using CNS Query API
		verifyVolPropertiesFromCnsQueryResults(e2eVSphere, volHandle)

		// Create Pod
		ginkgo.By(fmt.Sprintf("Create pod with pvc: %s", pvclaim.Name))
		pod := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaims[0]},
			admissionapi.LevelBaseline, execCommand)
		pod.Spec.Containers[0].Image = busyBoxImageOnGcr
		pod, err = client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
		framework.Logf("Error from creating pod: %v", err)
		// CSI driver ignores the ext4 and default to nfs4 fstype while mounting the file volume.
		// Hence, mounting of file volume should pass with ext4 fstype
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Verify File Volume is created without specifying fstype in pv spec

			1. Create StorageClass without specifying fsType
			2. Create a PVC1 with "ReadWriteMany" using the SC from above
			3. Wait for PVC1 to be Bound
			4. Get the VolumeID from PV
			5. Verify using CNS Query API if VolumeID retrieved from PV is present. Also verify
				Name, Capacity, VolumeType, Health matches
			6. Verify if VolumeID is created on one of the VSAN datastores from list of datacenters provided in vsphere.conf
			7. Create Pod1 using PVC1 created above at a mount path specified in PodSpec
			8. Create a file (file1.txt) at the mount path. Check if the creation is successful
			9. Delete the Pod and create a PVC2 with same file share created with PVC1
			10. Create Pod2 with PVC created above, Wait for Pod2 to be Running
			11. Read the file (file1.txt) created in Step 8 from Pod2. Check if reading is successful
			12. Create a new file (file2.txt) at the mount path from Pod2. Check if the creation is successful
		Cleanup:
			1. Delete all the Pods, pvcs and storage class and verify the deletion
	*/
	ginkgo.It("[cf-vanilla-file][csi-file-vanilla] Verify File Volume is created without specifying fstype in pv"+
		"spec", ginkgo.Label(p0, file, vanilla, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create Storage class and PVC
		ginkgo.By("Creating Storage Class and PVC With nfs4")
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace, nil,
			nil, "", nil, "", false, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Waiting for PVC to be bound
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle

		// clean up for pvc
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify variuos properties Capacity, VolumeType, datastore and datacenter of volume using CNS Query API
		verifyVolPropertiesFromCnsQueryResults(e2eVSphere, volHandle)

		// Create Pod1
		ginkgo.By(fmt.Sprintf("create pod with pvc: %s", pvclaim.Name))
		pod1, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// cleanup for Pod1
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod1.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod1.Spec.NodeName))
		}()

		// Create file1.txt on Pod1
		ginkgo.By("Create file1.txt on Pod1")
		err = e2eoutput.CreateEmptyFileOnPod(namespace, pod1.Name, filePath1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Write data on file1.txt on Pod1
		data := "This file file1 is written by Pod1"
		ginkgo.By("Write on file1.txt from Pod1")
		writeDataOnFileFromPod(namespace, pod1.Name, filePath1, data)

		// Deleting Pod
		ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod1.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod1.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod1.Spec.NodeName))

		// Create Pod 2 on statically provisioned pvc
		staticPVLabels := make(map[string]string)
		staticPVLabels["volumeId"] = "NewVolume"
		pv := getPersistentVolumeSpecFromVolume(volHandle,
			v1.PersistentVolumeReclaimDelete, staticPVLabels, v1.ReadOnlyMany)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = getPersistentVolumeClaimSpecFromVolume(namespace, pv.Name, staticPVLabels, v1.ReadOnlyMany)
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvclaim, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create Pod2 using the same pvc
		ginkgo.By("Create Pod2 with pvc created above")
		userid := int64(1000)
		var pod2 *v1.Pod
		pod2, err = CreatePodByUserID(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "", userid)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// cleanup for Pod2
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod2.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod2.Spec.NodeName))
		}()

		// Read file1.txt created from Pod2
		ginkgo.By("Read file1.txt from Pod2 created by Pod1")
		output := readFileFromPod(namespace, pod2.Name, filePath1)
		ginkgo.By(fmt.Sprintf("File contents from file1.txt are: %s", output))
		data = data + "\n"
		gomega.Expect(output == data).To(gomega.BeTrue(), "Pod2 is able to read file1 written by Pod1")

		// Create a file file2.txt from Pod2
		err = e2eoutput.CreateEmptyFileOnPod(namespace, pod2.Name, filePath2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Write to the file
		ginkgo.By("Write on file2.txt from Pod2")
		data = "This file file2 is written by Pod2"
		writeDataOnFileFromPod(namespace, pod2.Name, filePath2, data)

	})
})

// This is an internal method for multiple tests for file share. This method
// take care of creating 2 Pods mounted on same PVC or PVCs sharing same vsan
// file share and verify if both Pod can read files written by other Pod.
// Pod2 is created as normal user if secondPodForNonRootUser is true.
// Pod1 is deleted if isDeletePodAfterFileCreation is true.
// Pod2 is mounted on statically provisioned pvc backed by same file share id
// as Pvc1 if staticProvisionedPVCForSecondPod true.
func invokeTestForCreateFileVolumeAndMount(f *framework.Framework, client clientset.Interface,
	namespace string, accessMode v1.PersistentVolumeAccessMode, filePath1 string, filePath2 string,
	isDeletePodAfterFileCreation bool, secondPodForNonRootUser bool, staticProvisionedPVCForSecondPod bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scParameters := make(map[string]string)
	scParameters[scParamFsType] = nfs4FSType

	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class and PVC With nfs4")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error
	storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
		namespace, nil, scParameters, "", nil, "", false, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle

	// clean up for pvc
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Verify variuos properties Capacity, VolumeType, datastore and datacenter of volume using CNS Query API
	verifyVolPropertiesFromCnsQueryResults(e2eVSphere, volHandle)

	// Create Pod1 with pvc created above
	ginkgo.By("Create Pod1 with pvc created above")
	pod1, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// cleanup for Pod1
	defer func() {
		if !isDeletePodAfterFileCreation {
			ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod1.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod1.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod1.Spec.NodeName))
		}
	}()

	// Create file1.txt on Pod1
	ginkgo.By("Create file1.txt on Pod1")
	err = e2eoutput.CreateEmptyFileOnPod(namespace, pod1.Name, filePath1)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Write data on file1.txt on Pod1
	data := "This file file1 is written by Pod1"
	ginkgo.By("Write on file1.txt from Pod1")
	writeDataOnFileFromPod(namespace, pod1.Name, filePath1, data)

	// Delete Pod if needed
	if isDeletePodAfterFileCreation {
		ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod1.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod1.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod1.Spec.NodeName))
	}

	// Create Pod 2 on statically provisioned pvc
	if staticProvisionedPVCForSecondPod {
		// Creating label for PV.
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)
		staticPVLabels["volumeId"] = "NewVolume"
		pv := getPersistentVolumeSpecFromVolume(volHandle,
			v1.PersistentVolumeReclaimDelete, staticPVLabels, v1.ReadOnlyMany)
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = getPersistentVolumeClaimSpecFromVolume(namespace, pv.Name, staticPVLabels, v1.ReadOnlyMany)
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvclaim, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Create Pod2 using the same pvc
	ginkgo.By("Create Pod2 with pvc created above")
	userid := int64(1000)
	var pod2 *v1.Pod
	if secondPodForNonRootUser {
		pod2, err = CreatePodByUserID(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "", userid)
	} else {
		pod2, err = createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// cleanup for Pod2
	defer func() {
		ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod2.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod2.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod2.Spec.NodeName))
	}()

	// Read file1.txt created from Pod1
	ginkgo.By("Read file1.txt from Pod2 created by Pod1")
	output := readFileFromPod(namespace, pod2.Name, filePath1)
	ginkgo.By(fmt.Sprintf("File contents from file1.txt are: %s", output))
	data = data + "\n"
	gomega.Expect(output == data).To(gomega.BeTrue(), "Pod2 is able to read file1 written by Pod1")

	// Create a file file2.txt from Pod2
	err = e2eoutput.CreateEmptyFileOnPod(namespace, pod2.Name, filePath2)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Write to the file
	ginkgo.By("Write on file2.txt from Pod2")
	data = "This file file2 is written by Pod2"
	writeDataOnFileFromPod(namespace, pod2.Name, filePath2, data)

	if !isDeletePodAfterFileCreation {
		// Read file2.txt created from Pod1
		ginkgo.By("Read file2.txt from Pod1 created by Pod2")
		output = readFileFromPod(namespace, pod1.Name, filePath2)
		data = data + "\n"
		ginkgo.By(fmt.Sprintf("File content of file2.txt are: %s", output))
		gomega.Expect(output == data).To(gomega.BeTrue(), "Pod1 is able to read file2 written by Pod2")
	}
}
