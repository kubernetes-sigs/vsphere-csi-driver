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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("[rwm-csi-guest] File Volume Test for ReadOnlyMany", func() {
	f := framework.NewDefaultFramework("rwx-tkg-static")
	var (
		client            clientset.Interface
		namespace         string
		scParameters      map[string]string
		storagePolicyName string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	/*
		Test to verify Pod restricts write into PVC

		Steps
		1. Create StorageClass
		2. Create PVC with RWM mode which uses the StorageClass created in step 1 with ReadOnlyMany access
		3. Wait for PV to be provisioned
		4. Wait for PVC's status to become Bound
		5. Query CNS and check if the PVC entry is pushed into CNS or not
		6. Create pod using PVC with PodSpec + { readOnly: true }
		7. Verify CNSFileAccessConfig CRD is created or not for pod created in step 6
		8. Wait for Pod to be up and running
		9. Verify Write operation on File volume and check the write fails
		10. Delete pod and Wait for Volume Disk to be detached from the Node
		11. Vefiry CNSFileAccessConfig CRD is deleted
		12. Delete PVC, PV and Storage Class.
	*/

	ginkgo.It("Verify RWX volume is restricted to write by pod spec", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadOnlyMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		volumeID := getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s", queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
		)

		// Create a POD to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		pod.Spec.Volumes[0] = v1.Volume{Name: "volume1", VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: true}}}

		pod, err = client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Wait for pod to be up and running")
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		_, err = framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace), pod.Name, "--", "/bin/sh", "-c", "echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html")
		gomega.Expect(err).To(gomega.HaveOccurred())

	})

	/*
		Test to verify Pod restricts write into PVC with ReadOnlyMany and allows write into PVC with ReadwriteMany

		Steps
		1. Create StorageClass
		2. Create PVC with RWM mode which uses the StorageClass created in step 1 with ReadWriteMany access
		3. Create PVC with RWM mode which uses the StorageClass created in step 1 with ReadOnlyMany access
		4. Wait for PVs to be provisioned
		5. Wait for PVCs status to become Bound
		6. Query CNS and check if the PVC entry is pushed into CNS or not
		7. Create pod using PVC with PodSpec + { readOnly: true } for PVC2 and readOnly false for PVC1
		8. Verify CNSFileAccessConfig CRD is created or not for pod created in step 6
		9. Wait for Pod to be up and running
		10. Verify Read/Write operation on PVC1 succeeds
		11. Verify Write operation on PVC2 fails
		12. Delete pod
		12. Vefiry CNSFileAccessConfig CRD is deleted
		13. Delete PVCs, PVs and Storage Class.
	*/
	ginkgo.It("Verify Pod can identify ReadOnlyMany and ReadWriteMany volumes", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var storageclasspvc2 *storagev1.StorageClass
		var pvclaim2 *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating another PVC")
		storageclasspvc2, pvclaim2, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadOnlyMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim, pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		volumeID := getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		volHandle2 := persistentvolumes[1].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())
		volumeID2 := getVolumeIDFromSupervisorCluster(volHandle2)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, pvclaim2.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s", queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
		)

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult2.Volumes).ShouldNot(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s", queryResult2.Volumes[0].Name,
			queryResult2.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult2.Volumes[0].VolumeType, queryResult2.Volumes[0].HealthStatus,
			queryResult2.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
		)

		// Create a POD to use the PVC created above
		ginkgo.By("Creating pod to attach PV to the node")
		pod := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaim, pvclaim2}, false, execRWXCommandPod1)

		pod.Spec.Containers[0].VolumeMounts[0] = v1.VolumeMount{Name: "volume1", MountPath: "/mnt/" + "volume1"}
		pod.Spec.Volumes[0] = v1.Volume{Name: "volume1", VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}

		pod.Spec.Containers[0].VolumeMounts[1] = v1.VolumeMount{Name: "volume2", MountPath: "/mnt/" + "volume2"}
		pod.Spec.Volumes[1] = v1.Volume{Name: "volume2", VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim2.Name, ReadOnly: true}}}

		pod, err = client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig, crdVersion, crdGroup, false)
			verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle2, crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Wait for pod to be up and running")
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig, crdVersion, crdGroup, true)
		verifyCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle2, crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c", "cat /mnt/volume1/Pod1.html "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c", "echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		_, err = framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace), pod.Name, "--", "/bin/sh", "-c", "echo 'Hello message from test into Pod1' > /mnt/volume2/Pod1.html")
		gomega.Expect(err).To(gomega.HaveOccurred())

	})
})
