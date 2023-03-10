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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwm-csi-tkg] File Volume Test for ReadOnlyMany", func() {
	f := framework.NewDefaultFramework("rwx-tkg-readonly")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
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
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadOnlyMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
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
		ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
		)

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			"echo 'Hello message from Pod1' && while true ; do sleep 2 ; done")
		pod.Spec.Volumes[0] = v1.Volume{Name: "volume1", VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: true}}}

		pod, err = client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+volHandle))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Wait for pod to be up and running")
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// get fresh pod info
		pod, err = client.CoreV1().Pods(namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		_, err = framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace), pod.Name,
			"--", "/bin/sh", "-c", "echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html")
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
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating another PVC")
		storageclasspvc2, pvclaim2, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadOnlyMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim, pvclaim2}, framework.ClaimProvisionTimeout)
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
		ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
		)

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(volumeID2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult2.Volumes).ShouldNot(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult2.Volumes[0].Name,
			queryResult2.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult2.Volumes[0].VolumeType, queryResult2.Volumes[0].HealthStatus,
			queryResult2.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
		)

		// Create a Pod to use the PVC created above
		ginkgo.By("Creating pod to attach PV to the node")
		pod := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaim, pvclaim2}, false, execRWXCommandPod1)

		pod.Spec.Containers[0].VolumeMounts[0] = v1.VolumeMount{Name: "volume1", MountPath: "/mnt/" + "volume1"}
		pod.Spec.Volumes[0] = v1.Volume{Name: "volume1", VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}

		pod.Spec.Containers[0].VolumeMounts[1] = v1.VolumeMount{Name: "volume2", MountPath: "/mnt/" + "volume2"}
		pod.Spec.Volumes[1] = v1.Volume{Name: "volume2", VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim2.Name, ReadOnly: true}}}

		pod, err = client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod.Spec.NodeName+"-"+volHandle))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod.Spec.NodeName+"-"+volHandle2))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle2, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle2,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Wait for pod to be up and running")
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// get fresh pod info
		pod, err = client.CoreV1().Pods(namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle2,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

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

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		_, err = framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace), pod.Name, "--",
			"/bin/sh", "-c", "echo 'Hello message from test into Pod1' > /mnt/volume2/Pod1.html")
		gomega.Expect(err).To(gomega.HaveOccurred())

	})

	/*
		Test to verify file volume provision - two pods using the PVC one after
		the other with ReadWriteMany and ReadOnlyMany access.

		Steps
		1. Create StorageClass
		2. Create PVC which uses the StorageClass created in step 1
		3. Wait for PV to be provisioned
		4. Wait for PVC's status to become Bound
		5. Query CNS and check if the PVC entry is pushed into CNS or not
		6. Create pod using PVC
		7. Wait for Pod to be up and running and verify CnsFileAccessConfig CRD is created or not
		8. Verify Read/Write operation on File volume
		9. Delete Pod and wait for pods to be deleted and confirm the CnsFileAccessConfig is deleted
		10. Create another Pod to use the same PVC as ReadOnly: True
		11. Wait for the Pod to be up and running and verify CnsFileAccessConfig CRD is created or not
		12. Verfiy Read operation on the files created by Pod1
		13. Verify Write operation on the files volume and check the write fails
		14. Delete pod and confirm the CnsFileAccessConfig is deleted
		15. Delete PVC, PV and Storage Class
	*/

	ginkgo.It("Verify RWX ReadWriteMany Volume created from Pod1 in Pod2 as ReadOnlyMany access", func() {

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

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
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
		ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
		)

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1 %s",
			pod.Spec.NodeName+"-"+volHandle))
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

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

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod.Spec.NodeName+"-"+volHandle))
		err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
			crdVersion, crdGroup, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1 %s",
			pod.Spec.NodeName+"-"+volHandle))
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, false)

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod2 := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			"echo 'Hello message from Pod1' && while true ; do sleep 2 ; done")
		pod2.Spec.Volumes[0] = v1.Volume{Name: "volume1", VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: true}}}

		pod2, err = client.CoreV1().Pods(namespace).Create(ctx, pod2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod2 %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod2.Spec.NodeName+"-"+volHandle))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod2.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2 %s",
				pod2.Spec.NodeName+"-"+volHandle))
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Wait for pod2 to be up and running")
		err = fpod.WaitForPodNameRunningInNamespace(client, pod2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// get fresh pod info
		pod2, err = client.CoreV1().Pods(namespace).Get(context.TODO(), pod2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2 %s",
			pod2.Spec.NodeName+"-"+volHandle))
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		_, err = framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace), pod2.Name, "--",
			"/bin/sh", "-c", "echo 'Hello message from test into Pod2 file' > /mnt/volume1/Pod1.html")
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	/*
		Test to verify file volume provision - two pods using the PVC one after the other with ReadWriteMany
		and statically provisioned ReadOnlyMany access

		Steps
		1. Create StorageClass
		2. Create PVC which uses the StorageClass created in step 1
		3. Wait for PV to be provisioned
		4. Wait for PVC's status to become Bound
		5. Query CNS and check if the PVC entry is pushed into CNS or not
		6. Create pod using PVC
		7. Wait for Pod to be up and running and verify CnsFileAccessConfig CRD is created or not
		8. Verify Read/Write operation on File volume
		9. Delete Pod and wait for pods to be deleted and confirm the CnsFileAccessConfig is deleted
		10. Create a statically provisioned ReadOnlyMany PV
		11. Create a ReadOnlyMany PVC using the PV created above
		12. Wait for the PV and PVC to be bind
		13. Create a ReadWriteMany PVC3 with the SC created above
		14. Wait for the PVC3 status to become bound
		15. Query CNS and check if the PVC entry is pushed into CNS or not
		16. Create another Pod to use the same PVC as ReadOnly: True and PVC3 as ReadWriteMany
		17. Wait for the Pod to be up and running and verify CnsFileAccessConfig CRD is created or not
		18. Verfiy Read operation on the files created by Pod1
		19. Verify Write operation on the files volume and check the write fails
		20. Verfiy Read/Write operation on the files created on PVC3
		21. Delete pod and confirm the CnsFileAccessConfig is deleted
		22. Delete PVC, PV and Storage Class
	*/

	ginkgo.It("Verify RWX ReadWriteMany Volume created from Pod1 in Pod2 as statically provisioned "+
		"ReadOnlyMany access", func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var storageclasspvc3 *storagev1.StorageClass
		var pvclaim3 *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize,
			nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim},
			framework.ClaimProvisionTimeout)
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
		ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
		)

		// Create a POD to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1 %s",
			pod.Spec.NodeName+"-"+volHandle))
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
			crdVersion, crdGroup, true)

		ginkgo.By("Verify the Read and write on volume is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" +
			namespace, "--", "/bin/sh", "-c", "cat /mnt/volume1/Pod1.html "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1 %s",
			pod.Spec.NodeName+"-"+volHandle))
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
			crdVersion, crdGroup, false)

		// Creating label for PV.
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)

		ginkgo.By("Creating the PV in guest cluster")
		pv2 := getPersistentVolumeSpecForRWX(volHandle, v1.PersistentVolumeReclaimDelete, staticPVLabels,
			"2Gi", "", v1.ReadOnlyMany)
		pv2, err = client.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating the PVC in guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespace, staticPVLabels, pv2.Name, "2Gi")
		pvc2.Spec.AccessModes[0] = v1.ReadOnlyMany

		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace,
			pv2, pvc2))
		volumeHandle2 := pv2.Spec.CSI.VolumeHandle

		ginkgo.By("Creating a PVC")
		storageclasspvc3, pvclaim3, err = createPVCAndStorageClass(client, namespace, nil, scParameters,
			diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc3.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes3, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim3},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		volHandle3 := persistentvolumes3[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		volumeID3 := getVolumeIDFromSupervisorCluster(volHandle3)
		gomega.Expect(volumeID3).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim3.Name, pvclaim3.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle3)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID-3: %s", volumeID3))
		queryResult3, err := e2eVSphere.queryCNSVolumeWithResult(volumeID3)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult3.Volumes).ShouldNot(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult3.Volumes[0].Name,
			queryResult3.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult3.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult3.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
		)

		// Create a POD to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		execRWXCmd := "echo 'Hello message from Pod2' > /mnt/volume2/Pod2.html " +
			" && chmod o+rX /mnt /mnt/volume2/Pod2.html && while true ; do sleep 2 ; done"
		pod2 := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvc2, pvclaim3}, false, execRWXCmd)
		pod2.Spec.Volumes[0] = v1.Volume{Name: "volume1",
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc2.Name,
					ReadOnly: true}}}

		pod2, err = client.CoreV1().Pods(namespace).Create(ctx, pod2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod2 %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod2.Spec.NodeName+"-"+volumeHandle2))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod2.Spec.NodeName+"-"+volumeHandle2, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2 %s",
				pod2.Spec.NodeName+"-"+volumeHandle2))
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+volumeHandle2, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod2.Spec.NodeName+"-"+volHandle3))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod2.Spec.NodeName+"-"+volHandle3, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2 %s",
				pod2.Spec.NodeName+"-"+volHandle3))
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+volHandle3,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Wait for pod2 to be up and running")
		err = fpod.WaitForPodNameRunningInNamespace(client, pod2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// get fresh pod info
		pod2, err = client.CoreV1().Pods(namespace).Get(context.TODO(), pod2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2 %s",
			pod2.Spec.NodeName+"-"+volHandle))
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+volumeHandle2, crdCNSFileAccessConfig,
			crdVersion, crdGroup, true)

		ginkgo.By(fmt.Sprintf("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2 %s",
			pod2.Spec.NodeName+"-"+volHandle3))
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+volHandle3, crdCNSFileAccessConfig,
			crdVersion, crdGroup, true)

		ginkgo.By("Verify Read/write is possible on volume")
		cmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh",
			"-c", "cat /mnt/volume1/Pod1.html "}
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		_, output, err = framework.RunKubectlWithFullOutput(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace),
			pod2.Name, "--", "/bin/sh", "-c", "echo 'Hello message from test into Pod2 file' > /mnt/volume1/Pod1.html")
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(strings.Contains(output, "Read-only file system")).To(gomega.BeTrue())

		ginkgo.By("Verify Read/write is possible on volume")
		cmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh",
			"-c", "cat /mnt/volume2/Pod2.html "}
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod2")).NotTo(gomega.BeFalse())

		wrtiecmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh",
			"-c", "echo 'Hello message from test into Pod2' > /mnt/volume2/Pod2.html"}
		framework.RunKubectlOrDie(namespace, wrtiecmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2")).NotTo(gomega.BeFalse())
	})

})
