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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwm-csi-tkg] Basic File Volume Provision Test", func() {
	f := framework.NewDefaultFramework("rwx-tkg-basic")
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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
	})

	/*
		Test to verify file volume provision - basic tests.

		Steps
		1. Create StorageClass
		2. Create PVC which uses the StorageClass created in step 1
		3. Wait for PV to be provisioned
		4. Wait for PVC's status to become Bound
		5. Query CNS and check if the PVC entry is pushed into CNS or not
		6. Create pod using PVC
		7. Wait for Pod to be up and running and verify CnsFileAccessConfig CRD is created or not
		8. Verify Read/Write operation on File volume
		9. Delete pod and confirm the CnsFileAccessConfig is deleted
		10. Delete PVC, PV and Storage Class
	*/
	ginkgo.It("Verify Basic RWX volume provision", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		volumeID := getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
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
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+volHandle))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())
	})

	/*
		Test to verify file volume provision - two pods using the PVC at the same time.

		Steps
		1. Create StorageClass
		2. Create PVC which uses the StorageClass created in step 1
		3. Wait for PV to be provisioned
		4. Wait for PVC's status to become Bound
		5. Query CNS and check if the PVC entry is pushed into CNS or not
		6. Create pod using PVC
		7. Wait for Pod to be up and running and verify CnsFileAccessConfig CRD is created or not
		8. Verify Read/Write operation on File volume
		9. Create a Pod to use the same PVC
		10. Wait for the Pod to be up and running and verify CnsFileAccessConfig CRD is created or not
		11. Verfiy Read/Write operation on the files created by Pod1
		12. Delete pod and confirm the CnsFileAccessConfig is deleted
		13. Delete PVC, PV and Storage Class.
	*/

	ginkgo.It("Verify RWX volume can be accessed by multiple pods", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		volumeID := getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
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
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+volHandle))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod2 to attach PV to the node")
		pod2, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+volHandle))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod2.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod2.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Verify the volume is accessible and Read/write is possible from pod2")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2' > /mnt/volume1/Pod1.html"}
		e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd2...)
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2")).NotTo(gomega.BeFalse())
	})

	/*
		Test to verify file volume provision - two pods using the PVC one after the other.

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
		9. Create another Pod to use the same PVC
		10. Wait for the Pod to be up and running and verify CnsFileAccessConfig CRD is created or not
		11. Verfiy Read/Write operation on the files created by Pod1
		12. Delete pod and confirm the CnsFileAccessConfig is deleted
		13. Delete PVC, PV and Storage Class
	*/

	ginkgo.It("Verify RWX volume can be accessed by multiple pods - pod1 deleted before pvc used in pod2", func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		volumeID := getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
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
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
			pod.Spec.NodeName+"-"+volHandle))
		err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
			crdVersion, crdGroup, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, false)

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod2, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod2.Spec.NodeName+"-"+volHandle))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod2.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod2.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod2.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2 file' > /mnt/volume1/Pod1.html"}
		e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd2...)
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2 file")).NotTo(gomega.BeFalse())
	})

	/*
		Test 1:
		Export detached RWX PVC in TKG

		Running test in TKG Context

		Create SPBM Policy and Assign to TKG Namespace:
		Create a SPBM policy and assign it to the TKG namespace with sufficient quota.

		Create RWX PVC:
		Create a ReadWriteMany (RWX) PersistentVolumeClaim (PVC) named pvc1 in the test namespace in TKG.
		Verify CNS Metadata

		Create and Apply CnsUnregisterVolume CR (SVC Context):
		Create a CnsUnregisterVolume Custom Resource (CR) with the volumeID of pvc1 and
		apply it to the test namespace in the Supervisor cluster (SVC context).

		Check CnsUnregisterVolume Status (SVC Context):
		Check the CnsUnregisterVolume CR and verify that the status.Unregistered field is set to false (SVC context).

		Verify PV/PVC Deletion (SVC Context):
		Confirm that the PersistentVolume (PV) and PVC are not deleted from the Supervisor cluster (SVC context).
		Verify CNS Metadata

		Cleanup:
		Delete the File Share, the test namespace, and the SPBM policy.
	*/
	ginkgo.It("[csi-unregister-volume] Export detached RWX PVC in TKG", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		volumeID := getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
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

		// Get a config to talk to the apiserver
		restConfig := getRestConfigClient()

		svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)

		ginkgo.By("Create CNS unregister volume with above created FCD " + volumeID)

		cnsUnRegisterVolume := getCNSUnregisterVolumeSpec(svNamespace, volumeID)

		cnsRegisterVolumeName := cnsUnRegisterVolume.GetName()
		framework.Logf("CNS unregister volume name : %s", cnsRegisterVolumeName)

		err = createCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete CNS unregister volume CR by name " + cnsRegisterVolumeName)
			err = deleteCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for  CNS unregister volume to be unregistered")
		gomega.Expect(waitForCNSUnRegisterVolumeToGetUnregistered(ctx,
			restConfig, cnsUnRegisterVolume, poll,
			supervisorClusterOperationsTimeout)).To(gomega.HaveOccurred())
	})

	/*
		Test 2:
		Export RWX PVC attached in TKG

		Running test in TKG Context

		Create SPBM Policy and Assign to TKG Namespace:
		Create a SPBM policy and assign it to the TKG namespace with sufficient quota in the TKG context.

		Create PVC:
		Create a PersistentVolumeClaim (PVC) named pvc1 in the test namespace in the TKG context.
		Verify CNS Metadata

		Create and Use Pod:
		Create a Pod named pod1 in the test namespace using pvc1.
		Wait for pod1 to be up and running, then perform I/O operations on pvc1 through pod1.

		Create and Apply CnsUnregisterVolume CR (SVC Context):
		Create a CnsUnregisterVolume Custom Resource (CR) with the volumeID of pvc1 and
		apply it to the test namespace in the Supervisor cluster (SVC context).

		Check CnsUnregisterVolume Status (SVC Context):
		Check the CnsUnregisterVolume CR and verify that the status.Unregistered field is set to false (SVC context).

		Check PV/PVC Status (SVC Context):
		Ensure that the PersistentVolume (PV) and PVC are not deleted from the Supervisor cluster (SVC context).

		Delete Pod and Wait:
		Delete pod1 and wait until the pod is fully deleted.
		Reapply CnsUnregisterVolume CR (SVC Context):
		Create and apply a new CnsUnregisterVolume CR with the volumeID of pvc1 to
		the test namespace in the Supervisor cluster (SVC context).

		Check Updated CnsUnregisterVolume Status (SVC Context):
		Verify that the status.Unregistered field in the CnsUnregisterVolume CR is set to false (SVC context).

		Check PV/PVC Deletion (SVC Context):
		Confirm that the PV and PVC are not deleted from the Supervisor cluster (SVC context).
		Verify CNS Metadata

		Cleanup:
		Delete the File Share, the test namespace, and the SPBM policy.
	*/
	ginkgo.It("[csi-unregister-volume] Export RWX PVC attached in TKG", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		volumeID := getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
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
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+volHandle))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		// Get a config to talk to the apiserver
		restConfig := getRestConfigClient()

		svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)

		ginkgo.By("Create CNS unregister volume with above created FCD " + volumeID)

		cnsUnRegisterVolume := getCNSUnregisterVolumeSpec(svNamespace, volumeID)

		cnsRegisterVolumeName := cnsUnRegisterVolume.GetName()
		framework.Logf("CNS unregister volume name : %s", cnsRegisterVolumeName)

		err = createCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete CNS unregister volume CR by name " + cnsRegisterVolumeName)
			err = deleteCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for  CNS unregister volume to be unregistered")
		gomega.Expect(waitForCNSUnRegisterVolumeToGetUnregistered(ctx,
			restConfig, cnsUnRegisterVolume, poll,
			supervisorClusterOperationsTimeout)).To(gomega.HaveOccurred())
	})

	/*
		Test 3:
		Export RWX PVC attached to multiple Pods in TKG

		Running test in TKG Context

		Create SPBM Policy and Assign to TKG Namespace:
		Create a SPBM policy and assign it to the TKG namespace with sufficient quota in the TKG context.

		Create PVC:

		Create a PersistentVolumeClaim (PVC) named pvc1 in the test namespace in the TKG context.
		Verify CNS Metadata

		Create multiple (two) pods attaching it to same PVC:
		Create a multiple Pods in the test namespace using pvc1.
		Wait for pods are up and running.

		Create and Apply CnsUnregisterVolume CR (SVC Context):
		Create a CnsUnregisterVolume Custom Resource (CR) with the volumeID of pvc1 and
		apply it to the test namespace in the Supervisor cluster (SVC context).

		Check CnsUnregisterVolume Status (SVC Context):
		Check the CnsUnregisterVolume CR and verify that the status.Unregistered field is set to false (SVC context).

		Check PV/PVC Status (SVC Context):
		Ensure that the PersistentVolume (PV) and PVC are not deleted from the Supervisor cluster (SVC context).

		Delete Pod1 and Wait:
		Delete pod1 and wait until the pod is fully deleted.

		Reapply CnsUnregisterVolume CR (SVC Context):
		Create and apply a new CnsUnregisterVolume CR with the volumeID of pvc1 to the
		test namespace in the Supervisor cluster (SVC context).

		Check CnsUnregisterVolume Status (SVC Context):
		Check the CnsUnregisterVolume CR and verify that the status.Unregistered field is set to false (SVC context).

		Check PV/PVC Status (SVC Context):
		Ensure that the PersistentVolume (PV) and PVC are not deleted from the Supervisor cluster (SVC context).

		Delete pods and Wait:
		Delete pods and wait until the pod is fully deleted.

		Reapply CnsUnregisterVolume CR (SVC Context):
		Create and apply a new CnsUnregisterVolume CR with the volumeID of pvc1 to the
		test namespace in the Supervisor cluster (SVC context).

		Check Updated CnsUnregisterVolume Status (SVC Context):

		Verify that the status.Unregistered field in the CnsUnregisterVolume CR is set to false (SVC context).

		Check PV/PVC Deletion (SVC Context):
		Confirm that the PV and PVC are deleted from the Supervisor cluster (SVC context).
		Verify CNS Metadata

		Verify that the File Share associated with pvc1 is still available.
		Cleanup:

		Delete the File Share, the test namespace, and the SPBM policy.
	*/
	ginkgo.It("[csi-unregister-volume] Export RWX PVC attached to multiple Pods in TKG", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		volumeID := getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
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
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+volHandle))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod2 to attach PV to the node")
		pod2, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod.Spec.NodeName+"-"+volHandle))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod.Spec.NodeName+"-"+volHandle, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod2.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod2.Spec.NodeName+"-"+volHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		ginkgo.By("Verify the volume is accessible and Read/write is possible from pod2")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2' > /mnt/volume1/Pod1.html"}
		e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd2...)
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2")).NotTo(gomega.BeFalse())

		// Get a config to talk to the apiserver
		restConfig := getRestConfigClient()

		svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)

		ginkgo.By("Create CNS unregister volume with above created FCD " + volumeID)

		cnsUnRegisterVolume := getCNSUnregisterVolumeSpec(svNamespace, volumeID)

		cnsRegisterVolumeName := cnsUnRegisterVolume.GetName()
		framework.Logf("CNS unregister volume name : %s", cnsRegisterVolumeName)

		err = createCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete CNS unregister volume CR by name " + cnsRegisterVolumeName)
			err = deleteCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for  CNS unregister volume to be unregistered")
		gomega.Expect(waitForCNSUnRegisterVolumeToGetUnregistered(ctx,
			restConfig, cnsUnRegisterVolume, poll,
			supervisorClusterOperationsTimeout)).To(gomega.HaveOccurred())
	})
})
