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
	"time"

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

var _ = ginkgo.Describe("File Volume Test with security context", func() {
	f := framework.NewDefaultFramework("rwx-tkg-security")
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
		Verify Pod with SecurityContext can be created with PVC
		1. Create a SC
		2. Create a PVC with "ReadWriteMany" using the SC from above in GC
		3. Wait for PVC to be Bound in GC
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata CRD is created
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Create Pod (Pod1) with security context as root using PVC created above at a mount path specified in PodSpec
		9. Verify CnsFileAccessConfig CRD is created
		10. Verify Pod1 is in the Running phase
		11. Verify ACL net permission set by calling CNSQuery for the file volume
		12. Create a file (file1.txt) at the mount path. Check if the creation is successful.
		    (This means that file1.txt has privileges of the root user)
		13. Create Pod2 with the security context of a normal user with PVC created above
		14. Verify CnsFileAccessConfig CRD is created
		15. Verify Pod2 is in the Running phase
		16. Verify ACL net permission set by calling CNSQuery for the file volume
		17. Read the file (file1.txt) from Pod2 created in Step 12. Check if the reading operation fails which is expected
		18. Create a new file (file2.txt) with some data at the mount path. Check if the creation is successful
		19. Read the file (file1.txt) from Pod1 and see the reading is successful
		20. Delete Pod1 & Pod2
		21. Verify CnsFileAccessConfig CRD are deleted
		22. Verify if all the Pods are successfully deleted
		23. Delete PVC
		24. Verify if PVCs and PVs are deleted in the SV cluster and GC
		25. Verify CnsVolumeMetadata CRD is deleted
		26. Verify volume is deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify Pod with SecurityContext can be created with PVC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var runAsUser int64
		var err error
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Creating a Storage class and PVC")
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

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, pv, nil)
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

		runAsUser = 1000

		// Create a Pod to use this PVC
		ginkgo.By("Creating pod2 to attach PV to the node")
		podCommand := "echo 'Hello message from Pod2' > /mnt/volume1/Pod2.html && while true ; do sleep 2 ; done"
		pod2Spec := GetPodSpecByUserID(namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, true,
			podCommand, runAsUser)

		pod2, err := client.CoreV1().Pods(namespace).Create(ctx, pod2Spec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig
			//CRD is deleted %s", pod2.Spec.NodeName+"-"+pvcNameInSV))
			// err = waitTillCNSFileAccesscrdDeleted(ctx, f,
			// pod2.Spec.NodeName+"-"+pvcNameInSV, crdCNSFileAccessConfig,
			// 	crdVersion, crdGroup, false)
			// gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvcNameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		err = fpod.WaitForPodNameRunningInNamespace(client, pod2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod2, err = client.CoreV1().Pods(pod2.Namespace).Get(ctx, pod2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is not accessible and Read/write is possible from pod2")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output = framework.RunKubectlOrDie(namespace, cmd2...)
		framework.Logf("Output from the command is %s", output)

		writeCmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		_, err = framework.RunKubectl(namespace, writeCmd2...)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(gomega.ContainSubstring("Permission denied"))

		framework.Logf("End of the test")
	})

	/*
		Verify Pod with restricted permission to use files on PVC
		1. Create a SC
		2. Create a PVC with "ReadWriteMany" using the SC from above in GC
		3. Wait for PVC to be Bound in GC
		4. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata crd
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS

		8. Create Pod (Pod1) using PVC created above at a mount path specified in PodSpec
		9. Verify CnsFileAccessConfig CRD is created
		10. Verify Pod1 is in the Running phase
		11. Verify ACL net permission set by calling CNSQuery for the file volume
		12. Create a file (file1.txt) at the mount path. And change the owner permission on the file.
			Check if the creation is successful.

		13. Create Pod2 with the same PVC created above
		14. Verify Pod2 is in the Running phase
		15. Verify CnsFileAccessConfig CRD is created
		16. Verify ACL net permission set by calling CNSQuery for the file volume
		17. Read the file (file1.txt) from Pod2 created in Step 12. Check if the reading operation
			fails which is expected
		18. Create a new file (file2.txt) with some data at the mount path.
			Check if the creation is successful
		19. Read the file (file1.txt) from Pod1 and see the reading is successful
		20. Delete Pod1 & Pod2
		21. Verify CnsFileAccessConfig CRD are deleted
		22. Verify if all the Pods are successfully deleted
		23. Delete PVC
		24. Verify if PVCs and PVs are deleted in the SV cluster and GC
		25. Verify CnsVolumeMetadata CRD is deleted
		26. Verify volumes are deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify Pod with restricted permission to use files on PVC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var err error
		var runAsUser int64
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Creating a Storage class and PVC")
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

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, pv, nil)
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

		ginkgo.By("Verify the volume is accessible and Read/write is possible")
		chmodCmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"chmod 444 /mnt/volume1/Pod1.html "}
		_, err = framework.RunKubectl(namespace, chmodCmd...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		writeCmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespace, writeCmd...)
		output = framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		// Create a Pod to use this PVC
		ginkgo.By("Creating pod2 to attach PV to the node")
		runAsUser = 2000

		pod2Command := "echo 'Hello message from Pod2' > /mnt/volume1/Pod2.html && while true ; do sleep 2 ; done"
		pod2Spec := GetPodSpecByUserID(namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, true,
			pod2Command, runAsUser)

		pod2, err := client.CoreV1().Pods(namespace).Create(ctx, pod2Spec, metav1.CreateOptions{})
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

		err = fpod.WaitForPodNameRunningInNamespace(client, pod2.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod2, err = client.CoreV1().Pods(pod2.Namespace).Get(ctx, pod2.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is not accessible and Read/write is possible from pod2")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"ls -lh /mnt/volume1/Pod1.html "}
		output = framework.RunKubectlOrDie(namespace, cmd2...)
		framework.Logf("Output for ls -lh command is : %s", output)

		ginkgo.By("Verify the volume is not accessible and Read/write is possible from pod2")
		cmd2 = []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output = framework.RunKubectlOrDie(namespace, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		writeCmd2 := []string{"exec", pod2.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod1' > /mnt/volume1/Pod1.html"}
		_, err = framework.RunKubectl(namespace, writeCmd2...)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(gomega.ContainSubstring("Permission denied"))
	})

	/*
		Exceed resource quota, while provisioning (1Ti Disk Size)
		1. Create a SC with a limit set
		2. Create a PVC with "ReadWriteMany" using the Storage Class in GC claiming disk size for 1Ti
		3. Verify for PVC bound fails in GC
		4. Verify the error message on the failure
		5. Delete PVC
	*/
	ginkgo.It("[rwm-csi-tkg] Exceed resource quota while provisioning file volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Creating a Storage class and PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, "1Ti", nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	/*
		Verify PVC creation without storage class
		1. Assign Overall Storage quota to namespace
		2. Create PVC with "ReadWriteMany" without using the SC from above in GC
		3. Wait for PVCs to be Bound in GC
		4. Verify if the mapping PVCs are bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata CRD is created
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Create Pod using PVC created above at a mount path specified in PodSpec
		9. Verify CnsFileAccessConfig CRD is created
		10. Verify the pod is in the Running phase
		11. Verify ACL net permission set by calling CNSQuery for the file volume
		12. Create a file1.txt at the mount path and try writing into the file.
			Check if the creation and write is successful for Pod
		13. Delete the Pd
		14. Verify CnsFileAccessConfig CRD is deleted
		15. Verify if the pod is successfully deleted
		16. Delete the PVC
		17. Verify if PVCs and PVs are deleted in the SV cluster and GC
		18. Verify CnsVolumeMetadata CRD is deleted
		19. Verify volumes are deleted on CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify PVC creation without storage class", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var err error
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")

		framework.Logf("Patch storage class as default SC")
		err = updateSCtoDefault(ctx, client, storagePolicyName, "true")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = updateSCtoDefault(ctx, client, storagePolicyName, "false")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Creating PVC without passing the storage class")
		pvcSpec := getPersistentVolumeClaimSpecWithoutStorageClass(namespace, diskSize, nil, v1.ReadWriteMany)
		pvclaim, err = fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, pv, nil)
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
	})

	/*
		Exceed resource quota, while provisioning (1Ti Disk Size) on default namespaces limit
		1. Assign Overall Storage quota to the namespace to 100Mi
		2. Create a PVC with "ReadWriteMany" without using the Storage Class in GC claiming disk size for 105 Mi
		3. Verify for PVC bound fails in GC
		4. Verify the error message on the failure
		5. Delete PVC
	*/
	ginkgo.It("[rwm-csi-tkg] Exceed resource quota on default SC while provisioning file volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvclaim *v1.PersistentVolumeClaim
		defaultDatastore = getDefaultDatastore(ctx)

		ginkgo.By("CNS_TEST: Running for GC setup")

		framework.Logf("Patch storage class as default SC")
		err := updateSCtoDefault(ctx, client, storagePolicyName, "true")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = updateSCtoDefault(ctx, client, storagePolicyName, "false")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Creating PVC without passing the storage class")
		pvcSpec := getPersistentVolumeClaimSpecWithoutStorageClass(namespace, "1Ti", nil, v1.ReadWriteMany)
		pvclaim, err = fpv.CreatePVC(client, namespace, pvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute/2)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})
})
