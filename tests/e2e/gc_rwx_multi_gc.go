/*
Copyright 2022 The Kubernetes Authors.

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

var _ = ginkgo.Describe("[rwm-csi-tkg] Volume Provision Across TKG clusters", func() {
	f := framework.NewDefaultFramework("rwx-multi-gc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		clientNewGc       clientset.Interface
		namespace         string
		namespaceNewGC    string
		scParameters      map[string]string
		storagePolicyName string
		volHealthCheck    bool
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		// TODO: Read value from command line
		volHealthCheck = false
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		svcClient, svNamespace := getSvcClientAndNamespace()
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
		Verify static provisioning across Guest Clusters within the same WCP namespace
		while the pods use the volumes at the same time
		From Guest cluster-1
		1. Create a Storage Class
		2. Create a PVC with "ReadWriteMany" access mode in GC using any replicated storage class from the SV
		3. Wait for PVC to be in the Bound phase
		4. Verify CnsVolumeMetadata CRD is created
		5. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Create a Pod1 using PVC created above at a mount path specified in PodSpec
		9. Verify CnsFileAccessConfig CRD is created
		10. Verify Pod1 is in the Running phase
		11. Create a file (file1.txt) at the mount path. Check if the creation is successful

		From a new guest cluster (Guest cluster2 )in the same wcp namespace, do the following:
		12. Create PV with VolumeHandle from supervisor cluster's PVC in the GC
		13. Verify CnsVolumeMetadata CRD is created
		14. Wait for PV entry to be present in CNS
		15. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		16. Verify health status of PVC
		17. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		18. Create a Pod2 using PVC created above at a mount path specified in PodSpec
		19. Verify CnsFileAccessConfig CRD is created
		20. Verify Pod2 is in the Running phase
		21. Verify ACL net permission set by calling CNSQuery for the file volume
		22. Create a file (file1.txt) at the mount path. Check if the creation is successful
		23. Delete Pod2
		24. Verify CnsFileAccessConfig CRD is deleted
		25. Verify if the Pod is successfully deleted
		26. Cleanup PV & PVC from Guest cluster1 and Guest Cluster2
		28. Delete PVC from GCs
		29. Verify if PVC is deleted in GCs
		30. Delete PVs in GCs and SVCs
		31. Verify if PVs are deleted in the GCs and SVCs
		32. Verify CnsVolumeMetadata CRD is deleted
		33. Check if the VolumeID is deleted from CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify static provisioning across Guest Clusters within the "+
		"same WCP namespace - part 1", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newGcKubconfigPath := os.Getenv("NEW_GUEST_CLUSTER_KUBE_CONFIG")
		if newGcKubconfigPath == "" {
			ginkgo.Skip("Env NEW_GUEST_CLUSTER_KUBE_CONFIG is missing")
		}

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

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil,
			[]*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod1)
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

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, pv, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		// Getting the client for the second GC
		clientNewGc, err = createKubernetesClientFromConfig(newGcKubconfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Error creating k8s client with %v: %v", newGcKubconfigPath, err))

		ginkgo.By("Creating namespace on second GC")
		ns, err := framework.CreateTestingNS(f.BaseName, clientNewGc, map[string]string{
			"e2e-framework": f.BaseName,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on second GC")

		namespaceNewGC = ns.Name
		framework.Logf("Created namespace on second GC %v", namespaceNewGC)
		defer func() {
			err := clientNewGc.CoreV1().Namespaces().Delete(ctx, namespaceNewGC, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating label for PV
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)

		ginkgo.By("Creating the PV in Second guest cluster")
		pv2 := getPersistentVolumeSpecForRWX(pvcNameInSV, v1.PersistentVolumeReclaimDelete,
			staticPVLabels, "2Gi", "", v1.ReadWriteMany)
		pv2, err = clientNewGc.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PV2")
			err = clientNewGc.CoreV1().PersistentVolumes().Delete(ctx, pv2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating the PVC in second guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespaceNewGC, staticPVLabels, pv2.Name, "2Gi")
		pvc2, err = clientNewGc.CoreV1().PersistentVolumeClaims(namespaceNewGC).Create(ctx,
			pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PVC2")
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespaceNewGC)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(clientNewGc,
			framework.NewTimeoutContextWithDefaults(), namespaceNewGC, pv2, pvc2))

		pvc2NameInSV := pv2.Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcd2IDInCNS := getVolumeIDFromSupervisorCluster(pvc2NameInSV)
		gomega.Expect(fcd2IDInCNS).NotTo(gomega.BeEmpty())

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, clientNewGc, pvc2, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod2 to attach PV2 to the node")
		pod2, err := createPod(clientNewGc, namespaceNewGC, nil,
			[]*v1.PersistentVolumeClaim{pvc2}, false, execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespaceNewGC))
			err = fpod.DeletePodWithWait(clientNewGc, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod2.Spec.NodeName+"-"+pvc2NameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod2.Spec.NodeName+"-"+pvc2NameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvc2NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvc2NameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		oldKubeConfig := framework.TestContext.KubeConfig
		framework.TestContext.KubeConfig = newGcKubconfigPath
		defer func() {
			framework.TestContext.KubeConfig = oldKubeConfig
		}()

		ginkgo.By("Verify the volume is accessible and Read/write is possible from pod2")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespaceNewGC, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod1.html "}
		output = framework.RunKubectlOrDie(namespaceNewGC, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod1")).NotTo(gomega.BeFalse())

		wrtiecmd2 := []string{"exec", pod2.Name, "--namespace=" + namespaceNewGC, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2' > /mnt/volume1/Pod1.html"}
		framework.RunKubectlOrDie(namespaceNewGC, wrtiecmd2...)
		output = framework.RunKubectlOrDie(namespaceNewGC, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2")).NotTo(gomega.BeFalse())
	})

	/*
		Verify static provisioning across Guest Clusters within the same WCP namespace
		while the pods use volumes one after the other
		From Guest cluster-1
		1. Create a SC
		2. Create a PVC with "ReadWriteMany" access mode in GC using any replicated storage class from the SV
		3. Wait for PVC to be in the Bound phase
		4. Verify CnsVolumeMetadata CRD is created
		5. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS

		8. Change reclaimPolicy of PV to Retain in GC
		9. Delete PVC in GC
		10. Delete PV in GC
		11. Verify CnsVolumeMetadata CRD is deleted
		12. Verify if PVs are deleted in the GC & retained in the SVC

		From a new guest cluster in the same wcp namespace, do the following:

		13. Create PV2 with "ReadWriteMany" access mode statically backed
			by the same VolumeID (file share) from PV1
		14. Create a PVC-2 with "ReadWriteMany"  access mode in GC using the above PV-2
		15. Verify the PVC-2 is bound in GC
		16. Verify CnsVolumeMetadata CRD is created
		17. Wait for PV entry to be present in CNS
		18. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		19. Verify health status of PVC
		20. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		21. Create a Pod1 using PVC created above at a mount path specified in PodSpec
		22. Verify CnsFileAccessConfig CRD is created
		23. Verify Pod1 is in the Running phase
		24. Create a file (file1.txt) at the mount path. Check if the creation is successful
		25. Delete Pod1
		26. Verify CnsFileAccessConfig CRD is deleted
		27. Verify if the Pod is successfully deleted
		28. Delete PVC from GC
		29. Verify if PVC is deleted in GC
		30. Delete PVs in GC and SVC
		31. Verify if PVs are deleted in the GC and SVC
		32. Verify CnsVolumeMetadata CRD is deleted
		33. Check if the VolumeID is deleted from CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify static provisioning across Guest Clusters within the "+
		"same WCP namespace - part 2", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newGcKubconfigPath := os.Getenv("NEW_GUEST_CLUSTER_KUBE_CONFIG")
		if newGcKubconfigPath == "" {
			ginkgo.Skip("Env NEW_GUEST_CLUSTER_KUBE_CONFIG is missing")
		}

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

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, pv, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the PVC")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil

		ginkgo.By("Verifying if volume still exists in the Supervisor Cluster")
		fcdIDInCNS = getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		// Getting the client for the second GC
		clientNewGc, err = createKubernetesClientFromConfig(newGcKubconfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Error creating k8s client with %v: %v", newGcKubconfigPath, err))

		ginkgo.By("Creating namespace on second GC")
		ns, err := framework.CreateTestingNS(f.BaseName, clientNewGc, map[string]string{
			"e2e-framework": f.BaseName,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on second GC")

		namespaceNewGC = ns.Name
		framework.Logf("Created namespace on second GC %v", namespaceNewGC)
		defer func() {
			err := clientNewGc.CoreV1().Namespaces().Delete(ctx, namespaceNewGC, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating label for PV
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)

		ginkgo.By("Creating the PV in Second guest cluster")
		pv2 := getPersistentVolumeSpecForRWX(pvcNameInSV, v1.PersistentVolumeReclaimDelete,
			staticPVLabels, "2Gi", "", v1.ReadWriteMany)
		pv2, err = clientNewGc.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PV1")
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Deleting the PV2")
			err = clientNewGc.CoreV1().PersistentVolumes().Delete(ctx, pv2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating the PVC in second guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespaceNewGC, staticPVLabels, pv2.Name, "2Gi")
		pvc2, err = clientNewGc.CoreV1().PersistentVolumeClaims(namespaceNewGC).Create(ctx,
			pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PVC2")
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespaceNewGC)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(clientNewGc,
			framework.NewTimeoutContextWithDefaults(), namespaceNewGC, pv2, pvc2))

		pvc2NameInSV := pv2.Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcd2IDInCNS := getVolumeIDFromSupervisorCluster(pvc2NameInSV)
		gomega.Expect(fcd2IDInCNS).NotTo(gomega.BeEmpty())

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, clientNewGc, pvc2, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod2 to attach PV2 to the node")
		pod2, err := createPod(clientNewGc, namespaceNewGC, nil,
			[]*v1.PersistentVolumeClaim{pvc2}, false, execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespaceNewGC))
			err = fpod.DeletePodWithWait(clientNewGc, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s",
				pod2.Spec.NodeName+"-"+pvc2NameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod2.Spec.NodeName+"-"+pvc2NameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvc2NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvc2NameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		oldKubeConfig := framework.TestContext.KubeConfig
		framework.TestContext.KubeConfig = newGcKubconfigPath
		defer func() {
			framework.TestContext.KubeConfig = oldKubeConfig
		}()

		ginkgo.By("Verify the volume is accessible and Read/write is possible from pod2")
		cmd2 := []string{"exec", pod2.Name, "--namespace=" + namespaceNewGC, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod2.html "}
		output := framework.RunKubectlOrDie(namespaceNewGC, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from Pod2")).NotTo(gomega.BeFalse())

		wrtiecmd2 := []string{"exec", pod2.Name, "--namespace=" + namespaceNewGC, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2' > /mnt/volume1/Pod2.html"}
		framework.RunKubectlOrDie(namespaceNewGC, wrtiecmd2...)
		output = framework.RunKubectlOrDie(namespaceNewGC, cmd2...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2")).NotTo(gomega.BeFalse())
	})

	/*
		Verify static provisioning across Guest Clusters across WCP namespace
		while the pods use the volumes at the same time
		From Guest cluster-1
		1. Create a SC
		2. Create a PVC with "ReadWriteMany" access mode in GC using any replicated storage class from the SV
		3. Wait for PVC to be in the Bound phase
		4. Verify CnsVolumeMetadata CRD is created
		5. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS

		8. Change reclaimPolicy of PV to Retain in GC
		9. Delete PVC in GC
		10. Verify CnsVolumeMetadata CRD is deleted
		11. Verify if PVs are deleted in the GC & retained in the SVC

		From a new guest cluster in the different wcp namespace, do the following:
		12. Create PV with VolumeHandle from supervisor cluster's PVC in the GC
		13. Create PVC2 with "ReadWriteMany" access mode which gets bounds to PV-2 (reclaim policy=delete)
		14. Verify CnsVolumeMetadata CRD is created
		15. Verify health status of PVC
		16. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		17. Create Pod using PVC-2 created above
		18. Verify Pod creation fails
		19. Verify the error message returned on failure is correct
		20. Delete PVC and PVs from GC
		21. Verify if PVC and PVs are deleted in the SV cluster and GC
		22. Verify CnsVolumeMetadata CRD is deleted
		23. Check if the VolumeID is deleted from CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify static provisioning across Guest Clusters within the "+
		"same WCP namespace - part 3", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newGcKubconfigPath := os.Getenv("SECOND_GUEST_CLUSTER_KUBE_CONFIG")
		if newGcKubconfigPath == "" {
			ginkgo.Skip("Env SECOND_GUEST_CLUSTER_KUBE_CONFIG is missing")
		}

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

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(fcdIDInCNS, pvcNameInSV, pvclaim, pv, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the PVC")
		err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil

		ginkgo.By("Verifying if volume still exists in the Supervisor Cluster")
		fcdIDInCNS = getVolumeIDFromSupervisorCluster(pvcNameInSV)
		gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

		// Getting the client for the second GC
		clientNewGc, err = createKubernetesClientFromConfig(newGcKubconfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Error creating k8s client with %v: %v", newGcKubconfigPath, err))

		ginkgo.By("Creating namespace on second GC")
		ns, err := framework.CreateTestingNS(f.BaseName, clientNewGc, map[string]string{
			"e2e-framework": f.BaseName,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on second GC")

		namespaceNewGC = ns.Name
		framework.Logf("Created namespace on second GC %v", namespaceNewGC)
		defer func() {
			err := clientNewGc.CoreV1().Namespaces().Delete(ctx, namespaceNewGC, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating label for PV
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)

		ginkgo.By("Creating the PV in Second guest cluster")
		pv2 := getPersistentVolumeSpecForRWX(pvcNameInSV, v1.PersistentVolumeReclaimDelete,
			staticPVLabels, "2Gi", "", v1.ReadWriteMany)
		pv2, err = clientNewGc.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PV1")
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Deleting the PV2")
			err = clientNewGc.CoreV1().PersistentVolumes().Delete(ctx, pv2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating the PVC in second guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespaceNewGC, staticPVLabels, pv2.Name, "2Gi")
		pvc2, err = clientNewGc.CoreV1().PersistentVolumeClaims(namespaceNewGC).Create(ctx,
			pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PVC2")
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, namespaceNewGC)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(clientNewGc,
			framework.NewTimeoutContextWithDefaults(), namespaceNewGC, pv2, pvc2))

		pvc2NameInSV := pv2.Spec.CSI.VolumeHandle
		gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
		fcd2IDInCNS := getVolumeIDFromSupervisorCluster(pvc2NameInSV)
		gomega.Expect(fcd2IDInCNS).NotTo(gomega.BeEmpty())

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, clientNewGc, pvc2, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod2 to attach PV2 to the node")
		pod2, err := createPod(clientNewGc, namespaceNewGC, nil,
			[]*v1.PersistentVolumeClaim{pvc2}, false, execRWXCommandPod2)
		gomega.Expect(err).To(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespaceNewGC))
			err = fpod.DeletePodWithWait(clientNewGc, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Wait till the CnsFileAccessConfig CRD is deleted %s", pod2.Spec.NodeName+"-"+pvc2NameInSV))
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, pod2.Spec.NodeName+"-"+pvc2NameInSV, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvc2NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		}()
	})
})
