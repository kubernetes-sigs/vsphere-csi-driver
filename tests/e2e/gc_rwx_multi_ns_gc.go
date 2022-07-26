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

var _ = ginkgo.Describe("[rwm-csi-tkg] Volume Provision Across Namespace", func() {
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
		Verify static volume provisioning works across Namespace in the same GC
		1. Create a SC
		2. Create a PVC with "ReadWriteMany" access mode in namespace-1 using SC
		3. Wait for PVC to be in the Bound phase
		4. Verify CnsVolumeMetadata CRD is created
		5. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		6. Verify health status of PVC
		7. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Change reclaimPolicy of PV to Retain
		9. Delete PVC from namespace-1
		10. Verify CnsVolumeMetadata CRD is deleted
		11. Verify PVC name is removed from volume entry on CNS
		12. Create PV2 with "ReadWriteMany" access mode statically backed by the same VolumeID (file share) from PV1
		13. Create PVC2 with "ReadWriteMany" access mode which gets bounds to PV2 in namespace-2
		14. Wait for PVC to be in the Bound phase
		15. Verify CnsVolumeMetadata CRD is created
		16. Verify if the mapping PVC is bound in the SV cluster using the volume handler
		17. Verify health status of PVC
		18. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		19. Create a Pod1 using PVC2 created above at a mount path specified in PodSpec
		20. Verify CnsFileAccessConfig CRD is created
		21. Verify Pod1 is in the Running phase
		22. Verify ACL net permission set by calling CNSQuery for the file volume
		23. Create a file (file1.txt) at the mount path. Check if the creation is successful
		24. Delete Pod1
		25. Verify CnsFileAccessConfig CRD is deleted
		26. Verify if the Pod is successfully deleted
		27. Delete PVC in GC
		28. Verify if PVC is deleted in GC
		29. Delete PVs
		30. Verify if PVs are deleted in the SV cluster and GC
		31. Verify CnsVolumeMetadata CRD is deleted
		32. Check if the VolumeID is deleted from CNS by using CNSQuery API
	*/
	ginkgo.It("[rwm-csi-tkg] Verify static volume provisioning works across Namespace in the same GC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")
		ginkgo.By("Creating second namespace on GC")
		ns, err := framework.CreateTestingNS(f.BaseName, client, map[string]string{
			"e2e-framework": f.BaseName,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on second GC")

		newnamespace := ns.Name
		framework.Logf("Created second namespace on GC %v", newnamespace)
		defer func() {
			err := client.CoreV1().Namespaces().Delete(ctx, newnamespace, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Creating a PVC")
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pvcUID := string(pvclaim.GetUID())
		framework.Logf("PVC UUID in GC " + pvcUID)

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

		defer func() {
			if pv != nil {
				ginkgo.By("Deleting the PV1")
				err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

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

		// Creating label for PV
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)

		ginkgo.By("Creating the PV in guest cluster")
		pv2 := getPersistentVolumeSpecForRWX(pvcNameInSV, v1.PersistentVolumeReclaimDelete,
			staticPVLabels, diskSize, "", v1.ReadWriteMany)
		pv2, err = client.CoreV1().PersistentVolumes().Create(ctx, pv2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if pv2 != nil {
				ginkgo.By("Deleting the PV2")
				err = client.CoreV1().PersistentVolumes().Delete(ctx, pv2.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating the PVC in guest cluster's new namespace")
		pvc2 := getPersistentVolumeClaimSpecForRWX(newnamespace, staticPVLabels, pv2.Name, diskSize)
		pvc2, err = client.CoreV1().PersistentVolumeClaims(pvc2.Namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the PVC2")
			err = fpv.DeletePersistentVolumeClaim(client, pvc2.Name, pvc2.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			var svClient clientset.Interface
			if k8senvsv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senvsv != "" {
				svClient, err = createKubernetesClientFromConfig(k8senvsv)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			svcNamespace := os.Getenv("SVC_NAMESPACE")

			// Delete PVC in SVC.
			svcPVC, err := svClient.CoreV1().PersistentVolumeClaims(svcNamespace).Get(ctx, pvcNameInSV,
				metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Delete the PVC in SVC")
			err = fpv.DeletePersistentVolumeClaim(svClient, svcPVC.Name, svcPVC.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			//Add a check to validate CnsVolumeMetadata crd
			verifyCRDInSupervisorWithWait(ctx, f, pvcNameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)

			ginkgo.By("Deleting the PV1")
			err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pv = nil
		}()

		// Wait for PV and PVC to Bind
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client,
			framework.NewTimeoutContextWithDefaults(), pvc2.Namespace, pv2, pvc2))

		// Create a Pod to use this PVC
		ginkgo.By("Creating pod2 to attach PV to the node")
		pod2, err := createPod(client, pvc2.Namespace, nil, []*v1.PersistentVolumeClaim{pvc2},
			false, execRWXCommandPod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodNameRunningInNamespace(client, pod2.Name, pod2.Namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, pod2.Namespace))
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

		framework.Logf("PVC name in SV " + pvcNameInSV)

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
			crdCNSVolumeMetadatas, crdVersion, crdGroup, true, gcClusterID+pv2UID, false, nil, false)
		verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pod2UID, crdCNSVolumeMetadatas,
			crdVersion, crdGroup, true, pv2.Spec.CSI.VolumeHandle, false, nil, false)

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod2")
		verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod2.Spec.NodeName+"-"+pvcNameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, true)

		ginkgo.By("Verify the volume is accessible and Read/write is possible from pod2")
		cmd := []string{"exec", pod2.Name, "--namespace=" + pod2.Namespace, "--", "/bin/sh", "-c",
			"cat /mnt/volume1/Pod2.html "}

		writeCmd := []string{"exec", pod2.Name, "--namespace=" + pod2.Namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod2' > /mnt/volume1/Pod2.html"}
		framework.RunKubectlOrDie(pod2.Namespace, writeCmd...)
		output := framework.RunKubectlOrDie(pod2.Namespace, cmd...)
		gomega.Expect(strings.Contains(output, "Hello message from test into Pod2")).NotTo(gomega.BeFalse())
	})
})
