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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwm-csi-tkg] File Volume Provision with Deployments", func() {
	f := framework.NewDefaultFramework("rwx-tkg-deployment")
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
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
	})

	/*
		Test to verify file volume provision with DeploymentSets

		1. Create a SC
		2. Create two PVCs, PVC1 and PVC2 with "ReadWriteMany" access mode using the SC
		3. Wait for PVCs to be Bound in GC
		4. Verify if the mapping PVCs are also bound in the SV cluster using the volume handler
		5. Verify CnsVolumeMetadata CRD are created
		6. Verify health status of PVCs
		7. Verify volumes are created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		8. Create Deployment type application using the PVCs created above
		9. Create Deployment type with replica count as 3 using the Storage Policy obtained in Step 1
		10. Wait until all Pods are ready
		11. Verify CnsFileAccessConfig CRD is created
		12. Scale down the replica count 2
		13. Scale-up replica count 5
		14. Scale down to 0 replicas and delete all pods
		15. Delete the Deployment app
		16. Verify CnsFileAccessConfig CRD are deleted
		17. Verify if all the pods are successfully deleted
		18. Verify using CNS Query API if all 2 PV's still exists
		19. Delete PVCs
		20. Verify if PVCs and PVs are deleted in the SV cluster and GC
		21. Verify CnsVolumeMetadata CRD are deleted
		22. Check if the VolumeID is deleted from CNS by using CNSQuery API
		23. Cleanup SC
	*/
	ginkgo.It("Verify RWX volumes with Deployment", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var missingPod *v1.Pod
		ignoreLabels := make(map[string]string)

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

		ginkgo.By("Creating the PVC2 in guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespace, nil, "", diskSize)
		pvc2.Spec.AccessModes[0] = v1.ReadWriteMany
		pvc2.Spec.StorageClassName = &storageclasspvc.Name

		pvc2, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim, pvc2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvc1NameInSV := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(pvc1NameInSV).NotTo(gomega.BeEmpty())

		pvc2NameInSV := persistentvolumes[1].Spec.CSI.VolumeHandle
		gomega.Expect(pvc2NameInSV).NotTo(gomega.BeEmpty())

		fcd1IDInCNS := getVolumeIDFromSupervisorCluster(pvc1NameInSV)
		gomega.Expect(fcd1IDInCNS).NotTo(gomega.BeEmpty())

		fcd2IDInCNS := getVolumeIDFromSupervisorCluster(pvc2NameInSV)
		gomega.Expect(fcd2IDInCNS).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcd1IDInCNS)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc2.Name, pvc2.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcd2IDInCNS)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			//Add a check to validate CnsVolumeMetadata crd
			verifyCRDInSupervisorWithWait(ctx, f, pvc1NameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
			verifyCRDInSupervisorWithWait(ctx, f, pvc2NameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", fcd1IDInCNS))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcd1IDInCNS)
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

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", fcd2IDInCNS))
		queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(fcd2IDInCNS)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult2.Volumes).ShouldNot(gomega.BeEmpty())
		framework.Logf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
			queryResult2.Volumes[0].Name,
			queryResult2.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult2.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
			queryResult2.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints)

		ginkgo.By("Verifying volume type specified in PVC is honored")
		gomega.Expect(queryResult2.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(),
			"Volume type is not FILE")
		ginkgo.By("Verifying volume size is honored")
		gomega.Expect(queryResult2.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
			CapacityInMb == newSizeInMb).To(gomega.BeTrue(), "Volume Capaticy is not matching")

		if volHealthCheck {
			ginkgo.By("poll for health status annotation")
			err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = pvcHealthAnnotationWatcher(ctx, client, pvc2, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		//Add a check to validate CnsVolumeMetadata crd
		err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, fcd1IDInCNS, pvc1NameInSV, pvclaim, persistentvolumes[0], nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, fcd2IDInCNS, pvc2NameInSV, pvc2, persistentvolumes[1], nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		ginkgo.By("Creating a Deployment using pvc1 & pvc2")

		dep, err := createDeployment(ctx, client, 2, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim, pvc2}, "", false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		pods, err := fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, ddpod := range pods.Items {
			framework.Logf("Parsing the Pod %s", ddpod.Name)
			_, err := client.CoreV1().Pods(namespace).Get(ctx, ddpod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = fpod.WaitForPodNameRunningInNamespace(ctx, client, ddpod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod with pvc1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, ddpod.Spec.NodeName+"-"+pvc1NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, true)

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod with pvc2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, ddpod.Spec.NodeName+"-"+pvc2NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, true)
		}

		ginkgo.By("Scale down deployment to 1 replica")
		dep, err = client.AppsV1().Deployments(namespace).Get(ctx, dep.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		rep := dep.Spec.Replicas
		*rep = 1
		dep.Spec.Replicas = rep
		dep, err = client.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(sleepTimeOut * time.Second)

		_, err = fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		list_of_pods, err := fpod.GetPodsInNamespace(ctx, client, namespace, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if list_of_pods[0].Name == pods.Items[0].Name {
			missingPod = &pods.Items[1]
		} else {
			missingPod = &pods.Items[0]
		}

		framework.Logf("Missing Pod name is %s", missingPod.Name)

		ginkgo.By("Verifying whether Pod is Deleted or not")
		err = fpod.WaitForPodNotFoundInNamespace(ctx, client, missingPod.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod with pvc1")
		framework.Logf("Looking for the CRD " + missingPod.Spec.NodeName + "-" + pvc1NameInSV)
		err = waitTillCNSFileAccesscrdDeleted(ctx, f, missingPod.Spec.NodeName+"-"+pvc1NameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for Pod with pvc2")
		err = waitTillCNSFileAccesscrdDeleted(ctx, f, missingPod.Spec.NodeName+"-"+pvc2NameInSV,
			crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up deployment to 5 replica")
		dep, err = client.AppsV1().Deployments(namespace).Get(ctx, dep.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		*rep = 5
		dep.Spec.Replicas = rep
		dep, err = client.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(sleepTimeOut * time.Second)

		err = fpod.WaitForPodsRunningReady(ctx, client, namespace, int32(5), 0, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err = fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, ddpod := range pods.Items {
			framework.Logf("Parsing the Pod %s", ddpod.Name)
			_, err := client.CoreV1().Pods(namespace).Get(ctx, ddpod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = fpod.WaitForPodNameRunningInNamespace(ctx, client, ddpod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod with pvc1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, ddpod.Spec.NodeName+"-"+pvc1NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, true)

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod with pvc2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, ddpod.Spec.NodeName+"-"+pvc2NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, true)
		}

		ginkgo.By("Scale down deployment to 0 replica")
		dep, err = client.AppsV1().Deployments(namespace).Get(ctx, dep.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pods, err = fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		*rep = 0
		dep.Spec.Replicas = rep
		dep, err = client.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, ddpod := range pods.Items {
			err = fpod.WaitForPodNotFoundInNamespace(ctx, client, ddpod.Name, namespace, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not")
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, ddpod.Spec.NodeName+"-"+pvc1NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not")
			err = waitTillCNSFileAccesscrdDeleted(ctx, f, ddpod.Spec.NodeName+"-"+pvc2NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})
})
