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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwm-csi-destructive-tkg] Statefulsets with File Volumes and Delete Guest Cluster", func() {
	f := framework.NewDefaultFramework("rwx-delete-tkg")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		scParameters        map[string]string
		storagePolicyName   string
		volHealthCheck      bool
		isSTSDeleted        bool
		isServiceDeleted    bool
		isTKGDeleted        bool
		missingPodAndVolume map[string]string
		labels_ns           map[string]string
	)

	ginkgo.BeforeEach(func() {
		// TODO: Read value from command line
		volHealthCheck = false
		isSTSDeleted = false
		isServiceDeleted = false
		isTKGDeleted = false
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
		labels_ns = map[string]string{}
		labels_ns[admissionapi.EnforceLevelLabel] = string(admissionapi.LevelPrivileged)
		labels_ns["e2e-framework"] = f.BaseName
	})

	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
	})

	/*
		Test to verify the volumes are deleted from SV upon TKG deletion with statefulsets
		   1. Create a Storage Class
		   2. Create Nginx service
		   3. Create Nginx statefulset with 3 replicas and podManagementPolicy=OrderedReady
		   4. Wait until all Pods are ready and PVCs are bounded with PV
		   5. Verify CnsVolumeMetadata CRD are created
		   6. Verify CnsFileAccessConfig CRD are created
		   7. Verify health status of PVC
		   8. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		   9. Scale down statefulsets to 2 replicas
		   10. Scale-up statefulset to 5 replicas
		   11. Verify using CNS Query API if all 5 PV's still exists
		   12. Delete the Guest cluster (Ungraceful delete)
		   13. Verify if PVCs and PVs are deleted in the SV cluster
		   14. Verify CnsVolumeMetadata CRD are deleted
		   15. Check if the VolumeID is deleted from CNS by using CNSQuery API

		   	Test to verify the volumes are deleted from SV upon TKG deletion with deploymentsets
			1. Create a Storage Class
			2. Create two PVCs, PVC1 and PVC2 with "ReadWriteMany" access mode using the SC from above in GC
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
			14. Delete the Guest cluster (Ungraceful delete)
			15. Verify if PVCs and PVs are deleted in the SV cluster
			16. Verify CnsVolumeMetadata CRD are deleted
	*/
	ginkgo.It("TKG Destructive Test", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		var wcpToken string
		var missingPod *v1.Pod
		missingPodAndVolume = make(map[string]string)
		ginkgo.By("CNS_TEST: Running for GC setup")

		newGcKubconfigPath := os.Getenv("DELETE_GUEST_CLUSTER_KUBE_CONFIG")
		if newGcKubconfigPath == "" {
			ginkgo.Skip("Env DELETE_GUEST_CLUSTER_KUBE_CONFIG is missing")
		}

		tkg_cluster := os.Getenv("TKG_CLUSTER_TO_DELETE")
		if tkg_cluster == "" {
			ginkgo.Skip("Env TKG_CLUSTER_TO_DELETE is missing")
		}

		clientNewGc, err := createKubernetesClientFromConfig(newGcKubconfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Error creating k8s client with %v: %v", newGcKubconfigPath, err))

		ginkgo.By("Creating namespace on Deleting TKG")
		namespaceObj, err := framework.CreateTestingNS(ctx, f.BaseName, clientNewGc, labels_ns)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on Deleting TKG")
		namespace := namespaceObj.Name

		defer func() {
			if !isTKGDeleted {
				err := clientNewGc.CoreV1().Namespaces().Delete(ctx, namespace, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating StorageClass for Statefulset")
		scParameters[svStorageClassName] = storagePolicyName
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err := clientNewGc.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !isTKGDeleted {
				err := clientNewGc.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, clientNewGc)

		defer func() {
			if !isTKGDeleted && !isServiceDeleted {
				deleteService(namespace, clientNewGc, service)
			}
		}()

		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		scName := defaultNginxStorageClassName
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.AccessModes[0] =
			v1.ReadWriteMany
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &scName
		*statefulset.Spec.Replicas = 2
		CreateStatefulSet(namespace, statefulset, clientNewGc)
		replicas := *(statefulset.Spec.Replicas)

		defer func() {
			if !isTKGDeleted && !isSTSDeleted {
				ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
				fss.DeleteAllStatefulSets(ctx, clientNewGc, namespace)
			}
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(ctx, clientNewGc, statefulset, replicas)

		ssPodsBeforeScaledown := fss.GetPodList(ctx, clientNewGc, statefulset)
		gomega.Expect(ssPodsBeforeScaledown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaledown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Get the list of Volumes attached to Pods before scale down.
		var volumesBeforeScaleDown []string
		for _, sspod := range ssPodsBeforeScaledown.Items {
			tempPod, err := clientNewGc.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					persistentvolume := getPvFromClaim(clientNewGc, statefulset.Namespace,
						volumespec.PersistentVolumeClaim.ClaimName)
					pvclaim, err := clientNewGc.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
						volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(pvclaim).NotTo(gomega.BeNil())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pvcNameInSV := persistentvolume.Spec.CSI.VolumeHandle
					gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
					fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
					gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())
					missingPodAndVolume[tempPod.Name] = pvcNameInSV
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, pvcNameInSV)
					if volHealthCheck {
						ginkgo.By("poll for health status annotation")
						err = pvcHealthAnnotationWatcher(ctx, clientNewGc, pvclaim, healthStatusAccessible)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					//Add a check to validate CnsVolumeMetadata crd
					err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, fcdIDInCNS, pvcNameInSV, pvclaim,
						persistentvolume, tempPod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod")
					verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+pvcNameInSV,
						crdCNSFileAccessConfig, crdVersion, crdGroup, true)

					// Verify using CNS Query API if VolumeID retrieved from PV is present.
					framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", fcdIDInCNS)
					queryResult, err := e2eVSphere.queryCNSVolumeWithResult(fcdIDInCNS)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
					framework.Logf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
						queryResult.Volumes[0].Name,
						queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
							CapacityInMb,
						queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
						queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
							AccessPoints)
					ginkgo.By("Verifying volume type specified in PVC is honored")
					gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(),
						"Volume type is not FILE")
					ginkgo.By("Verifying volume size is honored")
					newSizeInMb := int64(1024)
					gomega.Expect(queryResult.Volumes[0].
						BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
						CapacityInMb == newSizeInMb).
						To(gomega.BeTrue(), "Volume Capaticy is not matching")
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas-1))
		_, scaledownErr := fss.Scale(ctx, clientNewGc, statefulset, replicas-1)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(ctx, clientNewGc, statefulset, replicas-1)
		ssPodsAfterScaleDown := fss.GetPodList(ctx, clientNewGc, statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas-1)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		//Find the missing pod and check if the cnsvolumemetadata is deleted or not
		if ssPodsAfterScaleDown.Items[0].Name == ssPodsBeforeScaledown.Items[0].Name {
			missingPod = &ssPodsBeforeScaledown.Items[1]
		} else {
			missingPod = &ssPodsBeforeScaledown.Items[0]
		}

		missingVolumeHandle := missingPodAndVolume[missingPod.Name]
		ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is Deleted or not for missing Pod")
		err = waitTillCNSFileAccesscrdDeleted(ctx, f, missingPod.Spec.NodeName+"-"+missingVolumeHandle,
			crdCNSFileAccessConfig, crdVersion, crdGroup, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Add a check to validate CnsVolumeMetadata crd
		verifyCRDInSupervisorWithWait(ctx, f, missingVolumeHandle, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)

		// After scale down, verify the attached volumes match those in CNS Cache
		for _, sspod := range ssPodsAfterScaleDown.Items {
			_, err := clientNewGc.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					persistentvolume := getPvFromClaim(clientNewGc, statefulset.Namespace,
						volumespec.PersistentVolumeClaim.ClaimName)
					pvcNameInSV := persistentvolume.Spec.CSI.VolumeHandle
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
						queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
							CapacityInMb,
						queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
						queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
							AccessPoints)
					ginkgo.By("Verifying volume type specified in PVC is honored")
					gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(),
						"Volume type is not FILE")
					ginkgo.By("Verifying volume size is honored")
					newSizeInMb := int64(1024)
					gomega.Expect(queryResult.Volumes[0].
						BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
						CapacityInMb == newSizeInMb).To(gomega.BeTrue(), "Volume Capaticy is not matching")
				}
			}
		}

		replicas += 2
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := fss.Scale(ctx, clientNewGc, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, clientNewGc, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(ctx, clientNewGc, statefulset, replicas)
		ssPodsAfterScaleUp := fss.GetPodList(ctx, clientNewGc, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitTimeoutForPodReadyInNamespace(ctx, clientNewGc, sspod.Name, statefulset.Namespace, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := clientNewGc.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					persistentvolume := getPvFromClaim(clientNewGc, statefulset.Namespace,
						volumespec.PersistentVolumeClaim.ClaimName)
					pvcNameInSV := persistentvolume.Spec.CSI.VolumeHandle
					gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
					fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
					gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())
					ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod")
					verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+pvcNameInSV,
						crdCNSFileAccessConfig, crdVersion, crdGroup, true)
				}
			}
		}

		//Test for the deployment
		ginkgo.By("Creating a PVC for Deployment test")
		storageclasspvc, pvclaim, err := createPVCAndStorageClass(ctx, clientNewGc,
			namespace, nil, scParameters, diskSize, nil, "", false, v1.ReadWriteMany)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !isTKGDeleted {
				err = clientNewGc.StorageV1().StorageClasses().Delete(ctx, storageclasspvc.Name,
					*metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating the PVC2 in guest cluster")
		pvc2 := getPersistentVolumeClaimSpecForRWX(namespace, nil, "", diskSize)
		pvc2.Spec.AccessModes[0] = v1.ReadWriteMany
		pvc2.Spec.StorageClassName = &storageclasspvc.Name

		pvc2, err = clientNewGc.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, clientNewGc,
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
			if !isTKGDeleted {
				err = fpv.DeletePersistentVolumeClaim(ctx, clientNewGc, pvclaim.Name, pvclaim.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcd1IDInCNS)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				err = fpv.DeletePersistentVolumeClaim(ctx, clientNewGc, pvc2.Name, pvc2.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(fcd2IDInCNS)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				//Add a check to validate CnsVolumeMetadata crd
				verifyCRDInSupervisorWithWait(ctx, f, pvc1NameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
				verifyCRDInSupervisorWithWait(ctx, f, pvc2NameInSV, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
			}
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
			err = pvcHealthAnnotationWatcher(ctx, clientNewGc, pvclaim, healthStatusAccessible)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = pvcHealthAnnotationWatcher(ctx, clientNewGc, pvc2, healthStatusAccessible)
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

		dep, err := createDeployment(ctx, clientNewGc, 2, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim, pvc2}, "", false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !isTKGDeleted {
				framework.Logf("Delete deployment set")
				err := clientNewGc.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		pods, err := fdep.GetPodsForDeployment(ctx, clientNewGc, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, ddpod := range pods.Items {
			framework.Logf("Parsing the Pod %s", ddpod.Name)
			_, err := clientNewGc.CoreV1().Pods(namespace).Get(ctx, ddpod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = fpod.WaitForPodNameRunningInNamespace(ctx, clientNewGc, ddpod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod with pvc1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, ddpod.Spec.NodeName+"-"+pvc1NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, true)

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod with pvc2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, ddpod.Spec.NodeName+"-"+pvc2NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, true)
		}

		ginkgo.By("Scale down deployment to 1 replica")
		dep, err = clientNewGc.AppsV1().Deployments(namespace).Get(ctx, dep.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		rep := dep.Spec.Replicas
		*rep = 1
		dep.Spec.Replicas = rep
		ignoreLabels := make(map[string]string)
		dep, err = clientNewGc.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(sleepTimeOut * time.Second)

		_, err = fdep.GetPodsForDeployment(ctx, clientNewGc, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		list_of_pods, err := fpod.GetPodsInNamespace(ctx, clientNewGc, namespace, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Finding the missing Pod name")
		if list_of_pods[0].Name == pods.Items[0].Name {
			missingPod = &pods.Items[1]
		} else {
			missingPod = &pods.Items[0]
		}

		framework.Logf("Missing Pod name is %s", missingPod.Name)

		ginkgo.By("Verifying whether Pod is Deleted or not")
		err = fpod.WaitForPodNotFoundInNamespace(ctx, clientNewGc, missingPod.Name, namespace, pollTimeout)
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
		dep, err = clientNewGc.AppsV1().Deployments(namespace).Get(ctx, dep.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		*rep = 5
		dep.Spec.Replicas = rep
		dep, err = clientNewGc.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(sleepTimeOut * time.Second)

		err = fpod.WaitForPodsRunningReady(ctx, clientNewGc, namespace, int32(5), 0, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err = fdep.GetPodsForDeployment(ctx, clientNewGc, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, ddpod := range pods.Items {
			framework.Logf("Parsing the Pod %s", ddpod.Name)
			_, err := clientNewGc.CoreV1().Pods(namespace).Get(ctx, ddpod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = fpod.WaitForPodNameRunningInNamespace(ctx, clientNewGc, ddpod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod with pvc1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, ddpod.Spec.NodeName+"-"+pvc1NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, true)

			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod with pvc2")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, ddpod.Spec.NodeName+"-"+pvc2NameInSV,
				crdCNSFileAccessConfig, crdVersion, crdGroup, true)
		}

		ginkgo.By("Delete TKG")
		framework.Logf("Get WCP session id")
		sessionID := getVCentreSessionId(e2eVSphere.Config.Global.VCenterHostname, e2eVSphere.Config.Global.User,
			e2eVSphere.Config.Global.Password)
		wcpCluster := getWCPCluster(sessionID, e2eVSphere.Config.Global.VCenterHostname)
		wcpHost := getWCPHost(wcpCluster, e2eVSphere.Config.Global.VCenterHostname, sessionID)
		framework.Logf("wcphost %s", wcpHost)
		wcpToken = getWCPSessionId(wcpHost, e2eVSphere.Config.Global.User, e2eVSphere.Config.Global.Password)

		err = deleteTKG(wcpHost, wcpToken, tkg_cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isTKGDeleted = true

		for _, volume := range volumesBeforeScaleDown {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})
})
