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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[rwm-csi-tkg] TKG RWX for STS with GC worker nodes scale up and down", func() {
	f := framework.NewDefaultFramework("rwx-tkg-sts")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		scParameters      map[string]string
		storagePolicyName string
		volHealthCheck    bool
		isSTSDeleted      bool
		isServiceDeleted  bool
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		// TODO: Read value from command line
		volHealthCheck = false
		isSTSDeleted = false
		isServiceDeleted = false
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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if !isSTSDeleted {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(ctx, client, namespace)
		}
		if !isServiceDeleted {
			ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
			err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
	})

	/*
	   Test to verify file volume provision with statefulsets on TKG nodes scale up
	   1. Create a Storage class
	   2. Create Nginx service
	   3. Create Nginx statefulset with 3 replicas and podManagementPolicy=OrderedReady using the
	       Storage Policy obtained in Step 1 (each replica to use a dedicated file volume)
	   4. Wait until all Pods are ready and PVCs are bounded with PV
	   5. Verify CnsVolumeMetadata CRD are created
	   6. Verify CnsFileAccessConfig CRD are created
	   7. Verify health status of PVC
	   8. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
	   9. Scale-up Guest cluster's master and worker nodes from +1
	   10. Wait for the new nodes to be ready
	   11. Scale-up statefulset to 5 replicas
	   12. Verify if all the pods are successfully created and running
	   13. Verify using CNS Query API if all 5 PV's exists
	   14. Delete the statefulset
	   15. Verify CnsFileAccessConfig CRD are deleted
	   16. Verify if all the pods are successfully deleted
	   17. Verify using CNS Query API if all 5 PV's still exists
	   18. Delete PVCs
	   19. Verify if PVCs and PVs are deleted in the SV cluster and GC
	   20. Verify CnsVolumeMetadata CRD are deleted
	   21. Check if the VolumeID is deleted from CNS by using CNSQuery API
	*/
	ginkgo.It("Verify file volume provision by TKG worker scale up", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")

		ginkgo.By("Creating StorageClass for Statefulset")
		scParameters[svStorageClassName] = storagePolicyName
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)

		defer func() {
			if !isServiceDeleted {
				deleteService(namespace, client, service)
				isServiceDeleted = true
			}
		}()

		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		scName := defaultNginxStorageClassName
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.AccessModes[0] =
			v1.ReadWriteMany
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &scName
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		defer func() {
			if !isSTSDeleted {
				ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
				fss.DeleteAllStatefulSets(ctx, client, namespace)
				isSTSDeleted = true
			}
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaledown := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsBeforeScaledown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaledown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Get the list of Volumes attached to Pods
		var volumesBeforeScaleDown []string
		var cnsFileAccessConfigCRDList []string
		for _, sspod := range ssPodsBeforeScaledown.Items {
			tempPod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					persistentvolume := getPvFromClaim(client, statefulset.Namespace,
						volumespec.PersistentVolumeClaim.ClaimName)
					pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
						volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(pvclaim).NotTo(gomega.BeNil())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					pvcNameInSV := persistentvolume.Spec.CSI.VolumeHandle
					gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
					fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
					gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

					if volHealthCheck {
						ginkgo.By("poll for health status annotation")
						err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}

					//Add a check to validate CnsVolumeMetadata crd
					err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, fcdIDInCNS, pvcNameInSV, pvclaim,
						persistentvolume, tempPod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod")
					verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, tempPod.Spec.NodeName+"-"+pvcNameInSV,
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

		ginkgo.By("Scale up TKGs worker nodes to 5")
		sessionID := getVCentreSessionId(e2eVSphere.Config.Global.VCenterHostname, e2eVSphere.Config.Global.User,
			e2eVSphere.Config.Global.Password)
		wcpCluster := getWCPCluster(sessionID, e2eVSphere.Config.Global.VCenterHostname)
		wcpHost := getWCPHost(wcpCluster, e2eVSphere.Config.Global.VCenterHostname, sessionID)
		framework.Logf("wcphost %s", wcpHost)
		wcpToken := getWCPSessionId(wcpHost, e2eVSphere.Config.Global.User, e2eVSphere.Config.Global.Password)
		scaleTKGWorker(wcpHost, wcpToken, devopsTKG, 5)

		defer func() {
			ginkgo.By("In defer function to reset the TKGs worker nodes")
			scaleTKGWorker(wcpHost, wcpToken, devopsTKG, 3)

			ginkgo.By("Wait for the TKG to be Ready")
			err = getGC(wcpHost, wcpToken, devopsTKG)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for the TKG to be Ready")
		err = getGC(wcpHost, wcpToken, devopsTKG)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas += 2
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)

		ssPodsAfterScaleUp := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitTimeoutForPodReadyInNamespace(ctx, client, sspod.Name, statefulset.Namespace, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					persistentvolume := getPvFromClaim(client, statefulset.Namespace,
						volumespec.PersistentVolumeClaim.ClaimName)

					pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
						volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(pvclaim).NotTo(gomega.BeNil())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					pvcNameInSV := persistentvolume.Spec.CSI.VolumeHandle
					gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
					fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
					gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

					volumesBeforeScaleDown = append(volumesBeforeScaleDown, pvcNameInSV)

					ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod")
					verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
						crdCNSFileAccessConfig, crdVersion, crdGroup, true)
					cnsFileAccessConfigCRDList = append(cnsFileAccessConfigCRDList,
						pod.Spec.NodeName+"-"+pvcNameInSV)

					if volHealthCheck {
						ginkgo.By("poll for health status annotation")
						err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}

					//Add a check to validate CnsVolumeMetadata crd
					err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, fcdIDInCNS, pvcNameInSV, pvclaim,
						persistentvolume, pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(ctx, client, namespace)
		isSTSDeleted = true

		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err = client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceDeleted = true

		ginkgo.By("Wait and verify PVC is fully deleted")
		for _, volume := range volumesBeforeScaleDown {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			//Add a check to validate CnsVolumeMetadata crd
			verifyCRDInSupervisorWithWait(ctx, f, volume, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
		}

		ginkgo.By("Wait and verify CNSFileAccessConfig CRD is fully deleted")
		for _, crdName := range cnsFileAccessConfigCRDList {
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, crdName, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
		}
	})

	/*
		Test to verify file volume provision with statefulsets on TKG nodes scale up
		1. Create a Storage class
		2. Create Nginx service
		3. Create Nginx statefulset with 3 replicas and
			podManagementPolicy=Parallel  using the Storage
			Policy obtained in Step 1 (each replica to use a dedicated file volume)
		4. Wait until all Pods are ready and PVCs are bounded with PV
		5. Verify CnsVolumeMetadata CRD are created
		6. Verify CnsFileAccessConfig CRD are created
		7. Verify health status of PVC
		8. Verify volume is created on CNS by using CNSQuery API and also check metadata is pushed to CNS
		9. Scale-down Guest cluster's master and worker nodes from -1
		10. Wait for the new nodes to be ready
		11. Scale-up statefulset to 5 replicas
		12. Verify if all the pods are successfully created and running
		13. Verify using CNS Query API if all 5 PV's exists
		14. Delete the statefulset
		15. Verify CnsFileAccessConfig CRD are deleted
		16. Verify if all the pods are successfully deleted
		17. Verify using CNS Query API if all 5 PV's still exists
		18. Delete PVCs
		19. Verify if PVCs and PVs are deleted in the SV cluster and GC
		20. Verify CnsVolumeMetadata CRD are deleted
		21. Check if the VolumeID is deleted from CNS by using CNSQuery API
	*/
	ginkgo.It("Verify file volume provision by TKG worker scale down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error

		ginkgo.By("CNS_TEST: Running for GC setup")

		ginkgo.By("Creating StorageClass for Statefulset")
		scParameters[svStorageClassName] = storagePolicyName
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			if !isServiceDeleted {
				deleteService(namespace, client, service)
				isServiceDeleted = true
			}
		}()
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		scName := defaultNginxStorageClassName
		statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.AccessModes[0] =
			v1.ReadWriteMany
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &scName
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		defer func() {
			if !isSTSDeleted {
				ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
				fss.DeleteAllStatefulSets(ctx, client, namespace)
				isSTSDeleted = true
			}
		}()
		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaledown := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsBeforeScaledown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaledown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Get the list of Volumes attached to Pods
		var volumesBeforeScaleDown []string
		var cnsFileAccessConfigCRDList []string
		for _, sspod := range ssPodsBeforeScaledown.Items {
			tempPod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					persistentvolume := getPvFromClaim(client, statefulset.Namespace,
						volumespec.PersistentVolumeClaim.ClaimName)
					pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
						volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(pvclaim).NotTo(gomega.BeNil())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					pvcNameInSV := persistentvolume.Spec.CSI.VolumeHandle
					gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
					fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
					gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

					volumesBeforeScaleDown = append(volumesBeforeScaleDown, pvcNameInSV)

					if volHealthCheck {
						ginkgo.By("poll for health status annotation")
						err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}

					//Add a check to validate CnsVolumeMetadata crd
					err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, fcdIDInCNS, pvcNameInSV, pvclaim,
						persistentvolume, tempPod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod")
					verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, tempPod.Spec.NodeName+"-"+pvcNameInSV,
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

		ginkgo.By("Scale down TKGs worker nodes to 2")
		sessionID := getVCentreSessionId(e2eVSphere.Config.Global.VCenterHostname, e2eVSphere.Config.Global.User,
			e2eVSphere.Config.Global.Password)
		wcpCluster := getWCPCluster(sessionID, e2eVSphere.Config.Global.VCenterHostname)
		wcpHost := getWCPHost(wcpCluster, e2eVSphere.Config.Global.VCenterHostname, sessionID)
		framework.Logf("wcphost %s", wcpHost)
		wcpToken := getWCPSessionId(wcpHost, e2eVSphere.Config.Global.User, e2eVSphere.Config.Global.Password)
		scaleTKGWorker(wcpHost, wcpToken, devopsTKG, 2)

		defer func() {
			ginkgo.By("In defer function to reset the TKGs worker nodes")
			scaleTKGWorker(wcpHost, wcpToken, devopsTKG, 3)

			ginkgo.By("Wait for the TKG to be Ready")
			err = getGC(wcpHost, wcpToken, devopsTKG)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait for the TKG to be Ready")
		err = getGC(wcpHost, wcpToken, devopsTKG)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas += 2
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		ssPodsAfterScaleUp := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitTimeoutForPodReadyInNamespace(ctx, client, sspod.Name, statefulset.Namespace, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					persistentvolume := getPvFromClaim(client, statefulset.Namespace,
						volumespec.PersistentVolumeClaim.ClaimName)

					pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
						volumespec.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
					gomega.Expect(pvclaim).NotTo(gomega.BeNil())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					pvcNameInSV := persistentvolume.Spec.CSI.VolumeHandle
					gomega.Expect(pvcNameInSV).NotTo(gomega.BeEmpty())
					fcdIDInCNS := getVolumeIDFromSupervisorCluster(pvcNameInSV)
					gomega.Expect(fcdIDInCNS).NotTo(gomega.BeEmpty())

					volumesBeforeScaleDown = append(volumesBeforeScaleDown, pvcNameInSV)

					ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod")
					verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, pod.Spec.NodeName+"-"+pvcNameInSV,
						crdCNSFileAccessConfig, crdVersion, crdGroup, true)
					cnsFileAccessConfigCRDList = append(cnsFileAccessConfigCRDList,
						pod.Spec.NodeName+"-"+pvcNameInSV)

					if volHealthCheck {
						ginkgo.By("poll for health status annotation")
						err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatusAccessible)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}

					//Add a check to validate CnsVolumeMetadata crd
					err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, fcdIDInCNS, pvcNameInSV, pvclaim,
						persistentvolume, pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(ctx, client, namespace)
		isSTSDeleted = true

		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err = client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceDeleted = true

		ginkgo.By("Wait and verify PVC is fully deleted")
		for _, volume := range volumesBeforeScaleDown {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			//Add a check to validate CnsVolumeMetadata crd
			verifyCRDInSupervisorWithWait(ctx, f, volume, crdCNSVolumeMetadatas, crdVersion, crdGroup, false)
		}

		ginkgo.By("Wait and verify CNSFileAccessConfig CRD is fully deleted")
		for _, crdName := range cnsFileAccessConfigCRDList {
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, f, crdName, crdCNSFileAccessConfig,
				crdVersion, crdGroup, false)
		}
	})
})
