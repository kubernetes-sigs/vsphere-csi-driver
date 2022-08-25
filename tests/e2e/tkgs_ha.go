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
	"strings"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-tkgs-ha] Tkgs-HA-SanityTests",
	func() {
		f := framework.NewDefaultFramework("e2e-tkgs-ha")
		f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
		var (
			client               clientset.Interface
			namespace            string
			scParameters         map[string]string
			allowedTopologyHAMap map[string][]string
			categories           []string
			zonalPolicy          string
			zonalWffcPolicy      string
		)
		ginkgo.BeforeEach(func() {
			//var cancel context.CancelFunc
			//ctx, cancel := context.WithCancel(context.Background())
			//defer cancel()
			client = f.ClientSet
			namespace = getNamespaceToRunTests(f)
			bootstrap()
			scParameters = make(map[string]string)
			topologyHaMap := GetAndExpectStringEnvVar(topologyHaMap)
			//topologyHAMap, categories := createAllowedTopolgies(topologyHaMap, tkgshaTopologyLevels)
			//allowedTopologyHAMap = createAllowedTopologiesMap(topologyHAMap)
			_, categories = createTopologyMapLevel5(topologyHaMap, tkgshaTopologyLevels)
			allowedTopologies := createAllowedTopolgies(topologyHaMap, tkgshaTopologyLevels)
			allowedTopologyHAMap = createAllowedTopologiesMap(allowedTopologies)
			framework.Logf("Topology map: %v, categories: %v", allowedTopologyHAMap, categories)
			zonalPolicy = GetAndExpectStringEnvVar(envZonalStoragePolicyName)
			if zonalPolicy == "" {
				ginkgo.Fail(envZonalStoragePolicyName + " env variable not set")
			}
			zonalWffcPolicy = GetAndExpectStringEnvVar(envZonalWffcStoragePolicyName)
			if zonalWffcPolicy == "" {
				ginkgo.Fail(envZonalWffcStoragePolicyName + " env variable not set")
			}
			framework.Logf("zonal policy: %s and zonal wffc policy: %s", zonalPolicy, zonalWffcPolicy)

			nodeList, err := fnodes.GetReadySchedulableNodes(client)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}

			if guestCluster {
				svcClient, svNamespace := getSvcClientAndNamespace()
				setResourceQuota(svcClient, svNamespace, rqLimit)
			}

		})

		/*
			Dynamic PVC -  Zonal storage and Immediate binding
			1. Create a zonal storage policy, on the datastore that is shared only to specific cluster
			2. Use the Zonal storage class and Immediate binding mode and create PVC
			3. wait for the gc-PVC to bound and make sure corresponding SVC-PVC
				will have annotation "csi.vsphere.volume-accessible-topology:"
				[{"topology.kubernetes.io/zone":"zone1"}]
				"csi.vsphere.volume-requested-topology:"
				[{"topology.kubernetes.io/zone":"zone-1"},{"topology.kubernetes.io/zone":"zone-2"},
				{"topology.kubernetes.io/zone":"zone-3"}]
			4. storageClassName: should point to svStorageclass
			5. Describe SVC-PV , and GC-PV  and verify node affinity, make sure appropriate node affinity gets added
			6. Create POD using Gc-pvc
			7. Wait  for the PODs to reach running state - make sure Pod scheduled on
				appropriate nodes preset in the availability zone
			8. Delete PVC,POD,SC
		*/
		ginkgo.It("Dynamic PVC -  Zonal storage and Immediate binding", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ginkgo.By("CNS_TEST: Running for GC setup")
			nodeList, err := fnodes.GetReadySchedulableNodes(client)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")

			ginkgo.By("Creating Pvc with Immediate topology storageclass")
			createResourceQuota(client, namespace, rqLimit, zonalPolicy)
			scParameters[svStorageClassName] = zonalPolicy
			storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			pvclaim, err := createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Wait for GC PVC to come to bound state")
			persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim},
				framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pv := persistentvolumes[0]
			volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			svcPVCName := pv.Spec.CSI.VolumeHandle
			svcPVC := getPVCFromSupervisorCluster(svcPVCName)

			defer func() {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By("Verify PVs, volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeID := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeID))

			}()

			ginkgo.By("Verify SV storageclass points to GC storageclass")
			gomega.Expect(*svcPVC.Spec.StorageClassName == storageclass.Name).To(
				gomega.BeTrue(), "SV storageclass does not match with gc storageclass")
			framework.Logf("GC PVC's storageclass matches SVC PVC's storageclass")

			ginkgo.By("Verify SV PVC has TKG HA annotations set")
			err = checkAnnotationOnSvcPvc(svcPVC, allowedTopologyHAMap, categories)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("SVC PVC: %s has TKG HA annotations set", svcPVC.Name)

			ginkgo.By("Verify GC PV has has required PV node affinity details")
			_, err = verifyVolumeTopologyForLevel5(pv, allowedTopologyHAMap)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("GC PV: %s has required Pv node affinity details", pv.Name)

			ginkgo.By("Verify SV PV has has required PV node affinity details")
			svcPV := getPvFromSupervisorCluster(svcPVCName)
			_, err = verifyVolumeTopologyForLevel5(svcPV, allowedTopologyHAMap)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("SVC PV: %s has required PV node affinity details", svcPV.Name)

			ginkgo.By("Create a pod and verify pod gets scheduled on appropriate " +
				"nodes preset in the availability zone")
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				ginkgo.By("Delete pod")
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By("Verify volume is detached from the node")
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}()

			_, err = verifyPodLocationLevel5(pod, nodeList, allowedTopologyHAMap)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

		/*
			Stateful set - storage class with Zonal storage and wffc and with parallel pod management policy
			1. Create a zonal storage policy, on the datastore that is shared only to specific cluster
			2. Use the Zonal storage class and WaitForFirstConsumer binding mode and create statefulset
				with parallel pod management policy with replica 3
			3. wait for all the gc-PVC to bound - Make sure corresponding SVC-PVC will have
				"csi.vsphere.volume-accessible-topology" annotation
				csi.vsphere.guest-cluster-topology=
				[{"topology.kubernetes.io/zone":"zone1"},{"topology.kubernetes.io/zone":"zone2"},
				{"topology.kubernetes.io/zone":"zone2"}]
			4. storageClassName: should point to gcStorageclass
			5. Wait for the PODs to reach running state - make sure Pod scheduled on appropriate nodes
				preset in the availability zone
			6. Describe SVC-PV , and GC-PV  and verify node affinity, make sure appropriate node affinity gets added
			7. Scale up the statefulset replica to 5 , and validate the node affinity
				on the newly create PV's and annotations on PVC's
			8. Validate the CNS metadata
			9. Scale down the sts to 0
			10.Delete PVC,POD,SC
		*/
		ginkgo.It("Stateful set - storage class with Zonal storage and wffc and"+
			" with parallel pod management policy", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ginkgo.By("CNS_TEST: Running for GC setup")
			nodeList, err := fnodes.GetReadySchedulableNodes(client)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")

			ginkgo.By("Create statefulset with parallel pod management policy with replica 3")
			createResourceQuota(client, namespace, rqLimit, zonalWffcPolicy)
			scParameters[svStorageClassName] = zonalWffcPolicy
			storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalWffcPolicy, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			// Creating StatefulSet service
			ginkgo.By("Creating service")
			service := CreateService(namespace, client)
			defer func() {
				deleteService(namespace, client, service)
			}()

			statefulset := GetStatefulSetFromManifest(namespace)
			ginkgo.By("Creating statefulset")
			statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
			*statefulset.Spec.Replicas = 3
			CreateStatefulSet(namespace, statefulset, client)
			replicas := *(statefulset.Spec.Replicas)

			defer func() {
				scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
				pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, claim := range pvcs.Items {
					pv := getPvFromClaim(client, namespace, claim.Name)
					err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
					err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
						pollTimeout)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					volumeHandle := pv.Spec.CSI.VolumeHandle
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(),
						fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
							"kubernetes", volumeHandle))
				}
			}()

			// Waiting for pods status to be Ready
			fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
			ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			ginkgo.By("Verify GC PV and SV PV has has required PV node affinity details")
			ginkgo.By("Verify SV PVC has TKG HA annotations set")
			// Get the list of Volumes attached to Pods before scale down
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				_, err = verifyPodLocationLevel5(&sspod, nodeList, allowedTopologyHAMap)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pvcName := volumespec.PersistentVolumeClaim.ClaimName
						pv := getPvFromClaim(client, statefulset.Namespace, pvcName)
						pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
							pvcName, metav1.GetOptions{})
						gomega.Expect(pvclaim).NotTo(gomega.BeNil())
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
						gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
						svcPVCName := pv.Spec.CSI.VolumeHandle
						svcPVC := getPVCFromSupervisorCluster(svcPVCName)
						gomega.Expect(*svcPVC.Spec.StorageClassName == zonalPolicy).To(
							gomega.BeTrue(), "mismatch in storage class name present in SVC PVC, expected: %s,"+
								"actual: %s", zonalPolicy, svcPVC.Spec.StorageClassName)
						framework.Logf("GC PVC's storageclass matches SVC PVC's storageclass")

						err = checkAnnotationOnSvcPvc(svcPVC, allowedTopologyHAMap, categories)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("SVC PVC: %s has TKG HA annotations set", svcPVC.Name)

						_, err = verifyVolumeTopologyForLevel5(pv, allowedTopologyHAMap)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("GC PV: %s has required Pv node affinity details", pv.Name)

						svcPV := getPvFromSupervisorCluster(svcPVCName)
						_, err = verifyVolumeTopologyForLevel5(svcPV, allowedTopologyHAMap)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("SVC PV: %s has required PV node affinity details", svcPV.Name)

						// Verify the attached volume match the one in CNS cache
						err = waitAndVerifyCnsVolumeMetadata4GCVol(volHandle, svcPVCName, pvclaim,
							pv, pod)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}

			replicas = 5
			framework.Logf(fmt.Sprintf("Scaling up statefulset: %v to number of Replica: %v",
				statefulset.Name, replicas))
			_, scaleupErr := fss.Scale(client, statefulset, replicas)
			gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())

			fss.WaitForStatusReplicas(client, statefulset, replicas)
			fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
			ssPodsAfterScaleUp := fss.GetPodList(client, statefulset)
			gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset %s, %v, should match with number of replicas %v",
				statefulset.Name, ssPodsAfterScaleUp.Size(), replicas,
			)

			// Get the list of Volumes attached to Pods before scale down
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pvcName := volumespec.PersistentVolumeClaim.ClaimName
						pv := getPvFromClaim(client, statefulset.Namespace, pvcName)
						pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
							pvcName, metav1.GetOptions{})
						gomega.Expect(pvclaim).NotTo(gomega.BeNil())
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
						gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
						svcPVCName := pv.Spec.CSI.VolumeHandle
						svcPVC := getPVCFromSupervisorCluster(svcPVCName)

						err = checkAnnotationOnSvcPvc(svcPVC, allowedTopologyHAMap, categories)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("SVC PVC: %s has TKG HA annotations set", svcPVC.Name)

						_, err = verifyVolumeTopologyForLevel5(pv, allowedTopologyHAMap)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("GC PV: %s has required Pv node affinity details", pv.Name)

						svcPV := getPvFromSupervisorCluster(svcPVCName)
						_, err = verifyVolumeTopologyForLevel5(svcPV, allowedTopologyHAMap)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("SVC PV: %s has required PV node affinity details", svcPV.Name)

						framework.Logf(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
							pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
						var vmUUID string
						var exists bool
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						if vanillaCluster {
							vmUUID = getNodeUUID(ctx, client, sspod.Spec.NodeName)
						} else if supervisorCluster {
							annotations := sspod.Annotations
							vmUUID, exists = annotations[vmUUIDLabel]
							gomega.Expect(exists).To(
								gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
							_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
							gomega.Expect(err).NotTo(gomega.HaveOccurred())
						} else {
							vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
							gomega.Expect(err).NotTo(gomega.HaveOccurred())
							verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
								crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
						}
						isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached to the node")
						framework.Logf("After scale up, verify the attached volumes match those in CNS Cache")
						err = waitAndVerifyCnsVolumeMetadata4GCVol(volHandle, svcPVCName, pvclaim,
							pv, pod)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}

		})

		/*
			Edit the svc-pvc and try to change annotation or SC values
			1. Create a zonal storage policy, on the datastore that is shared only to specific cluster
			2. Use the Zonal storage class and Immediate binding mode and create PVC
			3. wait for the gc-PVC to bound and make sure corresponding SVC-PVC will have annotation
				"csi.vsphere.volume-accessible-topology:"
				[{"topology.kubernetes.io/zone":"zone1"}]
				"csi.vsphere.volume-requested-topology:"
				[{"topology.kubernetes.io/zone":"zone-1"},{"topology.kubernetes.io/zone":"zone-2"},
				{"topology.kubernetes.io/zone":"zone-3"}]
			4. storageClassName: should point to svStorageclass
			5. Edit the PVC and try to change the annotation value from zone1 to zone2 - This operation should not be allowed
			6. Edit the SC parameter to some cross-zonal sc name - This operation should not be allowed
			7. Delete PVC,SC
		*/
		ginkgo.It("Edit the svc-pvc and try to change annotation or SC values", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ginkgo.By("CNS_TEST: Running for GC setup")
			ginkgo.By("Creating Pvc with Immediate topology storageclass")
			createResourceQuota(client, namespace, rqLimit, zonalPolicy)
			scParameters[svStorageClassName] = zonalPolicy
			storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			pvclaim, err := createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Wait for GC PVC to come to bound state")
			persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaim},
				framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pv := persistentvolumes[0]
			volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			svcPVCName := pv.Spec.CSI.VolumeHandle
			svcPVC := getPVCFromSupervisorCluster(svcPVCName)

			defer func() {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By("Verify PVs, volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeID := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeID))

			}()

			ginkgo.By("Verify SV storageclass points to GC storageclass")
			gomega.Expect(*svcPVC.Spec.StorageClassName == storageclass.Name).To(
				gomega.BeTrue(), "SV storageclass does not match with GC storageclass")
			framework.Logf("GC PVC's storageclass matches SVC PVC's storageclass")

			ginkgo.By("Verify SV PVC has TKG HA annotations set")
			err = checkAnnotationOnSvcPvc(svcPVC, allowedTopologyHAMap, categories)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("SVC PVC: %s has TKG HA annotations set", svcPVC.Name)

			ginkgo.By("Edit the PVC and try to change the annotation value")
			svClient, svNamespace := getSvcClientAndNamespace()
			accessibleTopoString := svcPVC.Annotations[tkgHAccessibleAnnotationKey]
			accessibleTopology := strings.Split(accessibleTopoString, ":")
			topoKey := strings.Split(accessibleTopology[0], "{")[1]
			newSvcAnnotationVal := "[{" + topoKey + ":" + "zone-4}]"
			svcPVC.Annotations[tkgHAccessibleAnnotationKey] = newSvcAnnotationVal
			_, err = svClient.CoreV1().PersistentVolumeClaims(svNamespace).Update(ctx, svcPVC, metav1.UpdateOptions{})
			framework.Logf("Error from changing annotation value is: %v", err)
			gomega.Expect(err).To(gomega.HaveOccurred())

			ginkgo.By("Edit the PVC and try to change the storageclass parameter value")
			*svcPVC.Spec.StorageClassName = "tkc1"
			_, err = svClient.CoreV1().PersistentVolumeClaims(svNamespace).Update(ctx, svcPVC, metav1.UpdateOptions{})
			framework.Logf("Error from changing storageclass parameter value is: %v", err)
			gomega.Expect(err).To(gomega.HaveOccurred())

		})

	})
