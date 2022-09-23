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
	"math/rand"
	"os/exec"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
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

			ginkgo.By("Create a pod and wait for it to come to Running state")
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

			ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
			ginkgo.By("Verify pod gets scheduled on appropriate nodes preset in the availability zone")
			verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod, nodeList, svcPVC, pv, svcPVCName)
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

			verifyVolumeMetadataOnStatefulsets(client, ctx, namespace, statefulset, replicas,
				allowedTopologyHAMap, categories, zonalPolicy, nodeList, f)

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

		/*
			Bring down VSAN during volume provisioning using zonal storage
			1. Create a zonal storage policy
			2. Bring down vsan-health
			3. Use the Zonal storage class and Immediate binding mode and create statefulset
			   with default pod management policy with replica 3
			4. PVC and POD creations should be in pending state since vsan is down
			5. Bring up VSan-health
			6. wait for all the PVC to bound
			7. Verify the  annotation csi.vsphere.guest-cluster-topology
			8. Wait  for all the PODs to reach running state
			9. Describe gc-PV and svc-pv verify node affinity
			10. Verify that POD's should come up on the nodes of appropriate  Availability zone
			11. Scale up the statefulset replica to 5 , and validate the node affinity on the newly create PV's
			12. Validate the CNS metadata
			13. Scale down the sts to 0
			14. Delete PVC,POD,SC
		*/
		ginkgo.It("Bring down VSAN during volume provisioning using zonal storage", func() {
			serviceName := vsanhealthServiceName
			isServiceStopped := false
			verifyVolumeProvisioningWithServiceDown(serviceName, namespace, client, zonalPolicy,
				allowedTopologyHAMap, categories, isServiceStopped, f)
		})

		/*
			Bring down sps during volume provisioning using zonal storage
			1. Create a zonal storage policy
			2. Bring down sps
			3. Use the Zonal storage class and Immediate binding mode and create statefulset
			   with default pod management policy with replica 3
			4. PVC and POD creations should be in pending state since vsan is down
			5. Bring up sps
			6. wait for all the PVC to bound
			7. Verify the  annotation csi.vsphere.guest-cluster-topology
			8. Wait  for all the PODs to reach running state
			9. Describe gc-PV and svc-pv verify node affinity
			10. Verify that POD's should come up on the nodes of appropriate  Availability zone
			11. Scale up the statefulset replica to 5 , and validate the node affinity on the newly create PV's
			12. Validate the CNS metadata
			13. Scale down the sts to 0
			14. Delete PVC,POD,SC
		*/
		ginkgo.It("Bring down sps during volume provisioning using zonal storage", func() {
			serviceName := spsServiceName
			isServiceStopped := false
			verifyVolumeProvisioningWithServiceDown(serviceName, namespace, client, zonalPolicy,
				allowedTopologyHAMap, categories, isServiceStopped, f)
		})

		/*
			Verify Online Volume expansion using zonal storage
			1. Create a zonal storage policy, on the datastore that is shared only to specific cluster
			2. Use the Zonal storage class and Immediate binding mode and create PVC
			3. wait for the gc-PVC to bound and make sure corresponding SVC-PVC will
				have annotation "csi.vsphere.volume-accessible-topology:"
			   [{"topology.kubernetes.io/zone":"zone1"}]
			"csi.vsphere.volume-requested-topology:"
			   [{"topology.kubernetes.io/zone":"zone-1"},{"topology.kubernetes.io/zone":"zone-2"},
			   {"topology.kubernetes.io/zone":"zone-3"}]
			4. storageClassName: should point to svStorageclass and Create POD using Gc-pvc and
				make sure Pod scheduled on appropriate nodes preset in the availability zone
			5. Trigger Online volume expansion on the GC-PVC
			6. Volume expansion should trigger in SVC-PVC
			7. Volume expansion should be successful
			8. Validate the SVC PVC and GC PVC should be of same size
			9. Validate CNS metadata
			10.Verify the FS size on the POD
			11.Clear all PVC,POD and sc
		*/
		ginkgo.It("Verify Online Volume expansion using zonal storage", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ginkgo.By("CNS_TEST: Running for GC setup")
			nodeList, _ := fnodes.GetReadySchedulableNodes(client)

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

			ginkgo.By("Create a pod and wait for it to come to Running state")
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

			ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
			ginkgo.By("Verify pod gets scheduled on appropriate nodes preset in the availability zone")
			verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod, nodeList, svcPVC, pv, svcPVCName)

			verifyOnlineVolumeExpansionOnGc(client, namespace, svcPVCName, volHandle, pvclaim, pod, f)

		})

		/*
			Verify offline Volume expansion using zonal storage
			1. Create a zonal storage policy, on the datastore that is shared only to specific cluster
			2. Use the Zonal storage class and Immediate binding mode and create PVC
			3. wait for the gc-PVC to bound and make sure corresponding SVC-PVC will have
				annotation "csi.vsphere.volume-accessible-topology:"
				[{"topology.kubernetes.io/zone":"zone1"}]
				"csi.vsphere.volume-requested-topology:"
				[{"topology.kubernetes.io/zone":"zone-1"},{"topology.kubernetes.io/zone":"zone-2"},
				{"topology.kubernetes.io/zone":"zone-3"}]
			4. storageClassName: should point to svStorageclass and Create POD using Gc-pvc and
				make sure Pod scheduled on appropriate nodes preset in the availability zone
			5. Trigger offline volume expansion on the GC-PVC
			6. Volume expansion should trigger in SVC-PVC , and it should reach FilesystemresizePending state
			7.  Create POD in GC using  GC-PVC
			8. Volume expansion should be successful
			9. Validate the SVC PVC and GC PVC should be of same size
			10. Verify the node affinity on svc-pv and gc-pv and validate CNS metadata
			11.Verify the FS size on the POD
			12.Clear all PVC,POD and sc
		*/
		ginkgo.It("Verify offline Volume expansion using zonal storage", func() {
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

			ginkgo.By("Create a pod and wait for it to come to Running state")
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

			ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
			ginkgo.By("Verify pod gets scheduled on appropriate nodes preset in the availability zone")
			verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod, nodeList, svcPVC, pv, svcPVCName)

			verifyOfflineVolumeExpansionOnGc(client, pvclaim, svcPVCName, namespace, volHandle, pod, pv, f)

		})

		/*
			Static volume provisioning using zonal storage
			1. Create a zonal storage policy, on the datastore that is shared only to specific cluster
			2. Use the Zonal storage class and Immediate binding mode
			3. Create a FCD in SVC namespace
			4. Create CNS register volume CRD to statically import PVC on SVC namespace
			5. Verify the statically created svc-pv and svc-pvc
			6. In Gc, Create Static gc-pv pointing the volume handle  of svc-pvc  and reclaim policy as retain
			7. Verify the node affinity of gc1-pv and svc-pv , It should have node affinity of all the zones
			8. Create POD, verify the status.
			9. Wait  for the PODs to reach running state - make sure Pod scheduled on
			   appropriate nodes preset in the availability zone
			10. Delete pod, gc1-pv and gc1-pvc and svc pvc.
		*/
		ginkgo.It("Static volume provisioning using zonal storage", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ginkgo.By("CNS_TEST: Running for GC setup")
			nodeList, err := fnodes.GetReadySchedulableNodes(client)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")

			svClient, svNamespace := getSvcClientAndNamespace()
			pvcAnnotations := make(map[string]string)
			annotationVal := "["
			var topoList []string

			for key, val := range allowedTopologyHAMap {
				for _, topoVal := range val {
					str := `{"` + key + `":"` + topoVal + `"}`
					topoList = append(topoList, str)
				}
			}
			framework.Logf("topoList: %v", topoList)
			annotationVal += strings.Join(topoList, ",") + "]"
			pvcAnnotations[tkgHARequestedAnnotationKey] = annotationVal
			framework.Logf("annotationVal :%s, pvcAnnotations: %v", annotationVal, pvcAnnotations)

			ginkgo.By("Creating Pvc with Immediate topology storageclass")
			createResourceQuota(client, namespace, rqLimit, zonalPolicy)
			scParameters[svStorageClassName] = zonalPolicy
			storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			pvcSpec := getPersistentVolumeClaimSpecWithStorageClass(svNamespace, "", storageclass, nil, "")
			pvcSpec.Annotations = pvcAnnotations
			svPvclaim, err := svClient.CoreV1().PersistentVolumeClaims(svNamespace).Create(context.TODO(),
				pvcSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Wait for SV PVC to come to bound state")
			svcPv, err := fpv.WaitForPVClaimBoundPhase(svClient, []*v1.PersistentVolumeClaim{svPvclaim},
				framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volumeID := svPvclaim.Name
			staticPVLabels := make(map[string]string)
			staticPVLabels["fcd-id"] = volumeID

			// Get allowed topologies for zonal storage
			allowedTopologies := getTopologySelector(allowedTopologyHAMap, categories,
				tkgshaTopologyLevels)

			ginkgo.By("Creating the PV")
			staticPv := getPersistentVolumeSpecWithStorageClassFCDNodeSelector(volumeID,
				v1.PersistentVolumeReclaimRetain, storageclass.Name, staticPVLabels,
				diskSize, allowedTopologies)
			staticPv, err = client.CoreV1().PersistentVolumes().Create(ctx, staticPv, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Creating the PVC")
			staticPvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, staticPv.Name)
			staticPvc.Spec.StorageClassName = &storageclass.Name
			staticPvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, staticPvc, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Wait for PV and PVC to Bind.
			framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(),
				namespace, staticPv, staticPvc))

			defer func() {
				err := fpv.DeletePersistentVolumeClaim(client, staticPvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				err = fpv.DeletePersistentVolume(client, staticPv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By("Verify PVs, volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, staticPv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeID))
				ginkgo.By("Verify volume is deleted in Supervisor Cluster")
				volumeExists := verifyVolumeExistInSupervisorCluster(svcPv[0].Spec.CSI.VolumeHandle)
				gomega.Expect(volumeExists).To(gomega.BeFalse())

			}()

			ginkgo.By("Verify SV storageclass points to GC storageclass")
			gomega.Expect(*svPvclaim.Spec.StorageClassName == storageclass.Name).To(
				gomega.BeTrue(), "SV storageclass does not match with gc storageclass")
			framework.Logf("GC PVC's storageclass matches SVC PVC's storageclass")

			ginkgo.By("Verify GV PV has has required PV node affinity details")
			_, err = verifyVolumeTopologyForLevel5(staticPv, allowedTopologyHAMap)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("GC PV: %s has required Pv node affinity details", staticPv.Name)

			ginkgo.By("Verify SV PV has has required PV node affinity details")
			_, err = verifyVolumeTopologyForLevel5(svcPv[0], allowedTopologyHAMap)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("SVC PV: %s has required PV node affinity details", svcPv[0].Name)

			ginkgo.By("Create a pod and verify pod gets scheduled on appropriate " +
				"nodes preset in the availability zone")
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{staticPvc}, false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				ginkgo.By("Delete pod")
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By("Verify volume is detached from the node")
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					staticPv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q",
						staticPv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			}()

			_, err = verifyPodLocationLevel5(pod, nodeList, allowedTopologyHAMap)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	})

// verifyVolumeProvisioningWithServiceDown brings the service down and creates the statefulset and then brings up
// the service and validates the volumes are bound and required annotations and node affinity are present
func verifyVolumeProvisioningWithServiceDown(serviceName string, namespace string, client clientset.Interface,
	storagePolicyName string, allowedTopologyHAMap map[string][]string, categories []string, isServiceStopped bool,
	f *framework.Framework) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ginkgo.By("CNS_TEST: Running for GC setup")
	nodeList, err := fnodes.GetReadySchedulableNodes(client)
	framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
	if !(len(nodeList.Items) > 0) {
		framework.Failf("Unable to find ready and schedulable Node")
	}

	ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
	vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
	err = invokeVCenterServiceControl(stopOperation, serviceName, vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	isServiceStopped = true
	err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcStoppedMessage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		if isServiceStopped {
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
			err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			bootstrap()
			isServiceStopped = false
		}
	}()

	ginkgo.By("Create statefulset with default pod management policy with replica 3")
	createResourceQuota(client, namespace, rqLimit, storagePolicyName)
	storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
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
	statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
		Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
	*statefulset.Spec.Replicas = 3
	_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
	framework.ExpectNoError(err)
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

	ginkgo.By(fmt.Sprintf("PVC and POD creations should be in pending state since %s is down", serviceName))
	pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, pvc := range pvcs.Items {
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimPending, client,
			pvc.Namespace, pvc.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Failed to find the volume in pending state with err: %v", err))
	}

	pods := fss.GetPodList(client, statefulset)
	for _, pod := range pods.Items {
		if pod.Status.Phase != v1.PodPending {
			framework.Failf("Expected pod to be in: %s state but is in: %s state", v1.PodPending,
				pod.Status.Phase)
		}
	}

	ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
	err = invokeVCenterServiceControl(startOperation, serviceName, vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	isServiceStopped = false
	err = waitVCenterServiceToBeInState(serviceName, vcAddress, svcRunningMessage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	bootstrap()

	verifyVolumeMetadataOnStatefulsets(client, ctx, namespace, statefulset, replicas,
		allowedTopologyHAMap, categories, storagePolicyName, nodeList, f)

}

// verifyOnlineVolumeExpansionOnGc is a util method which helps in verifying online volume expansion on gc
func verifyOnlineVolumeExpansionOnGc(client clientset.Interface, namespace string, svcPVCName string,
	volHandle string, pvclaim *v1.PersistentVolumeClaim, pod *v1.Pod, f *framework.Framework) {
	rand.Seed(time.Now().Unix())
	testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
	ginkgo.By(fmt.Sprintf("Creating a 512mb test data file %v", testdataFile))
	op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
		"bs=64k", "count=8000").Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		op, err = exec.Command("rm", "-f", testdataFile).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	_ = framework.RunKubectlOrDie(namespace, "cp", testdataFile,
		fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name))

	onlineVolumeResizeCheck(f, client, namespace, svcPVCName, volHandle, pvclaim, pod)

	ginkgo.By("Checking data consistency after PVC resize")
	_ = framework.RunKubectlOrDie(namespace, "cp",
		fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name), testdataFile+"_pod")
	defer func() {
		op, err = exec.Command("rm", "-f", testdataFile+"_pod").Output()
		fmt.Println("rm: ", op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	ginkgo.By("Running diff...")
	op, err = exec.Command("diff", testdataFile, testdataFile+"_pod").Output()
	fmt.Println("diff: ", op)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(len(op)).To(gomega.BeZero())

	ginkgo.By("File system resize finished successfully in GC")
	ginkgo.By("Checking for PVC resize completion on SVC PVC")
	_, err = waitForFSResizeInSvc(svcPVCName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// verifyOfflineVolumeExpansionOnGc is a util method which helps in verifying offline volume expansion on gc
func verifyOfflineVolumeExpansionOnGc(client clientset.Interface, pvclaim *v1.PersistentVolumeClaim, svcPVCName string,
	namespace string, volHandle string, pod *v1.Pod, pv *v1.PersistentVolume, f *framework.Framework) {
	cmd := []string{"exec", "", "--namespace=" + namespace, "--", "/bin/sh", "-c", "df -Tkm | grep /mnt/volume1"}
	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	cmd[1] = pod.Name
	lastOutput := framework.RunKubectlOrDie(namespace, cmd...)
	gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

	ginkgo.By("Check filesystem size for mount point /mnt/volume1 before expansion")
	originalFsSize, err := getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	rand.Seed(time.Now().Unix())
	testdataFile := fmt.Sprintf("/tmp/testdata_%v_%v", time.Now().Unix(), rand.Intn(1000))
	ginkgo.By(fmt.Sprintf("Creating a 512mb test data file %v", testdataFile))
	op, err := exec.Command("dd", "if=/dev/urandom", fmt.Sprintf("of=%v", testdataFile),
		"bs=64k", "count=8000").Output()
	fmt.Println(op)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		op, err = exec.Command("rm", "-f", testdataFile).Output()
		fmt.Println(op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	_ = framework.RunKubectlOrDie(namespace, "cp", testdataFile,
		fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name))

	// Delete POD.
	ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s before expansion", pod.Name, namespace))
	err = fpod.DeletePodWithWait(client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is detached from the node before expansion")
	isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
		pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
		fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))

	// Modify PVC spec to trigger volume expansion. We expand the PVC while
	// no pod is using it to ensure offline expansion.
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse(diskSize))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err = expandPVCSize(pvclaim, newSize, client)
	framework.ExpectNoError(err, "While updating pvc for more size")
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())

	pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	if pvcSize.Cmp(newSize) != 0 {
		framework.Failf("error updating pvc size %q", pvclaim.Name)
	}
	ginkgo.By("Checking for PVC request size change on SVC PVC")
	b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
	gomega.Expect(b).To(gomega.BeTrue())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Waiting for controller volume resize to finish")
	err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
	framework.ExpectNoError(err, "While waiting for pvc resize to finish")

	ginkgo.By("Checking for resize on SVC PV")
	verifyPVSizeinSupervisor(svcPVCName, newSize)

	ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
	err = waitForSvcPvcToReachFileSystemResizePendingCondition(svcPVCName, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Checking for conditions on pvc")
	pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if len(queryResult.Volumes) == 0 {
		err = fmt.Errorf("QueryCNSVolumeWithResult returned no volume")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verifying disk size requested in volume expansion is honored")
	newSizeInMb := convertGiStrToMibInt64(newSize)
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
		err = fmt.Errorf("got wrong disk size after volume expansion")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Create a new Pod to use this PVC, and verify volume has been attached.
	ginkgo.By("Creating a new pod to attach PV again to the node")
	pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		ginkgo.By("Delete pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By(fmt.Sprintf("Verify volume after expansion: %s is attached to the node: %s",
		pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	ginkgo.By("Verify after expansion the filesystem type is as expected")
	cmd[1] = pod.Name
	lastOutput = framework.RunKubectlOrDie(namespace, cmd...)
	gomega.Expect(strings.Contains(lastOutput, ext4FSType)).NotTo(gomega.BeFalse())

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	framework.ExpectNoError(err, "while waiting for fs resize to finish")

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	ginkgo.By("Verify filesystem size for mount point /mnt/volume1 after expansion")
	fsSize, err := getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Filesystem size may be smaller than the size of the block volume.
	// Here since filesystem was already formatted on the original volume,
	// we can compare the new filesystem size with the original filesystem size.
	if fsSize < originalFsSize {
		framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
	}

	ginkgo.By("Checking data consistency after PVC resize")
	_ = framework.RunKubectlOrDie(namespace, "cp",
		fmt.Sprintf("%v/%v:/mnt/volume1/testdata", namespace, pod.Name), testdataFile+"_pod")
	defer func() {
		op, err = exec.Command("rm", "-f", testdataFile+"_pod").Output()
		fmt.Println("rm: ", op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	ginkgo.By("Running diff...")
	op, err = exec.Command("diff", testdataFile, testdataFile+"_pod").Output()
	fmt.Println("diff: ", op)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(len(op)).To(gomega.BeZero())

	ginkgo.By("File system resize finished successfully in GC")
	ginkgo.By("Checking for PVC resize completion on SVC PVC")
	_, err = waitForFSResizeInSvc(svcPVCName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

}

// verifyVolumeMetadataOnStatefulsets verifies sts pod replicas and tkg annotations and
// node affinities on svc pvc and verify cns volume meetadata
func verifyVolumeMetadataOnStatefulsets(client clientset.Interface, ctx context.Context, namespace string,
	statefulset *appsv1.StatefulSet, replicas int32, allowedTopologyHAMap map[string][]string,
	categories []string, storagePolicyName string, nodeList *v1.NodeList, f *framework.Framework) {
	// Waiting for pods status to be Ready
	fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
	gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
	ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
	gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
		fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
	gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
		"Number of Pods in the statefulset should match with number of replicas")

	ginkgo.By("Verify GV PV and SV PV has has required PV node affinity details")
	ginkgo.By("Verify SV PVC has TKG HA annotations set")
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
				gomega.Expect(*svcPVC.Spec.StorageClassName == storagePolicyName).To(
					gomega.BeTrue(), "SV Pvc storageclass does not match with SV storageclass")
				framework.Logf("GC PVC's storageclass matches SVC PVC's storageclass")

				verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod,
					nodeList, svcPVC, pv, svcPVCName)

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

				verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod,
					nodeList, svcPVC, pv, svcPVCName)

				framework.Logf(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
				var vmUUID string
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
					crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

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

}
