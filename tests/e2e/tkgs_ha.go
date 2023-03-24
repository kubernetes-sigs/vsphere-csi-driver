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
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var _ = ginkgo.Describe("[csi-tkgs-ha] Tkgs-HA-SanityTests", func() {
	f := framework.NewDefaultFramework("e2e-tkgs-ha")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                     clientset.Interface
		namespace                  string
		scParameters               map[string]string
		allowedTopologyHAMap       map[string][]string
		categories                 []string
		zonalPolicy                string
		zonalWffcPolicy            string
		isVsanHealthServiceStopped bool
		isSPSServiceStopped        bool
		sshWcpConfig               *ssh.ClientConfig
		svcMasterIp                string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		scParameters = make(map[string]string)
		topologyHaMap := GetAndExpectStringEnvVar(topologyHaMap)
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

		svcMasterIp = GetAndExpectStringEnvVar(svcMasterIP)
		svcMasterPwd := GetAndExpectStringEnvVar(svcMasterPassword)
		framework.Logf("svc master ip: %s", svcMasterIp)
		sshWcpConfig = &ssh.ClientConfig{
			User: rootUser,
			Auth: []ssh.AuthMethod{
				ssh.Password(svcMasterPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
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

	/*
		Stateful set - storage class with Zonal storage and
		Immediate and with parallel pod management policy with nodeAffinity
		1. Create a zonal storage policy, on the datastore that is shared only to specific cluster
		2. Use the Zonal storage class and Immediate binding mode and create statefulset
			with parallel pod management policy with replica 3
		3. wait for all the gc-PVC to bound - Make sure corresponding SVC-PVC will
			have "csi.vsphere.volume-accessible-topology" annotation
			csi.vsphere.requested.cluster-topology=
			[{"topology.kubernetes.io/zone":"zone1"},{"topology.kubernetes.io/zone":"zone2"},
			{"topology.kubernetes.io/zone":"zone2"}]
		4. storageClassName: should point to gcStorageclass
		5. Wait for the PODs to reach running state - make sure Pod scheduled on appropriate nodes
			preset in the availability zone
		6. Describe SVC-PV , and GC-PV  and verify node affinity, make sure appropriate node affinity gets added
		7. Scale up the statefulset replica to 5 , and validate the node affinity on
		   the newly create PV's and annotations on PVC's
		8. Validate the CNS metadata
		9. Scale down the sts to 0
		10.Delete PVC,POD,SC
	*/
	ginkgo.It("Stateful set - storage class with Zonal storage and Immediate and"+
		" with parallel pod management policy with nodeAffinity", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		nodeList, _ := fnodes.GetReadySchedulableNodes(client)

		ginkgo.By("Create statefulset with parallel pod management policy with replica 3")
		createResourceQuota(client, namespace, rqLimit, zonalPolicy)
		scParameters[svStorageClassName] = zonalWffcPolicy
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
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
		ginkgo.By("Creating statefulset with node affinity")
		allowedTopologies := getTopologySelector(allowedTopologyHAMap, categories,
			tkgshaTopologyLevels)
		framework.Logf("allowedTopo: %v", allowedTopologies)
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
		statefulset.Spec.Template.Spec.Affinity = new(v1.Affinity)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity = new(v1.NodeAffinity)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity.
			RequiredDuringSchedulingIgnoredDuringExecution = new(v1.NodeSelector)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity.
			RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = getNodeSelectorTerms(allowedTopologies)
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

		ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
		ginkgo.By("Verify pod gets scheduled on appropriate nodes preset in the availability zone")
		verifyVolumeMetadataOnStatefulsets(client, ctx, namespace, statefulset, replicas,
			allowedTopologyHAMap, categories, zonalPolicy, nodeList, f)

	})

	/*
		Provision volume with zonal storage when no resource quota available
		1. Create a zonal storage policy.
		2. Delete the resource quota assigned to zonal storage class.
		3. Use the Zonal storage class and WaitForFirstConsumer binding mode and create statefulset
			with parallel pod management policy with replica 3.
		4. Statefulset pods and pvc should be in pending state.
		5. Increase the resource quota of the zonal SC.
		6. Wait for some time and make sure all the PVC
		   and POD's of sts are bound and in running state
		7. wait for all the gc-PVC to bound - Make sure corresponding SVC-PVC
		   will have "csi.vsphere.volume-accessible-topology" annotation
			csi.vsphere.guestcluster-topology=
			[{"topology.kubernetes.io/zone":"zone1"},{"topology.kubernetes.io/zone":"zone2"},
			{"topology.kubernetes.io/zone":"zone2"}]
		8. storageClassName: should point to gcStorageclass.
		9. Wait for the PODs to reach running state - make sure Pod scheduled on appropriate nodes
			preset in the availability zone.
		10. Describe SVC-PV , and GC-PV  and verify node affinity, make sure appropriate node affinity gets added.
		11. Scale up the statefulset replica to 5 , and validate the node affinity
		   on the newly create PV's and annotations on PVC's.
		12. Validate the CNS metadata.
		13. Scale down the sts to 0.
		14.Delete PVC,POD,SC.
	*/
	ginkgo.It("Provision volume with zonal storage when no resource quota available",
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ginkgo.By("CNS_TEST: Running for GC setup")
			nodeList, _ := fnodes.GetReadySchedulableNodes(client)

			scParameters[svStorageClassName] = zonalPolicy
			storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			// Creating StatefulSet service
			ginkgo.By("Creating service")
			service := CreateService(namespace, client)
			defer func() {
				deleteService(namespace, client, service)
			}()

			ginkgo.By("Decrease SVC storage policy resource quota")
			svcClient, svNamespace := getSvcClientAndNamespace()
			quotaName := svcNamespace + "-storagequota"
			framework.Logf("quotaName: %s", quotaName)
			resourceQuota := newTestResourceQuota(quotaName, "10Mi", zonalPolicy)
			resourceQuota, err = svcClient.CoreV1().ResourceQuotas(svNamespace).Update(
				ctx, resourceQuota, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Create Resource quota: %+v", resourceQuota))
			framework.Logf("Sleeping for 15 seconds to claim resource quota fully")
			time.Sleep(time.Duration(15) * time.Second)

			ginkgo.By("Create statefulset with parallel pod management policy with replica 1")
			statefulset := GetStatefulSetFromManifest(namespace)
			ginkgo.By("Creating statefulset")
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
			*statefulset.Spec.Replicas = 1
			replicas := *(statefulset.Spec.Replicas)

			_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
			framework.Logf("Error from creating statefulset when no resource quota available is: %v", err)
			//gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

			ginkgo.By("PVC and POD creations should be in pending state" +
				" since there is no resourcequota")
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

			ginkgo.By("Increase SVC storagepolicy resource quota")
			framework.Logf("quotaName: %s", quotaName)
			resourceQuota = newTestResourceQuota(quotaName, rqLimit, zonalPolicy)
			resourceQuota, err = svcClient.CoreV1().ResourceQuotas(svNamespace).Update(
				ctx, resourceQuota, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("ResourceQuota details: %+v", resourceQuota))
			framework.Logf("Sleeping for 15 seconds to claim resource quota fully")
			time.Sleep(time.Duration(15) * time.Second)

			ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
			ginkgo.By("Verify pod gets scheduled on appropriate nodes preset in the availability zone")
			verifyVolumeMetadataOnStatefulsets(client, ctx, namespace, statefulset, replicas,
				allowedTopologyHAMap, categories, zonalPolicy, nodeList, f)

		})

	/*
		Create PVC using zonal storage and deploy deployment POD
		1. Create a zonal storage policy
		2. Use the Zonal storage class and Immediate binding mode and gc-PVC
		3. wait for all the gc-PVC to bound
		4. Create Deployment POD using the above gc-PVC
		5. Wait  deployment to reach running state
		6. Describe gc-PV and verify node affinity
		7. Verify that POD's should come up on the nodes of appropriate Availability zone
		8. Delete deployment,POD,SC
	*/
	ginkgo.It("Create PVC using zonal storage and deploy deployment POD",
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ginkgo.By("CNS_TEST: Running for GC setup")
			nodeList, _ := fnodes.GetReadySchedulableNodes(client)

			createResourceQuota(client, namespace, rqLimit, zonalPolicy)
			scParameters[svStorageClassName] = zonalWffcPolicy
			storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By("Creating PVC")

			pvclaim, err := createPVC(client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaims = append(pvclaims, pvclaim)

			ginkgo.By("Expect the pvc to provision volume successfully")
			_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			labelsMap := make(map[string]string)
			labelsMap["app"] = "test"

			ginkgo.By("Creating deployment with PVC created earlier")
			deployment, err := createDeployment(
				ctx, client, 1, labelsMap, nil, namespace, pvclaims, "", false, busyBoxImageOnGcr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pods, err := fdep.GetPodsForDeployment(client, deployment)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod := pods.Items[0]
			err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

			ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
			ginkgo.By("Verify pod gets scheduled on appropriate nodes preset in the availability zone")
			verifyVolumeMetadataOnDeployments(ctx, client, deployment, namespace, allowedTopologyHAMap,
				categories, nodeList, zonalPolicy)

		})

	/*
		Re-start GC-CSI during sts creation
		1. Create a zonal storage policy, on the datastore that is shared only to specific cluster
		2. Use the Zonal storage class and Immediate binding mode and create statefulset
			with parallel pod management policy with replica 3
		3. Restart gc-csi and SVC CSI
		4. wait for all the gc-PVC to bound - Make sure corresponding SVC-PVC will
		    have "csi.vsphere.volume-accessible-topology" annotation
			csi.vsphere.requested.cluster-topology=
			[{"topology.kubernetes.io/zone":"zone1"},{"topology.kubernetes.io/zone":"zone2"},
			{"topology.kubernetes.io/zone":"zone2"}]
		5. storageClassName: should point to gcStorageclass
		6. Wait for the PODs to reach running state - make sure Pod scheduled on appropriate nodes
			preset in the availability zone
		7. Describe SVC-PV , and GC-PV  and verify node affinity, make sure appropriate node affinity gets added
		8. scale up the sts to 5, and validate the node affinity on the newly create PV's and annotations on PVC's
		9. Validate the CNS metadata
		10. Scale down the sts to 0
		11.Delete PVC,POD,SC
	*/
	ginkgo.It("Re-start GC-CSI during sts creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		nodeList, _ := fnodes.GetReadySchedulableNodes(client)

		ginkgo.By("Create statefulset with parallel pod management policy with replica 3")
		createResourceQuota(client, namespace, rqLimit, zonalPolicy)
		scParameters[svStorageClassName] = zonalWffcPolicy
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		svClient, _ := getSvcClientAndNamespace()
		csiNamespace := GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas := *csiDeployment.Spec.Replicas
		svcCsiDeployment, err := svClient.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		svcCsiReplicas := *svcCsiDeployment.Spec.Replicas

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

		ginkgo.By("Restarting GC CSI and SVC CSI")
		_ = updateDeploymentReplica(client, 0, vSphereCSIControllerPodNamePrefix, csiNamespace)
		_ = updateDeploymentReplica(svClient, 0, vSphereCSIControllerPodNamePrefix, csiNamespace)

		_ = updateDeploymentReplica(svClient, svcCsiReplicas, vSphereCSIControllerPodNamePrefix, csiNamespace)
		_ = updateDeploymentReplica(client, csiReplicas, vSphereCSIControllerPodNamePrefix, csiNamespace)

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

		ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
		ginkgo.By("Verify pod gets scheduled on appropriate nodes preset in the availability zone")
		verifyVolumeMetadataOnStatefulsets(client, ctx, namespace, statefulset, replicas,
			allowedTopologyHAMap, categories, zonalPolicy, nodeList, f)

	})

	/*
		Verify the behaviour when CSI Provisioner is deleted during statefulset creation
		1. Identify the Pod where CSI Provisioner is the leader.
		2. create storage policy on a shared datastore and assign it to gc-namespace
		3. Add resource quota to Storageclass
		4. Using the cross-zonal SC with immediate binding mode ,
		   Create 5 statefulset with parallel POD management policy each with 3 replica's
		5. While the Statefulsets is creating PVCs and Pods, kill CSI Provisioner container
		   identified in the step 1, where CSI provisioner is the leader.
		   csi-provisioner in other replica should take the leadership to help provisioning of the volume.
		6. Wait until all gc-PVCs and gc-Pods are created for Statefulsets.
		7. Expect all PVCs for Statefulsets to be in the bound state.
		8. Verify node affinity details on PV's.
		9. Expect all Pods for Statefulsets to be in the running state.
		10.Describe PV and Verify the node affinity rule . Make sure node affinity
		   should contain appropriate topology details.
		11.POD should be running in the appropriate nodes.
		12.Scale Down replica count 5 and verify node affinity and cns volume metadata.
		13.Delete Stateful set PVC's.
		14.All the PVC's and PV's should get deleted. No orphan volumes should be left on the system.
		15.Delete Statefulsets.
	*/
	ginkgo.It("Verify the behaviour when CSI Provisioner is deleted during statefulset creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		nodeList, err := fnodes.GetReadySchedulableNodes(client)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		var statefulSetReplicaCount int32 = 3
		var stsList []*appsv1.StatefulSet
		stsCount := 5

		framework.Logf("sshwcpConfig: %v", sshWcpConfig)
		csiControllerpod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshWcpConfig, provisionerContainerName)
		framework.Logf("%s leader is running on pod %s "+
			"which is running on master node %s", provisionerContainerName, csiControllerpod, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create statefulset with parallel pod management policy with replica 3")
		createResourceQuota(client, namespace, rqLimit, zonalPolicy)
		scParameters[svStorageClassName] = zonalPolicy
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Creating StatefulSet service
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		// Create multiple StatefulSets Specs in parallel
		ginkgo.By("Creating multiple StatefulSets")

		ginkgo.By("During statefulset creation, kill CSI provisioner container")
		for i := 0; i < stsCount; i++ {
			statefulset := GetStatefulSetFromManifest(namespace)
			ginkgo.By("Creating statefulset")
			statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
			statefulset.Name = "sts-" + strconv.Itoa(i) + "-" + statefulset.Name
			statefulset.Spec.Template.Labels["app"] = statefulset.Name
			statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
			*statefulset.Spec.Replicas = statefulSetReplicaCount
			CreateStatefulSet(namespace, statefulset, client)
			stsList = append(stsList, statefulset)
			if i == 2 {
				ginkgo.By("Kill container CSI-Provisioner on the master node where elected leader " +
					"csi controller pod is running")
				err = execStopContainerOnGc(sshWcpConfig, svcMasterIp,
					provisionerContainerName, k8sMasterIP, svcNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

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

		for i := 0; i < len(stsList); i++ {
			verifyVolumeMetadataOnStatefulsets(client, ctx, namespace, stsList[i], statefulSetReplicaCount,
				allowedTopologyHAMap, categories, zonalPolicy, nodeList, f)
		}

	})

	/*
		Verify the behaviour when CSI Attacher is deleted during statefulset creation
		1. Identify the Pod where CSI attacher is the leader.
		2. create storage policy on a shared datastore and assign it to gc-namespace
		3. Add resource quota to Storageclass
		4. Using the zonal SC with WaitForFirstConsumer binding mode ,
		   Create 5 statefulset with parallel POD management policy each with 3 replica's
		5. While the Statefulsets is creating PVCs and Pods, kill csi-attacher container
		   identified in the step 1, where csi-attacher is the leader.
		   csi-attacher in other replica should take the leadership to help provisioning of the volume.
		6. Wait until all gc-PVCs and gc-Pods are created for Statefulsets.
		7. Expect all PVCs for Statefulsets to be in the bound state.
		8. Verify node affinity details on PV's.
		9. Expect all Pods for Statefulsets to be in the running state.
		10.Describe PV and Verify the node affinity rule . Make sure node affinity
		   should contain appropriate topology details.
		11.POD should be running in the appropriate nodes.
		12.Identify the Pod where CSI attacher is the leader.
		13.Scale down the Statefulsets replica count to 5 , During scale down
		   delete CSI controller POD identified where csi attacher leaader is running.
		14.Wait until the POD count goes down to 5
		   csi-Attacher in other replica should take the leadership to detach Volume
		15.Delete Statefulsets and Delete PVCs.
	*/
	ginkgo.It("Verify the behaviour when CSI Attacher is deleted during statefulset creation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		nodeList, err := fnodes.GetReadySchedulableNodes(client)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		var statefulSetReplicaCount int32 = 10
		stsCount := 5
		var stsList []*appsv1.StatefulSet

		csiControllerpod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshWcpConfig, attacherContainerName)
		framework.Logf("%s leader is running on pod %s "+
			"which is running on master node %s", attacherContainerName, csiControllerpod, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

		// Create multiple StatefulSets Specs in parallel
		ginkgo.By("Creating multiple StatefulSets")

		ginkgo.By("During statefulset creation, kill CSI Attacher container")
		for i := 0; i < stsCount; i++ {
			//go createParallelStatefulSets(client, namespace, statefulSets[i],
			//	statefulSetReplicaCount, &wg)
			statefulset := GetStatefulSetFromManifest(namespace)
			ginkgo.By("Creating statefulset")
			statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
			statefulset.Name = "sts-" + strconv.Itoa(i) + "-" + statefulset.Name
			statefulset.Spec.Template.Labels["app"] = statefulset.Name
			statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
			*statefulset.Spec.Replicas = statefulSetReplicaCount
			CreateStatefulSet(namespace, statefulset, client)
			stsList = append(stsList, statefulset)
			if i == 2 {
				/* Kill container CSI-Provisioner on the master node where elected leader CSi-Controller-Pod
				is running */
				err = execStopContainerOnGc(sshWcpConfig, svcMasterIp,
					attacherContainerName, k8sMasterIP, svcNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

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

		var ssPods *v1.PodList
		// Waiting for pods status to be Ready
		for _, statefulset := range stsList {
			fss.WaitForStatusReadyReplicas(client, statefulset, statefulSetReplicaCount)
			gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
			ssPods = fss.GetPodList(client, statefulset)
			gomega.Expect(ssPods.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPods.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			ginkgo.By("Verify GV PV and SV PV has has required PV node affinity details")
			ginkgo.By("Verify SV PVC has TKG HA annotations set")
			// Get the list of Volumes attached to Pods before scale down
			for _, sspod := range ssPods.Items {
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
						gomega.Expect(*svcPVC.Spec.StorageClassName == zonalPolicy).To(
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
		}

		statefulSetReplicaCount = 5
		for i := 0; i < len(stsList); i++ {
			framework.Logf(fmt.Sprintf("Scaling down statefulset: %v to number of Replica: %v",
				stsList[i].Name, statefulSetReplicaCount))

			_, scaleDownErr := fss.Scale(client, stsList[i], statefulSetReplicaCount)
			gomega.Expect(scaleDownErr).NotTo(gomega.HaveOccurred())
			if i == 2 {
				ginkgo.By("During statefulset scale down, kill CSI attacher container")
				err = execStopContainerOnGc(sshWcpConfig, svcMasterIp,
					attacherContainerName, k8sMasterIP, svcNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		for _, statefulset := range stsList {
			fss.WaitForStatusReplicas(client, statefulset, statefulSetReplicaCount)
			fss.WaitForStatusReadyReplicas(client, statefulset, statefulSetReplicaCount)
			ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
			gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(statefulSetReplicaCount)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset %s, %v, should match with number of replicas %v",
				statefulset.Name, ssPodsAfterScaleDown.Size(), statefulSetReplicaCount,
			)

			// Get the list of Volumes attached to Pods before scale down
			for _, sspod := range ssPodsAfterScaleDown.Items {
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

						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						//vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
						//gomega.Expect(err).NotTo(gomega.HaveOccurred())
						verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
							crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

						framework.Logf("Verify volume: %s is detached from PodVM with vmUUID: %s",
							pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)

						isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
							pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
							fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
						ginkgo.By("Waiting for CnsNodeVMAttachment controller to reconcile resource")
						verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
							crdCNSNodeVMAttachment, crdVersion, crdGroup, false)

						// Verify the attached volume match the one in CNS cache
						err = waitAndVerifyCnsVolumeMetadata4GCVol(volHandle, svcPVCName, pvclaim,
							pv, pod)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

					}
				}
			}

		}
	})

	/*
		Verify the behaviour when CSI-resizer deleted and VSAN-Health is down during online Volume expansion
		1. Identify the Pod where CSI-resizer is the leader.
		2. create storage policy on a shared datastore and assign it to gc-namespace
		3. Add resource quota to Storageclass
		4. Using the cross-zonal SC with immediate binding mode
		5. Create multiple PVC's (around 10) using above SC
		6. Verify node affinity details on PV's
		7. Create multiple Pod's using the PVC's Created in the Step 3.
		8. Bring down vsan-health service ( Login to VC and execute : service-control --stop vsan-health)
		9. Trigger online volume expansion on all the gc-PVC's. At the same time delete the Pod identified in the Step 1
		10.Expand volume should fail with error service unavailable
		11.Bring up the VSAN-health ( Login to VC and execute : service-control --start vsan-health)
		12.Expect Volume should be expanded by the newly elected csi-resizer leader,
		   and filesystem for the volume on the pod should also be expanded.
		13.Describe PV and Verify the node affinity rule . Make sure node affinity has appropriate topology details
		14.POD should be running in the appropriate nodes
		15.Delete Pod, PVC and SC
	*/
	ginkgo.It("Verify the behaviour when CSI-resizer deleted and VSAN-Health is down"+
		" during online Volume expansion", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		nodeList, err := fnodes.GetReadySchedulableNodes(client)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		volumeOpsScale := 10
		var podList []*v1.Pod
		var originalSizes []int64

		csiControllerPod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshWcpConfig, resizerContainerName)
		framework.Logf("%s leader is running on pod %s "+
			"which is running on master node %s", resizerContainerName, csiControllerPod, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create 10 PVCs with with zonal SC")
		createResourceQuota(client, namespace, rqLimit, zonalPolicy)
		scParameters[svStorageClassName] = zonalPolicy
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, volumeOpsScale)
		_, err = fpv.WaitForPVClaimBoundPhase(client,
			pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for _, pvclaim := range pvclaimsList {
				pv := getPvFromClaim(client, namespace, pvclaim.Name)
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
			}
		}()

		ginkgo.By("Wait for GC PVCs to come to bound state and create POD for each PVC")
		for _, pvc := range pvclaimsList {
			pv := getPvFromClaim(client, namespace, pvc.Name)
			volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			svcPVCName := pv.Spec.CSI.VolumeHandle
			svcPVC := getPVCFromSupervisorCluster(svcPVCName)
			ginkgo.By("Verify SV storageclass points to GC storageclass")
			gomega.Expect(*svcPVC.Spec.StorageClassName == storageclass.Name).To(
				gomega.BeTrue(), "SV storageclass does not match with gc storageclass")
			framework.Logf("GC PVC's storageclass matches SVC PVC's storageclass")

			ginkgo.By("Create a pod and wait for it to come to Running state")
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("created pod: %s with pvc name: %s", pod.Name, pvc.Name)
			podList = append(podList, pod)

			defer func() {
				ginkgo.By("Delete pod")
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
			ginkgo.By("Verify pod gets scheduled on appropriate nodes preset in the availability zone")
			verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod, nodeList, svcPVC, pv, svcPVCName)
		}

		// Stopping vsan-health service on vcenter host
		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", vsanhealthServiceName))
		isVsanHealthServiceStopped = true
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort

		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if isVsanHealthServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
			}
		}()

		// Expanding pvc when vsan-health service on vcenter host is down
		ginkgo.By("Expanding pvc when vsan-health service on vcenter host is down")
		for i := 0; i < len(pvclaimsList); i++ {
			currentPvcSize := pvclaimsList[i].Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("1Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			pvclaim, err := expandPVCSize(pvclaimsList[i], newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvclaim).NotTo(gomega.BeNil())

			originalSizeInMb, err := getFSSizeMb(f, podList[i])
			framework.Logf("original size : %s", originalSizeInMb)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			originalSizes = append(originalSizes, originalSizeInMb)

			// File system resize should not succeed Since Vsan-health is down. Expect an error
			ginkgo.By("File system resize should not succeed Since Vsan-health is down. Expect an error")
			expectedErrMsg := "didn't find a plugin capable of expanding the volume"
			framework.Logf("Expected failure message: %+q", expectedErrMsg)
			err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if i == 1 {
				ginkgo.By("Delete elected leader Csi-Controller-Pod where CSi-Attacher is running")
				err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csiControllerPod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
		// Starting vsan-health service on vcenter host
		ginkgo.By("Bringup vsanhealth service")
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		/* Get current leader Csi-Controller-Pod where CSI Resizer is running" +
		find master node IP where this Csi-Controller-Pod is running */
		ginkgo.By("Get current Leader Csi-Controller-Pod where CSI Resizer is running and " +
			"find the master node IP where this Csi-Controller-Pod is running")
		csiControllerPod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshWcpConfig, resizerContainerName)
		framework.Logf("CSI-Resizer is running on elected Leader Pod %s "+
			"which is running on master node %s", csiControllerPod, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Expanding pvc when vsan-health service on vcenter host is started
		ginkgo.By("Expanding pvc when vsan-health service on vcenter host is started")
		for i := 0; i < len(pvclaimsList); i++ {
			pvclaim := pvclaimsList[i]
			pv := getPvFromClaim(client, namespace, pvclaimsList[i].Name)
			volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			svcPVCName := pv.Spec.CSI.VolumeHandle
			framework.Logf("pvc: %s, pod: %s", pvclaimsList[i].Name, podList[i].Name)

			_, err = waitForFSResizeInSvc(svcPVCName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for file system resize to finish")
			pvclaim, err = waitForFSResize(pvclaim, client)
			framework.ExpectNoError(err, "while waiting for fs resize to finish")

			pvcConditions := pvclaim.Status.Conditions
			expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if len(queryResult.Volumes) == 0 {
				err = fmt.Errorf("QueryCNSVolumeWithResult returned no volume")
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			var fsSize int64
			ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
			fsSize, err = getFSSizeMb(f, podList[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("File system size after expansion : %s", fsSize)
			// Filesystem size may be smaller than the size of the block volume
			// so here we are checking if the new filesystem size is greater than
			// the original volume size as the filesystem is formatted for the
			// first time.
			gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizes[i]),
				fmt.Sprintf("error updating filesystem size for %q."+
					"Resulting filesystem size is %d", pvclaim.Name, fsSize))
			ginkgo.By("File system resize finished successfully")

			framework.Logf("Online volume expansion in GC PVC is successful")
		}
	})

	/*
		Verify the behaviour when CSI-resizer deleted during offline volume expansion
		1. Identify the Pod where CSI-resizer is the leader.
		2. create storage policy on a shared datastore and assign it to gc-namespace
		3. Add resource quota to Storageclass
		4. Using the cross-zonal SC with immediate binding mode
		5. Create multiple PVC's (around 10) using above SC
		6. Verify node affinity details on PV's
		7. Create multiple Pod's using the PVC's Created in the Step 3.
		8. Trigger offline volume expansion on all the gc-PVC's. At the same time delete
			the Pod identified in the Step 1
		9.Expect Volume should be expanded by the newly elected csi-resizer leader, and
		   filesystem for the volume on the pod should also be expanded.
		10.Describe PV and Verify the node affinity rule . Make sure node affinity
		   has appropriate topology details
		11.POD should be running in the appropriate nodes
		12.Delete Pod, PVC and SC
	*/
	ginkgo.It("Verify the behaviour when CSI-resizer deleted during offline volume expansion",
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ginkgo.By("CNS_TEST: Running for GC setup")
			nodeList, err := fnodes.GetReadySchedulableNodes(client)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}
			volumeOpsScale := 10
			var podList []*v1.Pod

			csiControllerPod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				client, sshWcpConfig, resizerContainerName)
			framework.Logf("%s leader is running on pod %s "+
				"which is running on master node %s", resizerContainerName, csiControllerPod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create 10 PVCs with with zonal SC")
			createResourceQuota(client, namespace, rqLimit, zonalPolicy)
			scParameters[svStorageClassName] = zonalPolicy
			storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, volumeOpsScale)
			_, err = fpv.WaitForPVClaimBoundPhase(client,
				pvclaimsList, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				for _, pvclaim := range pvclaimsList {
					pv := getPvFromClaim(client, namespace, pvclaim.Name)
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
				}
			}()

			ginkgo.By("Wait for GC PVCs to come to bound state and create POD for each PVC")
			for _, pvc := range pvclaimsList {
				pv := getPvFromClaim(client, namespace, pvc.Name)
				volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				svcPVCName := pv.Spec.CSI.VolumeHandle
				svcPVC := getPVCFromSupervisorCluster(svcPVCName)
				ginkgo.By("Verify SV storageclass points to GC storageclass")
				gomega.Expect(*svcPVC.Spec.StorageClassName == storageclass.Name).To(
					gomega.BeTrue(), "SV storageclass does not match with gc storageclass")
				framework.Logf("GC PVC's storageclass matches SVC PVC's storageclass")

				ginkgo.By("Create a pod and wait for it to come to Running state")
				pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				podList = append(podList, pod)

				defer func() {
					ginkgo.By("Delete pod")
					err = fpod.DeletePodWithWait(client, pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}()

				ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
				ginkgo.By("Verify pod gets scheduled on appropriate nodes preset in the availability zone")
				verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod, nodeList, svcPVC, pv, svcPVCName)
			}

			// Expanding pvc when vsan-health service on vcenter host is down
			ginkgo.By("Expanding pvc and deleting pod where csi-resizer leader is present")
			for i := 0; i < len(pvclaimsList); i++ {
				pv := getPvFromClaim(client, namespace, pvclaimsList[i].Name)
				volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				svcPVCName := pv.Spec.CSI.VolumeHandle
				verifyOfflineVolumeExpansionOnGc(client, pvclaimsList[i],
					svcPVCName, namespace, volHandle, podList[i], pv, f)

				if i == 4 {
					ginkgo.By("Delete elected leader Csi-Controller-Pod where CSi-Attacher is running")
					err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csiControllerPod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}

		})

	/*
		Verify the behaviour when CSI syncer is deleted and check fullsync
		1. Identify the Pod where CSI syncer is the leader.
		2. create storage policy on a shared datastore and assign it to gc-namespace
		3. Add resource quota to Storageclass
		4. Using the cross-zonal SC with immediate binding mode
		5. Create FCD on the shared datastore accessible to all nodes.
		6. Create PV/PVC Statically using the above FCD and using reclaim policy retain.
		7. At the same time kill CSI syncer container identified in the Step 1.
		8. Syncer container in other replica should take leadership and take over tasks for pushing metadata of the volumes.
		9. Create dynamic PVC's where reclaim policy is delete
		10.Verify node affinity details on PV's
		11.Create one POD's, one using both static PVC's and another one using dynamic PVC's.
		12.Wait for POD to be in running state.
		13.Delete POD 's
		14.Delete PVC where reclaim policy is retain
		15.Delete claim ref in PV's which are in released state and wait till it reaches available state.
		16.Re-create PVC using reclaim PV which is in Available state
		17.Create two PODone using static PVC and another using dynamic PVC
		18.Wait for two full sync cycle
		19.Expect all volume metadata, PVC metadata, Pod metadata should be present on the CNS.
		20.Delete the POD's , PVC's and PV's
	*/
	ginkgo.It("Verify the behaviour when CSI syncer is deleted and check fullsync",
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ginkgo.By("CNS_TEST: Running for GC setup")
			nodeList, err := fnodes.GetReadySchedulableNodes(client)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}

			var pods []*v1.Pod

			csiControllerPod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
				client, sshWcpConfig, syncerContainerName)
			framework.Logf("%s leader is running on pod %s "+
				"which is running on master node %s", syncerContainerName, csiControllerPod, k8sMasterIP)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
			staticPod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{staticPvc}, false, "")
			pods = append(pods, staticPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				ginkgo.By("Delete pod")
				err = fpod.DeletePodWithWait(client, staticPod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			_, err = verifyPodLocationLevel5(staticPod, nodeList, allowedTopologyHAMap)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Creating Pvc with Immediate topology storageclass")
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
			pods = append(pods, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				ginkgo.By("Delete pod")
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
			ginkgo.By("Verify pod gets scheduled on appropriate nodes preset in the availability zone")
			verifyAnnotationsAndNodeAffinity(allowedTopologyHAMap, categories, pod, nodeList, svcPVC, pv, svcPVCName)

			// Deleting Pod's
			for i := 0; i < len(pods); i++ {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pods[i].Name, namespace))
				deletePodAndWaitForVolsToDetach(ctx, client, pods[i])
			}

			// Deleting PVC
			ginkgo.By("Delete static PVC")
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, staticPvc.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("PVC %s is deleted successfully", staticPvc.Name)
			// Verify PV exist and is in released status
			ginkgo.By("Check PV exists and is released")
			staticPv, err = waitForPvToBeReleased(ctx, client, staticPv.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("PV status after deleting PVC: %s", staticPv.Status.Phase)
			// Remove claim from PV and check its status.
			ginkgo.By("Remove claimRef from PV")
			staticPv.Spec.ClaimRef = nil
			staticPv, err = client.CoreV1().PersistentVolumes().Update(ctx, staticPv, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("PV status after removing claim : %s", staticPv.Status.Phase)

			// Recreate PVC with same name as created above
			ginkgo.By("ReCreating the PVC")
			newStaticPvclaim := getPersistentVolumeClaimSpec(namespace, nil, staticPv.Name)
			newStaticPvclaim.Spec.StorageClassName = &storageclass.Name
			newStaticPvclaim.Name = staticPvc.Name + "-recreated"
			newStaticPvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, newStaticPvclaim,
				metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Wait for newly created PVC to bind to the existing PV
			ginkgo.By("Wait for the PVC to bind the lingering pv")
			err = fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, staticPv,
				newStaticPvclaim)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := fpv.DeletePersistentVolumeClaim(client, newStaticPvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			}()

			// Creating new Pod using static pvc
			var newPods []*v1.Pod
			ginkgo.By("Creating new Pod using static pvc")
			newstaticPod, err := createPod(client, namespace, nil,
				[]*v1.PersistentVolumeClaim{newStaticPvclaim}, false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			newPods = append(newPods, newstaticPod)
			ginkgo.By("Creating new Pod using dynamic pvc")
			newDynamicPod, err := createPod(client, namespace, nil, pvclaims, false, "")
			newPods = append(newPods, newDynamicPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				for _, pod := range newPods {
					ginkgo.By("Deleting the Pod")
					deletePodAndWaitForVolsToDetach(ctx, client, pod)
				}
			}()

			// verify volume is attached to the node
			ginkgo.By("Verify volume is attached to the node for static pod")
			vmUUID, err := getVMUUIDFromNodeName(newstaticPod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			framework.Logf("Sleeping for 5 mins")
			time.Sleep(5 * time.Minute)

			// verify volume is attached to the node
			ginkgo.By("Verify volume is attached to the node for dynamic pod")
			vmUUID, err = getVMUUIDFromNodeName(newDynamicPod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			restConfig := getRestConfigClient()
			cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			enableFullSyncTriggerFss(ctx, client, csiSystemNamespace, fullSyncFss)
			triggerFullSync(ctx, client, cnsOperatorClient)

			// Verify volume metadata for static POD, PVC and PV
			ginkgo.By("Verify volume metadata for static POD, PVC and PV")
			err = waitAndVerifyCnsVolumeMetadata4GCVol(volumeID, staticPv.Spec.CSI.VolumeHandle,
				newStaticPvclaim, staticPv, newstaticPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verify volume metadata for dynamic POD, PVC and PV
			ginkgo.By("Verify volume metadata for dynamic POD, PVC and PV")
			err = waitAndVerifyCnsVolumeMetadata4GCVol(volHandle, svcPVCName, pvclaim,
				pv, newDynamicPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	/*
		Verify the behaviour when SPS service is down along with CSI Provisioner
		1. Identify the process where CSI Provisioner is the leader.
		2. create storage policy on a shared datastore and assign it to gc-namespace
		3. Add resource quota to Storageclass
		4. Using the zonal SC with immediate binding mode
		5. Bring down SPS service (service-control --stop sps)
		6. Using the above created SC , Create around 5 statefulsets with parallel POD
			management policy each with 10 replica's
		7. While the Statefulsets is creating PVCs and Pods, delete the CSI controller
			Pod identified in the step 1, where CSI provisioner is the leader.
			csi-provisioner in other replica should take the leadership to help provisioning
			of the volume.
		8. Bring up SPS service (service-control --start sps)
		9. Wait until all PVCs and Pods are created for Statefulsets
		10.Expect all PVCs for Statefulsets to be in the bound state.
		11.Verify node affinity details on PV's
		12.Expect all Pods for Statefulsets to be in the running state
		13.Delete Statefulsets and Delete PVCs.
	*/
	ginkgo.It("Verify the behaviour when SPS service is down along with CSI Provisioner", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		nodeList, err := fnodes.GetReadySchedulableNodes(client)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		volumeOpsScale := 5
		var replicas int32 = 3
		var stsList []*appsv1.StatefulSet

		csiControllerPod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshWcpConfig, provisionerContainerName)
		framework.Logf("%s leader is running on pod %s "+
			"which is running on master node %s", provisionerContainerName, csiControllerPod, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		createResourceQuota(client, namespace, rqLimit, zonalPolicy)
		scParameters[svStorageClassName] = zonalPolicy
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Bring down SPS service
		ginkgo.By("Bring down SPS service")
		isSPSServiceStopped = true
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
		}()

		// Creating Service for StatefulSet
		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Trigger multiple StatefulSets creation in parallel. During StatefulSets " +
			"creation, in between delete elected leader Csi-Controller-Pod where CSI-Provisioner " +
			"is running")
		for i := 0; i < volumeOpsScale; i++ {
			statefulset := GetStatefulSetFromManifest(namespace)
			ginkgo.By("Creating statefulset")
			statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
			statefulset.Name = "sts-" + strconv.Itoa(i) + "-" + statefulset.Name
			statefulset.Spec.Template.Labels["app"] = statefulset.Name
			statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Annotations["volume.beta.kubernetes.io/storage-class"] = storageclass.Name
			*statefulset.Spec.Replicas = replicas
			_, err := client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			stsList = append(stsList, statefulset)
			if i == 2 {
				/* Delete elected leader CSi-Controller-Pod where CSI-Attacher is running */
				ginkgo.By("Delete elected leader CSi-Controller-Pod where CSI-Provisioner is running")
				err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csiControllerPod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}

		ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI " +
			"Provisioner is running and find the master node IP where " +
			"this Csi-Controller-Pod is running")
		csiControllerPod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshWcpConfig, provisionerContainerName)
		framework.Logf("%s is running on newly elected Leader Pod %s "+
			"which is running on master node %s", provisionerContainerName, csiControllerPod, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Bring up SPS service
		if isSPSServiceStopped {
			startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
		}

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

		ginkgo.By("Verify SVC PVC annotations and node affinities on GC and SVC PVs")
		for _, statefulset := range stsList {
			verifyVolumeMetadataOnStatefulsets(client, ctx, namespace, statefulset, replicas,
				allowedTopologyHAMap, categories, zonalPolicy, nodeList, f)
		}

	})

	/*
		verify Label update when syncer container goes down
		1. Identify the process where CSI syncer is the leader.
		2. create storage policy on a shared datastore and assign it to gc-namespace
		3. Add resource quota to Storageclass
		4. Using the cross-zonal SC with immediate binding mode
		5. Create multiple PVC's (around 10) using above SC
		6. Add labels to PVC's and PV's
		7. Delete the CSI process identified in the step 1,
		   where CSI syncer is the leader.
		8. csi-syncer in another replica should take the leadership to help label update.
		9. Verify CNS metadata for PVC's to check newly added labels
		10.Identify the process where CSI syncer is the leader
		11.Delete labels from PVC's and PV's
		12.Delete the CSI process identified in the step 8, where CSI syncer is the leader.
		13.Verify CNS metadata for PVC's and PV's , Make sure label entries should got removed.
	*/
	ginkgo.It("verify Label update when syncer container goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("CNS_TEST: Running for GC setup")
		nodeList, err := fnodes.GetReadySchedulableNodes(client)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		volumeOpsScale := 10
		labelKey := "app"
		labelValue := "e2e-labels"

		csiControllerPod, k8sMasterIP, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
			client, sshWcpConfig, syncerContainerName)
		framework.Logf("%s leader is running on pod %s "+
			"which is running on master node %s", syncerContainerName, csiControllerPod, k8sMasterIP)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create 10 PVCs with with zonal SC")
		createResourceQuota(client, namespace, rqLimit, zonalPolicy)
		scParameters[svStorageClassName] = zonalPolicy
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, storageclass, volumeOpsScale)
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for _, pvclaim := range pvclaimsList {
				pv := getPvFromClaim(client, namespace, pvclaim.Name)
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
			}
		}()

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		ginkgo.By("Wait for GC PVCs to come to bound state and create POD for each PVC")
		for i := 0; i < len(pvclaimsList); i++ {
			pvc := pvclaimsList[i]
			pv := pvs[i]
			volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			svcPVCName := pv.Spec.CSI.VolumeHandle
			svcPVC := getPVCFromSupervisorCluster(svcPVCName)
			ginkgo.By("Verify SV storageclass points to GC storageclass")
			gomega.Expect(*svcPVC.Spec.StorageClassName == storageclass.Name).To(
				gomega.BeTrue(), "SV storageclass does not match with gc storageclass")
			framework.Logf("GC PVC's storageclass matches SVC PVC's storageclass")

			ginkgo.By("Verify annotations on SVC PV and required node affinity details on SVC PV and GC PV")
			svcPV := getPvFromSupervisorCluster(svcPVCName)
			_, err = verifyVolumeTopologyForLevel5(pv, allowedTopologyHAMap)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("GC PV: %s has required Pv node affinity details", pv.Name)

			ginkgo.By("Verify SV PV has has required PV node affinity details")
			_, err = verifyVolumeTopologyForLevel5(svcPV, allowedTopologyHAMap)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("SVC PV: %s has required PV node affinity details", svcPV.Name)

			ginkgo.By(fmt.Sprintf("Updating labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
			pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvc.Labels = labels
			_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Updating labels %+v for pv %s", labels, pv.Name))
			pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pv.Labels = labels
			_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if i == 4 {
				ginkgo.By("Delete elected leader CSi-Controller-Pod where vsphere-syncer is running")
				err = deleteCsiControllerPodWhereLeaderIsRunning(ctx, client, csiControllerPod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				/* Get newly elected current leader Csi-Controller-Pod where CSI Syncer is running" +
				find new master node IP where this Csi-Controller-Pod is running */
				ginkgo.By("Get newly elected current Leader Csi-Controller-Pod where CSI Syncer is " +
					"running and find the master node IP where this Csi-Controller-Pod is running")
				csiControllerPod, k8sMasterIP, err = getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx,
					client, sshWcpConfig, syncerContainerName)
				framework.Logf("%s is running on elected Leader Pod %s which is running "+
					"on master node %s", syncerContainerName, csiControllerPod, k8sMasterIP)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		for i := 0; i < len(pvclaimsList); i++ {
			pvc := pvclaimsList[i]
			pv := pvs[i]
			volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

			ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
				labels, pvc.Name, pvc.Namespace))
			err = e2eVSphere.waitForLabelsToBeUpdated(volHandle,
				pvc.Labels, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s", labels, pv.Name))
			err = e2eVSphere.waitForLabelsToBeUpdated(volHandle,
				pv.Labels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		for i := 0; i < len(pvclaimsList); i++ {
			pvc := pvclaimsList[i]
			pv := pvs[i]
			volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

			ginkgo.By(fmt.Sprintf("Fetching updated pvc %s in namespace %s", pvc.Name, pvc.Namespace))
			pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Deleting labels %+v for pvc %s in namespace %s", labels, pvc.Name, pvc.Namespace))
			pvc.Labels = make(map[string]string)
			_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be deleted for pvc %s in namespace %s",
				labels, pvc.Name, pvc.Namespace))
			err = e2eVSphere.waitForLabelsToBeUpdated(volHandle,
				pvc.Labels, string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Fetching updated pv %s", pv.Name))
			pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Deleting labels %+v for pv %s", labels, pv.Name))
			pv.Labels = make(map[string]string)
			_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be deleted for pv %s", labels, pv.Name))
			err = e2eVSphere.waitForLabelsToBeUpdated(volHandle,
				pv.Labels, string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})
})
