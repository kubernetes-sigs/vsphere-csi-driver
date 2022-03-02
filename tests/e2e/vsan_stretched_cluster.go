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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

var _ = ginkgo.Describe("[vsan-stretch-vanilla] vsan stretched cluster tests", func() {
	f := framework.NewDefaultFramework("vsan-stretch")
	const defaultVolumeOpsScale = 30
	var (
		client                 clientset.Interface
		namespace              string
		nodeList               *v1.NodeList
		storagePolicyName      string
		scParameters           map[string]string
		storageClassName       string
		csiNs                  string
		fullSyncWaitTime       int
		volumeOpsScale         int
		storageThickPolicyName string
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))

		csiNs = GetAndExpectStringEnvVar(envCSINamespace)

		initialiseFdsVar(ctx)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		framework.ExpectNoError(err, "cluster not completely healthy")

		// TODO: verify csi pods are up

		nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		storageThickPolicyName = os.Getenv(envStoragePolicyNameWithThickProvision)
		if storageThickPolicyName == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		framework.Logf("Storageclass is %v and err is %v", sc, err)
		if (!strings.Contains(err.Error(), "not found")) && sc != nil {
			framework.ExpectNoError(client.StorageV1().StorageClasses().Delete(ctx, defaultNginxStorageClassName,
				*metav1.NewDeleteOptions(0)), "Unable to delete storage class "+defaultNginxStorageClassName)
		}
		scParameters = make(map[string]string)
		nodeList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "Unable to list k8s nodes")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find k8s nodes")
		}
		if os.Getenv("VOLUME_OPS_SCALE") != "" {
			volumeOpsScale, err = strconv.Atoi(os.Getenv(envVolumeOperationsScale))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			volumeOpsScale = defaultVolumeOpsScale
		}
		framework.Logf("VOLUME_OPS_SCALE is set to %d", volumeOpsScale)

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		err = client.StorageV1().StorageClasses().Delete(ctx, storageClassName, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	ginkgo.JustAfterEach(func() {
		if len(fds.hostsDown) > 0 {
			powerOnHostParallel(fds.hostsDown)
			fds.hostsDown = []string{}
		}
		if len(fds.hostsPartitioned) > 0 {
			toggleNetworkFailureParallel(fds.hostsPartitioned, false)
			fds.hostsPartitioned = []string{}
		}
		fds.primarySiteHosts = []string{}
		fds.secondarySiteHosts = []string{}
	})

	/*
		Primary site down
		Steps:
		1.	Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Create a statefulset, deployment with volumes from the stretched datastore
		3.	Bring down the primary site
		4.	Verify that the VMs hosted by esx servers are brought up on the other site
		5.	Verify that the k8s cluster is healthy and all the k8s constructs created in step 2 are running and volume
			and application lifecycle actions work fine
		6.	Bring primary site up and wait for testbed to be back to normal
		7.	Delete all objects created in step 2 and 5
	*/
	ginkgo.It("Primary site down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		var pvclaims []*v1.PersistentVolumeClaim

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating PVC")
		pvclaim, err := createPVC(client, namespace, nil, diskSize, sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims = append(pvclaims, pvclaim)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = storageClassName
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			"Unable to get list of Pods from the Statefulset: %v", statefulset.Name)
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset %s, %v, should match with number of required replicas %v",
			statefulset.Name, ssPodsBeforeScaleDown.Size(), replicas)

		// Get the list of Volumes attached to Pods before scale down
		var volumesBeforeScaleDown []string
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By("Creating Deployment")
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
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

		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the primary site")
		siteFailover(true)

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(true)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc1, err := createPVC(client, namespace, nil, diskSize, sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvs, err := fpv.WaitForPVClaimBoundPhase(
			client, []*v1.PersistentVolumeClaim{pvc1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc1}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpod.DeletePodWithWait(client, pod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		deletePodAndWaitForVolsToDetach(ctx, client, pod1)

		err = fpv.DeletePersistentVolumeClaim(client, pvc1.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleDownReplicas := replicas - 1
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", scaleDownReplicas))
		_, scaledownErr := fss.Scale(client, statefulset, scaleDownReplicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, scaleDownReplicas)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(scaleDownReplicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset %s, %v, should match with number of replicas %v",
			statefulset.Name, ssPodsAfterScaleDown.Size(), scaleDownReplicas,
		)

		// After scale down, verify vSphere volumes are detached from deleted pods
		ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			if err != nil {
				gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						if vanillaCluster {
							isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
								client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							gomega.Expect(err).NotTo(gomega.HaveOccurred())
							gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
								fmt.Sprintf("Volume %q is not detached from the node %q",
									pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
						} else {
							annotations := sspod.Annotations
							vmUUID, exists := annotations[vmUUIDLabel]
							gomega.Expect(exists).To(gomega.BeTrue(),
								fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

							ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
								pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
							gomega.Expect(err).To(gomega.HaveOccurred(),
								fmt.Sprintf(
									"PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
									vmUUID, sspod.Spec.NodeName))
						}
					}
				}
			}
		}

		// After scale down, verify the attached volumes match those in CNS Cache
		for _, sspod := range ssPodsAfterScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
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

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitForPodsReady(client, statefulset.Namespace, sspod.Name, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By("Verify scale up operation should not introduced new volume")
					gomega.Expect(contains(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)).To(gomega.BeTrue())
					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					var exists bool
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if vanillaCluster {
						vmUUID = getNodeUUID(ctx, client, sspod.Spec.NodeName)
					} else {
						annotations := pod.Annotations
						vmUUID, exists = annotations[vmUUIDLabel]
						gomega.Expect(exists).To(
							gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
						_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
					ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
					err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(true)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)

	})

	/*
	   Statefulset scale up/down while primary site goes down
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Create two statefulset with replica count 1(sts1) and 5(sts2) respectively using a thick provision policy
	       and wait for all replicas to be running
	   3.  Change replica count of sts1 and sts2 to 3
	   4.  Bring down primary site
	   5.  Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
	   6.  Verify there were no issue with replica scale up/down and verify pod entry in CNS volumemetadata for the
	       volumes associated with the PVC used by statefulsets are updated
	   7.  Change replica count of sts1 to 5 a sts2 to 1 and verify they are successful
	   8.  Delete statefulsets and its pvcs created in step 2
	   9.  Bring primary site up and wait for testbed to be back to normal
	*/
	ginkgo.It("Statefulset scale up/down while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-thick"

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		statefulset1 := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset statefulset1")
		statefulset1.Spec.VolumeClaimTemplates[len(statefulset1.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = storageClassName
		*(statefulset1.Spec.Replicas) = 1
		CreateStatefulSet(namespace, statefulset1, client)
		replicas1 := *(statefulset1.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset1, replicas1)
		gomega.Expect(fss.CheckMount(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ss1PodsBeforeScaleDown := fss.GetPodList(client, statefulset1)
		gomega.Expect(ss1PodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			"Unable to get list of Pods from the Statefulset: %v", statefulset1.Name)
		gomega.Expect(len(ss1PodsBeforeScaleDown.Items) == int(replicas1)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset: %v should match with number of replicas: %v",
			statefulset1.Name, replicas1)

		// Get the list of Volumes attached to Pods of sts1 before scale up
		for _, ss1pod := range ss1PodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, ss1pod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range ss1pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset1.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, ss1pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		statefulset2 := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset statefulset2")
		statefulset2.Spec.VolumeClaimTemplates[len(statefulset2.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = storageClassName
		statefulset2.Name = "web-nginx"
		statefulset2.Spec.Template.Labels["app"] = statefulset2.Name
		statefulset2.Spec.Selector.MatchLabels["app"] = statefulset2.Name
		*(statefulset2.Spec.Replicas) = 5
		CreateStatefulSet(namespace, statefulset2, client)
		replicas2 := *(statefulset2.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset2, replicas2)
		gomega.Expect(fss.CheckMount(client, statefulset2, mountPath)).NotTo(gomega.HaveOccurred())
		ss2PodsBeforeScaleDown := fss.GetPodList(client, statefulset2)
		gomega.Expect(ss2PodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			"Unable to get list of Pods from the Statefulset: %v", statefulset2.Name)
		gomega.Expect(len(ss2PodsBeforeScaleDown.Items) == int(replicas2)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset: %v should match with number of replicas: %v",
			statefulset2.Name, replicas2)

		// Get the list of Volumes attached to Pods of sts2 before scale down
		for _, ss2pod := range ss2PodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, ss2pod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range ss2pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset2.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, ss2pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
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

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas1 += 2
		ginkgo.By(fmt.Sprintf("Scaling up statefulset %v to number of Replica: %v", statefulset1.Name, replicas1))
		fss.UpdateReplicas(client, statefulset1, replicas1)

		replicas2 -= 2
		ginkgo.By(fmt.Sprintf("Scaling down statefulset: %v to number of Replica: %v", statefulset2.Name, replicas2))
		fss.UpdateReplicas(client, statefulset2, replicas2)

		ginkgo.By("Bring down the primary site")
		siteFailover(true)

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(true)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		fss.WaitForStatusReadyReplicas(client, statefulset1, replicas1)
		ss1PodsAfterScaleUp := fss.GetPodList(client, statefulset1)
		gomega.Expect(ss1PodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			"Unable to get list of Pods from the Statefulset: %v", statefulset1.Name)
		gomega.Expect(len(ss1PodsAfterScaleUp.Items) == int(replicas1)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset: %v should match with number of replicas: %v",
			statefulset1.Name, replicas1)

		fss.WaitForStatusReadyReplicas(client, statefulset2, replicas2)
		ss2PodsAfterScaleDown := fss.GetPodList(client, statefulset2)
		gomega.Expect(ss2PodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			"Unable to get list of Pods from the Statefulset: %v", statefulset2.Name)
		gomega.Expect(len(ss2PodsAfterScaleDown.Items) == int(replicas2)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset: %v should match with number of replicas %v",
			statefulset2.Name, replicas2)

		// After scale up of sts1, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, ss1pod := range ss1PodsAfterScaleUp.Items {
			err := fpod.WaitForPodsReady(client, statefulset1.Namespace, ss1pod.Name, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, ss1pod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset1.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, ss1pod.Spec.NodeName))
					var vmUUID string
					var exists bool
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if vanillaCluster {
						vmUUID = getNodeUUID(ctx, client, ss1pod.Spec.NodeName)
					} else {
						annotations := pod.Annotations
						vmUUID, exists = annotations[vmUUIDLabel]
						gomega.Expect(exists).To(
							gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
						_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
					ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
					err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, ss1pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		// After scale down of sts2, verify vSphere volumes are detached from deleted pods
		ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
		for _, ss2pod := range ss2PodsAfterScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, ss2pod.Name, metav1.GetOptions{})
			if err != nil {
				gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range ss2pod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset2.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						if vanillaCluster {
							isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
								client, pv.Spec.CSI.VolumeHandle, ss2pod.Spec.NodeName)
							gomega.Expect(err).NotTo(gomega.HaveOccurred())
							gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
								fmt.Sprintf("Volume %q is not detached from the node %q",
									pv.Spec.CSI.VolumeHandle, ss2pod.Spec.NodeName))
						} else {
							annotations := ss2pod.Annotations
							vmUUID, exists := annotations[vmUUIDLabel]
							gomega.Expect(exists).To(gomega.BeTrue(),
								fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

							ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
								pv.Spec.CSI.VolumeHandle, ss2pod.Spec.NodeName))
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
							gomega.Expect(err).To(gomega.HaveOccurred(),
								fmt.Sprintf(
									"PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
									vmUUID, ss2pod.Spec.NodeName))
						}
					}
				}
			}
		}

		// After scale down of sts2, verify the attached volumes match those in CNS Cache
		for _, ss2pod := range ss2PodsAfterScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, ss2pod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range ss2pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset2.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, ss2pod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		// Scaling up statefulset sts1
		replicas1 += 2
		ginkgo.By(fmt.Sprintf("Scaling up statefulset: %v to number of Replica: %v", statefulset1.Name, replicas1))
		_, scaleupErr := fss.Scale(client, statefulset1, replicas1)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset1, replicas1)
		fss.WaitForStatusReadyReplicas(client, statefulset1, replicas1)

		ss1PodsAfterScaleUp = fss.GetPodList(client, statefulset1)
		gomega.Expect(ss1PodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			"Unable to get list of Pods from the Statefulset: %v", statefulset1.Name)
		gomega.Expect(len(ss1PodsAfterScaleUp.Items) == int(replicas1)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset: %v should match with number of replicas: %v",
			statefulset1.Name, replicas1)

		// Scaling down statefulset sts2
		replicas2 -= 2
		ginkgo.By(fmt.Sprintf("Scaling down statefulset: %v to number of Replica: %v", statefulset2.Name, replicas2))
		_, scaledownErr := fss.Scale(client, statefulset2, replicas2)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset2, replicas2)
		fss.WaitForStatusReadyReplicas(client, statefulset2, replicas2)

		ss2PodsAfterScaleDown = fss.GetPodList(client, statefulset2)
		gomega.Expect(ss2PodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			"Unable to get list of Pods from the Statefulset: %v", statefulset2.Name)
		gomega.Expect(len(ss2PodsAfterScaleDown.Items) == int(replicas2)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset: %v should match with number of replicas: %v",
			statefulset2.Name, replicas2)

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

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(true)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Pod deletion while primary site goes down
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Create 30 PVCs and wait for its binding of each pvc with a PV
	   3.  Create a pod with each PVC created in step 2
	   4.  Delete the pod created in step 3
	   5.  Bring down primary site
	   6.  Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
	   7.  wait for full sync
	   8.  Verify volume attachment are also deleted for the pods and pod entry in CNS volumemetadata for the
	       volume associated with the PVC created in step 2 is removed
	   9.  Bring primary site up and wait for testbed to be back to normal
	   10. Delete the PVCs created in step 2

	*/
	ginkgo.It("Pod deletion while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err := strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}
		var pods []*v1.Pod
		var pvclaims []*v1.PersistentVolumeClaim = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc")
			pvclaims[i], err = createPVC(client, namespace, nil, diskSize, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		ginkgo.By("Create pods")
		for i := 0; i < volumeOpsScale; i++ {
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaims[i]}, false, execCommand)
			framework.Logf("Created pod %s", pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pods = append(pods, pod)
		}
		defer func() {
			for _, pod := range pods {
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the primary site while deleting pods")
		var wg sync.WaitGroup
		wg.Add(2)
		go deletePodsInParallel(client, namespace, pods, &wg)
		go siteFailureInParallel(true, &wg)
		wg.Wait()

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(true)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Sleeping full-sync interval for all the pod Metadata " +
			"to be deleted")
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Verify volume is detached from the node")
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pods[i].Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pods[i].Spec.NodeName))

			err = verifyVolumeMetadataInCNS(&e2eVSphere, volHandle,
				pvclaims[i].Name, persistentvolumes[i].Name, pods[i].Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(true)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
	   PVC creation while primary site goes down
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Create 30 PVCs using a thick provision policy so that it takes some time for PVC creation to go through
	   3.  Bring down primary site
	   4.  Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
	   5.  Verify that the PVCs created in step 2 is bound successfully
	   6.  Bring primary site up and wait for testbed to be back to normal
	   7.  Delete PVCs created in step 2
	*/
	ginkgo.It("PVC creation while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-thick"
		var pvclaims []*v1.PersistentVolumeClaim

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the primary site while creating pvcs")
		var wg sync.WaitGroup
		ch := make(chan *v1.PersistentVolumeClaim)
		lock := &sync.Mutex{}
		wg.Add(2)
		go createPvcInParallel(client, namespace, diskSize, sc, ch, lock, &wg, volumeOpsScale)
		go func() {
			for v := range ch {
				pvclaims = append(pvclaims, v)
			}
		}()
		go siteFailureInParallel(true, &wg)
		wg.Wait()
		close(ch)

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(true)
				fds.hostsDown = []string{}
			}
		}()

		defer func() {
			for _, pvclaim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		for _, pvclaim := range pvclaims {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		// TODO: List orphan volumes

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(true)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Primary site network isolation
		Steps:
		1.	Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Create a statefulset, deployment with volumes from the stretched datastore
		3.	Isolate primary site from witness and secondary site
		4.	Verify that the VMs hosted by esx servers are brought up on the other site
		5.	Verify that the k8s cluster is healthy and all the k8s constructs created in step 2 are running
			and volume and application lifecycle actions work fine
		6.	Re-establish primary site network and wait for testbed to be back to normal
		7.	Delete all objects created in step 2
	*/
	ginkgo.It("Primary site network isolation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating PVC")
		pvclaim, err := createPVC(client, namespace, nil, diskSize, sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims = append(pvclaims, pvclaim)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = storageClassName
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			"Unable to get list of Pods from the Statefulset: %v", statefulset.Name)
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset %s, %v, should match with number of required replicas %v",
			statefulset.Name, ssPodsBeforeScaleDown.Size(), replicas)

		// Get the list of Volumes attached to Pods before scale down
		var volumesBeforeScaleDown []string
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By("Creating Deployment")
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
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

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Cause a network failure on primary site
		ginkgo.By("Isolate primary site from witness and secondary site")
		siteNetworkFailure(true, false)

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			siteNetworkFailure(true, true)
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after network failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking if volumes and pods post network failure are healthy")
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

		pvc1, err := createPVC(client, namespace, nil, diskSize, sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvs, err := fpv.WaitForPVClaimBoundPhase(
			client, []*v1.PersistentVolumeClaim{pvc1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc1}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		deletePodAndWaitForVolsToDetach(ctx, client, pod1)

		err = fpv.DeletePersistentVolumeClaim(client, pvc1.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleDownReplicas := replicas - 1
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", scaleDownReplicas))
		_, scaledownErr := fss.Scale(client, statefulset, scaleDownReplicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, scaleDownReplicas)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(scaleDownReplicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset %s, %v, should match with number of replicas %v",
			statefulset.Name, ssPodsAfterScaleDown.Size(), scaleDownReplicas,
		)

		// After scale down, verify vSphere volumes are detached from deleted pods
		ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			if err != nil {
				gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						if vanillaCluster {
							isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
								client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							gomega.Expect(err).NotTo(gomega.HaveOccurred())
							gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
								fmt.Sprintf("Volume %q is not detached from the node %q",
									pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
						} else {
							annotations := sspod.Annotations
							vmUUID, exists := annotations[vmUUIDLabel]
							gomega.Expect(exists).To(gomega.BeTrue(),
								fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

							ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
								pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
							gomega.Expect(err).To(gomega.HaveOccurred(),
								fmt.Sprintf(
									"PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
									vmUUID, sspod.Spec.NodeName))
						}
					}
				}
			}
		}

		// After scale down, verify the attached volumes match those in CNS Cache
		for _, sspod := range ssPodsAfterScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
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

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitForPodsReady(client, statefulset.Namespace, sspod.Name, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By("Verify scale up operation should not introduced new volume")
					gomega.Expect(contains(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)).To(gomega.BeTrue())
					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					var exists bool
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if vanillaCluster {
						vmUUID = getNodeUUID(ctx, client, sspod.Spec.NodeName)
					} else {
						annotations := pod.Annotations
						vmUUID, exists = annotations[vmUUIDLabel]
						gomega.Expect(exists).To(
							gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
						_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
					ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
					err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By("Bring up the primary site")
		siteNetworkFailure(true, true)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
	})

	/*
		PVC deletion while primary site goes down
		Steps:
		1.	Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Create 30 PVCs and wait for each pvc to bind to its PV
		3.	Delete the PVCs created in step2
		4.	Bring down primary site
		5.	Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
		6.	Verify PVs and CNS volumes associated with PVCs created in step 2 are also deleted successfully
		7.	Bring primary site up and wait for testbed to be back to normal
	*/
	ginkgo.It("PVC deletion while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		var pvclaims []*v1.PersistentVolumeClaim = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc %v", i)
			pvclaims[i], err = createPVC(client, namespace, nil, diskSize, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the primary site while deleting pvcs")
		var wg sync.WaitGroup
		wg.Add(2)
		go deletePvcInParallel(client, pvclaims, namespace, &wg)
		go siteFailureInParallel(true, &wg)
		wg.Wait()

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(true)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			if !apierrors.IsNotFound(err) {
				framework.Logf("Persistent Volume %v still not deleted with err %v", pv.Name, err)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				// Orphan volumes may be left over here, hence logging those PVs and ignoring the error for now.
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				framework.Logf("Volume %v still not deleted from CNS with err %v", pv.Name, err)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}
		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(true)
			fds.hostsDown = []string{}
		}

		siteRestore(true)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Pod creation while primary site goes down
		Steps:
		1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Create 30 PVCs and wait for each pvc to bind to its PV
		3.	Create a POD using each PVC created in step 2
		4.	Bring down primary site
		5.	Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
		6.	Verify that the pod created in step 4 come up successfully
		7.	Bring primary site up and wait for testbed to be back to normal
		8.	Delete pods created in step 3
		9.	Delete PVCs created in step 2

	*/
	ginkgo.It("Pod creation while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		var pods []*v1.Pod
		var pvclaims []*v1.PersistentVolumeClaim = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc %v", i)
			pvclaims[i], err = createPVC(client, namespace, nil, diskSize, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			for _, pvclaim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim = nil
			}
		}()

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		/// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the primary site while creating pods")
		var wg sync.WaitGroup
		wg.Add(2)
		ch := make(chan *v1.Pod)
		lock := &sync.Mutex{}
		go createPodsInParallel(client, namespace, pvclaims, ctx, lock, ch, &wg, volumeOpsScale)
		go func() {
			for v := range ch {
				pods = append(pods, v)
			}
		}()
		go siteFailureInParallel(true, &wg)
		wg.Wait()
		close(ch)

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(true)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking whether pods are in Running or ExitCode:0 state")
		for _, pod := range pods {
			framework.Logf("Pod is %s", pod.Name)
			err = waitForPodsToBeInErrorOrRunning(client, pod.Name, namespace, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(true)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pod := range pods {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

})
