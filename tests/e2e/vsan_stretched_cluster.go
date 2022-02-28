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
	var (
		client            clientset.Interface
		namespace         string
		nodeList          *v1.NodeList
		storagePolicyName string
		scParameters      map[string]string
		storageClassName  string
		csiNs             string
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
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		framework.ExpectNoError(err, "Unable to fetch storage class "+defaultNginxStorageClassName)
		if sc != nil {
			framework.ExpectNoError(client.StorageV1().StorageClasses().Delete(ctx, defaultNginxStorageClassName,
				*metav1.NewDeleteOptions(0)), "Unable to delete storage class "+defaultNginxStorageClassName)
		}
		scParameters = make(map[string]string)
		nodeList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "Unable to list k8s nodes")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find k8s nodes")
		}

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
		siteRestore(true)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
	})

})
