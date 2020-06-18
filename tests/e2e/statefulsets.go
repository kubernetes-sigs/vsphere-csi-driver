/*
Copyright 2019 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	apps "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	manifestPath     = "tests/e2e/testing-manifests/statefulset/nginx"
	mountPath        = "/usr/share/nginx/html"
	storageclassname = "nginx-sc"
	servicename      = "nginx"
)

/*
	Test performs following operations

	Steps
	1. Create a storage class.
	2. Create nginx service.
	3. Create nginx statefulsets with 3 replicas.
	4. Wait until all Pods are ready and PVCs are bounded with PV.
	5. Scale down statefulsets to 2 replicas.
	6. Scale up statefulsets to 3 replicas.
	7. Scale down statefulsets to 0 replicas and delete all pods.
	8. Delete all PVCs from the tests namespace.
	9. Delete the storage class.
*/

var _ = ginkgo.Describe("[csi-block-vanilla] [csi-supervisor] statefulset", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-statefulset")
	var (
		namespace         string
		client            clientset.Interface
		storagePolicyName string
		scParameters      map[string]string
	)
	ginkgo.BeforeEach(func() {
		namespace = getNamespaceToRunTests(f)
		client = f.ClientSet
		bootstrap()
		sc, err := client.StorageV1().StorageClasses().Get(storageclassname, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(sc.Name, nil)).NotTo(gomega.HaveOccurred())
		}
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})

	ginkgo.AfterEach(func() {
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}
		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		framework.DeleteAllStatefulSets(client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(servicename, &metav1.DeleteOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	ginkgo.It("Statefulset testing with default podManagementPolicy", func() {
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters = nil
		} else {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storageclassname)
		}

		scSpec := getVSphereStorageClassSpec(storageclassname, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(scSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(sc.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		statefulsetTester := framework.NewStatefulSetTester(client)
		ginkgo.By("Creating service")
		CreateService(namespace, client)
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, statefulsetTester, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		statefulsetTester.WaitForStatusReadyReplicas(statefulset, replicas)
		gomega.Expect(statefulsetTester.CheckMount(statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := statefulsetTester.GetPodList(statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		// Get the list of Volumes attached to Pods before scale down
		var volumesBeforeScaleDown []string
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas-1))
		_, scaledownErr := statefulsetTester.Scale(statefulset, replicas-1)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		statefulsetTester.WaitForStatusReadyReplicas(statefulset, replicas-1)
		ssPodsAfterScaleDown := statefulsetTester.GetPodList(statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas-1)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		// After scale down, verify vSphere volumes are detached from deleted pods
		ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(sspod.Name, metav1.GetOptions{})
			if err != nil {
				gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						if vanillaCluster {
							isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							gomega.Expect(err).NotTo(gomega.HaveOccurred())
							gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
						} else {
							annotations := sspod.Annotations
							vmUUID, exists := annotations[vmUUIDLabel]
							gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
							ginkgo.By("Wait for 3 minutes for the volume to detach from the pod VM")
							time.Sleep(supervisorClusterOperationsTimeout)
							ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
							gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", vmUUID, sspod.Spec.NodeName))
						}
					}
				}
			}
		}

		// After scale down, verify the attached volumes match those in CNS Cache
		for _, sspod := range ssPodsAfterScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := statefulsetTester.Scale(statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		statefulsetTester.WaitForStatusReplicas(statefulset, replicas)
		statefulsetTester.WaitForStatusReadyReplicas(statefulset, replicas)

		ssPodsAfterScaleUp := statefulsetTester.GetPodList(statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := framework.WaitForPodsReady(client, statefulset.Namespace, sspod.Name, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By("Verify scale up operation should not introduced new volume")
					gomega.Expect(contains(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)).To(gomega.BeTrue())
					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					var exists bool
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if vanillaCluster {
						vmUUID = getNodeUUID(client, sspod.Spec.NodeName)
					} else {
						annotations := pod.Annotations
						vmUUID, exists = annotations[vmUUIDLabel]
						gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
						_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
					ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
					err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}
		replicas = 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr = statefulsetTester.Scale(statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		statefulsetTester.WaitForStatusReplicas(statefulset, replicas)
		ssPodsAfterScaleDown = statefulsetTester.GetPodList(statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")
	})
	/*
		Test performs following operations

		Steps
		1. Create a storage class.
		2. Create nginx service.
		3. Create nginx statefulsets with 8 replicas.
		4. Wait until all Pods are ready and PVCs are bounded with PV.
		5. Scale down statefulsets to 5 replicas.
		6. Scale up statefulsets to 12 replicas.
		7. Scale down statefulsets to 0 replicas and delete all pods.
		8. Delete all PVCs from the tests namespace.
		9. Delete the storage class.
	*/
	ginkgo.It("Statefulset testing with parallel podManagementPolicy", func() {
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters = nil
		} else {
			ginkgo.By("Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimit, storageclassname)
		}

		scSpec := getVSphereStorageClassSpec(storageclassname, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(scSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(sc.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		statefulsetTester := framework.NewStatefulSetTester(client)
		ginkgo.By("Creating service")
		CreateService(namespace, client)
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Updating replicas to 8 and podManagement Policy as Parallel")
		*(statefulset.Spec.Replicas) = 8
		statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, statefulsetTester, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		statefulsetTester.WaitForStatusReadyReplicas(statefulset, replicas)
		gomega.Expect(statefulsetTester.CheckMount(statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := statefulsetTester.GetPodList(statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		replicas -= 3
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr := statefulsetTester.Scale(statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		statefulsetTester.WaitForStatusReadyReplicas(statefulset, replicas)
		ssPodsAfterScaleDown := statefulsetTester.GetPodList(statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		// After scale down, verify vSphere volumes are detached from deleted pods
		ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(sspod.Name, metav1.GetOptions{})
			if err != nil {
				gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						if vanillaCluster {
							isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							gomega.Expect(err).NotTo(gomega.HaveOccurred())
							gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
						} else {
							annotations := sspod.Annotations
							vmUUID, exists := annotations[vmUUIDLabel]
							gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
							ginkgo.By("Wait for 3 minutes for the volume to detach from the pod VM")
							time.Sleep(supervisorClusterOperationsTimeout)
							ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
							gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", vmUUID, sspod.Spec.NodeName))
						}
					}
				}
			}
		}

		// After scale down, verify the attached volumes match those in CNS Cache
		for _, sspod := range ssPodsAfterScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		replicas += 7
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := statefulsetTester.Scale(statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		statefulsetTester.WaitForStatusReplicas(statefulset, replicas)
		statefulsetTester.WaitForStatusReadyReplicas(statefulset, replicas)

		ssPodsAfterScaleUp := statefulsetTester.GetPodList(statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(), fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := framework.WaitForPodsReady(client, statefulset.Namespace, sspod.Name, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					var exists bool
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if vanillaCluster {
						vmUUID = getNodeUUID(client, sspod.Spec.NodeName)
					} else {
						annotations := pod.Annotations
						vmUUID, exists = annotations[vmUUIDLabel]
						gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
						_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
					ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
					err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}
		replicas = 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr = statefulsetTester.Scale(statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		statefulsetTester.WaitForStatusReplicas(statefulset, replicas)
		ssPodsAfterScaleDown = statefulsetTester.GetPodList(statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(), "Number of Pods in the statefulset should match with number of replicas")
	})
})

// check whether the slice contains an element
func contains(volumes []string, volumeID string) bool {
	for _, volumeUUID := range volumes {
		if volumeUUID == volumeID {
			return true
		}
	}
	return false
}

// CreateStatefulSet creates a StatefulSet from the manifest at manifestPath in the given namespace.
func CreateStatefulSet(ns string, ss *apps.StatefulSet, s *framework.StatefulSetTester,
	c clientset.Interface) {
	framework.Logf(fmt.Sprintf("Creating statefulset %v/%v with %d replicas and selector %+v",
		ss.Namespace, ss.Name, *(ss.Spec.Replicas), ss.Spec.Selector))
	_, err := c.AppsV1().StatefulSets(ns).Create(ss)
	framework.ExpectNoError(err)
	s.WaitForRunningAndReady(*ss.Spec.Replicas, ss)
}
