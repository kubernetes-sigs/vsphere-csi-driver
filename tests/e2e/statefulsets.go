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
	"fmt"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2elog "k8s.io/kubernetes/test/e2e/framework"
)

const (
	manifestPath     = "tests/e2e/testing-manifests/statefulset/nginx"
	mountPath        = "/usr/share/nginx/html"
	storageclassname = "nginx-sc"
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

var _ = ginkgo.Describe("[csi-block-e2e] statefulset", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-statefulset")
	var (
		namespace string
		client    clientset.Interface
	)
	ginkgo.BeforeEach(func() {
		namespace = f.Namespace.Name
		client = f.ClientSet
		bootstrap()
		sc, err := client.StorageV1().StorageClasses().Get(storageclassname, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(sc.Name, nil)).NotTo(gomega.HaveOccurred())
		}
	})

	ginkgo.AfterEach(func() {
		e2elog.Logf("Deleting all statefulset in namespace: %v", namespace)
		framework.DeleteAllStatefulSets(client, namespace)
	})

	ginkgo.It("vSphere statefulset testing", func() {
		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(storageclassname, nil, nil, "", "")
		sc, err := client.StorageV1().StorageClasses().Create(scSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer client.StorageV1().StorageClasses().Delete(sc.Name, nil)

		ginkgo.By("Creating statefulset")
		statefulsetTester := framework.NewStatefulSetTester(client)
		statefulset := statefulsetTester.CreateStatefulSet(manifestPath, namespace)
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
				gomega.Expect(apierrs.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(isDiskDetached).To(gomega.BeTrue(), fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
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
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToNode(client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
					ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
					err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}
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
