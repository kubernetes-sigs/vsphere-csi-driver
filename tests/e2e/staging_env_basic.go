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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	"k8s.io/kubernetes/test/e2e/framework/manifest"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"

	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

var _ = ginkgo.Describe("[csi-supervisor-staging] Tests for WCP env with minimal permission user", func() {

	var (
		client            clientset.Interface
		namespace         string
		storagePolicyName string
	)

	ginkgo.BeforeEach(func() {
		var err error
		if k8senvsv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senvsv != "" {
			client, err = createKubernetesClientFromConfig(k8senvsv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		namespace = os.Getenv("SVC_NAMESPACE")

		bootstrap()
		_, cancel := context.WithCancel(context.Background())
		defer cancel()

		nodeList, err := fnodes.GetReadySchedulableNodes(client)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})

	/*
		Verify online volume expansion on dynamic volume

		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		8. Verify the resized PVC by doing CNS query
		9. Make sure data is intact on the PV mounted on the pod
		10.  Make sure file system has increased

	*/
	ginkgo.It("Verify online volume expansion on staging dynamic volume", func() {
		ginkgo.By("Verify online volume expansion on staging dynamic volume")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume

		ginkgo.By("Creating a PVC")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim, err = fpv.CreatePVC(client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, v1.ReadWriteOnce))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		pv = persistentvolumes[0]

		defer func() {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Pod using the above PVC")
		pod, vmUUID := createPODandVerifyVolumeMountWithoutF(ctx, client, namespace, pvclaim, volHandle)

		defer func() {
			// Delete Pod.
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
				pv.Spec.CSI.VolumeHandle, vmUUID))
			_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(),
				fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
					vmUUID, pv.Spec.CSI.VolumeHandle))
		}()

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPodWithoutF(client, namespace, pvclaim, pod)
	})

	ginkgo.It("Staging Statefulset testing with default podManagementPolicy", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Generate a random name for Statefulset app
		min := 32770
		max := 32775
		port := int32(rand.Intn(max-min) + min)
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)

		svcManifestFilePath := filepath.Join(manifestPath, "service.yaml")
		framework.Logf("Parsing service from %v", svcManifestFilePath)
		svc, err := manifest.SvcFromManifest(svcManifestFilePath)
		framework.ExpectNoError(err)

		var lableName = "app-" + curtimestring + val + strconv.Itoa(min)
		var lableValue = "nginx-" + curtimestring + val + strconv.Itoa(min)

		labels := map[string]string{lableName: lableValue}

		svc.Name = "nginx-" + curtimestring + val + strconv.Itoa(min)
		svc.Spec.Selector = labels
		svc.Spec.Ports[0].Port = port
		svc.Spec.Ports[0].Name = "web" + val + strconv.Itoa(min)
		svc.Spec.Selector = labels

		service, err := client.CoreV1().Services(namespace).Create(ctx, svc, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Name = "sts-" + curtimestring + val + strconv.Itoa(min)
		statefulset.Spec.ServiceName = svc.Name
		statefulset.Spec.Selector.MatchLabels = labels
		statefulset.Spec.Template.Labels = labels
		statefulset.Spec.Template.Spec.Containers[0].Name = svc.Name
		statefulset.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort = port
		statefulset.Spec.Template.Spec.Containers[0].Ports[0].Name = "web" + val + strconv.Itoa(min)
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = storagePolicyName

		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		defer func() {
			framework.Logf("Deleting statefulset %v", statefulset.Name)
			err := client.AppsV1().StatefulSets(statefulset.Namespace).Delete(context.TODO(),
				statefulset.Name, metav1.DeleteOptions{OrphanDependents: new(bool)})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		// Get the list of Volumes attached to Pods before scale down
		var volumesBeforeScaleDown []string
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas-1))
		_, scaledownErr := fss.Scale(client, statefulset, replicas-1)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas-1)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas-1)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// After scale down, verify vSphere volumes are detached from deleted pods
		ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			if err != nil {
				gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						volumesBeforeScaleDown = append(volumesBeforeScaleDown, volumespec.PersistentVolumeClaim.ClaimName)

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
								fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
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
			"Number of Pods in the statefulset should match with number of replicas")

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitTimeoutForPodReadyInNamespace(client, sspod.Name, statefulset.Namespace, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, volumespec.PersistentVolumeClaim.ClaimName)

					var vmUUID string
					var exists bool
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					annotations := pod.Annotations
					vmUUID, exists = annotations[vmUUIDLabel]
					gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
					_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
		replicas = 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr = fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		ssPodsAfterScaleDown = fss.GetPodList(client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		framework.Logf("Deleting statefulset %v", statefulset.Name)
		fss.WaitForStatusReadyReplicas(client, statefulset, 0)
		err = client.AppsV1().StatefulSets(statefulset.Namespace).Delete(context.TODO(),
			statefulset.Name, metav1.DeleteOptions{OrphanDependents: new(bool)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVC is fully deleted")
		for _, volume := range volumesBeforeScaleDown {
			ginkgo.By(fmt.Sprintf("Wait and verify PVC is fully deleted %s", volume))
			err := fpv.DeletePersistentVolumeClaim(client, volume, namespace)
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	/*
		Test performs following operations

		Steps
		1. Create a storage class.
		2. Create nginx service.
		3. Create nginx statefulsets with 3 replicas.
		4. Wait until all Pods are ready and PVCs are bounded with PV.
		5. Scale down statefulsets to 1 replicas.
		6. Scale up statefulsets to 4 replicas.
		7. Scale down statefulsets to 0 replicas and delete all pods.
		8. Delete all PVCs from the tests namespace.
		9. Delete the storage class.
	*/
	ginkgo.It("Staging Statefulset testing with parallel podManagementPolicy", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Generate a random name for Statefulset app
		min := 32778
		max := 32781
		port := int32(rand.Intn(max-min) + min)
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)

		svcManifestFilePath := filepath.Join(manifestPath, "service.yaml")
		framework.Logf("Parsing service from %v", svcManifestFilePath)
		svc, err := manifest.SvcFromManifest(svcManifestFilePath)
		framework.ExpectNoError(err)

		var lableName = "app-" + curtimestring + val + strconv.Itoa(min)
		var lableValue = "nginx-" + curtimestring + val + strconv.Itoa(min)

		labels := map[string]string{lableName: lableValue}

		svc.Name = "nginx-" + curtimestring + val + strconv.Itoa(min)
		svc.Spec.Selector = labels
		svc.Spec.Ports[0].Port = port
		svc.Spec.Ports[0].Name = "web" + val + strconv.Itoa(min)
		svc.Spec.Selector = labels

		service, err := client.CoreV1().Services(namespace).Create(ctx, svc, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Name = "sts-" + curtimestring + val + strconv.Itoa(min)
		statefulset.Spec.ServiceName = svc.Name
		*(statefulset.Spec.Replicas) = 3
		statefulset.Spec.Selector.MatchLabels = labels
		statefulset.Spec.Template.Labels = labels
		statefulset.Spec.Template.Spec.Containers[0].Name = svc.Name
		statefulset.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort = port
		statefulset.Spec.Template.Spec.Containers[0].Ports[0].Name = "web" + val + strconv.Itoa(min)
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = storagePolicyName

		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		defer func() {
			framework.Logf("Deleting statefulset %v", statefulset.Name)
			err := client.AppsV1().StatefulSets(statefulset.Namespace).Delete(context.TODO(),
				statefulset.Name, metav1.DeleteOptions{OrphanDependents: new(bool)})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		// Get the list of Volumes attached to Pods before scale down
		var volumesBeforeScaleDown []string
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, volumespec.PersistentVolumeClaim.ClaimName)
				}
			}
		}

		replicas -= 1
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr := fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

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
								fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
									vmUUID, sspod.Spec.NodeName))
						}
						volumesBeforeScaleDown = append(volumesBeforeScaleDown, volumespec.PersistentVolumeClaim.ClaimName)
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

		replicas += 4
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
		_, scaleupErr := fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)

		ssPodsAfterScaleUp := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// After scale up, verify all vSphere volumes are attached to node VMs.
		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitTimeoutForPodReadyInNamespace(client, sspod.Name, statefulset.Namespace, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
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
						gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
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

					volumesBeforeScaleDown = append(volumesBeforeScaleDown, volumespec.PersistentVolumeClaim.ClaimName)
				}
			}
		}
		replicas = 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr = fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		ssPodsAfterScaleDown = fss.GetPodList(client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		framework.Logf("Deleting statefulset %v", statefulset.Name)
		fss.WaitForStatusReadyReplicas(client, statefulset, 0)
		err = client.AppsV1().StatefulSets(statefulset.Namespace).Delete(context.TODO(),
			statefulset.Name, metav1.DeleteOptions{OrphanDependents: new(bool)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVC is fully deleted")
		for _, volume := range volumesBeforeScaleDown {
			ginkgo.By(fmt.Sprintf("Wait and verify PVC is fully deleted %s", volume))
			err := fpv.DeletePersistentVolumeClaim(client, volume, namespace)
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	/*
		verify online volume expansion on statefulset
			1. Create a SC with allowVolumeExpansion set to 'true' in SVC
			2. create statefulset with replica 3 using the above created SC
			3. Once all the statefull set PODs are up follow the below step to edit statefulset
			4. kubectl edit pvc <pvcName>  for each PVC in the StatefulSet, to increase its capacity.
			5. kubectl delete sts --cascade=false <statefullSetName>  to delete the StatefulSet and leave its pods.
			6. vi statefulset.yaml and edit the storage and increase the size to the size you have edited the PVC in step4
			7. create the same statefulset again
			8. Scaleup statefulset
			9. Newly created statefulset should have the increased size
			10. scale down statefulset to 0
			11. delete statefulset and all PVC's and SC's
	*/
	ginkgo.It("Staging Verify online volume expansion on statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvcSizeBeforeExpansion int64

		// Generate a random name for Statefulset app
		min := 32782
		max := 32787
		port := int32(rand.Intn(max-min) + min)
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)

		svcManifestFilePath := filepath.Join(manifestPath, "service.yaml")
		framework.Logf("Parsing service from %v", svcManifestFilePath)
		svc, err := manifest.SvcFromManifest(svcManifestFilePath)
		framework.ExpectNoError(err)

		var lableName = "app-" + curtimestring + val + strconv.Itoa(min)
		var lableValue = "nginx-" + curtimestring + val + strconv.Itoa(min)

		labels := map[string]string{lableName: lableValue}

		svc.Name = "nginx-" + curtimestring + val + strconv.Itoa(min)
		svc.Spec.Selector = labels
		svc.Spec.Ports[0].Port = port
		svc.Spec.Ports[0].Name = "web" + val + strconv.Itoa(min)
		svc.Spec.Selector = labels

		service, err := client.CoreV1().Services(namespace).Create(ctx, svc, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Name = "sts-" + curtimestring + val + strconv.Itoa(min)
		statefulset.Spec.ServiceName = svc.Name
		statefulset.Spec.Selector.MatchLabels = labels
		statefulset.Spec.Template.Labels = labels
		statefulset.Spec.Template.Spec.Containers[0].Name = svc.Name
		statefulset.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort = port
		statefulset.Spec.Template.Spec.Containers[0].Ports[0].Name = "web" + val + strconv.Itoa(min)
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = storagePolicyName

		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		defer func() {
			framework.Logf("Deleting statefulset %v", statefulset.Name)
			err := client.AppsV1().StatefulSets(statefulset.Namespace).Delete(context.TODO(),
				statefulset.Name, metav1.DeleteOptions{OrphanDependents: new(bool)})
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up and increase PVC size")
		// Get the list of Volumes attached to Pods before scale down
		var volumesBeforeScaleDown []string
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pvclaimName := volumespec.PersistentVolumeClaim.ClaimName

					pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(
						ctx, pvclaimName, metav1.GetOptions{})
					gomega.Expect(pvclaim).NotTo(gomega.BeNil())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, pvclaimName)

					ginkgo.By("Expanding current pvc")
					sizeBeforeexpansion := pvclaim.Status.Capacity[v1.ResourceStorage]
					pvcSizeBeforeExpansion, _ = sizeBeforeexpansion.AsInt64()
					framework.Logf("pvcsize : %d", pvcSizeBeforeExpansion)
					currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
					newSize := currentPvcSize.DeepCopy()
					newSize.Add(resource.MustParse("1Gi"))
					framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
					pvclaim, err = expandPVCSize(pvclaim, newSize, client)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(pvclaim).NotTo(gomega.BeNil())

					ginkgo.By("Waiting for file system resize to finish")
					_, err = waitForFSResize(pvclaim, client)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}
		ginkgo.By("Delete statefulset with cascade = false")
		cascade := false
		err = client.AppsV1().StatefulSets(namespace).Delete(context.TODO(),
			statefulset.Name, metav1.DeleteOptions{OrphanDependents: &cascade})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		statefulset = GetResizedStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = storagePolicyName
		CreateStatefulSet(namespace, statefulset, client)
		replicas = *(statefulset.Spec.Replicas)

		incresedReplicaCount := replicas + 1
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", incresedReplicaCount))
		_, scaleupErr := fss.Scale(client, statefulset, incresedReplicaCount)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, incresedReplicaCount)
		fss.WaitForStatusReadyReplicas(client, statefulset, incresedReplicaCount)

		ssPodsBeforeScaleDown = fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(incresedReplicaCount)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up , " +
			"and also verify the increased PVC size")
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pvclaimName := volumespec.PersistentVolumeClaim.ClaimName

					pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(
						ctx, pvclaimName, metav1.GetOptions{})
					gomega.Expect(pvclaim).NotTo(gomega.BeNil())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					sizeAfterExpansion := pvclaim.Status.Capacity[v1.ResourceStorage]
					pvcSizeAfterExpansion, _ := sizeAfterExpansion.AsInt64()

					volumesBeforeScaleDown = append(volumesBeforeScaleDown, pvclaimName)

					framework.Logf("newSize : %d", pvcSizeAfterExpansion)
					gomega.Expect(pvcSizeAfterExpansion).Should(gomega.BeNumerically(">", pvcSizeBeforeExpansion),
						fmt.Sprintf("error updating  size for statefulset. PVCName: %s pvcSizeAfterExpansion: %v "+
							"pvcSizeBeforeExpansion: %v", pvclaim.Name, pvcSizeAfterExpansion, pvcSizeBeforeExpansion))
					ginkgo.By("File system resize finished successfully")

				}
			}
		}

		replicas = 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr := fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		ssPodsAfterScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		framework.Logf("Deleting statefulset %v", statefulset.Name)
		fss.WaitForStatusReadyReplicas(client, statefulset, 0)
		err = client.AppsV1().StatefulSets(statefulset.Namespace).Delete(context.TODO(),
			statefulset.Name, metav1.DeleteOptions{OrphanDependents: new(bool)})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVC is fully deleted")
		for _, volume := range volumesBeforeScaleDown {
			ginkgo.By(fmt.Sprintf("Wait and verify PVC is fully deleted %s", volume))
			err := fpv.DeletePersistentVolumeClaim(client, volume, namespace)
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	})

	ginkgo.It("Staging Should create and delete pod with the same volume source", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvc *v1.PersistentVolumeClaim
		var err error

		ginkgo.By("Create PVC ")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc, err = fpv.CreatePVC(client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, v1.ReadWriteOnce))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle

		defer func() {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		var vmUUID string
		var exists bool

		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		var volumeFiles []string
		// Create an empty file on the mounted volumes on the pod.
		ginkgo.By(fmt.Sprintf("Creating an empty file on the volume mounted on: %v", pod.Name))
		newEmptyFileName := fmt.Sprintf("/mnt/volume1/%v_file_A.txt", namespace)
		volumeFiles = append(volumeFiles, newEmptyFileName)
		createAndVerifyFilesOnVolume(namespace, pod.Name, []string{newEmptyFileName}, volumeFiles)

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
			pv.Spec.CSI.VolumeHandle, vmUUID))
		_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(),
			fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
				vmUUID, pv.Spec.CSI.VolumeHandle))

		ginkgo.By("Creating a new pod using the same volume")
		pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		annotations = pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Create another empty file on the mounted volume on the pod and
		// verify newly and previously created files present on the volume
		// mounted on the pod.
		ginkgo.By(fmt.Sprintf("Creating a second empty file on the same volume mounted on: %v", pod.Name))
		newEmptyFileName = fmt.Sprintf("/mnt/volume1/%v_file_B.txt", namespace)
		volumeFiles = append(volumeFiles, newEmptyFileName)
		createAndVerifyFilesOnVolume(namespace, pod.Name, []string{newEmptyFileName}, volumeFiles)
		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
			pv.Spec.CSI.VolumeHandle, vmUUID))
		_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(),
			fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
				vmUUID, pv.Spec.CSI.VolumeHandle))

	})

	// Test for Pod creation works when SecurityContext has FSGroup
	ginkgo.It("Staging Verify Pod Creation works when SecurityContext has FSGroup", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pvclaim *v1.PersistentVolumeClaim
		var err error
		var fsGroup int64
		var runAsUser int64

		ginkgo.By("Creating a PVC")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim, err = fpv.CreatePVC(client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, v1.ReadWriteOnce))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")

		fsGroup = 1000
		runAsUser = 2000

		fsGroupInt64 := &fsGroup
		runAsUserInt64 := &runAsUser
		pod, err := createPodForFSGroup(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
			false, execCommand, fsGroupInt64, runAsUserInt64)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pv := persistentvolumes[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		var vmUUID string
		var exists bool
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and filegroup type is as expected")
		cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"ls -lh /mnt/volume1/fstype "}
		output := framework.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, strconv.Itoa(int(fsGroup)))).NotTo(gomega.BeFalse())
		gomega.Expect(strings.Contains(output, strconv.Itoa(int(runAsUser)))).NotTo(gomega.BeFalse())

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s", volumeID, vmUUID))
		_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(),
			fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
				vmUUID, pv.Spec.CSI.VolumeHandle))
	})

	/*
		Verify online volume expansion on deployment

		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create nginx deployment DEP using above created PVC with 1 replica
		6. wait for all the replicas to come up , verify the deployment POD
		7. Trigger online volume expansion by editing coresponding PVC
		8. Wait for some time for resize to be successful  and verify the PVC size
		9. Verify the resized PVC's by doing CNS query
		10. Make sure file system has increased
		11. Scale down deployment set to 0 replicas and delete all pods, PVC and SC

	*/
	ginkgo.It("Staging verify online volume expansion on deployment", func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvclaim *v1.PersistentVolumeClaim

		ginkgo.By("Creating a PVC")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim, err = fpv.CreatePVC(client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, v1.ReadWriteOnce))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		ginkgo.By("Creating a Deployment using pvc1")

		dep, err := createDeployment(ctx, client, 1, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim}, "", false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err := fdep.GetPodsForDeployment(client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		increaseSizeOfPvcAttachedToPodWithoutF(client, namespace, pvclaim, &pod)

		ginkgo.By("Scale down deployment to 0 replica")
		dep, err = client.AppsV1().Deployments(namespace).Get(ctx, dep.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pods, err = fdep.GetPodsForDeployment(client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod = pods.Items[0]
		rep := dep.Spec.Replicas
		*rep = 0
		dep.Spec.Replicas = rep
		dep, err = client.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodNotFoundInNamespace(client, pod.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
	})

	/*
		Verify online volume expansion on PVC volume when Pod is deleted and re-created

		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. Delete POD
		8. Re-create Pod using same PVC
		9. Verify the resized PVC by doing CNS query
		10. Make sure data is intact on the PV mounted on the pod
		11.  Make sure file system has increased

	*/
	ginkgo.It("Staging verify online volume expansion when POD is deleted and re-created", func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var originalSizeInMb int64
		var err error

		ginkgo.By("Creating a PVC")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim, err = fpv.CreatePVC(client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, v1.ReadWriteOnce))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		pv = persistentvolumes[0]

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Pod using the above PVC")
		pod, vmUUID := createPODandVerifyVolumeMountWithoutF(ctx, client, namespace, pvclaim, volHandle)

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
				pv.Spec.CSI.VolumeHandle, vmUUID))
			_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(),
				fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
					vmUUID, pv.Spec.CSI.VolumeHandle))

		}()

		//Fetch original FileSystemSize
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
		originalSizeInMb, err = getFSSizeMbWithoutF(namespace, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//resize PVC
		// Modify PVC spec to trigger volume expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("Deleting Pod after triggering online expansion on PVC")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
			pv.Spec.CSI.VolumeHandle, vmUUID))
		_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(),
			fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
				vmUUID, pv.Spec.CSI.VolumeHandle))

		ginkgo.By("re-create Pod using the same PVC")
		pod, vmUUID = createPODandVerifyVolumeMountWithoutF(ctx, client, namespace, pvclaim, volHandle)

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		var fsSize int64
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFSSizeMbWithoutF(namespace, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %s", fsSize)
		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
		ginkgo.By("File system resize finished successfully")
	})

	// Test for valid disk size of 2Gi
	ginkgo.It("Verify dynamic provisioning of pv using storageclass with a valid disk size passes", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Invoking Test for valid disk size")
		var pvclaim *v1.PersistentVolumeClaim
		var err error

		// decide which test setup is available to run
		ginkgo.By("Creating a PVC")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim, err = fpv.CreatePVC(client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, v1.ReadWriteOnce))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size specified in PVC in honored")
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != diskSizeInMb {
			err = fmt.Errorf("wrong disk size provisioned ")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})

// createPODandVerifyVolumeMount this method creates Pod and verifies VolumeMount
func createPODandVerifyVolumeMountWithoutF(ctx context.Context, client clientset.Interface,
	namespace string, pvclaim *v1.PersistentVolumeClaim, volHandle string) (*v1.Pod, string) {
	// Create a Pod to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		err = client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	var exists bool
	var vmUUID string
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))

	annotations := pod.Annotations
	vmUUID, exists = annotations[vmUUIDLabel]
	gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

	framework.Logf("VMUUID : %s", vmUUID)
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
		"Volume is not attached to the node volHandle: %s, vmUUID: %s", volHandle, vmUUID)

	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	_, err = framework.LookForStringInPodExec(namespace, pod.Name,
		[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return pod, vmUUID
}

// getFSSizeMb returns filesystem size in Mb
func getFSSizeMbWithoutF(namespace string, pod *v1.Pod) (int64, error) {
	var output string
	var err error

	cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c", "df -Tkm | grep /mnt/volume1"}
	output = framework.RunKubectlOrDie(namespace, cmd...)
	gomega.Expect(strings.Contains(output, ext4FSType)).NotTo(gomega.BeFalse())

	arrMountOut := strings.Fields(string(output))
	if len(arrMountOut) <= 0 {
		return -1, fmt.Errorf("error when parsing output of `df -T`. output: %s", string(output))
	}
	var devicePath, strSize string
	devicePath = arrMountOut[0]
	if devicePath == "" {
		return -1, fmt.Errorf("error when parsing output of `df -T` to find out devicePath of /mnt/volume1. output: %s",
			string(output))
	}
	strSize = arrMountOut[2]
	if strSize == "" {
		return -1, fmt.Errorf("error when parsing output of `df -T` to find out size of /mnt/volume1: output: %s",
			string(output))
	}

	intSizeInMb, err := strconv.ParseInt(strSize, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("failed to parse size %s into int size", strSize)
	}

	return intSizeInMb, nil
}

// increaseSizeOfPvcAttachedToPod this method increases the PVC size, which is attached to POD
func increaseSizeOfPvcAttachedToPodWithoutF(client clientset.Interface,
	namespace string, pvclaim *v1.PersistentVolumeClaim, pod *v1.Pod) {
	var originalSizeInMb int64
	var err error
	//Fetch original FileSystemSize
	ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
	originalSizeInMb, err = getFSSizeMbWithoutF(namespace, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	//resize PVC
	// Modify PVC spec to trigger volume expansion
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err = expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	var fsSize int64
	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err = getFSSizeMbWithoutF(namespace, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("File system size after expansion : %v", fsSize)
	// Filesystem size may be smaller than the size of the block volume
	// so here we are checking if the new filesystem size is greater than
	// the original volume size as the filesystem is formatted for the
	// first time
	gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
		fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
	ginkgo.By("File system resize finished successfully")
}
