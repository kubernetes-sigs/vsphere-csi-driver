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
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	manifestPath                 = "tests/e2e/testing-manifests/statefulset/nginx"
	mountPath                    = "/usr/share/nginx/html"
	defaultNginxStorageClassName = "nginx-sc"
	servicename                  = "nginx"
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

var _ = ginkgo.Describe("statefulset", func() {

	f := framework.NewDefaultFramework("e2e-vsphere-statefulset")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		namespace         string
		client            clientset.Interface
		storagePolicyName string
		scParameters      map[string]string
		storageClassName  string
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		namespace = getNamespaceToRunTests(f)
		client = f.ClientSet
		bootstrap()
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(ctx, client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
			dumpSvcNsEventsOnTestFailure(client, namespace)
		}
	})

	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-block-vanilla-parallelized] Statefulset "+
		"testing with default podManagementPolicy", ginkgo.Label(p0, vanilla, block, wcp, core), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters = nil
			storageClassName = defaultNginxStorageClassName
			scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
			sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			storageClassName = storagePolicyName
			ginkgo.By("Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &storageClassName
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(ctx, client, statefulset)
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
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas-1))
		_, scaledownErr := fss.Scale(ctx, client, statefulset, replicas-1)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas-1)
		ssPodsAfterScaleDown := fss.GetPodList(ctx, client, statefulset)
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
				}
			}
		}
		replicas = 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr = fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		ssPodsAfterScaleDown = fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
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
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-block-vanilla-parallelized] Statefulset "+
		"testing with parallel podManagementPolicy", ginkgo.Label(p0, vanilla, block, wcp, core), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters = nil
			storageClassName = defaultNginxStorageClassName
			scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
			sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

		} else {
			storageClassName = storagePolicyName
			ginkgo.By("Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Updating replicas to 8 and podManagement Policy as Parallel")
		*(statefulset.Spec.Replicas) = 8
		statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &storageClassName
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

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
				}
			}
		}

		replicas -= 3
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr := fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		ssPodsAfterScaleDown := fss.GetPodList(ctx, client, statefulset)
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

		replicas += 7
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
				}
			}
		}
		replicas = 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr = fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		ssPodsAfterScaleDown = fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
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
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify online volume "+
		"expansion on statefulset", ginkgo.Label(p0, vanilla, block, wcp, core), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvcSizeBeforeExpansion int64
		var sc, scSpec *storagev1.StorageClass
		var err error
		var volHandle string
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType

		if vanillaCluster {
			storageClassName = "nginx-sc-expansion"
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
			scParameters[scParamDatastoreURL] = sharedVSANDatastoreURL
		} else {
			storageClassName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
			framework.Logf("storageClassName %v", storageClassName)
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storageClassName)
			scParameters[scParamStoragePolicyID] = profileID
		}

		if !vcptocsi {
			scSpec = getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", true)
		} else {
			scSpec = getVcpVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", true)
		}

		if !supervisorCluster {
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &storageClassName
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up and increase PVC size")
		// Get the list of Volumes attached to Pods before scale down
		//var volumesBeforeScaleDown []string
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
					//Minimum Version of k8s Support for Resize migrated volume is k8s 1.26 and
					//CSI by default migrates volume.Hence Manual Migration is not needed
					if vcptocsi {
						ginkgo.By("Verify annotations on PVCs created after migration")
						_, err := waitForPvcMigAnnotations(ctx, client, pvclaimName, pvclaim.Namespace, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						ginkgo.By("Verify annotations on PV created after migration")
						_, err = waitForPvMigAnnotations(ctx, client, pv.Name, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvclaimName)
						crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						volHandle = crd.Spec.VolumeID

					} else {
						volHandle = pv.Spec.CSI.VolumeHandle
					}

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

					err = verifyVolumeMetadataInCNS(&e2eVSphere, volHandle,
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
			Spec.StorageClassName = &storageClassName
		CreateStatefulSet(namespace, statefulset, client)
		replicas = *(statefulset.Spec.Replicas)

		incresedReplicaCount := replicas + 1
		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", incresedReplicaCount))
		_, scaleupErr := fss.Scale(ctx, client, statefulset, incresedReplicaCount)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, incresedReplicaCount)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, incresedReplicaCount)

		ssPodsBeforeScaleDown = fss.GetPodList(ctx, client, statefulset)
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
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

					if vcptocsi {
						ginkgo.By("Verify annotations on PVCs created after migration")
						_, err = waitForPvcMigAnnotations(ctx, client, pvclaimName, pvclaim.Namespace, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						ginkgo.By("Verify annotations on PV created after migration")
						_, err = waitForPvMigAnnotations(ctx, client, pv.Name, false)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}

					sizeAfterExpansion := pvclaim.Status.Capacity[v1.ResourceStorage]
					pvcSizeAfterExpansion, _ := sizeAfterExpansion.AsInt64()

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
		_, scaledownErr := fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		ssPodsAfterScaleDown := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

	})

	/*
	  Verify List volume Response on vsphere-csi-controller logs
	  Note: ist volume Threshold is set to 1 , and query limit set to 3
	  1. Create SC
	  2. Create statefull set with 3 replica
	  3. Bring down the CSI driver replica to 1 , so that it is easy to validate the List volume Response.
	  4. Wait for all the PVC to reach bound and PODs to reach running state.
	  5. Note down the PV volume handle
	  6. Verify the Listvolume response in logs. It should contain all the 3 volumeID's noted in step 5
	  7. Scale up the Statefullset replica to 5 and validate the Pagination.
	    The 1st List volume Response will have the "token for next set:"
	  8. Delete 2 volumes from the CNS , verify the error message in the controller logs
	  9. Delete All the volumes
	  10. Verify list volume response for 0 volume.
	  11. Clean up the statefull set
	  12. Inncrease the CSI driver  replica to 3

	*/
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] ListVolumeResponse "+
		"Validation", ginkgo.Label(p1, listVolume, block, vanilla, wcp, core), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var svcMasterPswd string
		var volumesBeforeScaleUp []string
		var sshClientConfig *ssh.ClientConfig
		containerName := "vsphere-csi-controller"
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters = nil
			storageClassName = "nginx-sc-default"
		} else {
			ginkgo.By("Running for WCP setup")

			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
		}

		ginkgo.By("scale down CSI driver POD to 1 , so that it will" +
			"be easy to validate all Listvolume response on one driver POD")
		collectPodLogs(ctx, client, csiSystemNamespace)
		scaledownCSIDriver, err := scaleCSIDriver(ctx, client, namespace, 1)
		gomega.Expect(scaledownCSIDriver).To(gomega.BeTrue(), "csi driver scaledown is not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Scale up the csi-driver replica to 3")
			success, err := scaleCSIDriver(ctx, client, namespace, 3)
			gomega.Expect(success).To(gomega.BeTrue(), "csi driver scale up to 3 replica not successful")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		if !supervisorCluster {
			scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
			sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &storageClassName
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Get the list of Volumes attached to Pods before scale up
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					volumesBeforeScaleUp = append(volumesBeforeScaleUp, pv.Spec.CSI.VolumeHandle)
					// Verify the attached volume match the one in CNS cache
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}

		//List volume responses will show up in the interval of every 1 minute.
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate ListVolume Response for all the volumes")
		var logMessage string
		if vanillaCluster {
			logMessage = "List volume response: entries:"
			nimbusGeneratedK8sVmPwd := GetAndExpectStringEnvVar(nimbusK8sVmPwd)
			sshClientConfig = &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(nimbusGeneratedK8sVmPwd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}
		}
		if supervisorCluster {
			logMessage = "ListVolumes:"
			svcMasterPswd = GetAndExpectStringEnvVar(svcMasterPassword)
			sshClientConfig = &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(svcMasterPswd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}
		}
		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig,
			containerName, logMessage, volumesBeforeScaleUp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas = replicas + 2
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

		ginkgo.By("Validate pagination")
		logMessage = "token for next set: 3"
		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if vanillaCluster {
			ginkgo.By("Delete volume from CNS and verify the error message")
			logMessage = "difference between number of K8s volumes and CNS volumes is greater than threshold"
			_, err = e2eVSphere.deleteCNSvolume(volumesBeforeScaleUp[0], false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = e2eVSphere.deleteCNSvolume(volumesBeforeScaleUp[1], false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			//List volume responses will show up in the interval of every 1 minute.
			//To see the error, It is required to wait for 1 min after deleteting few Volumes
			time.Sleep(pollTimeoutShort)
			_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		replicas = 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr := fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		ssPodsAfterScaleDown := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		pvcList := getAllPVCFromNamespace(client, namespace)
		for _, pvc := range pvcList.Items {
			framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace),
				"Failed to delete PVC", pvc.Name)
		}
		//List volume responses will show up in the interval of every 1 minute.
		//To see the empty response, It is required to wait for 1 min after deleteting all the PVC's
		time.Sleep(pollTimeoutShort)

		ginkgo.By("Validate ListVolume Response when no volumes are present")
		logMessage = "ListVolumes served 0 results"

		_, _, err = getCSIPodWhereListVolumeResponseIsPresent(ctx, client, sshClientConfig, containerName, logMessage, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Steps :
		1. Create statefulset with 5 replicas and deployment.
		2. EMM host in EvacuateAllData mode.
		3. Verify EMM passes.
		4. Verify CSI pods are running and statefulsets are in running state.
		5. Scale up replica to 5.
		6. Exit MM and clean up all pods and PVs.
	*/
	ginkgo.It("[csi-supervisor] Test MM workflow on statefulset", ginkgo.Label(
		p1, block, wcp, core), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var mmTimeout int32 = 300
		var hostInMM *object.HostSystem

		// create resource quota
		createResourceQuota(client, namespace, rqLimit, storagePolicyName)

		ginkgo.By("Get the storageclass from Supervisor")
		sc, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statfulset and deployment from storageclass")
		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, true,
			false, 0, "", "")
		replicas := *(statefulset.Spec.Replicas)
		csiNs := GetAndExpectStringEnvVar(envCSINamespace)
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		ssPodsBeforeScaleDown := fss.GetPodList(ctx, client, statefulset)
		stsPod := ssPodsBeforeScaleDown.Items[0]
		nodeName := stsPod.Spec.NodeName
		framework.Logf("nodeName: %v", nodeName)
		clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Get host name where statfulset pod is located")
		computeCluster := GetAndExpectStringEnvVar(envComputeClusterName)
		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, computeCluster)
		for _, host := range hostsInCluster {
			hostPath := host.Common.InventoryPath
			hostDetails := strings.Split(hostPath, "/")
			hostIP := hostDetails[len(hostDetails)-1]
			hostName := getHostName(hostIP)
			hostName = strings.Trim(hostName, ".")
			framework.Logf("hostname: %v", hostName)
			if hostName == nodeName {
				hostInMM = host
				break
			}
		}

		ginkgo.By("Put host into EvacuateAlldata maintenance mode")
		enterHostIntoMM(ctx, hostInMM, evacMModeType, mmTimeout, true)
		enterMaintenanceMode := true
		defer func() {
			if enterMaintenanceMode {
				framework.Logf("Exit the host from MM before terminating the test")
				exitHostMM(ctx, hostInMM, mmTimeout)
			}
		}()

		err = fpod.WaitForPodsRunningReady(ctx, client, csiNs, int32(csipods.Size()), 0, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Scale up statefulset replica to 5")
		replicas = replicas + 2
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			replicas, true, true)

		ginkgo.By("Exit the host from maintenance mode")
		exitHostMM(ctx, hostInMM, mmTimeout)
		enterMaintenanceMode = false

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
func CreateStatefulSet(ns string, ss *apps.StatefulSet, c clientset.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	framework.Logf(fmt.Sprintf("Creating statefulset %v/%v with %d replicas and selector %+v",
		ss.Namespace, ss.Name, *(ss.Spec.Replicas), ss.Spec.Selector))
	_, err := c.AppsV1().StatefulSets(ns).Create(ctx, ss, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	fss.WaitForRunningAndReady(ctx, c, *ss.Spec.Replicas, ss)
}
