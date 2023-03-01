/*
Copyright 2023 The Kubernetes Authors.

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
	"time"

	"github.com/hashicorp/go-version"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"
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
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration/v1alpha1"
)

const (
	statefulset_volname    string = "block-vol"
	statefulset_devicePath string = "/dev/testblk"
	pod_devicePathPrefix   string = "/mnt/volume"
)

var _ = ginkgo.Describe("raw block volume support", func() {

	f := framework.NewDefaultFramework("e2e-raw-block-volume")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		namespace                 string
		client                    clientset.Interface
		defaultDatacenter         *object.Datacenter
		datastoreURL              string
		scParameters              map[string]string
		storageClassName          string
		storagePolicyName         string
		svcPVCName                string
		rawBlockVolumeMode        = corev1.PersistentVolumeBlock
		pandoraSyncWaitTime       int
		deleteFCDRequired         bool
		fcdID                     string
		pv                        *corev1.PersistentVolume
		pvc                       *corev1.PersistentVolumeClaim
		vcpPvcsPreMig             []*corev1.PersistentVolumeClaim
		vcpPvsPreMig              []*corev1.PersistentVolume
		vcpPvcsPostMig            []*corev1.PersistentVolumeClaim
		vcpPvsPostMig             []*corev1.PersistentVolume
		kcmMigEnabled             bool
		kubectlMigEnabled         bool
		pvsToDelete               []*corev1.PersistentVolume
		migrationEnabledByDefault bool
		err                       error
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		namespace = getNamespaceToRunTests(f)
		client = f.ClientSet
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false, namespace)
		kubectlMigEnabled = false

		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = false

		vcpPvcsPreMig = []*corev1.PersistentVolumeClaim{}
		vcpPvcsPostMig = []*corev1.PersistentVolumeClaim{}
		vcpPvsPreMig = nil
		vcpPvsPostMig = nil
		pvsToDelete = []*corev1.PersistentVolume{}
		v, err := client.Discovery().ServerVersion()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		v1, err := version.NewVersion(v.GitVersion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		v2, err := version.NewVersion("v1.25.0")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if v1.GreaterThanOrEqual(v2) {
			migrationEnabledByDefault = true
		} else {
			migrationEnabledByDefault = false
		}

		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		deleteFCDRequired = false

		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		for _, dc := range datacenters {
			defaultDatacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, defaultrqLimit)
		}
		if deleteFCDRequired {
			ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
			err := e2eVSphere.deleteFCD(ctx, fcdID, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		var pvcsToDelete []*corev1.PersistentVolumeClaim
		if kcmMigEnabled {
			pvcsToDelete = append(pvcsToDelete, vcpPvcsPreMig...)
			pvcsToDelete = append(pvcsToDelete, vcpPvcsPostMig...)
		} else {
			pvcsToDelete = append(pvcsToDelete, vcpPvcsPreMig...)
		}
		vcpPvcsPreMig = []*corev1.PersistentVolumeClaim{}
		vcpPvcsPostMig = []*corev1.PersistentVolumeClaim{}

		if kubectlMigEnabled {
			ginkgo.By("Disable CSI migration feature gates on kublets on k8s nodes")
			toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false, namespace)
		}

		crds := []*v1alpha1.CnsVSphereVolumeMigration{}
		for _, pvc := range pvcsToDelete {
			pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vPath := pv.Spec.VsphereVolume.VolumePath
			if kcmMigEnabled {
				found, crd := getCnsVSphereVolumeMigrationCrd(ctx, vPath)
				if found {
					crds = append(crds, crd)
				}
			}
			pvsToDelete = append(pvsToDelete, pv)

			framework.Logf("Deleting PVC %v", pvc.Name)
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		var defaultDatastore *object.Datastore
		esxHost := GetAndExpectStringEnvVar(envEsxHostIP)
		for _, pv := range pvsToDelete {
			if pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain {
				err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if defaultDatastore == nil {
					defaultDatastore = getDefaultDatastore(ctx)
				}
				if pv.Spec.CSI != nil {
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = e2eVSphere.deleteFCD(ctx, pv.Spec.CSI.VolumeHandle, defaultDatastore.Reference())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else {
					if kcmMigEnabled {
						found, crd := getCnsVSphereVolumeMigrationCrd(ctx, pv.Spec.VsphereVolume.VolumePath)
						gomega.Expect(found).To(gomega.BeTrue())
						err = e2eVSphere.waitForCNSVolumeToBeDeleted(crd.Spec.VolumeID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						err = e2eVSphere.deleteFCD(ctx, crd.Spec.VolumeID, defaultDatastore.Reference())
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					err = deleteVmdk(esxHost, pv.Spec.VsphereVolume.VolumePath)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			if pv.Spec.CSI != nil {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = waitForVmdkDeletion(ctx, pv.Spec.VsphereVolume.VolumePath)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		for _, crd := range crds {
			framework.Logf("Waiting for CnsVSphereVolumeMigration crd %v to be deleted", crd.Spec.VolumeID)
			err = waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx, crd)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		vcpPvsPreMig = nil
		vcpPvsPostMig = nil

		if kcmMigEnabled {
			err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

	/*
		Test statefulset scaleup/scaledown operations with raw block volume
		Steps
		1. Create a storage class.
		2. Create nginx service.
		3. Create nginx statefulsets with 3 replicas and using raw block volume.
		4. Wait until all Pods are ready and PVCs are bounded with PV.
		5. Scale down statefulsets to 2 replicas.
		6. Scale up statefulsets to 3 replicas.
		7. Scale down statefulsets to 0 replicas and delete all pods.
		8. Delete all PVCs from the tests namespace.
		9. Delete the storage class.
	*/
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] [csi-guest]"+
		"Statefulset testing with raw block volume and default podManagementPolicy", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		storageClassName = defaultNginxStorageClassName
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			storageClassName = defaultNginxStorageClassName
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Creating StorageClass for Statefulset")
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

		ginkgo.By("Creating statefulset with raw block volume")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Spec.Template.Spec.Containers[len(statefulset.Spec.Template.Spec.Containers)-1].VolumeMounts = nil
		statefulset.Spec.Template.Spec.Containers[len(statefulset.Spec.Template.Spec.Containers)-1].
			VolumeDevices = []corev1.VolumeDevice{
			{
				Name:       statefulset_volname,
				DevicePath: statefulset_devicePath,
			},
		}
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = defaultNginxStorageClassName
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].ObjectMeta.Name =
			statefulset_volname
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.VolumeMode =
			&rawBlockVolumeMode
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		// Check if raw device available inside all pods of statefulset
		gomega.Expect(CheckDevice(client, statefulset, statefulset_devicePath)).NotTo(gomega.HaveOccurred())

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
					volumeID := pv.Spec.CSI.VolumeHandle
					if guestCluster {
						volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
					}
					volumesBeforeScaleDown = append(volumesBeforeScaleDown, volumeID)
					// Verify the attached volume match the one in CNS cache
					queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
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
						volumeID := pv.Spec.CSI.VolumeHandle
						if guestCluster {
							volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
						}
						isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
							client, volumeID, sspod.Spec.NodeName)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
							fmt.Sprintf("Volume %q is not detached from the node %q",
								volumeID, sspod.Spec.NodeName))
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
					volumeID := pv.Spec.CSI.VolumeHandle
					if guestCluster {
						volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
					}
					queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
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
					volumeID := pv.Spec.CSI.VolumeHandle
					if guestCluster {
						volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
					}
					ginkgo.By("Verify scale up operation should not introduced new volume")
					gomega.Expect(contains(volumesBeforeScaleDown, volumeID)).To(gomega.BeTrue())
					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						volumeID, sspod.Spec.NodeName))
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					vmUUID := getNodeUUID(ctx, client, sspod.Spec.NodeName)
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
					ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
					queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())
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
	})

	/*
		Test dynamic volume provisioning with raw block volume
		Steps
		1. Create a PVC.
		2. Create pod and wait for pod to become ready.
		3. Verify volume is attached.
		4. Write some test data to raw block device inside pod.
		5. Verify the data written on the volume correctly.
		6. Delete pod.
		7. Create a new pod using the previously created volume and wait for pod to
		    become ready.
		8. Verify previously written data using a read on volume.
		9. Write some new test data and verify it.
		10. Delete pod.
		11. Wait for volume to be detached.
	*/
	ginkgo.It("[csi-block-vanilla] [csi-guest] [csi-block-vanilla-parallelized] "+
		"Should create and delete pod with the same raw block volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating Storage Class and PVC")
		// Decide which test setup is available to run.
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
		}
		sc, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating raw block PVC")
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", sc, nil, "")
		pvcspec.Spec.VolumeMode = &rawBlockVolumeMode
		pvc, err = fpv.CreatePVC(client, namespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*corev1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName = volumeID
			volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating pod")
		pod, err := createPod(client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			volumeID, pod.Spec.NodeName))
		var vmUUID string
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		}
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		// Write and read some data on raw block volume inside the pod.
		// Use same devicePath for raw block volume here as used inside podSpec by createPod().
		// Refer setVolumes() for more information on naming of devicePath.
		volumeIndex := 1
		devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataOnRawBlockVolume(namespace, pod.Name, devicePath, "This is first write..")

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		if guestCluster {
			ginkgo.By("Waiting for CnsNodeVMAttachment controller to reconcile resource")
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}

		ginkgo.By("Creating a new pod using the same volume")
		pod, err = createPod(client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		}
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Verify previously written data. Later perform another write and verify it.
		ginkgo.By(fmt.Sprintf("Verify previously written data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		readDataFromRawBlockVolume(namespace, pod.Name, devicePath, "This is first write..")
		ginkgo.By(fmt.Sprintf("Write and read new data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataOnRawBlockVolume(namespace, pod.Name, devicePath, "This is second write..")

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client,
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		if guestCluster {
			ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
			time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}
	})

	/*
	   Test static volume provisioning with raw block volume
	   Steps:
	   1. Create FCD and wait for fcd to allow syncing with pandora.
	   2. Create PV Spec with volumeID set to FCDID created in Step-1, and
	      PersistentVolumeReclaimPolicy is set to Delete.
	   3. Create PVC with the storage request set to PV's storage capacity.
	   4. Wait for PV and PVC to bound.
	   5. Create a POD.
	   6. Verify volume is attached to the node and volume is accessible in the pod.
	   7. Verify container volume metadata is present in CNS cache.
	   8. Delete POD.
	   9. Verify volume is detached from the node.
	   10. Delete PVC.
	   11. Verify PV is deleted automatically.
	*/
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] Verify basic static provisioning workflow"+
		" with raw block volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Creating FCD Disk")
		fcdID, err := e2eVSphere.createFCD(ctx, "BasicStaticFCD", diskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		deleteFCDRequired = true

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		// Creating label for PV.
		// PVC will use this label as Selector to find PV.
		staticPVLabels := make(map[string]string)
		staticPVLabels["fcd-id"] = fcdID

		ginkgo.By("Creating raw block PV")
		pv = getPersistentVolumeSpec(fcdID, corev1.PersistentVolumeReclaimDelete, staticPVLabels, "")
		pv.Spec.VolumeMode = &rawBlockVolumeMode
		pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := fpv.DeletePersistentVolume(client, pv.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating raw block PVC")
		pvc = getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc.Spec.VolumeMode = &rawBlockVolumeMode
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, pv, pvc))
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		// Set deleteFCDRequired to false.
		// After PV, PVC is in the bind state, Deleting PVC should delete
		// container volume. So no need to delete FCD directly using vSphere
		// API call.
		deleteFCDRequired = false

		ginkgo.By("Verifying CNS entry is present in cache")
		_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the Pod")
		var pvclaims []*corev1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		pod, err := createPod(client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached")

		// Write and read some data on raw block volume inside the pod.
		// Use same devicePath for raw block volume here as used inside podSpec by createPod().
		// Refer setVolumes() for more information on naming of devicePath.
		volumeIndex := 1
		devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataOnRawBlockVolume(namespace, pod.Name, devicePath, "This is first write..")

		ginkgo.By("Verify container volume metadata is present in CNS cache")
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolume with VolumeID: %s", pv.Spec.CSI.VolumeHandle))
		_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labels := []types.KeyValue{{Key: "fcd-id", Value: fcdID}}
		ginkgo.By("Verify container volume metadata is matching the one in CNS cache")
		err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
			pvc.Name, pv.ObjectMeta.Name, pod.Name, labels...)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the Pod")
		framework.ExpectNoError(fpod.DeletePodWithWait(client, pod), "Failed to delete pod", pod.Name)

		ginkgo.By(fmt.Sprintf("Verify volume is detached from the node: %s", pod.Spec.NodeName))
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(), "Volume is not detached from the node")
	})

	/*
	   Test VCP-to-CSI migration with raw block volume
	   Steps:
	   1. Create SC1 VCP SC.
	   2. Create PVC1 using SC1 with volumeMode=Block and wait for binding with PV (say PV1).
	   3. Create nginx deployment DEP1 using PVC1 with 1 replica.
	   4. Wait for all the replicas to come up.
	   5. Write and read some data on raw block PVC PVC1 inside deployment pod.
	   6. Enable CSIMigration and CSIMigrationvSphere feature gates on
	      kube-controller-manager (& restart).
	   7. Repeat the following steps for all the nodes in the k8s cluster.
	      a. Drain and Cordon off the node.
	      b. Enable CSIMigration and CSIMigrationvSphere feature gates on the
	         kubelet and Restart kubelet.
	      c. Verify CSI node for the corresponding K8s node has the following
	         annotation - storage.alpha.kubernetes.io/migrated-plugins.
	      d. Enable scheduling on the node.
	   8. Verify all PVC1 and PV1 and have the following annotation -
	      "pv.kubernetes.io/migrated-to": "csi.vsphere.vmware.com".
	   9. Verify cnsvspherevolumemigrations crd is created for PVC1 and PV1.
	   10. Verify CNS entries are present for all PVC1 and PV1 and all PVCs has
	       correct pod names.
	   11. Verify data written before migration on PVC1.
	   12. Create PVC2 using SC1 with volumeMode=Block and wait for binding with PV (say PV2).
	   13. Verify cnsvspherevolumemigrations crd is created for PVC2 and PV2.
	   14. Patch DEP1 to use PVC2 as well.
	   15. Verify CNS entries are present for present for PV2 and PVC2.
	   16. Verify CNS entries for PVC1 and PVC2 have correct pod names.
	   17. Write and read some data on raw block PVC PVC2 inside deployment pod.
	   18. Scale down DEP1 replicas to 0 replicas and wait for PVC1 and PVC2
	       to detach.
	   19. Verify CNS entries for PVC1 and PVC2 have pod names removed.
	   20. Delete DEP1.
	   21. Wait for PV1 and PV2 and respective vmdks to get deleted.
	   22. Verify cnsvspherevolumemigrations crds are removed for all PV1, PV2,
	       PVC1 and PVC2.
	   23. Verify CNS entries are removed for PV1, PV2, PVC1 and PVC2.
	   Following steps will be done as a part of AfterEach() call.
	   24. Delete SC1.
	   25. Repeat the following steps for all the nodes in the k8s cluster.
	      a. Drain and Cordon off the node.
	      b. Disable CSIMigration and CSIMigrationvSphere feature gates on the
	         kubelet and Restart kubelet.
	      c. Verify CSI node for the corresponding K8s node does not have the
	         following annotation - storage.alpha.kubernetes.io/migrated-plugins.
	      d. Enable scheduling on the node.
	   26. Disable CSIMigration and CSIMigrationvSphere feature gates on
	       kube-controller-manager (& restart).
	*/
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] Test VCP-to-CSI migration "+
		"with raw block volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating VCP SC")
		scParameters = make(map[string]string)
		scParameters[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, vcpSc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating VCP PVC pvc1 with raw block volumemode before migration")
		vcpPvcSpec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", vcpSc, nil, "")
		vcpPvcSpec.Spec.VolumeMode = &rawBlockVolumeMode
		pvc1, err := fpv.CreatePVC(client, namespace, vcpPvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc1)

		ginkgo.By("Waiting for all claims created before migration to be in bound state")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPreMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		labelsMap := make(map[string]string)
		labelsMap["dep-lkey"] = "lval"
		ginkgo.By("Creating a Deployment using raw block PVC pvc1")
		volumeIndex := 1
		volumeName := fmt.Sprintf("volume%v", volumeIndex)
		pvc1_devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		depSpec := getDeploymentSpec(ctx, client, 1, labelsMap, nil,
			namespace, []*corev1.PersistentVolumeClaim{pvc1},
			"trap exit TERM; while true; do sleep 1; done", false, busyBoxImageOnGcr)
		depSpec.Spec.Template.Spec.Containers[len(depSpec.Spec.Template.Spec.Containers)-1].VolumeMounts = nil
		depSpec.Spec.Template.Spec.Containers[len(depSpec.Spec.Template.Spec.Containers)-1].
			VolumeDevices = []corev1.VolumeDevice{
			{
				Name:       volumeName,
				DevicePath: pvc1_devicePath,
			},
		}
		dep1, err := client.AppsV1().Deployments(namespace).Create(ctx, depSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Errorf("deployment %q Create API error: %v", depSpec.Name, err))

		ginkgo.By("Waiting deployment to complete")
		err = fdep.WaitForDeploymentComplete(client, dep1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Errorf("deployment %q failed to complete: %v", depSpec.Name, err))

		pods, err := fdep.GetPodsForDeployment(client, dep1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Write and read some data on raw block volume inside the deployment pod.
		// Use same devicePath for raw block volume here as used inside podSpec by getDeploymentSpec().
		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v "+
			"before enabling migration", pod.Name, pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataOnRawBlockVolume(namespace, pod.Name, pvc1_devicePath, "This is write to first VCP volume")

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPreMig, vcpPvsPreMig, true, migrationEnabledByDefault)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc created before migration")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPreMig)

		ginkgo.By("Enable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, true, namespace)
		kubectlMigEnabled = true

		// After migration, verify the original data on VCP PVC pvc1
		ginkgo.By(fmt.Sprintf("Verify previously written data on migrated raw volume attached to: %v at path %v",
			pod.Name, pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		readDataFromRawBlockVolume(namespace, pod.Name, pvc1_devicePath, "This is write to first VCP volume")

		ginkgo.By("Creating VCP PVC pvc2 with raw block volumemode post migration")
		pvc2, err := fpv.CreatePVC(client, namespace, vcpPvcSpec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc2)

		ginkgo.By("Waiting for all claims created post migration to be in bound state")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify annotations on PV/PVCs created post migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, vcpPvcsPostMig, vcpPvsPostMig, false, migrationEnabledByDefault)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, vcpPvcsPostMig)

		dep1, err = client.AppsV1().Deployments(namespace).Get(ctx, dep1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pods, err = fdep.GetPodsForDeployment(client, dep1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod = pods.Items[0]
		rep := dep1.Spec.Replicas
		*rep = 0
		dep1.Spec.Replicas = rep
		ginkgo.By("Scale down deployment to 0 replica")
		dep1, err = client.AppsV1().Deployments(namespace).Update(ctx, dep1, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodNotFoundInNamespace(client, pod.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaims := []*corev1.PersistentVolumeClaim{pvc1, pvc2}
		var volumeDevices = make([]corev1.VolumeDevice, len(pvclaims))
		var volumes = make([]corev1.Volume, len(pvclaims))
		for index, pvclaim := range pvclaims {
			volumename := fmt.Sprintf("volume%v", index+1)
			volumeDevices[index] = corev1.VolumeDevice{Name: volumename, DevicePath: "/mnt/" + volumename}
			volumes[index] = corev1.Volume{Name: volumename, VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}
		}
		dep1, err = client.AppsV1().Deployments(namespace).Get(ctx, dep1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dep1.Spec.Template.Spec.Containers[0].VolumeDevices = volumeDevices
		dep1.Spec.Template.Spec.Volumes = volumes
		*rep = 1
		dep1.Spec.Replicas = rep
		ginkgo.By("Update deployment to use pvc1 and pvc2")
		dep1, err = client.AppsV1().Deployments(namespace).Update(ctx, dep1, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fdep.WaitForDeploymentComplete(client, dep1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("sleep for 1 min...")
		time.Sleep(1 * time.Minute)
		dep1, err = client.AppsV1().Deployments(namespace).Get(ctx, dep1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pods, err = wait4DeploymentPodsCreation(client, dep1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(pods.Items)).NotTo(gomega.BeZero())
		pod = pods.Items[0]
		err = fpod.WaitTimeoutForPodReadyInNamespace(client, pod.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client,
			[]*corev1.PersistentVolumeClaim{pvc1, pvc2})

		// Write and read some data on VCP PVC pvc2
		volumeIndex++
		pvc2_devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		verifyDataOnRawBlockVolume(namespace, pod.Name, pvc2_devicePath, "This is write to second VCP volume")

		ginkgo.By("Scale down deployment to 0 replica")
		dep1, err = client.AppsV1().Deployments(namespace).Get(ctx, dep1.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		*rep = 0
		dep1.Spec.Replicas = rep
		_, err = client.AppsV1().Deployments(namespace).Update(ctx, dep1, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodNotFoundInNamespace(client, pod.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, dep1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration " +
			"along with their respective CnsVSphereVolumeMigration CRDs")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client,
			[]*corev1.PersistentVolumeClaim{pvc1, pvc2})
	})

	/*
		Test online volume expansion on dynamic raw block volume
		Steps:
		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create raw block PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		8. Verify the resized PVC by doing CNS query
		9. Make sure data is intact on the PV mounted on the pod
		10.  Make sure file system has increased

	*/
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] [csi-guest] "+
		"Verify online volume expansion on dynamic raw block volume", func() {
		ginkgo.By("Invoking Test for online Volume Expansion on raw block volume")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create StorageClass with allowVolumeExpansion set to true")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters[scParamDatastoreURL] = sharedVSANDatastoreURL
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
		}
		scParameters[scParamFsType] = ext4FSType
		sc, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating raw block PVC")
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", sc, nil, "")
		pvcspec.Spec.VolumeMode = &rawBlockVolumeMode
		pvc, err = fpv.CreatePVC(client, namespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, []*corev1.PersistentVolumeClaim{pvc},
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv = pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		if guestCluster {
			// svcPVCName refers to PVC Name in the supervisor cluster.
			svcPVCName = volumeID
			volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Pod using the above raw block PVC")
		var pvclaims []*corev1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		pod, err := createPod(client, namespace, nil, pvclaims, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			volumeID, pod.Spec.NodeName))
		var vmUUID string
		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
		}
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

		// Write and read some data on raw block volume inside the pod.
		// Use same devicePath for raw block volume here as used inside podSpec by createPod().
		// Refer setVolumes() for more information on naming of devicePath.
		volumeIndex := 1
		devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataOnRawBlockVolume(namespace, pod.Name, devicePath, "This is write before expansion")

		defer func() {
			// Delete Pod.
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			if guestCluster {
				ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
				time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
				verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
					crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
			}
		}()

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvc, pod)

		// Verify original data on raw block volume after expansion
		ginkgo.By(fmt.Sprintf("Verify previously written data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		readDataFromRawBlockVolume(namespace, pod.Name, devicePath, "This is write before expansion")
	})

	/*
	   Test to verify volume expansion is supported if allowVolumeExpansion
	   is true in StorageClass, PVC is created and offline and not attached
	   to a Pod before the expansion.
	   Steps
	   1. Create StorageClass with allowVolumeExpansion set to true.
	   2. Create raw block PVC which uses the StorageClass created in step 1.
	   3. Wait for PV to be provisioned.
	   4. Wait for PVC's status to become Bound.
	   5. Create pod using PVC on specific node.
	   6. Wait for Disk to be attached to the node.
	   7. Write some data to raw block PVC.
	   8. Detach the volume.
	   9. Modify PVC's size to trigger offline volume expansion.
	   10. Create pod again using PVC on specific node.
	   11. Wait for Disk to be attached to the node.
	   12. Verify data written on PVC before expansion.
	   13. Delete pod and Wait for Volume Disk to be detached from the Node.
	   14. Delete PVC, PV and Storage Class.
	*/
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] [csi-guest] "+
		"Verify offline volume expansion with raw block volume", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Invoking Test for Offline Volume Expansion")
		// Create Storage class and PVC
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType

		// Create a StorageClass that sets allowVolumeExpansion to true
		ginkgo.By("Creating Storage Class with allowVolumeExpansion = true")
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		} else if guestCluster {
			ginkgo.By("CNS_TEST: Running for GC setup")
			scParameters[svStorageClassName] = storagePolicyName
		}
		sc, err := createStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating raw block PVC")
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(namespace, "", sc, nil, "")
		pvcspec.Spec.VolumeMode = &rawBlockVolumeMode
		pvc, err = fpv.CreatePVC(client, namespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		// Waiting for PVC to be bound
		var pvclaims []*corev1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvc)
		ginkgo.By("Waiting for all claims to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volumeID := pv.Spec.CSI.VolumeHandle
		svcPVCName := pv.Spec.CSI.VolumeHandle
		if guestCluster {
			volumeID = getVolumeIDFromSupervisorCluster(volumeID)
			gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll, pollTimeoutShort)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var vmUUID string
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		// Get the size of block device and verify if device is accessible by performing write and read.
		volumeIndex := 1
		devicePath := fmt.Sprintf("%v%v", pod_devicePathPrefix, volumeIndex)
		ginkgo.By(fmt.Sprintf("Check size for block device at devicePath %v before expansion", devicePath))
		originalBlockDevSize, err := getBlockDevSizeInSectors(f, namespace, pod, devicePath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Write and read data on raw volume attached to: %v at path %v", pod.Name,
			pod.Spec.Containers[0].VolumeDevices[0].DevicePath))
		verifyDataOnRawBlockVolume(namespace, pod.Name, devicePath, "This is write before offline expansion")

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
			client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		if guestCluster {
			ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
			time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}

		// Modify PVC spec to trigger volume expansion
		// We expand the PVC while no pod is using it to ensure offline expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvc, err = expandPVCSize(pvc, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvc).NotTo(gomega.BeNil())

		pvcSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvc.Name)
		}
		if guestCluster {
			ginkgo.By("Checking for PVC request size change on SVC PVC")
			b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(b).To(gomega.BeTrue())
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvc, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if guestCluster {
			ginkgo.By("Checking for resize on SVC PV")
			verifyPVSizeinSupervisor(svcPVCName, newSize)
		}

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volumeID))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(3072)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a new Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating a new pod to attach PV again to the node")
		pod, err = createPod(client, namespace, nil, []*corev1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume after expansion: %s is attached to the node: %s",
			volumeID, pod.Spec.NodeName))
		vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		pvcConditions := pvc.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify volume size after expansion")
		blockDevSize, err := getBlockDevSizeInSectors(f, namespace, pod, devicePath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Filesystem size may be smaller than the size of the block volume.
		// Here since filesystem was already formatted on the original volume,
		// we can compare the new filesystem size with the original filesystem size.
		if blockDevSize < originalBlockDevSize {
			framework.Failf("error updating volume size for %q. Resulting volume size is %d", pvc.Name, blockDevSize)
		}
		ginkgo.By("Resized volume has been attached successfully")

		// Delete POD
		ginkgo.By(fmt.Sprintf("Deleting the new pod %s in namespace %s after expansion", pod.Name, namespace))
		err = fpod.DeletePodWithWait(client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify volume is detached from the node after expansion")
		isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		if guestCluster {
			ginkgo.By("Waiting for 30 seconds to allow CnsNodeVMAttachment controller to reconcile resource")
			time.Sleep(waitTimeForCNSNodeVMAttachmentReconciler)
			verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
				crdCNSNodeVMAttachment, crdVersion, crdGroup, false)
		}
	})
})
