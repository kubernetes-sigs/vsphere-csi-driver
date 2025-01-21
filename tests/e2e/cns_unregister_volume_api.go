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
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	cnsunregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsunregistervolume/v1alpha1"
)

var _ = ginkgo.Describe("[csi-unregister-volume] CNS Unregister Volume", func() {
	f := framework.NewDefaultFramework("cns-unregister-volume")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	const defaultVolumeOpsScale = 30
	const defaultVolumeOpsScaleWCP = 29
	var (
		client               clientset.Interface
		c                    clientset.Interface
		fullSyncWaitTime     int
		namespace            string
		scParameters         map[string]string
		storagePolicyName    string
		volumeOpsScale       int
		isServiceStopped     bool
		serviceName          string
		csiReplicaCount      int32
		deployment           *appsv1.Deployment
		zonalPolicy          string
		zonalWffcPolicy      string
		categories           []string
		labels_ns            map[string]string
		allowedTopologyHAMap map[string][]string
		allowedTopologies    []v1.TopologySelectorLabelRequirement
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		isServiceStopped = false
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		if stretchedSVC {
			storagePolicyName = GetAndExpectStringEnvVar(envZonalStoragePolicyName)
			labels_ns = map[string]string{}
			labels_ns[admissionapi.EnforceLevelLabel] = string(admissionapi.LevelPrivileged)
			labels_ns["e2e-framework"] = f.BaseName

			if storagePolicyName == "" {
				ginkgo.Fail(envZonalStoragePolicyName + " env variable not set")
			}
			zonalWffcPolicy = GetAndExpectStringEnvVar(envZonalWffcStoragePolicyName)
			if zonalWffcPolicy == "" {
				ginkgo.Fail(envZonalWffcStoragePolicyName + " env variable not set")
			}
			framework.Logf("zonal policy: %s and zonal wffc policy: %s", zonalPolicy, zonalWffcPolicy)

			topologyHaMap := GetAndExpectStringEnvVar(topologyHaMap)
			_, categories = createTopologyMapLevel5(topologyHaMap)
			allowedTopologies = createAllowedTopolgies(topologyHaMap)
			allowedTopologyHAMap = createAllowedTopologiesMap(allowedTopologies)
			framework.Logf("Topology map: %v, categories: %v", allowedTopologyHAMap, categories)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")

		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}

		if os.Getenv("VOLUME_OPS_SCALE") != "" {
			volumeOpsScale, err = strconv.Atoi(os.Getenv(envVolumeOperationsScale))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			if vanillaCluster {
				volumeOpsScale = defaultVolumeOpsScale
			} else {
				volumeOpsScale = defaultVolumeOpsScaleWCP
			}
		}
		framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Full sync interval can be 1 min at minimum so full sync wait time has to be more than 120s
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}

		// Get CSI Controller's replica count from the setup
		controllerClusterConfig := os.Getenv(contollerClusterKubeConfig)
		c = client
		if controllerClusterConfig != "" {
			framework.Logf("Creating client for remote kubeconfig")
			remoteC, err := createKubernetesClientFromConfig(controllerClusterConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			c = remoteC
		}
		deployment, err = c.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount = *deployment.Spec.Replicas
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if isServiceStopped {
			if serviceName == "CSI" {
				framework.Logf("Starting CSI driver")
				ignoreLabels := make(map[string]string)
				err := updateDeploymentReplicawithWait(c, csiReplicaCount, vSphereCSIControllerPodNamePrefix,
					csiSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Wait for the CSI Pods to be up and Running
				list_of_pods, err := fpod.GetPodsInNamespace(ctx, client, csiSystemNamespace, ignoreLabels)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				num_csi_pods := len(list_of_pods)
				err = fpod.WaitForPodsRunningReady(ctx, client, csiSystemNamespace, int(num_csi_pods),
					time.Duration(pollTimeout))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else if serviceName == hostdServiceName {
				framework.Logf("In afterEach function to start the hostd service on all hosts")
				hostIPs := getAllHostsIP(ctx, true)
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
			} else {
				vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err := invokeVCenterServiceControl(ctx, startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		ginkgo.By(fmt.Sprintf("Resetting provisioner time interval to %s sec", defaultProvisionerTimeInSec))
		updateCSIDeploymentProvisionerTimeout(c, csiSystemNamespace, defaultProvisionerTimeInSec)

		if supervisorCluster {
			deleteResourceQuota(client, namespace)
			dumpSvcNsEventsOnTestFailure(client, namespace)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, defaultrqLimit)
			dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}
	})

	/*
		Export detached PVC

		Create SPBM Policy:
		Define and create a Storage Policy-Based Management (SPBM) policy.

		Assign Policy to Namespace:
		Apply the SPBM policy to the test namespace with sufficient storage quota.

		Create PVC:
		Create a PersistentVolumeClaim (PVC) named pvc1 in the test namespace.
		Verify CNS Metadata

		Verify Storage Quota:
		Check that the storage quota in the test namespace reflects the space occupied by pvc1.

		Create CnsUnregisterVolume CR:
		Create a CnsUnregisterVolume Custom Resource (CR) with the volumeID of pvc1 and apply it to the test namespace.

		Check CnsUnregisterVolume Status:
		Verify that the status.Unregistered field in the CnsUnregisterVolume CR is set to true.

		Verify Deletion:
		Ensure that the PersistentVolume (PV) and PVC are deleted from the Supervisor cluster.
		Verify CNS Metadata

		Check Quota Release:
		Confirm that the storage quota has been freed after the deletion of PV/PVC.

		Verify FCD:
		Use the RetrieveVStorageObject API from vCenter MOB to confirm that the FCD associated with the PVC has not been deleted.

		Cleanup:
		Delete the FCD, the test namespace, and the SPBM policy.
	*/
	ginkgo.It("[csi-unregister-volume] Export detached PVC", func() {
		exportDetachedVolume(namespace, client, storagePolicyName, scParameters,
			volumeOpsScale, true)
	})

	/*
		Export PVC attached with Pod

		Create SPBM Policy and Assign to Namespace:
		Create a SPBM policy and assign it to the test namespace with sufficient quota.

		Create PVC:
		Create a PersistentVolumeClaim (PVC) named pvc1 in the test namespace.
		Verify CNS Metadata
		Verify List Volume results lists volume ID

		Verify Storage Quota:
		Verify that the storage quota in the test namespace appropriately shows the occupied quota from pvc1.

		Create Pod and Use PVC:
		Create a Pod named pod1 in the test namespace using pvc1.
		Wait for pod1 to be up and running, then perform I/O operations on pvc1 through pod1.

		Create and Apply CnsUnregisterVolume CR:
		Create a CnsUnregisterVolume CR with the volumeID of pvc1 and apply it to the test namespace.

		Check CnsUnregisterVolume Status:
		Check the CnsUnregisterVolume CR and verify that the status.Unregistered field is set to false.

		Verify PV/PVC Not Deleted:
		Verify that the PersistentVolume (PV) and PVC are not deleted from the Supervisor cluster.

		Delete Pod and Wait:
		Delete pod1 and wait until the pod is fully deleted.

		Reapply CnsUnregisterVolume CR:
		Create and apply a new CnsUnregisterVolume CR with the volumeID of pvc1 to the test namespace.

		Check Updated CnsUnregisterVolume Status:
		Verify that the status.Unregistered field in the CnsUnregisterVolume CR is now set to true.

		Check PV/PVC Deletion:
		Confirm that the PV and PVC are deleted from the Supervisor cluster.
		Verify CNS Metadata
		Verify List Volume results does not lists volume ID

		Verify Storage Quota Freed:
		Verify that the storage quota has been freed following the deletion of PV/PVC.

		Verify FCD Status:
		Use the RetrieveVStorageObject API from vCenter MOB to ensure that the FCD is not deleted.

		Cleanup:
		Delete the FCD, the test namespace, and the SPBM policy.
	*/
	ginkgo.It("[csi-unregister-volume] Export PVC attached with Pod", func() {
		exportAttachedVolume(namespace, client, storagePolicyName, scParameters,
			volumeOpsScale, true)
	})

	/*
		Export detached PVC and PVC attached in TKG

		Running test in TKG Context

		Create SPBM Policy and Assign to TKG Namespace:
		Create a SPBM policy and assign it to the TKG namespace with sufficient quota.

		Create PVC in TKG test namespace:
		Create a PersistentVolumeClaim (PVC) named pvc1 in the test namespace.
		Verify CNS Metadata

		Verify Storage Quota:
		Verify that the storage quota in the TKG namespace appropriately shows the space occupied by pvc1.

		Create and Use Pod:
		Create a Pod named pod1 in the test namespace using pvc1.
		Wait for pod1 to be up and running, then perform I/O operations on pvc1 through pod1.

		Create and Apply CnsUnregisterVolume CR (SVC Context):
		Create a CnsUnregisterVolume Custom Resource (CR) with the volumeID of pvc1 and apply it to the TKG namespace in the Supervisor cluster (SVC context).

		Check CnsUnregisterVolume Status (SVC Context):
		Verify that the status.Unregistered field in the CnsUnregisterVolume CR is set to false (SVC context).

		Check PV/PVC Status (SVC Context):
		Confirm that the PersistentVolume (PV) and PVC are not deleted from the Supervisor cluster (SVC context).

		Delete Pod and Wait:
		Delete pod1 and wait until the pod is fully deleted.

		Reapply CnsUnregisterVolume CR (SVC Context):
		Create and apply a new CnsUnregisterVolume CR with the volumeID of pvc1 to the test namespace in the Supervisor cluster (SVC context).

		Check Updated CnsUnregisterVolume Status (SVC Context):
		Verify that the status.Unregistered field in the CnsUnregisterVolume CR is set to false (SVC context).

		Check PV/PVC Deletion (SVC Context):
		Verify that the PV and PVC are not deleted from the Supervisor cluster (SVC context).
		Verify CNS Metadata

		Cleanup:
		Delete the PVC, the test namespace, and the SPBM policy.
	*/
	ginkgo.It("[csi-unregister-volume] Export detached PVC and PVC attached in TKG", func() {
		exportAttachedVolumeInTKG(namespace, client, storagePolicyName, scParameters,
			volumeOpsScale, true)
	})

	/*
		Export detached PVC while the services are down

		vSAN Health is down
		HostD is down
		VPXD is down
		SpS is down
		CSI Pods are down

		Create SPBM Policy and Assign to Namespace:
		Create a SPBM policy and assign it to the test namespace with sufficient quota.

		Create PVC:
		Create a PersistentVolumeClaim (PVC) named pvc1 in the test namespace.
		Verify CNS Metadata

		Bring Down VC Services:
		Bring down the VC services: sps, vpxd, vsan, and hostd.
		Wait for Services to be Down:
		Wait for the services to be fully down.

		Create and Apply CnsUnregisterVolume CR:
		Create a CnsUnregisterVolume Custom Resource (CR) with the volumeID of pvc1 and apply it to the test namespace.

		Check CnsUnregisterVolume Status:
		Verify that the status.Unregistered field in the CnsUnregisterVolume CR is set to false.

		Bring Up VC Services:
		Bring up the VC services: sps, vpxd, vsan, and hostd.

		Wait for Services to be Up:
		Wait for the services to be fully up and running.

		Wait for Full Sync Time:
		Wait for the system to complete the full synchronization.

		Check Updated CnsUnregisterVolume Status:
		Verify that the status.Unregistered field in the CnsUnregisterVolume CR is now set to true.

		Check PV/PVC Deletion:
		Confirm that the PersistentVolume (PV) and PVC are deleted from the Supervisor cluster.
		Verify CNS Metadata

		Verify Storage Quota Freed:
		Verify that the storage quota has been freed following the deletion of PV/PVC.

		Verify FCD Status:
		Invoke the FCD API RetrieveVStorageObject from vCenter MOB and verify that the FCD is not deleted.

		Cleanup:
		Delete the FCD, the test namespace, and the SPBM policy.
	*/
	ginkgo.It("[csi-unregister-volume] Export detached PVC while the services are down - HOSTD", func() {
		serviceName = hostdServiceName
		exportVolumeWithServiceDown(serviceName, namespace, client,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

	ginkgo.It("[csi-unregister-volume] Export detached PVC while the services are down - VSAN", func() {
		serviceName = vsanhealthServiceName
		exportVolumeWithServiceDown(serviceName, namespace, client,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

	ginkgo.It("[csi-unregister-volume] Export detached PVC while the services are down - VPXD", func() {
		serviceName = vpxdServiceName
		exportVolumeWithServiceDown(serviceName, namespace, client,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

	ginkgo.It("[csi-unregister-volume] Export detached PVC while the services are down - SPS", func() {
		serviceName = spsServiceName
		exportVolumeWithServiceDown(serviceName, namespace, client,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

	ginkgo.It("[csi-unregister-volume] Export detached PVC while the services are down - CSI", func() {
		serviceName = "CSI"
		exportVolumeWithServiceDown(serviceName, namespace, client,
			scParameters, volumeOpsScale, isServiceStopped, c)
	})

})

func exportDetachedVolume(namespace string, client clientset.Interface,
	storagePolicyName string, scParameters map[string]string, volumeOpsScale int, extendVolume bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var storageclass *storagev1.StorageClass
	var persistentvolumes []*v1.PersistentVolume
	var pvclaims []*v1.PersistentVolumeClaim
	var err error
	//var fullSyncWaitTime int
	pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

	// Get a config to talk to the apiserver
	restConfig := getRestConfigClient()

	framework.Logf("storagePolicyName %v", storagePolicyName)
	framework.Logf("extendVolume %v", extendVolume)

	if stretchedSVC {
		ginkgo.By("CNS_TEST: Running for Stretch setup")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	} else if supervisorCluster {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		profileID := e2eVSphere.GetSpbmPolicyID(thickProvPolicy)
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", true, thickProvPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		ginkgo.By("CNS_TEST: Running for GC setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		scParameters[svStorageClassName] = thickProvPolicy
		scParameters[scParamFsType] = ext4FSType
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, thickProvPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		var allowExpansion = true
		storageclass.AllowVolumeExpansion = &allowExpansion
		storageclass, err = client.StorageV1().StorageClasses().Update(ctx, storageclass, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Creating PVCs using the Storage Class")
	framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
	for i := 0; i < volumeOpsScale; i++ {
		framework.Logf("Creating pvc-%v", i)
		pvclaims[i], err = fpv.CreatePVC(ctx, client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, ""))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
		2*framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// TODO: Add a logic to check for the no orphan volumes
	defer func() {
		for _, claim := range pvclaims {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
				framework.PodDeleteTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the "+
					"CNS after it is deleted from kubernetes", volumeID))
		}
	}()

	ginkgo.By("Invoking CNS Unregister Volume API for all the FCD's created above")
	for _, pv := range persistentvolumes {
		volumeID := pv.Spec.CSI.VolumeHandle
		time.Sleep(30 * time.Second)

		ginkgo.By("Create CNS unregister volume with above created FCD " + pv.Spec.CSI.VolumeHandle)

		cnsUnRegisterVolume := getCNSUnregisterVolumeSpec(namespace, volumeID)

		cnsRegisterVolumeName := cnsUnRegisterVolume.GetName()
		framework.Logf("CNS unregister volume name : %s", cnsRegisterVolumeName)

		err = createCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete CNS unregister volume CR by name " + cnsRegisterVolumeName)
			err = deleteCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for  CNS unregister volume to be unregistered")
		framework.ExpectNoError(waitForCNSUnRegisterVolumeToGetUnregistered(ctx,
			restConfig, cnsUnRegisterVolume, poll, supervisorClusterOperationsTimeout))
	}

	ginkgo.By("Verify PVs, volumes are also deleted from CNS")
	for _, pv := range persistentvolumes {
		err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
			framework.PodDeleteTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID := pv.Spec.CSI.VolumeHandle
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Volume: %s should not be present in the "+
				"CNS after it is deleted from kubernetes", volumeID))
	}

	defaultDatastore = getDefaultDatastore(ctx)
	ginkgo.By(fmt.Sprintf("defaultDatastore %v sec", defaultDatastore))

	for _, pv1 := range persistentvolumes {
		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", pv1.Spec.CSI.VolumeHandle))
		err = deleteFcdWithRetriesForSpecificErr(ctx, pv1.Spec.CSI.VolumeHandle, defaultDatastore.Reference(),
			[]string{disklibUnlinkErr}, []string{objOrItemNotFoundErr})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

func exportAttachedVolume(namespace string, client clientset.Interface,
	storagePolicyName string, scParameters map[string]string, volumeOpsScale int, extendVolume bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var storageclass *storagev1.StorageClass
	var persistentvolumes []*v1.PersistentVolume
	var pvclaims []*v1.PersistentVolumeClaim
	var err error
	//var fullSyncWaitTime int
	pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

	// Get a config to talk to the apiserver
	restConfig := getRestConfigClient()

	framework.Logf("storagePolicyName %v", storagePolicyName)
	framework.Logf("extendVolume %v", extendVolume)

	if stretchedSVC {
		ginkgo.By("CNS_TEST: Running for Stretch setup")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	} else if supervisorCluster {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		profileID := e2eVSphere.GetSpbmPolicyID(thickProvPolicy)
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", true, thickProvPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		ginkgo.By("CNS_TEST: Running for GC setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		scParameters[svStorageClassName] = thickProvPolicy
		scParameters[scParamFsType] = ext4FSType
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, thickProvPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		var allowExpansion = true
		storageclass.AllowVolumeExpansion = &allowExpansion

		//Priya to fix
		//storageclass, err = client.StorageV1().StorageClasses().Update(ctx, storageclass, metav1.UpdateOptions{})
		//gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Creating PVCs using the Storage Class")
	framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
	for i := 0; i < volumeOpsScale; i++ {
		framework.Logf("Creating pvc%v", i)
		pvclaims[i], err = fpv.CreatePVC(ctx, client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, ""))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
		2*framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// TODO: Add a logic to check for the no orphan volumes
	defer func() {
		for _, claim := range pvclaims {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
				framework.PodDeleteTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the "+
					"CNS after it is deleted from kubernetes", volumeID))
		}
	}()

	ginkgo.By("Create POD")
	pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Invoking CNS Unregister Volume API for all the FCD's created above")
	for _, pv := range persistentvolumes {
		volumeID := pv.Spec.CSI.VolumeHandle
		time.Sleep(30 * time.Second)

		ginkgo.By("Create CNS unregister volume with above created FCD " + pv.Spec.CSI.VolumeHandle)

		cnsUnRegisterVolume := getCNSUnregisterVolumeSpec(namespace, volumeID)

		cnsRegisterVolumeName := cnsUnRegisterVolume.GetName()
		framework.Logf("CNS unregister volume name : %s", cnsRegisterVolumeName)

		err = createCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete CNS unregister volume CR by name " + cnsRegisterVolumeName)
			err = deleteCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for  CNS unregister volume to be unregistered")
		gomega.Expect(waitForCNSUnRegisterVolumeToGetUnregistered(ctx,
			restConfig, cnsUnRegisterVolume, poll,
			supervisorClusterOperationsTimeout)).To(gomega.HaveOccurred())
	}

	ginkgo.By("Deleting the pod")
	err = fpod.DeletePodWithWait(ctx, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Retry: Invoking CNS Unregister Volume API for all the FCD's created above")
	for _, pv := range persistentvolumes {
		volumeID := pv.Spec.CSI.VolumeHandle
		time.Sleep(30 * time.Second)

		ginkgo.By("Create CNS unregister volume with above created FCD " + pv.Spec.CSI.VolumeHandle)

		cnsUnRegisterVolume := getCNSUnregisterVolumeSpec(namespace, volumeID)

		cnsRegisterVolumeName := cnsUnRegisterVolume.GetName()
		framework.Logf("CNS unregister volume name : %s", cnsRegisterVolumeName)

		err = createCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete CNS unregister volume CR by name " + cnsRegisterVolumeName)
			err = deleteCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for  CNS unregister volume to be unregistered")
		framework.ExpectNoError(waitForCNSUnRegisterVolumeToGetUnregistered(ctx,
			restConfig, cnsUnRegisterVolume, poll, supervisorClusterOperationsTimeout))
	}

	ginkgo.By("Verify PVs, volumes are deleted from CNS")
	for _, pv := range persistentvolumes {
		err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
			framework.PodDeleteTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID := pv.Spec.CSI.VolumeHandle
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Volume: %s should not be present in the "+
				"CNS after it is deleted from kubernetes", volumeID))
	}

	defaultDatastore = getDefaultDatastore(ctx)
	ginkgo.By(fmt.Sprintf("defaultDatastore %v sec", defaultDatastore))

	for _, pv1 := range persistentvolumes {
		ginkgo.By(fmt.Sprintf("Deleting FCD: %s", pv1.Spec.CSI.VolumeHandle))
		err = deleteFcdWithRetriesForSpecificErr(ctx, pv1.Spec.CSI.VolumeHandle, defaultDatastore.Reference(),
			[]string{disklibUnlinkErr}, []string{objOrItemNotFoundErr})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

func exportAttachedVolumeInTKG(namespace string, client clientset.Interface,
	storagePolicyName string, scParameters map[string]string, volumeOpsScale int, extendVolume bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var storageclass *storagev1.StorageClass
	var persistentvolumes []*v1.PersistentVolume
	var pvclaims []*v1.PersistentVolumeClaim
	var err error
	//var fullSyncWaitTime int
	pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

	svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)

	// Get a config to talk to the apiserver
	restConfig := getRestConfigClient()

	framework.Logf("storagePolicyName %v", storagePolicyName)
	framework.Logf("extendVolume %v", extendVolume)

	if stretchedSVC {
		ginkgo.By("CNS_TEST: Running for Stretch setup")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	} else if supervisorCluster {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		profileID := e2eVSphere.GetSpbmPolicyID(thickProvPolicy)
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", true, thickProvPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		ginkgo.By("CNS_TEST: Running for GC setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		scParameters[svStorageClassName] = thickProvPolicy
		scParameters[scParamFsType] = ext4FSType
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, thickProvPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		var allowExpansion = true
		storageclass.AllowVolumeExpansion = &allowExpansion

		//Priya to fix
		//storageclass, err = client.StorageV1().StorageClasses().Update(ctx, storageclass, metav1.UpdateOptions{})
		//gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Creating PVCs using the Storage Class")
	framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
	for i := 0; i < volumeOpsScale; i++ {
		framework.Logf("Creating pvc%v", i)
		pvclaims[i], err = fpv.CreatePVC(ctx, client, namespace,
			getPersistentVolumeClaimSpecWithStorageClass(namespace, diskSize, storageclass, nil, ""))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
		2*framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// TODO: Add a logic to check for the no orphan volumes
	defer func() {
		for _, claim := range pvclaims {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
				framework.PodDeleteTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the "+
					"CNS after it is deleted from kubernetes", volumeID))
		}
	}()

	ginkgo.By("Create POD")
	pod, err := createPod(ctx, client, namespace, nil, pvclaims, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Invoking CNS Unregister Volume API for all the FCD's created above")
	for _, pv := range persistentvolumes {
		volumeID := pv.Spec.CSI.VolumeHandle
		time.Sleep(30 * time.Second)

		if guestCluster {
			volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
		}

		ginkgo.By("Create CNS unregister volume with above created FCD " + volumeID)

		cnsUnRegisterVolume := getCNSUnregisterVolumeSpec(svNamespace, volumeID)

		cnsRegisterVolumeName := cnsUnRegisterVolume.GetName()
		framework.Logf("CNS unregister volume name : %s", cnsRegisterVolumeName)

		err = createCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete CNS unregister volume CR by name " + cnsRegisterVolumeName)
			err = deleteCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for  CNS unregister volume to be unregistered")
		gomega.Expect(waitForCNSUnRegisterVolumeToGetUnregistered(ctx,
			restConfig, cnsUnRegisterVolume, poll,
			supervisorClusterOperationsTimeout)).To(gomega.HaveOccurred())
	}

	ginkgo.By("Deleting the pod")
	err = fpod.DeletePodWithWait(ctx, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Retry: Invoking CNS Unregister Volume API for all the FCD's created above")
	for _, pv := range persistentvolumes {
		volumeID := pv.Spec.CSI.VolumeHandle
		time.Sleep(30 * time.Second)

		if guestCluster {
			volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
		}

		ginkgo.By("Create CNS unregister volume with above created FCD " + pv.Spec.CSI.VolumeHandle)

		cnsUnRegisterVolume := getCNSUnregisterVolumeSpec(svNamespace, volumeID)

		cnsRegisterVolumeName := cnsUnRegisterVolume.GetName()
		framework.Logf("CNS unregister volume name : %s", cnsRegisterVolumeName)

		err = createCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Delete CNS unregister volume CR by name " + cnsRegisterVolumeName)
			err = deleteCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Waiting for  CNS unregister volume to be unregistered")
		gomega.Expect(waitForCNSUnRegisterVolumeToGetUnregistered(ctx,
			restConfig, cnsUnRegisterVolume, poll,
			supervisorClusterOperationsTimeout)).To(gomega.HaveOccurred())
	}
}

// exportVolumeWithServiceDown creates the volumes and immediately stops the services and wait for
// the service to be up again and validates the volumes are bound
func exportVolumeWithServiceDown(serviceName string, namespace string, client clientset.Interface,
	scParameters map[string]string, volumeOpsScale int, isServiceStopped bool,
	c clientset.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By(fmt.Sprintf("Invoking Test for create volume when %v goes down", serviceName))
	var storageclass *storagev1.StorageClass
	var persistentvolumes []*v1.PersistentVolume
	var pvclaims []*v1.PersistentVolumeClaim
	var cnscrds []*cnsunregistervolumev1alpha1.CnsUnregisterVolume
	var err error
	var fullSyncWaitTime int
	pvclaims = make([]*v1.PersistentVolumeClaim, volumeOpsScale)
	cnscrds = make([]*cnsunregistervolumev1alpha1.CnsUnregisterVolume, volumeOpsScale)

	// Decide which test setup is available to run
	if vanillaCluster {
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		// TODO: Create Thick Storage Policy from Pre-setup to support 6.7 Setups
		scParameters[scParamStoragePolicyName] = "Management Storage Policy - Regular"
		// Check if it is file volumes setups
		if rwxAccessMode {
			scParameters[scParamFsType] = nfs4FSType
		}
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		scName := "idempotency" + curtimestring + val
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, scName)
	} else if stretchedSVC {
		ginkgo.By("CNS_TEST: Running for Stretch setup")
		storagePolicyName := GetAndExpectStringEnvVar(envZonalStoragePolicyName)
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	} else if supervisorCluster {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		profileID := e2eVSphere.GetSpbmPolicyID(thickProvPolicy)
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		storageclass, err = createStorageClass(client, scParameters, nil, "", "", false, thickProvPolicy)
	} else {
		ginkgo.By("CNS_TEST: Running for GC setup")
		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		scParameters[svStorageClassName] = thickProvPolicy
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, thickProvPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		if vanillaCluster {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name,
				*metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	ginkgo.By("Creating PVCs using the Storage Class")
	framework.Logf("VOLUME_OPS_SCALE is set to %v", volumeOpsScale)
	for i := 0; i < volumeOpsScale; i++ {
		framework.Logf("Creating pvc%v", i)
		accessMode := v1.ReadWriteOnce

		// Check if it is file volumes setups
		if rwxAccessMode {
			accessMode = v1.ReadWriteMany
		}
		pvclaims[i], err = createPVC(ctx, client, namespace, nil, "", storageclass, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
		2*framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if serviceName == "CSI" {
		// Get CSI Controller's replica count from the setup
		deployment, err := c.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount := *deployment.Spec.Replicas

		ginkgo.By("Stopping CSI driver")
		isServiceStopped, err = stopCSIPods(ctx, c, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				framework.Logf("Starting CSI driver")
				isServiceStopped, err = startCSIPods(ctx, c, csiReplicaCount, csiSystemNamespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Get a config to talk to the apiserver
		restConfig := getRestConfigClient()
		time.Sleep(30 * time.Second)

		for i := 0; i < volumeOpsScale; i++ {

			volumeID := persistentvolumes[i].Spec.CSI.VolumeHandle

			if guestCluster {
				volumeID = getVolumeIDFromSupervisorCluster(persistentvolumes[i].Spec.CSI.VolumeHandle)
			}

			ginkgo.By("Create CNS unregister volume with above created FCD " + persistentvolumes[i].Spec.CSI.VolumeHandle)

			cnsUnRegisterVolume := getCNSUnregisterVolumeSpec(namespace, volumeID)
			cnscrds[i] = cnsUnRegisterVolume

			cnsRegisterVolumeName := cnsUnRegisterVolume.GetName()
			framework.Logf("CNS unregister volume name : %s", cnsRegisterVolumeName)

			err = createCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			defer func() {
				ginkgo.By("Delete CNS unregister volume CR by name " + cnsRegisterVolumeName)
				err = deleteCNSUnRegisterVolume(ctx, restConfig, cnsUnRegisterVolume)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("CNS unregister volume should fail")
			gomega.Expect(waitForCNSUnRegisterVolumeToGetUnregistered(ctx,
				restConfig, cnsUnRegisterVolume, poll,
				supervisorClusterOperationsTimeout)).To(gomega.HaveOccurred())
		}

		framework.Logf("Starting CSI driver")
		isServiceStopped, err = startCSIPods(ctx, c, csiReplicaCount, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Full sync interval can be 1 min at minimum so full sync wait time has to be more than 120s
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", fullSyncWaitTime))
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		for i := 0; i < volumeOpsScale; i++ {
			ginkgo.By("Waiting for  CNS unregister volume to be unregistered")
			framework.ExpectNoError(waitForCNSUnRegisterVolumeToGetUnregistered(ctx,
				restConfig, cnscrds[i], poll, supervisorClusterOperationsTimeout))
		}

	} else if serviceName == hostdServiceName {
		ginkgo.By("Fetch IPs for the all the hosts in the cluster")
		hostIPs := getAllHostsIP(ctx, true)
		isServiceStopped = true

		var wg sync.WaitGroup
		wg.Add(len(hostIPs))

		for _, hostIP := range hostIPs {
			go stopHostD(ctx, hostIP, &wg)
		}
		wg.Wait()

		defer func() {
			framework.Logf("In defer function to start the hostd service on all hosts")
			if isServiceStopped {
				for _, hostIP := range hostIPs {
					startHostDOnHost(ctx, hostIP)
				}
				isServiceStopped = false
			}
		}()

		ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
		time.Sleep(pollTimeoutSixMin)

		for _, hostIP := range hostIPs {
			startHostDOnHost(ctx, hostIP)
		}
		isServiceStopped = false
	} else {
		ginkgo.By(fmt.Sprintf("Stopping %v on the vCenter host", serviceName))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(ctx, stopOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = true
		err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isServiceStopped {
				ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
				err = invokeVCenterServiceControl(ctx, startOperation, serviceName, vcAddress)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcRunningMessage)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isServiceStopped = false
			}
		}()

		ginkgo.By("Sleeping for 5+1 min for default provisioner timeout")
		time.Sleep(pollTimeoutSixMin)

		ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", serviceName))
		err = invokeVCenterServiceControl(ctx, startOperation, serviceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isServiceStopped = false
		err = waitVCenterServiceToBeInState(ctx, serviceName, vcAddress, svcRunningMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Sleeping for full sync interval")
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)
	}

	//After service restart
	bootstrap()

	// TODO: Add a logic to check for the no orphan volumes
	defer func() {
		for _, claim := range pvclaims {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, framework.Poll,
				framework.PodDeleteTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeID := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeID))
		}
	}()
}
