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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"golang.org/x/crypto/ssh"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	pkgtypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubectl/pkg/drain"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	"k8s.io/kubernetes/test/e2e/framework/manifest"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

var (
	svcClient              clientset.Interface
	svcNamespace           string
	vsanHealthClient       *VsanClient
	clusterComputeResource []*object.ClusterComputeResource
	hosts                  []*object.HostSystem
	defaultDatastore       *object.Datastore
	restConfig             *rest.Config
	pvclaims               []*v1.PersistentVolumeClaim
	pvclaimsToDelete       []*v1.PersistentVolumeClaim
)

// getVSphereStorageClassSpec returns Storage Class Spec with supplied storage
// class parameters.
func getVSphereStorageClassSpec(scName string, scParameters map[string]string,
	allowedTopologies []v1.TopologySelectorLabelRequirement, scReclaimPolicy v1.PersistentVolumeReclaimPolicy,
	bindingMode storagev1.VolumeBindingMode, allowVolumeExpansion bool) *storagev1.StorageClass {
	if bindingMode == "" {
		bindingMode = storagev1.VolumeBindingImmediate
	}
	var sc = &storagev1.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "sc-",
		},
		Provisioner:          e2evSphereCSIDriverName,
		VolumeBindingMode:    &bindingMode,
		AllowVolumeExpansion: &allowVolumeExpansion,
	}
	// If scName is specified, use that name, else auto-generate storage class
	// name.
	if scName != "" {
		sc.ObjectMeta.Name = scName
	}
	if scParameters != nil {
		sc.Parameters = scParameters
	}
	if allowedTopologies != nil {
		sc.AllowedTopologies = []v1.TopologySelectorTerm{
			{
				MatchLabelExpressions: allowedTopologies,
			},
		}
	}
	if scReclaimPolicy != "" {
		sc.ReclaimPolicy = &scReclaimPolicy
	}

	return sc
}

// getPvFromClaim returns PersistentVolume for requested claim.
func getPvFromClaim(client clientset.Interface, namespace string, claimName string) *v1.PersistentVolume {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvclaim.Spec.VolumeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return pv
}

// getNodeUUID returns Node VM UUID for requested node.
func getNodeUUID(client clientset.Interface, nodeName string) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vmUUID := strings.TrimPrefix(node.Spec.ProviderID, providerPrefix)
	gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
	ginkgo.By(fmt.Sprintf("VM UUID is: %s for node: %s", vmUUID, nodeName))
	return vmUUID
}

// getVMUUIDFromNodeName returns the vmUUID for a given node vm and datacenter.
func getVMUUIDFromNodeName(nodeName string) (string, error) {
	var datacenters []string
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
	var vm *object.VirtualMachine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, dc := range datacenters {
		dataCenter, err := finder.Datacenter(ctx, dc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		finder := find.NewFinder(dataCenter.Client(), false)
		finder.SetDatacenter(dataCenter)
		vm, err = finder.VirtualMachine(ctx, nodeName)
		if err != nil {
			continue
		}
		vmUUID := vm.UUID(ctx)
		gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("VM UUID is: %s for node: %s", vmUUID, nodeName))
		return vmUUID, nil
	}
	return "", err
}

// verifyVolumeMetadataInCNS verifies container volume metadata is matching the
// one is CNS cache.
func verifyVolumeMetadataInCNS(vs *vSphere, volumeID string,
	PersistentVolumeClaimName string, PersistentVolumeName string,
	PodName string, Labels ...vim25types.KeyValue) error {
	queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
	if err != nil {
		return err
	}
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
		return fmt.Errorf("failed to query cns volume %s", volumeID)
	}
	for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
		kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
		if kubernetesMetadata.EntityType == "POD" && kubernetesMetadata.EntityName != PodName {
			return fmt.Errorf("entity Pod with name %s not found for volume %s", PodName, volumeID)
		} else if kubernetesMetadata.EntityType == "PERSISTENT_VOLUME" &&
			kubernetesMetadata.EntityName != PersistentVolumeName {
			return fmt.Errorf("entity PV with name %s not found for volume %s", PersistentVolumeName, volumeID)
		} else if kubernetesMetadata.EntityType == "PERSISTENT_VOLUME_CLAIM" &&
			kubernetesMetadata.EntityName != PersistentVolumeClaimName {
			return fmt.Errorf("entity PVC with name %s not found for volume %s", PersistentVolumeClaimName, volumeID)
		}
	}
	labelMap := make(map[string]string)
	for _, e := range queryResult.Volumes[0].Metadata.EntityMetadata {
		if e == nil {
			continue
		}
		if e.GetCnsEntityMetadata().Labels == nil {
			continue
		}
		for _, al := range e.GetCnsEntityMetadata().Labels {
			// These are the actual labels in the provisioned PV. Populate them
			// in the label map.
			labelMap[al.Key] = al.Value
		}
		for _, el := range Labels {
			// Traverse through the slice of expected labels and see if all of them
			// are present in the label map.
			if val, ok := labelMap[el.Key]; ok {
				gomega.Expect(el.Value == val).To(gomega.BeTrue(),
					fmt.Sprintf("Actual label Value of the statically provisioned PV is %s but expected is %s",
						val, el.Value))
			} else {
				return fmt.Errorf("label(%s:%s) is expected in the provisioned PV but its not found", el.Key, el.Value)
			}
		}
	}
	ginkgo.By(fmt.Sprintf("successfully verified metadata of the volume %q", volumeID))
	return nil
}

// getVirtualDeviceByDiskID gets the virtual device by diskID.
func getVirtualDeviceByDiskID(ctx context.Context, vm *object.VirtualMachine,
	diskID string) (vim25types.BaseVirtualDevice, error) {
	vmname, err := vm.Common.ObjectName(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		framework.Logf("failed to get the devices for VM: %q. err: %+v", vmname, err)
		return nil, err
	}
	for _, device := range vmDevices {
		if vmDevices.TypeName(device) == "VirtualDisk" {
			if virtualDisk, ok := device.(*vim25types.VirtualDisk); ok {
				if virtualDisk.VDiskId != nil && virtualDisk.VDiskId.Id == diskID {
					framework.Logf("Found FCDID %q attached to VM %q", diskID, vmname)
					return device, nil
				}
			}
		}
	}
	framework.Logf("failed to find FCDID %q attached to VM %q", diskID, vmname)
	return nil, nil
}

// getPersistentVolumeClaimSpecWithStorageClass return the PersistentVolumeClaim
// spec with specified storage class.
func getPersistentVolumeClaimSpecWithStorageClass(namespace string, ds string, storageclass *storagev1.StorageClass,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	disksize := diskSize
	if ds != "" {
		disksize = ds
	}
	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteOnce
	}
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(disksize),
				},
			},
			StorageClassName: &(storageclass.Name),
		},
	}

	if pvclaimlabels != nil {
		claim.Labels = pvclaimlabels
	}

	return claim
}

// createPVCAndStorageClass helps creates a storage class with specified name,
// storageclass parameters and PVC using storage class.
func createPVCAndStorageClass(client clientset.Interface, pvcnamespace string,
	pvclaimlabels map[string]string, scParameters map[string]string, ds string,
	allowedTopologies []v1.TopologySelectorLabelRequirement, bindingMode storagev1.VolumeBindingMode,
	allowVolumeExpansion bool, accessMode v1.PersistentVolumeAccessMode,
	names ...string) (*storagev1.StorageClass, *v1.PersistentVolumeClaim, error) {
	scName := ""
	if len(names) > 0 {
		scName = names[0]
	}
	storageclass, err := createStorageClass(client, scParameters,
		allowedTopologies, "", bindingMode, allowVolumeExpansion, scName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvclaim, err := createPVC(client, pvcnamespace, pvclaimlabels, ds, storageclass, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return storageclass, pvclaim, err
}

// createStorageClass helps creates a storage class with specified name,
// storageclass parameters.
func createStorageClass(client clientset.Interface, scParameters map[string]string,
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	scReclaimPolicy v1.PersistentVolumeReclaimPolicy, bindingMode storagev1.VolumeBindingMode,
	allowVolumeExpansion bool, scName string) (*storagev1.StorageClass, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By(fmt.Sprintf("Creating StorageClass %s with scParameters: %+v and allowedTopologies: %+v "+
		"and ReclaimPolicy: %+v and allowVolumeExpansion: %t",
		scName, scParameters, allowedTopologies, scReclaimPolicy, allowVolumeExpansion))
	storageclass, err := client.StorageV1().StorageClasses().Create(ctx, getVSphereStorageClassSpec(scName,
		scParameters, allowedTopologies, scReclaimPolicy, bindingMode, allowVolumeExpansion), metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create storage class with err: %v", err))
	return storageclass, err
}

// createPVC helps creates pvc with given namespace and labels using given
// storage class.
func createPVC(client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string, ds string,
	storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode) (*v1.PersistentVolumeClaim, error) {
	pvcspec := getPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
	ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s and labels: %+v accessMode: %+v",
		storageclass.Name, ds, pvclaimlabels, accessMode))
	pvclaim, err := fpv.CreatePVC(client, pvcnamespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))
	framework.Logf("PVC created: %v in namespace: %v", pvclaim.Name, pvcnamespace)
	return pvclaim, err
}

// createPVC helps creates pvc with given namespace and labels using given
// storage class.
func scaleCreatePVC(client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string, ds string,
	storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode, wg *sync.WaitGroup) {
	defer wg.Done()

	pvcspec := getPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
	ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s and labels: %+v accessMode: %+v",
		storageclass.Name, ds, pvclaimlabels, accessMode))
	pvclaim, err := fpv.CreatePVC(client, pvcnamespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))
	pvclaims = append(pvclaims, pvclaim)

}

// scaleCreateDeletePVC helps create and delete pvc with given namespace and
// labels, using given storage class (envWorkerPerRoutine * envNumberOfGoRoutines)
// times. Create and Delete PVC happens synchronously. PVC is picked randomly
// for deletion.
func scaleCreateDeletePVC(client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string,
	ds string, storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode,
	wg *sync.WaitGroup, lock *sync.Mutex, worker int) {
	ctx, cancel := context.WithCancel(context.Background())
	var totalPVCDeleted int = 0
	defer cancel()
	defer wg.Done()
	for index := 1; index <= worker; index++ {
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
		ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s and labels: %+v accessMode: %+v",
			storageclass.Name, ds, pvclaimlabels, accessMode))
		pvclaim, err := fpv.CreatePVC(client, pvcnamespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		lock.Lock()
		pvclaims = append(pvclaims, pvclaim)
		pvclaimToDelete := randomPickPVC()
		lock.Unlock()

		// Check if volume is present or not.
		pvclaimToDelete, err = client.CoreV1().PersistentVolumeClaims(pvclaimToDelete.Namespace).Get(
			ctx, pvclaimToDelete.Name, metav1.GetOptions{})

		if err == nil {
			// Waiting for PVC to be bound.
			pvclaimsToDelete := append(pvclaimsToDelete, pvclaimToDelete)
			_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaimsToDelete, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaimToDelete.Name, pvcnamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			totalPVCDeleted++
		}
	}
	framework.Logf("Total number of deleted PVCs %v", totalPVCDeleted)
}

// randomPickPVC returns randomly picked PVC from list of PVCs.
func randomPickPVC() *v1.PersistentVolumeClaim {
	index := rand.Intn(len(pvclaims))
	pvclaims[len(pvclaims)-1], pvclaims[index] = pvclaims[index], pvclaims[len(pvclaims)-1]
	pvclaimToDelete := pvclaims[len(pvclaims)-1]
	pvclaims = pvclaims[:len(pvclaims)-1]
	framework.Logf("pvc to delete %v", pvclaimToDelete.Name)
	return pvclaimToDelete
}

// createStatefulSetWithOneReplica helps create a stateful set with one replica.
func createStatefulSetWithOneReplica(client clientset.Interface, manifestPath string,
	namespace string) (*appsv1.StatefulSet, *v1.Service) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mkpath := func(file string) string {
		return filepath.Join(manifestPath, file)
	}
	statefulSet, err := manifest.StatefulSetFromManifest(mkpath("statefulset.yaml"), namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	service, err := manifest.SvcFromManifest(mkpath("service.yaml"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	service, err = client.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	*statefulSet.Spec.Replicas = 1
	_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulSet, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return statefulSet, service
}

// updateDeploymentReplicawithWait helps to update the replica for a deployment
// with wait.
func updateDeploymentReplicawithWait(client clientset.Interface, count int32, name string, namespace string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var deployment *appsv1.Deployment
	var err error
	waitErr := wait.Poll(healthStatusPollInterval, healthStatusPollTimeout, func() (bool, error) {
		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if err != nil {
			return false, nil
		}
		*deployment.Spec.Replicas = count
		ginkgo.By("Waiting for update operation on deployment to take effect")
		deployment, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			return false, err
		}
		err = fdep.WaitForDeploymentComplete(client, deployment)
		if err != nil {
			return false, err
		}
		return true, nil
	})
	return waitErr
}

// updateDeploymentReplica helps to update the replica for a deployment.
func updateDeploymentReplica(client clientset.Interface,
	count int32, name string, namespace string) *appsv1.Deployment {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deployment, err := client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	*deployment.Spec.Replicas = count
	deployment, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = fdep.WaitForDeploymentComplete(client, deployment)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return deployment
}

// bringDownCsiController helps to bring the csi controller pod down.
// Default namespace used here is csiSystemNamespace.
func bringDownCsiController(Client clientset.Interface, namespace ...string) {
	if len(namespace) == 0 {
		updateDeploymentReplica(Client, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
	} else {
		updateDeploymentReplica(Client, 0, vSphereCSIControllerPodNamePrefix, namespace[0])
	}
	ginkgo.By("Controller is down")
}

// bringDownTKGController helps to bring the TKG control manager pod down.
// Its taks svc client as input.
func bringDownTKGController(Client clientset.Interface) {
	updateDeploymentReplica(Client, 0, vsphereControllerManager, vsphereTKGSystemNamespace)
	ginkgo.By("TKGControllManager replica is set to 0")
}

// bringUpCsiController helps to bring the csi controller pod down.
// Default namespace used here is csiSystemNamespace.
func bringUpCsiController(Client clientset.Interface, namespace ...string) {
	if len(namespace) == 0 {
		err := updateDeploymentReplicawithWait(Client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		err := updateDeploymentReplicawithWait(Client, 1, vSphereCSIControllerPodNamePrefix, namespace[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	ginkgo.By("Controller is up")
}

// bringUpTKGController helps to bring the TKG control manager pod up.
// Its taks svc client as input.
func bringUpTKGController(Client clientset.Interface) {
	err := updateDeploymentReplicawithWait(Client, 1, vsphereControllerManager, vsphereTKGSystemNamespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("TKGControllManager is up")
}

func getSvcClientAndNamespace() (clientset.Interface, string) {
	var err error
	if svcClient == nil {
		if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
			svcClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		svcNamespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	}
	return svcClient, svcNamespace
}

// updateCSIDeploymentTemplateFullSyncInterval helps to update the
// FULL_SYNC_INTERVAL_MINUTES in deployment template. For this to take effect,
// we need to terminate the running csi controller pod.
// Returns fsync interval value before the change.
func updateCSIDeploymentTemplateFullSyncInterval(client clientset.Interface, mins string, namespace string) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deployment, err := client.AppsV1().Deployments(namespace).Get(
		ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	containers := deployment.Spec.Template.Spec.Containers
	oldValue := ""
	for _, c := range containers {
		if c.Name == "vsphere-syncer" {
			for _, e := range c.Env {
				if e.Name == "FULL_SYNC_INTERVAL_MINUTES" {
					oldValue = e.Value
					e.Value = mins
				}
			}
		}
	}
	deployment.Spec.Template.Spec.Containers = containers
	_, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Waiting for a min for update operation on deployment to take effect...")
	time.Sleep(1 * time.Minute)
	return oldValue
}

// getLabelsMapFromKeyValue returns map[string]string for given array of
// vim25types.KeyValue.
func getLabelsMapFromKeyValue(labels []vim25types.KeyValue) map[string]string {
	labelsMap := make(map[string]string)
	for _, label := range labels {
		labelsMap[label.Key] = label.Value
	}
	return labelsMap
}

// getDatastoreByURL returns the *Datastore instance given its URL.
func getDatastoreByURL(ctx context.Context, datastoreURL string, dc *object.Datacenter) (*object.Datastore, error) {
	finder := find.NewFinder(dc.Client(), false)
	finder.SetDatacenter(dc)
	datastores, err := finder.DatastoreList(ctx, "*")
	if err != nil {
		framework.Logf("failed to get all the datastores. err: %+v", err)
		return nil, err
	}
	var dsList []vim25types.ManagedObjectReference
	for _, ds := range datastores {
		dsList = append(dsList, ds.Reference())
	}

	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(dc.Client())
	properties := []string{"info"}
	err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
	if err != nil {
		framework.Logf("failed to get Datastore managed objects from datastore objects."+
			" dsObjList: %+v, properties: %+v, err: %v", dsList, properties, err)
		return nil, err
	}
	for _, dsMo := range dsMoList {
		if dsMo.Info.GetDatastoreInfo().Url == datastoreURL {
			return object.NewDatastore(dc.Client(),
				dsMo.Reference()), nil
		}
	}
	err = fmt.Errorf("couldn't find Datastore given URL %q", datastoreURL)
	return nil, err
}

// getPersistentVolumeClaimSpec gets vsphere persistent volume spec with given
// selector labels and binds it to given pv.
func getPersistentVolumeClaimSpec(namespace string, labels map[string]string, pvName string) *v1.PersistentVolumeClaim {
	var (
		pvc *v1.PersistentVolumeClaim
	)
	sc := ""
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
				},
			},
			VolumeName:       pvName,
			StorageClassName: &sc,
		},
	}
	if labels != nil {
		pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}

	return pvc
}

// Create PV volume spec with given FCD ID, Reclaim Policy and labels.
func getPersistentVolumeSpec(fcdID string, persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy,
	labels map[string]string) *v1.PersistentVolume {
	var (
		pvConfig fpv.PersistentVolumeConfig
		pv       *v1.PersistentVolume
		claimRef *v1.ObjectReference
	)
	pvConfig = fpv.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       e2evSphereCSIDriverName,
				VolumeHandle: fcdID,
				ReadOnly:     false,
				FSType:       "ext4",
			},
		},
		Prebind: nil,
	}

	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			ClaimRef:         claimRef,
			StorageClassName: "",
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = e2evSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// Create PVC spec with given namespace, labels and pvName.
func getPersistentVolumeClaimSpecForRWX(namespace string, labels map[string]string,
	pvName string, pvSize string) *v1.PersistentVolumeClaim {
	var (
		pvc *v1.PersistentVolumeClaim
	)
	if pvSize == "" {
		pvSize = "2Gi"
	}
	sc := ""
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteMany,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(pvSize),
				},
			},
			VolumeName:       pvName,
			StorageClassName: &sc,
		},
	}
	if labels != nil {
		pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}

	return pvc
}

// Create PV volume spec with given FCD ID, Reclaim Policy and labels.
func getPersistentVolumeSpecForRWX(fcdID string, persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy,
	labels map[string]string, pvSize string, storageclass string,
	accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolume {
	var (
		pvConfig fpv.PersistentVolumeConfig
		pv       *v1.PersistentVolume
		claimRef *v1.ObjectReference
	)

	if pvSize == "" {
		pvSize = "2Gi"
	}

	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteMany
	}

	pvConfig = fpv.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       e2evSphereCSIDriverName,
				VolumeHandle: fcdID,
				ReadOnly:     false,
			},
		},
		Prebind: nil,
	}

	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse(pvSize),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			ClaimRef:         claimRef,
			StorageClassName: storageclass,
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = e2evSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// invokeVCenterReboot invokes reboot command on the given vCenter over SSH.
func invokeVCenterReboot(host string) error {
	sshCmd := "reboot"
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	return nil
}

// invokeVCenterServiceControl invokes the given command for the given service
// via service-control on the given vCenter host over SSH.
func invokeVCenterServiceControl(command, service, host string) error {
	sshCmd := fmt.Sprintf("service-control --%s %s", command, service)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	return nil
}

// replacePasswordRotationTime invokes the given command to replace the password
// rotation time to 0, so that password roation happens immediately on the given
// vCenter over SSH. Vmon-cli is used to restart the wcp service after changing
// the time.
func replacePasswordRotationTime(file, host string) error {
	sshCmd := fmt.Sprintf("sed -i '3 c\\0' %s", file)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	sshCmd = fmt.Sprintf("vmon-cli -r %s", wcpServiceName)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err = fssh.SSH(sshCmd, host, framework.TestContext.Provider)
	time.Sleep(sleepTimeOut)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	return nil
}

// writeToFile will take two parameters:
// 1. the absolute path of the file(including filename) to be created.
// 2. data content to be written into the file.
// Returns nil on Success and error on failure.
func writeToFile(filePath, data string) error {
	if filePath == "" {
		return fmt.Errorf("invalid filename")
	}
	f, err := os.Create(filePath)
	if err != nil {
		framework.Logf("Error: %v", err)
		return err
	}
	_, err = f.WriteString(data)
	if err != nil {
		framework.Logf("Error: %v", err)
		f.Close()
		return err
	}
	err = f.Close()
	if err != nil {
		framework.Logf("Error: %v", err)
		return err
	}
	return nil
}

// invokeVCenterChangePassword invokes `dir-cli password reset` command on the
// given vCenter host over SSH, thereby resetting the currentPassword of the
// `user` to the `newPassword`.
func invokeVCenterChangePassword(user, adminPassword, newPassword, host string) error {
	// Create an input file and write passwords into it.
	path := "input.txt"
	data := fmt.Sprintf("%s\n%s\n", adminPassword, newPassword)
	err := writeToFile(path, data)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		// Delete the input file containing passwords.
		err = os.Remove(path)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	// Remote copy this input file to VC.
	copyCmd := fmt.Sprintf("/bin/cat %s | /usr/bin/ssh root@%s '/usr/bin/cat >> input_copy.txt'",
		path, e2eVSphere.Config.Global.VCenterHostname)
	fmt.Printf("Executing the command: %s\n", copyCmd)
	_, err = exec.Command("/bin/sh", "-c", copyCmd).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		// Remove the input_copy.txt file from VC.
		removeCmd := fmt.Sprintf("/usr/bin/ssh root@%s '/usr/bin/rm input_copy.txt'",
			e2eVSphere.Config.Global.VCenterHostname)
		_, err = exec.Command("/bin/sh", "-c", removeCmd).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	sshCmd :=
		fmt.Sprintf("/usr/bin/cat input_copy.txt | /usr/lib/vmware-vmafd/bin/dir-cli password reset --account %s", user)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	if !strings.Contains(result.Stdout, "Password was reset successfully for ") {
		framework.Logf("failed to change the password for user %s: %s", user, result.Stdout)
		return err
	}
	framework.Logf("password changed successfully for user: %s", user)
	return nil
}

// verifyVolumeTopology verifies that the Node Affinity rules in the volume.
// Match the topology constraints specified in the storage class.
func verifyVolumeTopology(pv *v1.PersistentVolume, zoneValues []string, regionValues []string) (string, string, error) {
	if pv.Spec.NodeAffinity == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return "", "", fmt.Errorf("node Affinity rules for PV should exist in topology aware provisioning")
	}
	var pvZone string
	var pvRegion string
	for _, labels := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions {
		if labels.Key == zoneKey {
			for _, value := range labels.Values {
				gomega.Expect(zoneValues).To(gomega.ContainElement(value),
					fmt.Sprintf("Node Affinity rules for PV %s: %v does not contain zone specified in storage class %v",
						pv.Name, value, zoneValues))
				pvZone = value
			}
		}
		if labels.Key == regionKey {
			for _, value := range labels.Values {
				gomega.Expect(regionValues).To(gomega.ContainElement(value),
					fmt.Sprintf("Node Affinity rules for PV %s: %v does not contain region specified in storage class %v",
						pv.Name, value, regionValues))
				pvRegion = value
			}
		}
	}
	framework.Logf("PV %s is located in zone: %s and region: %s", pv.Name, pvZone, pvRegion)
	return pvRegion, pvZone, nil
}

// verifyPodLocation verifies that a pod is scheduled on a node that belongs
// to the topology on which PV is provisioned.
func verifyPodLocation(pod *v1.Pod, nodeList *v1.NodeList, zoneValue string, regionValue string) error {
	for _, node := range nodeList.Items {
		if pod.Spec.NodeName == node.Name {
			for labelKey, labelValue := range node.Labels {
				if labelKey == zoneKey && zoneValue != "" {
					gomega.Expect(zoneValue).To(gomega.Equal(labelValue),
						fmt.Sprintf("Pod %s is not running on Node located in zone %v", pod.Name, zoneValue))
				}
				if labelKey == regionKey && regionValue != "" {
					gomega.Expect(regionValue).To(gomega.Equal(labelValue),
						fmt.Sprintf("Pod %s is not running on Node located in region %v", pod.Name, regionValue))
				}
			}
		}
	}
	return nil
}

// getTopologyFromPod rturn topology value from node affinity information.
func getTopologyFromPod(pod *v1.Pod, nodeList *v1.NodeList) (string, string, error) {
	for _, node := range nodeList.Items {
		if pod.Spec.NodeName == node.Name {
			podRegion := node.Labels[v1.LabelZoneRegion]
			podZone := node.Labels[v1.LabelZoneFailureDomain]
			return podRegion, podZone, nil
		}
	}
	err := errors.New("could not find the topology from pod")
	return "", "", err
}

// topologyParameterForStorageClass creates a topology map using the topology
// values ENV variables. Returns the allowedTopologies parameters required for
// the Storage Class.
// Input : <region-1>:<zone-1>, <region-1>:<zone-2>
// Output : [region-1], [zone-1, zone-2] {region-1: zone-1, region-1:zone-2}
func topologyParameterForStorageClass(topology string) ([]string, []string, []v1.TopologySelectorLabelRequirement) {
	topologyMap := createTopologyMap(topology)
	regionValues, zoneValues := getValidTopology(topologyMap)
	allowedTopologies := []v1.TopologySelectorLabelRequirement{
		{
			Key:    regionKey,
			Values: regionValues,
		},
		{
			Key:    zoneKey,
			Values: zoneValues,
		},
	}
	return regionValues, zoneValues, allowedTopologies
}

// createTopologyMap strips the topology string provided in the environment
// variable into a map from region to zones.
// example envTopology = "r1:z1,r1:z2,r2:z3"
func createTopologyMap(topologyString string) map[string][]string {
	topologyMap := make(map[string][]string)
	for _, t := range strings.Split(topologyString, ",") {
		t = strings.TrimSpace(t)
		topology := strings.Split(t, ":")
		if len(topology) != 2 {
			continue
		}
		topologyMap[topology[0]] = append(topologyMap[topology[0]], topology[1])
	}
	return topologyMap
}

// getValidTopology returns the regions and zones from the input topology map
// so that they can be provided to a storage class.
func getValidTopology(topologyMap map[string][]string) ([]string, []string) {
	var regionValues []string
	var zoneValues []string
	for region, zones := range topologyMap {
		regionValues = append(regionValues, region)
		zoneValues = append(zoneValues, zones...)
	}
	return regionValues, zoneValues
}

// createResourceQuota creates resource quota for the specified namespace.
func createResourceQuota(client clientset.Interface, namespace string, size string, scName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitTime := 15
	// deleteResourceQuota if already present.
	deleteResourceQuota(client, namespace)

	resourceQuota := newTestResourceQuota(quotaName, size, scName)
	resourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Create(ctx, resourceQuota, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("Create Resource quota: %+v", resourceQuota))
	ginkgo.By(fmt.Sprintf("Waiting for %v seconds to allow resourceQuota to be claimed", waitTime))
	time.Sleep(time.Duration(waitTime) * time.Second)
}

// deleteResourceQuota deletes resource quota for the specified namespace,
// if it exists.
func deleteResourceQuota(client clientset.Interface, namespace string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := client.CoreV1().ResourceQuotas(namespace).Get(ctx, quotaName, metav1.GetOptions{})
	if err == nil {
		err = client.CoreV1().ResourceQuotas(namespace).Delete(ctx, quotaName, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Deleted Resource quota: %+v", quotaName))
	}
}

// setResourceQuota resource quota to the specified limit for the given namespace.
func setResourceQuota(client clientset.Interface, namespace string, size string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// deleteResourceQuota if already present.
	deleteResourceQuota(client, namespace)

	existingResourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Get(ctx, namespace, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("existingResourceQuota name %s", existingResourceQuota.GetName())
	requestStorageQuota := updatedSpec4ExistingResourceQuota(existingResourceQuota.GetName(), size)
	testResourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Update(
		ctx, requestStorageQuota, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("ResourceQuota details: %+v", testResourceQuota))
	// TODO: Add polling instead of static wait time and assert against the
	// updated quota.
	ginkgo.By(fmt.Sprintf("Sleeping for %v seconds", sleepTimeOut))
	time.Sleep(sleepTimeOut * time.Second)

}

// newTestResourceQuota returns a quota that enforces default constraints for
// testing.
func newTestResourceQuota(name string, size string, scName string) *v1.ResourceQuota {
	hard := v1.ResourceList{}
	// Test quota on discovered resource type.
	hard[v1.ResourceName(scName+rqStorageType)] = resource.MustParse(size)
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.ResourceQuotaSpec{Hard: hard},
	}
}

// updatedSpec4ExistingResourceQuota returns a quota that enforces default
// constraints for testing.
func updatedSpec4ExistingResourceQuota(name string, size string) *v1.ResourceQuota {
	updateQuota := v1.ResourceList{}
	// Test quota on discovered resource type.
	updateQuota[v1.ResourceName("requests.storage")] = resource.MustParse(size)
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.ResourceQuotaSpec{Hard: updateQuota},
	}
}

// checkEventsforError prints the list of all events that occurred in the
// namespace and searches for expectedErrorMsg among these events.
func checkEventsforError(client clientset.Interface, namespace string,
	listOptions metav1.ListOptions, expectedErrorMsg string) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventList, _ := client.CoreV1().Events(namespace).List(ctx, listOptions)
	isFailureFound := false
	for _, item := range eventList.Items {
		framework.Logf("EventList item: %q", item.Message)
		if strings.Contains(item.Message, expectedErrorMsg) {
			isFailureFound = true
			break
		}
	}
	return isFailureFound
}

// getNamespaceToRunTests returns the namespace in which the tests are expected
// to run. For Vanilla & GuestCluster test setups, returns random namespace name
// generated by the framework. For SupervisorCluster test setup, returns the
// user created namespace where pod vms will be provisioned.
func getNamespaceToRunTests(f *framework.Framework) string {
	if supervisorCluster {
		return GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	}
	return f.Namespace.Name
}

// getPVCFromSupervisorCluster takes name of the persistentVolumeClaim as input,
// returns the corresponding persistentVolumeClaim object.
func getPVCFromSupervisorCluster(pvcName string) *v1.PersistentVolumeClaim {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svcClient, svcNamespace := getSvcClientAndNamespace()
	pvc, err := svcClient.CoreV1().PersistentVolumeClaims(svcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvc).NotTo(gomega.BeNil())
	return pvc
}

// getVolumeIDFromSupervisorCluster returns SV PV volume handle for given SVC
// PVC name.
func getVolumeIDFromSupervisorCluster(pvcName string) string {
	var svcClient clientset.Interface
	var err error
	if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		svcClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	svcPV := getPvFromClaim(svcClient, svNamespace, pvcName)
	volumeHandle := svcPV.Spec.CSI.VolumeHandle
	ginkgo.By(fmt.Sprintf("Found volume in Supervisor cluster with VolumeID: %s", volumeHandle))
	return volumeHandle
}

// getPvFromSupervisorCluster returns SV PV for given SVC PVC name.
func getPvFromSupervisorCluster(pvcName string) *v1.PersistentVolume {
	var svcClient clientset.Interface
	var err error
	if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		svcClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	svcPV := getPvFromClaim(svcClient, svNamespace, pvcName)
	return svcPV
}

func verifyFilesExistOnVSphereVolume(namespace string, podName string, filePaths ...string) {
	for _, filePath := range filePaths {
		_, err := framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace),
			podName, "--", "/bin/ls", filePath)
		framework.ExpectNoError(err, fmt.Sprintf("failed to verify file: %q on the pod: %q", filePath, podName))
	}
}

func createEmptyFilesOnVSphereVolume(namespace string, podName string, filePaths []string) {
	for _, filePath := range filePaths {
		err := framework.CreateEmptyFileOnPod(namespace, podName, filePath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// CreateService creates a k8s service as described in the service.yaml present
// in the manifest path and returns that service to the caller.
func CreateService(ns string, c clientset.Interface) *v1.Service {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svcManifestFilePath := filepath.Join(manifestPath, "service.yaml")
	framework.Logf("Parsing service from %v", svcManifestFilePath)
	svc, err := manifest.SvcFromManifest(svcManifestFilePath)
	framework.ExpectNoError(err)

	service, err := c.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return service
}

// deleteService deletes a k8s service.
func deleteService(ns string, c clientset.Interface, service *v1.Service) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.CoreV1().Services(ns).Delete(ctx, service.Name, *metav1.NewDeleteOptions(0))
	framework.ExpectNoError(err)
}

// GetStatefulSetFromManifest creates a StatefulSet from the statefulset.yaml
// file present in the manifest path.
func GetStatefulSetFromManifest(ns string) *appsv1.StatefulSet {
	ssManifestFilePath := filepath.Join(manifestPath, "statefulset.yaml")
	framework.Logf("Parsing statefulset from %v", ssManifestFilePath)
	ss, err := manifest.StatefulSetFromManifest(ssManifestFilePath, ns)
	framework.ExpectNoError(err)
	return ss
}

// isDatastoreBelongsToDatacenterSpecifiedInConfig checks whether the given
// datastoreURL belongs to the datacenter specified in the vSphere.conf file.
func isDatastoreBelongsToDatacenterSpecifiedInConfig(datastoreURL string) bool {
	var datacenters []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		defaultDatacenter, _ := finder.Datacenter(ctx, dc)
		finder.SetDatacenter(defaultDatacenter)
		defaultDatastore, err := getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
		if defaultDatastore != nil && err == nil {
			return true
		}
	}

	// Loop through all datacenters specified in conf file, and cannot find this
	// given datastore.
	return false
}

func getTargetvSANFileShareDatastoreURLsFromConfig() []string {
	var targetDsURLs []string
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if cfg.Global.TargetvSANFileShareDatastoreURLs != "" {
		targetDsURLs = strings.Split(cfg.Global.TargetvSANFileShareDatastoreURLs, ",")
	}
	return targetDsURLs
}

func isDatastorePresentinTargetvSANFileShareDatastoreURLs(datastoreURL string) bool {
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	datastoreURL = strings.TrimSpace(datastoreURL)
	targetDatastoreUrls := strings.Split(cfg.Global.TargetvSANFileShareDatastoreURLs, ",")
	for _, dsURL := range targetDatastoreUrls {
		dsURL = strings.TrimSpace(dsURL)
		if datastoreURL == dsURL {
			return true
		}
	}
	return false
}

func verifyVolumeExistInSupervisorCluster(pvcName string) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svcClient, svNamespace := getSvcClientAndNamespace()
	svPvc, err := svcClient.CoreV1().PersistentVolumeClaims(svNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	framework.Logf("PVC in supervisor namespace: %s", svPvc.Name)
	return true
}

// returns crd if found by name.
func getCnsNodeVMAttachmentByName(ctx context.Context, f *framework.Framework, expectedInstanceName string,
	crdVersion string, crdGroup string) *cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment {
	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: "cnsnodevmattachments"}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	list, err := resourceClient.List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, crd := range list.Items {
		instance := &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if expectedInstanceName == instance.Name {
			ginkgo.By(fmt.Sprintf("Found CNSNodeVMAttachment crd: %v, expected: %v", instance, expectedInstanceName))
			framework.Logf("instance attached  is : %t\n", instance.Status.Attached)
			return instance
		}
	}
	return nil
}

// verifyIsAttachedInSupervisor verifies the crd instance is attached in
// supervisor.
func verifyIsAttachedInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdVersion string, crdGroup string) {
	instance := getCnsNodeVMAttachmentByName(ctx, f, expectedInstanceName, crdVersion, crdGroup)
	if instance != nil {
		framework.Logf("instance attached found to be : %t\n", instance.Status.Attached)
		gomega.Expect(instance.Status.Attached).To(gomega.BeTrue())
	}
	gomega.Expect(instance).NotTo(gomega.BeNil())
}

// verifyIsDetachedInSupervisor verifies the crd instance is detached from
// supervisor.
func verifyIsDetachedInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdVersion string, crdGroup string) {
	instance := getCnsNodeVMAttachmentByName(ctx, f, expectedInstanceName, crdVersion, crdGroup)
	if instance != nil {
		framework.Logf("instance attached found to be : %t\n", instance.Status.Attached)
		gomega.Expect(instance.Status.Attached).To(gomega.BeFalse())
	}
	gomega.Expect(instance).To(gomega.BeNil())
}

// verifyPodCreation helps to create/verify and delete the pod in given
// namespace. It takes client, namespace, pvc, pv as input.
func verifyPodCreation(f *framework.Framework, client clientset.Interface, namespace string,
	pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) {
	ginkgo.By("Create pod and wait for this to be in running phase")
	pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// svcPVCName refers to PVC Name in the supervisor cluster.
	svcPVCName := pv.Spec.CSI.VolumeHandle

	ginkgo.By(fmt.Sprintf("Verify cnsnodevmattachment is created with name : %s ", pod.Spec.NodeName))
	verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
		crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
	verifyIsAttachedInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdVersion, crdGroup)

	ginkgo.By("Deleting the pod")
	err = fpod.DeletePodWithWait(client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is detached from the node")
	isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
		fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

	ginkgo.By("Verify CnsNodeVmAttachment CRDs are deleted")
	verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
		crdCNSNodeVMAttachment, crdVersion, crdGroup, false)

}

// verifyCRDInSupervisor is a helper method to check if a given crd is
// created/deleted in the supervisor cluster. This method will fetch the list
// of CRD Objects for a given crdName, Version and Group and then verifies
// if the given expectedInstanceName exist in the list.
func verifyCRDInSupervisorWithWait(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdName string, crdVersion string, crdGroup string, isCreated bool) {
	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	var instanceFound bool

	const timeout time.Duration = 30
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		list, err := resourceClient.List(ctx, metav1.ListOptions{})
		if err != nil || list == nil {
			continue
		}
		if list != nil {
			for _, crd := range list.Items {
				if crdName == "cnsnodevmattachments" {
					instance := &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}
					err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					if expectedInstanceName == instance.Name {
						ginkgo.By(fmt.Sprintf("Found CNSNodeVMAttachment crd: %v, expected: %v",
							instance, expectedInstanceName))
						instanceFound = true
						break
					}
				}
				if crdName == "cnsvolumemetadatas" {
					instance := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
					err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					if expectedInstanceName == instance.Name {
						ginkgo.By(fmt.Sprintf("Found CNSVolumeMetadata crd: %v, expected: %v",
							instance, expectedInstanceName))
						instanceFound = true
						break
					}
				}
			}
		}
		if isCreated {
			gomega.Expect(instanceFound).To(gomega.BeTrue())
		} else {
			gomega.Expect(instanceFound).To(gomega.BeFalse())
		}
	}
}

// verifyCRDInSupervisor is a helper method to check if a given crd is
// created/deleted in the supervisor cluster. This method will fetch the list
// of CRD Objects for a given crdName, Version and Group and then verifies
// if the given expectedInstanceName exist in the list.
func verifyCRDInSupervisor(ctx context.Context, f *framework.Framework, expectedInstanceName string,
	crdName string, crdVersion string, crdGroup string, isCreated bool) {
	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	list, err := resourceClient.List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var instanceFound bool
	for _, crd := range list.Items {
		if crdName == "cnsnodevmattachments" {
			instance := &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSNodeVMAttachment crd: %v, expected: %v", instance, expectedInstanceName))
				instanceFound = true
				break
			}

		}
		if crdName == "cnsvolumemetadatas" {
			instance := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSVolumeMetadata crd: %v, expected: %v", instance, expectedInstanceName))
				instanceFound = true
				break
			}
		}
		if crdName == "cnsfileaccessconfigs" {
			instance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSFileAccessConfig crd: %v, expected: %v", instance, expectedInstanceName))
				instanceFound = true
				break
			}
		}
	}
	if isCreated {
		gomega.Expect(instanceFound).To(gomega.BeTrue())
	} else {
		gomega.Expect(instanceFound).To(gomega.BeFalse())
	}
}

// verifyCNSFileAccessConfigCRDInSupervisor is a helper method to check if a
// given crd is created/deleted in the supervisor cluster. This method will
// fetch the list of CRD Objects for a given crdName, Version and Group and then
// verifies if the given expectedInstanceName exist in the list.
func verifyCNSFileAccessConfigCRDInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdName string, crdVersion string, crdGroup string, isCreated bool) {
	// Adding an explicit wait time for the recounciler to refresh the status.
	time.Sleep(30 * time.Second)

	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	list, err := resourceClient.List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var instanceFound bool
	for _, crd := range list.Items {
		if crdName == "cnsfileaccessconfigs" {
			instance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSFileAccessConfig crd: %v, expected: %v", instance, expectedInstanceName))
				instanceFound = true
				break
			}
		}
	}
	if isCreated {
		gomega.Expect(instanceFound).To(gomega.BeTrue())
	} else {
		gomega.Expect(instanceFound).To(gomega.BeFalse())
	}
}

// verifyEntityReferenceForKubEntities takes context,client, pv, pvc and pod
// as inputs and verifies crdCNSVolumeMetadata is attached.
func verifyEntityReferenceForKubEntities(ctx context.Context, f *framework.Framework,
	client clientset.Interface, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, pod *v1.Pod) {
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	volumeID := pv.Spec.CSI.VolumeHandle
	// svcPVCName refers to PVC Name in the supervisor cluster.
	svcPVCName := volumeID
	volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
	gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

	podUID := string(pod.UID)
	fmt.Println("Pod uuid :", podUID)
	fmt.Println("PVC name in SV", svcPVCName)
	pvcUID := string(pvc.GetUID())
	fmt.Println("PVC UUID in GC", pvcUID)
	gcClusterID := strings.Replace(svcPVCName, pvcUID, "", -1)

	fmt.Println("gcClusterId", gcClusterID)
	pvUID := string(pv.UID)
	fmt.Println("PV uuid", pvUID)

	verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle, crdCNSVolumeMetadatas,
		crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
	verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+podUID, crdCNSVolumeMetadatas,
		crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
	verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID, crdCNSVolumeMetadatas,
		crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
}

// verifyEntityReferenceInCRDInSupervisor is a helper method to check
// CnsVolumeMetadata CRDs exists and has expected labels.
func verifyEntityReferenceInCRDInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdName string, crdVersion string, crdGroup string,
	isCreated bool, volumeNames string, checkLabel bool, labels map[string]string, labelPresent bool) {
	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// This sleep is to make sure the entity reference is populated.
	time.Sleep(10 * time.Second)
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	list, err := resourceClient.List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("expected instancename is : %v", expectedInstanceName)

	var instanceFound bool
	for _, crd := range list.Items {
		if crdName == "cnsvolumemetadatas" {
			instance := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Found CNSVolumeMetadata type crd instance: %v", instance.Name)
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSVolumeMetadata crd: %v, expected: %v", instance, expectedInstanceName))
				vol := instance.Spec.VolumeNames
				if vol[0] == volumeNames {
					ginkgo.By(fmt.Sprintf("Entity reference for the volume: %v ,found in the CRD : %v",
						volumeNames, expectedInstanceName))
				}

				if checkLabel {
					ginkgo.By(fmt.Sprintf("Checking the cnsvolumemetadatas for the expected label %v", labels))
					labelsPresent := instance.Spec.Labels
					labelFound := reflect.DeepEqual(labelsPresent, labels)
					if labelFound {
						gomega.Expect(labelFound).To(gomega.BeTrue())
					} else {
						gomega.Expect(labelFound).To(gomega.BeFalse())
					}
				}
				instanceFound = true
				break
			}
		}
	}
	if isCreated {
		gomega.Expect(instanceFound).To(gomega.BeTrue())
	} else {
		gomega.Expect(instanceFound).To(gomega.BeFalse())
	}
}

// trimQuotes takes a quoted string as input and returns the same string unquoted.
func trimQuotes(str string) string {
	str = strings.TrimPrefix(str, "\"")
	str = strings.TrimSuffix(str, "\"")
	return str
}

// readConfigFromSecretString takes input string of the form:
//    [Global]
//    insecure-flag = "true"
//    cluster-id = "domain-c1047"
//    cluster-distribution = "CSI-Vanilla"
//    [VirtualCenter "wdc-rdops-vm09-dhcp-238-224.eng.vmware.com"]
//    user = "workload_storage_management-792c9cce-3cd2-4618-8853-52f521400e05@vsphere.local"
//    password = "qd?\\/\"K=O_<ZQw~s4g(S"
//    datacenters = "datacenter-1033"
//    port = "443"
// Returns a de-serialized structured config data
func readConfigFromSecretString(cfg string) (e2eTestConfig, error) {
	var config e2eTestConfig
	key, value := "", ""
	lines := strings.Split(cfg, "\n")
	for index, line := range lines {
		if index == 0 {
			// Skip [Global].
			continue
		}
		words := strings.Split(line, " = ")
		if len(words) == 1 {
			// Case VirtualCenter.
			words = strings.Split(line, " ")
			if strings.Contains(words[0], "VirtualCenter") {
				value = words[1]
				// Remove trailing '"]' characters from value.
				value = strings.TrimSuffix(value, "]")
				config.Global.VCenterHostname = trimQuotes(value)
				fmt.Printf("Key: VirtualCenter, Value: %s\n", value)
			}
			continue
		}
		key = words[0]
		value = trimQuotes(words[1])
		var strconvErr error
		switch key {
		case "insecure-flag":
			if strings.Contains(value, "true") {
				config.Global.InsecureFlag = true
			} else {
				config.Global.InsecureFlag = false
			}
		case "cluster-id":
			config.Global.ClusterID = value
		case "cluster-distribution":
			config.Global.ClusterDistribution = value
		case "user":
			config.Global.User = value
		case "password":
			config.Global.Password = value
		case "datacenters":
			config.Global.Datacenters = value
		case "port":
			config.Global.VCenterPort = value
		case "cnsregistervolumes-cleanup-intervalinmin":
			config.Global.CnsRegisterVolumesCleanupIntervalInMin, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		default:
			return config, fmt.Errorf("unknown key %s in the input string", key)
		}
	}
	return config, nil
}

// writeConfigToSecretString takes in a structured config data and serializes
// that into a string.
func writeConfigToSecretString(cfg e2eTestConfig) (string, error) {
	result := fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\n"+
		"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"\n",
		cfg.Global.InsecureFlag, cfg.Global.VCenterHostname, cfg.Global.User, cfg.Global.Password,
		cfg.Global.Datacenters, cfg.Global.VCenterPort)
	return result, nil
}

// Function to create CnsRegisterVolume spec, with given FCD ID and PVC name.
func getCNSRegisterVolumeSpec(ctx context.Context, namespace string, fcdID string,
	vmdkPath string, persistentVolumeClaimName string,
	accessMode v1.PersistentVolumeAccessMode) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	var (
		cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume
	)
	log := logger.GetLogger(ctx)
	log.Infof("get CNSRegisterVolume spec")
	cnsRegisterVolume = &cnsregistervolumev1alpha1.CnsRegisterVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "cnsregvol-",
			Namespace:    namespace,
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName: persistentVolumeClaimName,
			AccessMode: v1.PersistentVolumeAccessMode(
				accessMode,
			),
		},
	}

	if vmdkPath != "" {
		cnsRegisterVolume.Spec.DiskURLPath = vmdkPath
	}

	if fcdID != "" {
		cnsRegisterVolume.Spec.VolumeID = fcdID
	}
	return cnsRegisterVolume
}

// Create CNS register volume.
func createCNSRegisterVolume(ctx context.Context, restConfig *rest.Config,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume) error {
	log := logger.GetLogger(ctx)

	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	log.Infof("Create CNSRegisterVolume")
	err = cnsOperatorClient.Create(ctx, cnsRegisterVolume)

	return err
}

// Query CNS Register volume. Returns true if the CNSRegisterVolume is
// available otherwise false.
func queryCNSRegisterVolume(ctx context.Context, restClientConfig *rest.Config,
	cnsRegistervolumeName string, namespace string) bool {
	isPresent := false
	log := logger.GetLogger(ctx)
	log.Infof("cleanUpCnsRegisterVolumeInstances: start")
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Get list of CnsRegisterVolume instances from all namespaces.
	cnsRegisterVolumesList := &cnsregistervolumev1alpha1.CnsRegisterVolumeList{}
	err = cnsOperatorClient.List(ctx, cnsRegisterVolumesList)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	cns := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	err = cnsOperatorClient.Get(ctx, pkgtypes.NamespacedName{Name: cnsRegistervolumeName, Namespace: namespace}, cns)
	if err == nil {
		log.Infof("CNS RegisterVolume %s Found in the namespace  %s:", cnsRegistervolumeName, namespace)
		isPresent = true
	}

	return isPresent

}

// Verify Bi-directional referance of Pv and PVC in case of static volume
// provisioning.
func verifyBidirectionalReferenceOfPVandPVC(ctx context.Context, client clientset.Interface,
	pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume, fcdID string) {
	log := logger.GetLogger(ctx)

	pvcName := pvc.GetName()
	pvcNamespace := pvc.GetNamespace()
	pvcvolume := pvc.Spec.VolumeName
	pvccapacity := pvc.Status.Capacity.StorageEphemeral().Size()

	pvName := pv.GetName()
	pvClaimRefName := pv.Spec.ClaimRef.Name
	pvClaimRefNamespace := pv.Spec.ClaimRef.Namespace
	pvcapacity := pv.Spec.Capacity.StorageEphemeral().Size()
	pvvolumeHandle := pv.Spec.PersistentVolumeSource.CSI.VolumeHandle

	if pvClaimRefName != pvcName && pvClaimRefNamespace != pvcNamespace {
		log.Infof("PVC Name :%s PVC namespace : %s", pvcName, pvcNamespace)
		log.Infof("PV Name :%s PVnamespace : %s", pvcName, pvcNamespace)
		log.Errorf("Mismatch in PV and PVC name and namespace")
	}

	if pvcvolume != pvName {
		log.Errorf("PVC volume :%s PV name : %s expected to be same", pvcvolume, pvName)
	}

	if pvvolumeHandle != fcdID {
		log.Errorf("Mismatch in PV volumeHandle:%s and the actual fcdID: %s", pvvolumeHandle, fcdID)
	}

	if pvccapacity != pvcapacity {
		log.Errorf("Mismatch in pv capacity:%d and pvc capacity: %d", pvcapacity, pvccapacity)
	}
}

// Get CNS register volume.
func getCNSRegistervolume(ctx context.Context, restClientConfig *rest.Config,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	cns := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: cnsRegisterVolume.Name, Namespace: cnsRegisterVolume.Namespace}, cns)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return cns
}

// Update CNS register volume.
func updateCNSRegistervolume(ctx context.Context, restClientConfig *rest.Config,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	err = cnsOperatorClient.Update(ctx, cnsRegisterVolume)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return cnsRegisterVolume

}

// CreatePodByUserID with given claims based on node selector. This method is
// addition to CreatePod method. Here userID can be specified for pod user.
func CreatePodByUserID(client clientset.Interface, namespace string, nodeSelector map[string]string,
	pvclaims []*v1.PersistentVolumeClaim, isPrivileged bool, command string, userID int64) (*v1.Pod, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pod := GetPodSpecByUserID(namespace, nodeSelector, pvclaims, isPrivileged, command, userID)
	pod, err := client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Pod creation failed")
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %v", err)
	}
	// Waiting for pod to be running.
	err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Running: %v", pod.Name, err)
	}
	// Get fresh pod info.
	pod, err = client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %v", err)
	}
	return pod, nil
}

// GetPodSpecByUserID returns a pod definition based on the namespace.
// The pod references the PVC's name.
// This method is addition to MakePod method.
// Here userID can be specified for pod user.
func GetPodSpecByUserID(ns string, nodeSelector map[string]string, pvclaims []*v1.PersistentVolumeClaim,
	isPrivileged bool, command string, userID int64) *v1.Pod {
	if len(command) == 0 {
		command = "trap exit TERM; while true; do sleep 1; done"
	}
	podSpec := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-tester-",
			Namespace:    ns,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "write-pod",
					Image:   busyBoxImageOnGcr,
					Command: []string{"/bin/sh"},
					Args:    []string{"-c", command},
					SecurityContext: &v1.SecurityContext{
						Privileged: &isPrivileged,
						RunAsUser:  &userID,
					},
				},
			},
			RestartPolicy: v1.RestartPolicyOnFailure,
		},
	}
	var volumeMounts = make([]v1.VolumeMount, len(pvclaims))
	var volumes = make([]v1.Volume, len(pvclaims))
	for index, pvclaim := range pvclaims {
		volumename := fmt.Sprintf("volume%v", index+1)
		volumeMounts[index] = v1.VolumeMount{Name: volumename, MountPath: "/mnt/" + volumename}
		volumes[index] = v1.Volume{Name: volumename, VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}
	}
	podSpec.Spec.Containers[0].VolumeMounts = volumeMounts
	podSpec.Spec.Volumes = volumes
	if nodeSelector != nil {
		podSpec.Spec.NodeSelector = nodeSelector
	}
	return podSpec
}

// writeDataOnFileFromPod writes specified data from given Pod at the given.
func writeDataOnFileFromPod(namespace string, podName string, filePath string, data string) {
	_, err := framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace),
		podName, "--", "/bin/sh", "-c", fmt.Sprintf(" echo %s >  %s ", data, filePath))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// readFileFromPod read data from given Pod and the given file.
func readFileFromPod(namespace string, podName string, filePath string) string {
	output, err := framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace),
		podName, "--", "/bin/sh", "-c", fmt.Sprintf("less  %s", filePath))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return output
}

// getPersistentVolumeClaimSpecFromVolume gets vsphere persistent volume spec
// with given selector labels and binds it to given pv.
func getPersistentVolumeClaimSpecFromVolume(namespace string, pvName string,
	labels map[string]string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	var (
		pvc *v1.PersistentVolumeClaim
	)
	sc := ""
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
				},
			},
			VolumeName:       pvName,
			StorageClassName: &sc,
		},
	}
	if labels != nil {
		pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}

	return pvc
}

// getPersistentVolumeSpecFromVolume gets static PV volume spec with given
// Volume ID, Reclaim Policy and labels.
func getPersistentVolumeSpecFromVolume(volumeID string, persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy,
	labels map[string]string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolume {
	var (
		pvConfig fpv.PersistentVolumeConfig
		pv       *v1.PersistentVolume
		claimRef *v1.ObjectReference
	)
	pvConfig = fpv.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       e2evSphereCSIDriverName,
				VolumeHandle: volumeID,
				FSType:       "nfs4",
				ReadOnly:     true,
			},
		},
		Prebind: nil,
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = e2evSphereCSIDriverName

	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
			Annotations:  annotations,
			Labels:       labels,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			ClaimRef:         claimRef,
			StorageClassName: "",
		},
		Status: v1.PersistentVolumeStatus{},
	}
	return pv
}

// DeleteStatefulPodAtIndex deletes pod given index in the desired statefulset.
func DeleteStatefulPodAtIndex(client clientset.Interface, index int, ss *appsv1.StatefulSet) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	name := fmt.Sprintf("%v-%v", ss.Name, index)
	noGrace := int64(0)
	err := client.CoreV1().Pods(ss.Namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &noGrace})
	if err != nil {
		framework.Failf("Failed to delete stateful pod %v for StatefulSet %v/%v: %v", name, ss.Namespace, ss.Name, err)
	}

}

// getClusterComputeResource returns the clusterComputeResource and vSANClient.
func getClusterComputeResource(ctx context.Context, vs *vSphere) ([]*object.ClusterComputeResource, *VsanClient) {
	var err error
	if clusterComputeResource == nil {
		clusterComputeResource, vsanHealthClient, err = getClusterName(ctx, vs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	return clusterComputeResource, vsanHealthClient
}

// findIP returns the IP from the input string.
func findIP(input string) string {
	numBlock := "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])"
	regexPattern := numBlock + "\\." + numBlock + "\\." + numBlock + "\\." + numBlock
	regEx := regexp.MustCompile(regexPattern)
	return regEx.FindString(input)
}

// getHosts returns list of hosts and it takes clusterComputeResource as input.
// This method is used by WCP and GC tests.
func getHosts(ctx context.Context, clusterComputeResource []*object.ClusterComputeResource) []*object.HostSystem {
	var err error
	if hosts == nil {
		computeCluster := os.Getenv(envComputeClusterName)
		if computeCluster == "" {
			if guestCluster {
				computeCluster = "compute-cluster"
			} else if supervisorCluster {
				computeCluster = "wcp-app-platform-sanity-cluster"
			}
			framework.Logf("Default cluster is chosen for test")
		}
		for _, cluster := range clusterComputeResource {
			framework.Logf("clusterComputeResource %v", clusterComputeResource)
			if strings.Contains(cluster.Name(), computeCluster) {
				hosts, err = cluster.Hosts(ctx)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
	gomega.Expect(hosts).NotTo(gomega.BeNil())
	return hosts
}

// waitForAllHostsToBeUp will check and wait till the host is reachable.
func waitForAllHostsToBeUp(ctx context.Context, vs *vSphere) {
	clusterComputeResource, _ = getClusterComputeResource(ctx, vs)
	hosts = getHosts(ctx, clusterComputeResource)
	framework.Logf("host information %v", hosts)
	for index := range hosts {
		ip := findIP(hosts[index].String())
		err := waitForHostToBeUp(ip)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// psodHostWithPv methods finds the esx host where pv is residing and psods it.
// It uses VsanObjIndentities and QueryVsanObjects apis to achieve it and
// returns the host ip.
func psodHostWithPv(ctx context.Context, vs *vSphere, pvName string) string {
	ginkgo.By("VsanObjIndentities")
	framework.Logf("pvName %v", pvName)
	vsanObjuuid := VsanObjIndentities(ctx, &e2eVSphere, pvName)
	framework.Logf("vsanObjuuid %v", vsanObjuuid)
	gomega.Expect(vsanObjuuid).NotTo(gomega.BeNil())

	ginkgo.By("Get host info using queryVsanObj")
	hostInfo := queryVsanObj(ctx, &e2eVSphere, vsanObjuuid)
	framework.Logf("vsan object ID %v", hostInfo)
	gomega.Expect(hostInfo).NotTo(gomega.BeEmpty())
	hostIP := e2eVSphere.getHostUUID(ctx, hostInfo)
	framework.Logf("hostIP %v", hostIP)
	gomega.Expect(hostIP).NotTo(gomega.BeEmpty())

	ginkgo.By("PSOD")
	sshCmd := fmt.Sprintf("vsish -e set /config/Misc/intOpts/BlueScreenTimeout %s", psodTime)
	op, err := connectESX("root", hostIP, sshCmd)
	framework.Logf(op)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Injecting PSOD ")
	psodCmd := "vsish -e set /reliability/crashMe/Panic 1"
	op, err = connectESX("root", hostIP, psodCmd)
	framework.Logf(op)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	return hostIP
}

// VsanObjIndentities returns the vsanObjectsUUID.
func VsanObjIndentities(ctx context.Context, vs *vSphere, pvName string) string {
	var vsanObjUUID string
	computeCluster := os.Getenv(envComputeClusterName)
	if computeCluster == "" {
		if guestCluster {
			computeCluster = "compute-cluster"
		} else if supervisorCluster {
			computeCluster = "wcp-app-platform-sanity-cluster"
		}
		framework.Logf("Default cluster is chosen for test")
	}
	clusterComputeResource, vsanHealthClient = getClusterComputeResource(ctx, vs)

	for _, cluster := range clusterComputeResource {
		if strings.Contains(cluster.Name(), computeCluster) {
			clusterConfig, err := vsanHealthClient.VsanQueryObjectIdentities(ctx, cluster.Reference())
			framework.Logf("clusterconfig %v", clusterConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for index := range clusterConfig.Identities {
				if strings.Contains(clusterConfig.Identities[index].Description, pvName) {
					vsanObjUUID = clusterConfig.Identities[index].Uuid
					framework.Logf("vsanObjUUID is %v", vsanObjUUID)
					break
				}
			}
		}
	}
	gomega.Expect(vsanObjUUID).NotTo(gomega.BeNil())
	return vsanObjUUID
}

// queryVsanObj takes vsanObjuuid as input and resturns vsanObj info such as
// hostUUID.
func queryVsanObj(ctx context.Context, vs *vSphere, vsanObjuuid string) string {
	c := newClient(ctx, vs)
	datacenter := e2eVSphere.Config.Global.Datacenters

	vsanHealthClient, err := newVsanHealthSvcClient(ctx, c.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	finder := find.NewFinder(vsanHealthClient.vim25Client, false)
	dc, err := finder.Datacenter(ctx, datacenter)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)
	result, err := vsanHealthClient.QueryVsanObjects(ctx, []string{vsanObjuuid}, vs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return result
}

// hostLogin methods sets the ESX host password.
func hostLogin(user, instruction string, questions []string, echos []bool) (answers []string, err error) {
	answers = make([]string, len(questions))
	for n := range questions {
		answers[n] = esxPassword
	}
	return answers, nil
}

// connectESX executes ssh commands on the give ESX host and returns the bash
// result.
func connectESX(username string, addr string, cmd string) (string, error) {
	// Authentication.
	config := &ssh.ClientConfig{
		User:            username,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.KeyboardInteractive(hostLogin),
		},
	}
	// Connect.
	client, err := ssh.Dial("tcp", net.JoinHostPort(addr, sshdPort), config)
	if err != nil {
		framework.Logf("connection failed due to %v", err)
		return "", err
	}
	// Create a session. It is one session per command.
	session, err := client.NewSession()
	if err != nil {
		framework.Logf("session creation failed due to %v", err)
		return "", err
	}
	defer session.Close()
	var b bytes.Buffer
	session.Stdout = &b
	err = session.Start(cmd)
	return b.String(), err
}

// getPersistentVolumeSpecWithStorageclass is to create PV volume spec with
// given FCD ID, Reclaim Policy and labels.
func getPersistentVolumeSpecWithStorageclass(volumeHandle string,
	persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy, storageClass string,
	labels map[string]string, sizeOfDisk string) *v1.PersistentVolume {
	var (
		pvConfig fpv.PersistentVolumeConfig
		pv       *v1.PersistentVolume
		claimRef *v1.ObjectReference
	)
	pvConfig = fpv.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       e2evSphereCSIDriverName,
				VolumeHandle: volumeHandle,
				ReadOnly:     false,
				FSType:       "ext4",
			},
		},
		Prebind: nil,
	}

	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse(sizeOfDisk),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			ClaimRef:         claimRef,
			StorageClassName: storageClass,
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = e2evSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// getPVCSpecWithPVandStorageClass is to create PVC spec with given PV, storage
// class and label details.
func getPVCSpecWithPVandStorageClass(pvcName string, namespace string, labels map[string]string,
	pvName string, storageclass string, sizeOfDisk string) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvcName,
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(sizeOfDisk),
				},
			},
			VolumeName:       pvName,
			StorageClassName: &storageclass,
		},
	}
	if labels != nil {
		pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}

	return pvc
}

// waitForEvent waits for and event with specified message substr for a given
// object name.
func waitForEvent(ctx context.Context, client clientset.Interface,
	namespace string, substr string, name string) error {
	waitErr := wait.PollImmediate(poll, pollTimeoutShort, func() (bool, error) {
		eventList, err := client.CoreV1().Events(namespace).List(ctx,
			metav1.ListOptions{FieldSelector: "involvedObject.name=" + name})
		if err != nil {
			return false, err
		}
		for _, item := range eventList.Items {
			if strings.Contains(item.Message, substr) {
				framework.Logf("Found event %v", item)
				return true, nil
			}
		}
		return false, nil
	})
	return waitErr
}

// bringSvcK8sAPIServerDown function moves the static kube-apiserver.yaml out
// of k8's manifests directory. It takes VC IP and SV K8's master IP as input.
func bringSvcK8sAPIServerDown(vc string) error {
	file := "master.txt"
	token := "token.txt"
	// Note: /usr/lib/vmware-wcp/decryptK8Pwd.py is not an officially supported
	// API and may change at any time.
	sshCmd := fmt.Sprintf("/usr/lib/vmware-wcp/decryptK8Pwd.py > %s", file)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err := fssh.SSH(sshCmd, vc, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	sshCmd = fmt.Sprintf("(awk 'FNR == 7 {print $2}' %s) > %s", file, token)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err = fssh.SSH(sshCmd, vc, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	sshCmd = fmt.Sprintf(
		"sshpass -f %s ssh root@$(awk 'FNR == 6 {print $2}' master.txt) -o 'StrictHostKeyChecking no' 'mv %s/%s /root'",
		token, kubeAPIPath, kubeAPIfile)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err = fssh.SSH(sshCmd, vc, framework.TestContext.Provider)
	time.Sleep(kubeAPIRecoveryTime)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	return nil
}

// bringSvcK8sAPIServerUp function moves the static kube-apiserver.yaml to
// k8's manifests directory. It takes VC IP and SV K8's master IP as input.
func bringSvcK8sAPIServerUp(ctx context.Context, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, vc, healthStatus string) error {
	sshCmd := fmt.Sprintf("sshpass -f token.txt ssh root@$(awk 'FNR == 6 {print $2}' master.txt) "+
		"-o 'StrictHostKeyChecking no' 'mv /root/%s %s'", kubeAPIfile, kubeAPIPath)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err := fssh.SSH(sshCmd, vc, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	ginkgo.By(fmt.Sprintf("polling for %v minutes...", healthStatusPollTimeout))
	err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatus)
	if err != nil {
		return err
	}
	return nil
}

// pvcHealthAnnotationWatcher polls the health status of pvc and returns error
// if any.
func pvcHealthAnnotationWatcher(ctx context.Context, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, healthStatus string) error {
	framework.Logf("Waiting for health annotation for pvclaim %v", pvclaim.Name)
	waitErr := wait.Poll(healthStatusPollInterval, healthStatusPollTimeout, func() (bool, error) {
		framework.Logf("wait for next poll %v", healthStatusPollInterval)
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if pvc.Annotations[pvcHealthAnnotation] == healthStatus {
			framework.Logf("health annonation added :%v", pvc.Annotations[pvcHealthAnnotation])
			return true, nil
		}
		return false, nil
	})
	return waitErr
}

// waitForHostToBeUp will check the status of hosts and also wait for
// pollTimeout minutes to make sure host is reachable.
func waitForHostToBeUp(ip string) error {
	framework.Logf("checking host status of %v", ip)
	gomega.Expect(ip).NotTo(gomega.BeNil())
	timeout := 1 * time.Second
	waitErr := wait.Poll(timeout, healthStatusPollTimeout, func() (bool, error) {
		framework.Logf("wait until %v seconds", vsanHealthServiceWaitTime)
		_, err := net.DialTimeout("tcp", ip+":22", timeout)
		if err != nil {
			framework.Logf("host unreachable, error: ", err)
			return false, nil
		}
		framework.Logf("host reachable")
		return true, nil
	})
	return waitErr
}

// waitForNamespaceToGetDeleted waits for a namespace to get deleted or until
// timeout occurs, whichever comes first.
func waitForNamespaceToGetDeleted(ctx context.Context, c clientset.Interface,
	namespaceToDelete string, Poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for namespace %s to get deleted", timeout, namespaceToDelete)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		namespace, err := c.CoreV1().Namespaces().Get(ctx, namespaceToDelete, metav1.GetOptions{})
		if err == nil {
			framework.Logf("Namespace %s found and status=%s (%v)", namespaceToDelete, namespace.Status, time.Since(start))
			continue
		}
		if apierrors.IsNotFound(err) {
			framework.Logf("namespace %s was removed", namespaceToDelete)
			return nil
		}
		framework.Logf("Get namespace %s is failed, ignoring for %v: %v", namespaceToDelete, Poll, err)
	}
	return fmt.Errorf("namespace %s still exists within %v", namespaceToDelete, timeout)
}

// waitForCNSRegisterVolumeToGetCreated waits for a cnsRegisterVolume to get
// created or until timeout occurs, whichever comes first.
func waitForCNSRegisterVolumeToGetCreated(ctx context.Context, restConfig *rest.Config, namespace string,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume, Poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for CnsRegisterVolume %s to get created", timeout, cnsRegisterVolume)

	cnsRegisterVolumeName := cnsRegisterVolume.GetName()
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		cnsRegisterVolume = getCNSRegistervolume(ctx, restConfig, cnsRegisterVolume)
		flag := cnsRegisterVolume.Status.Registered
		if !flag {
			continue
		} else {
			return nil
		}
	}

	return fmt.Errorf("cnsRegisterVolume %s creation is failed within %v", cnsRegisterVolumeName, timeout)
}

// waitForCNSRegisterVolumeToGetDeleted waits for a cnsRegisterVolume to get
// deleted or until timeout occurs, whichever comes first.
func waitForCNSRegisterVolumeToGetDeleted(ctx context.Context, restConfig *rest.Config, namespace string,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume, Poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for cnsRegisterVolume %s to get deleted", timeout, cnsRegisterVolume)

	cnsRegisterVolumeName := cnsRegisterVolume.GetName()
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		flag := queryCNSRegisterVolume(ctx, restConfig, cnsRegisterVolumeName, namespace)
		if flag {
			framework.Logf("CnsRegisterVolume %s is not yet deleted. Deletion flag status  =%s (%v)",
				cnsRegisterVolumeName, flag, time.Since(start))
			continue
		}
		return nil
	}

	return fmt.Errorf("CnsRegisterVolume %s deletion is failed within %v", cnsRegisterVolumeName, timeout)
}

// getK8sMasterIP gets k8s master ip in vanilla setup.
func getK8sMasterIP(ctx context.Context, client clientset.Interface) string {
	var err error
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var k8sMasterIP string
	for _, node := range nodes.Items {
		if strings.Contains(node.Name, "master") || strings.Contains(node.Name, "control") {
			addrs := node.Status.Addresses
			for _, addr := range addrs {
				if addr.Type == v1.NodeExternalIP && (net.ParseIP(addr.Address)).To4() != nil {
					k8sMasterIP = addr.Address
					break
				}
			}
		}
	}
	gomega.Expect(k8sMasterIP).NotTo(gomega.BeNil(), "Unable to find k8s control plane IP")
	return k8sMasterIP
}

// toggleCSIMigrationFeatureGatesOnKubeControllerManager adds/removes
// CSIMigration and CSIMigrationvSphere feature gates to/from
// kube-controller-manager.
func toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx context.Context,
	client clientset.Interface, add bool) error {
	var err error
	sshCmd := ""
	if !vanillaCluster {
		return fmt.Errorf(
			"'toggleCSIMigrationFeatureGatesToKubeControllerManager' is implemented for vanilla cluster alone")
	}
	if add {
		sshCmd =
			"sed -i -e 's/CSIMigration=false,CSIMigrationvSphere=false/CSIMigration=true,CSIMigrationvSphere=true/g' " +
				kcmManifest
	} else {
		sshCmd = "sed -i '/CSIMigration/d' " + kcmManifest
	}
	grepCmd := "grep CSIMigration " + kcmManifest
	k8sMasterIP := getK8sMasterIP(ctx, client)
	framework.Logf("Invoking command '%v' on host %v", grepCmd, k8sMasterIP)
	sshClientConfig := &ssh.ClientConfig{
		User: "root",
		Auth: []ssh.AuthMethod{
			ssh.Password("ca$hc0w"),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	result, err := sshExec(sshClientConfig, k8sMasterIP, grepCmd)
	if err != nil {
		return err
	}
	if err != nil {
		fssh.LogResult(result)
		return fmt.Errorf("command failed/couldn't execute command: %s on host: %v , error: %s",
			grepCmd, k8sMasterIP, err)
	}
	if result.Code != 0 {
		if add {
			// nolint:misspell
			sshCmd = "gawk -i inplace '/--bind-addres/ " +
				"{ print; print \"    - --feature-gates=CSIMigration=true,CSIMigrationvSphere=true\"; next }1' " +
				kcmManifest
		} else {
			return nil
		}
	}
	framework.Logf("Invoking command %v on host %v", sshCmd, k8sMasterIP)
	result, err = sshExec(sshClientConfig, k8sMasterIP, sshCmd)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s", sshCmd, k8sMasterIP, err)
	}
	// Sleeping for two seconds so that the change made to manifest file is
	// recognised.
	time.Sleep(2 * time.Second)
	framework.Logf("Waiting for 'kube-controller-manager' controller pod to come up within %v seconds", pollTimeout)
	label := labels.SelectorFromSet(labels.Set(map[string]string{"component": "kube-controller-manager"}))
	_, err = fpod.WaitForPodsWithLabelRunningReady(client, kubeSystemNamespace, label, 1, pollTimeout)
	framework.Logf("'kube-controller-manager' controller pod is up and ready within %v seconds", pollTimeout)
	return err
}

// sshExec runs a command on the host via ssh.
func sshExec(sshClientConfig *ssh.ClientConfig, host string, cmd string) (fssh.Result, error) {
	result := fssh.Result{Host: host, Cmd: cmd}
	sshClient, err := ssh.Dial("tcp", host+":22", sshClientConfig)
	if err != nil {
		result.Stdout = ""
		result.Stderr = ""
		result.Code = 0
		return result, err
	}
	defer sshClient.Close()
	sshSession, err := sshClient.NewSession()
	if err != nil {
		result.Stdout = ""
		result.Stderr = ""
		result.Code = 0
		return result, err
	}
	defer sshSession.Close()
	// Run the command.
	code := 0
	var bytesStdout, bytesStderr bytes.Buffer
	sshSession.Stdout, sshSession.Stderr = &bytesStdout, &bytesStderr
	if err = sshSession.Run(cmd); err != nil {
		if exiterr, ok := err.(*ssh.ExitError); ok {
			// If we got an ExitError and the exit code is nonzero, we'll
			// consider the SSH itself successful but cmd failed on the host.
			if code = exiterr.ExitStatus(); code != 0 {
				err = nil
			}
		} else {
			err = fmt.Errorf("failed running `%s` on %s@%s: '%v'", cmd, sshClientConfig.User, host, err)
		}
	}
	result.Stdout = bytesStdout.String()
	result.Stderr = bytesStderr.String()
	result.Code = code
	return result, err
}

// createPod with given claims based on node selector.
func createPod(client clientset.Interface, namespace string, nodeSelector map[string]string,
	pvclaims []*v1.PersistentVolumeClaim, isPrivileged bool, command string) (*v1.Pod, error) {
	pod := fpod.MakePod(namespace, nodeSelector, pvclaims, isPrivileged, command)
	pod.Spec.Containers[0].Image = busyBoxImageOnGcr
	pod, err := client.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %v", err)
	}
	// Waiting for pod to be running.
	err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Running: %v", pod.Name, err)
	}
	// Get fresh pod info.
	pod, err = client.CoreV1().Pods(namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %v", err)
	}
	return pod, nil
}

// createDeployment create a deployment with 1 replica for given pvcs and node
// selector.
func createDeployment(ctx context.Context, client clientset.Interface, replicas int32,
	podLabels map[string]string, nodeSelector map[string]string, namespace string,
	pvclaims []*v1.PersistentVolumeClaim, command string, isPrivileged bool) (*appsv1.Deployment, error) {
	if len(command) == 0 {
		command = "trap exit TERM; while true; do sleep 1; done"
	}
	zero := int64(0)
	deploymentName := "deployment-" + string(uuid.NewUUID())
	deploymentSpec := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: podLabels,
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
				},
				Spec: v1.PodSpec{
					TerminationGracePeriodSeconds: &zero,
					Containers: []v1.Container{
						{
							Name:    "write-pod",
							Image:   busyBoxImageOnGcr,
							Command: []string{"/bin/sh"},
							Args:    []string{"-c", command},
							SecurityContext: &v1.SecurityContext{
								Privileged: &isPrivileged,
							},
						},
					},
					RestartPolicy: v1.RestartPolicyAlways,
				},
			},
		},
	}
	var volumeMounts = make([]v1.VolumeMount, len(pvclaims))
	var volumes = make([]v1.Volume, len(pvclaims))
	for index, pvclaim := range pvclaims {
		volumename := fmt.Sprintf("volume%v", index+1)
		volumeMounts[index] = v1.VolumeMount{Name: volumename, MountPath: "/mnt/" + volumename}
		volumes[index] = v1.Volume{Name: volumename, VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}
	}
	deploymentSpec.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
	deploymentSpec.Spec.Template.Spec.Volumes = volumes
	if nodeSelector != nil {
		deploymentSpec.Spec.Template.Spec.NodeSelector = nodeSelector
	}
	deployment, err := client.AppsV1().Deployments(namespace).Create(ctx, deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("deployment %q Create API error: %v", deploymentSpec.Name, err)
	}
	framework.Logf("Waiting deployment %q to complete", deploymentSpec.Name)
	err = fdep.WaitForDeploymentComplete(client, deployment)
	if err != nil {
		return nil, fmt.Errorf("deployment %q failed to complete: %v", deploymentSpec.Name, err)
	}
	return deployment, nil
}

// createPodForFSGroup helps create pod with fsGroup.
func createPodForFSGroup(client clientset.Interface, namespace string,
	nodeSelector map[string]string, pvclaims []*v1.PersistentVolumeClaim,
	isPrivileged bool, command string, fsGroup *int64, runAsUser *int64) (*v1.Pod, error) {
	if len(command) == 0 {
		command = "trap exit TERM; while true; do sleep 1; done"
	}
	if fsGroup == nil {
		fsGroup = func(i int64) *int64 {
			return &i
		}(1000)
	}
	if runAsUser == nil {
		runAsUser = func(i int64) *int64 {
			return &i
		}(2000)
	}

	pod := fpod.MakePod(namespace, nodeSelector, pvclaims, isPrivileged, command)
	pod.Spec.Containers[0].Image = busyBoxImageOnGcr
	pod.Spec.SecurityContext = &v1.PodSecurityContext{
		RunAsUser: runAsUser,
		FSGroup:   fsGroup,
	}
	pod, err := client.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %v", err)
	}
	// Waiting for pod to be running.
	err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Running: %v", pod.Name, err)
	}
	// Get fresh pod info.
	pod, err = client.CoreV1().Pods(namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %v", err)
	}
	return pod, nil
}

// getPersistentVolumeSpecForFileShare returns the PersistentVolume spec.
func getPersistentVolumeSpecForFileShare(fileshareID string,
	persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy, labels map[string]string,
	accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolume {
	pv := getPersistentVolumeSpec(fileshareID, persistentVolumeReclaimPolicy, labels)
	pv.Spec.AccessModes = []v1.PersistentVolumeAccessMode{accessMode}
	return pv
}

// getPersistentVolumeClaimSpecForFileShare return the PersistentVolumeClaim
// spec in the specified namespace.
func getPersistentVolumeClaimSpecForFileShare(namespace string, labels map[string]string,
	pvName string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	pvc := getPersistentVolumeClaimSpec(namespace, labels, pvName)
	pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{accessMode}
	return pvc
}

// deleteFcdWithRetriesForSpecificErr method to retry fcd deletion when a
// specific error is encountered.
func deleteFcdWithRetriesForSpecificErr(ctx context.Context, fcdID string,
	dsRef vim25types.ManagedObjectReference, errsToIgnore []string, errsToContinue []string) error {
	var err error
	waitErr := wait.PollImmediate(poll*15, pollTimeout, func() (bool, error) {
		framework.Logf("Trying to delete FCD: %s", fcdID)
		err = e2eVSphere.deleteFCD(ctx, fcdID, dsRef)
		if err != nil {
			for _, errToIgnore := range errsToIgnore {
				if strings.Contains(err.Error(), errToIgnore) {
					// In FCD, there is a background thread that makes calls to host
					// to sync datastore every minute.
					framework.Logf("Hit error '%s' while trying to delete FCD: %s, will retry after %v seconds ...",
						err.Error(), fcdID, poll*15)
					return false, nil
				}
			}
			for _, errToContinue := range errsToContinue {
				if strings.Contains(err.Error(), errToContinue) {
					framework.Logf("Hit error '%s' while trying to delete FCD: %s, "+
						"will ignore this error(treat as success) and proceed to next steps...",
						err.Error(), fcdID)
					return true, nil
				}
			}
			return false, err
		}
		return true, nil
	})
	return waitErr
}

// getDefaultDatastore returns default datastore.
func getDefaultDatastore(ctx context.Context) *object.Datastore {
	if defaultDatastore == nil {
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		datacenters := []string{}
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		for _, dc := range datacenters {
			defaultDatacenter, err := finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)
			framework.Logf("Looking for default datastore in DC: " + dc)
			datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			if err == nil {
				framework.Logf("Datstore found for DS URL:" + datastoreURL)
				break
			}
		}
		gomega.Expect(defaultDatastore).NotTo(gomega.BeNil())
	}

	return defaultDatastore
}

// setClusterDistribution sets the input cluster-distribution in
// vsphere-config-secret.
func setClusterDistribution(ctx context.Context, client clientset.Interface, clusterDistribution string) {
	framework.Logf("Cluster distribution to set is = %s", clusterDistribution)

	// Get the current cluster-distribution value from secret.
	currentSecret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Secret Name is %s", currentSecret.Name)

	// Read and map the content of csi-vsphere.conf to a variable.
	originalConf := string(currentSecret.Data[vSphereCSIConf])
	cfg, err := readConfigFromSecretString(originalConf)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Current value of cluster-distribution is.
	framework.Logf("Cluster-distribution value before modifying is = %s", cfg.Global.ClusterDistribution)

	// Check if the cluster-distribution value is as required or reset.
	if cfg.Global.ClusterDistribution != clusterDistribution {
		// Modify csi-vsphere.conf file.
		modifiedConf := fmt.Sprintf(
			"[Global]\ninsecure-flag = \"%t\"\ncluster-id = \"%s\"\ncluster-distribution = \"%s\"\n\n"+
				"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"\n",
			cfg.Global.InsecureFlag, cfg.Global.ClusterID, clusterDistribution,
			cfg.Global.VCenterHostname, cfg.Global.User, cfg.Global.Password, cfg.Global.Datacenters,
			cfg.Global.VCenterPort)

		// Set modified csi-vsphere.conf file and update.
		currentSecret.Data[vSphereCSIConf] = []byte(modifiedConf)
		_, err := client.CoreV1().Secrets(csiSystemNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Adding a explicit wait of one min for the Cluster-distribution to
		// reflect latest value.
		time.Sleep(time.Duration(pollTimeoutShort))

		framework.Logf("Cluster distribution value is now set to = %s", clusterDistribution)

	} else {
		framework.Logf("Cluster-distribution value is already as expected, no changes done. Value is %s",
			cfg.Global.ClusterDistribution)
	}
}

// toggleCSIMigrationFeatureGatesOnK8snodes to toggle CSI migration feature
// gates on kublets for worker nodes.
func toggleCSIMigrationFeatureGatesOnK8snodes(ctx context.Context, client clientset.Interface, shouldEnable bool) {
	var err error
	var found bool
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, node := range nodes.Items {
		if strings.Contains(node.Name, "master") || strings.Contains(node.Name, "control") {
			continue
		}
		found = isCSIMigrationFeatureGatesEnabledOnKubelet(ctx, client, node.Name)
		if found == shouldEnable {
			continue
		}
		dh := drain.Helper{
			Ctx:                 ctx,
			Client:              client,
			Force:               true,
			IgnoreAllDaemonSets: true,
			Out:                 ginkgo.GinkgoWriter,
			ErrOut:              ginkgo.GinkgoWriter,
		}
		ginkgo.By("Cordoning of node: " + node.Name)
		err = drain.RunCordonOrUncordon(&dh, &node, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Draining of node: " + node.Name)
		err = drain.RunNodeDrain(&dh, node.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Modifying feature gates in kubelet config yaml of node: " + node.Name)
		nodeIP := getK8sNodeIP(&node)
		toggleCSIMigrationFeatureGatesOnkublet(ctx, client, nodeIP, shouldEnable)
		ginkgo.By("Wait for feature gates update on the k8s CSI node: " + node.Name)
		err = waitForCSIMigrationFeatureGatesToggleOnkublet(ctx, client, node.Name, shouldEnable)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Uncordoning of node: " + node.Name)
		err = drain.RunCordonOrUncordon(&dh, &node, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// isCSIMigrationFeatureGatesEnabledOnKubelet checks whether CSIMigration
// Feature Gates are enabled on CSI Node.
func isCSIMigrationFeatureGatesEnabledOnKubelet(ctx context.Context, client clientset.Interface, nodeName string) bool {
	csinode, err := client.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	found := false
	for annotation, value := range csinode.Annotations {
		framework.Logf("Annotation seen on CSI node - %s:%s", annotation, value)
		if annotation == migratedPluginAnnotation && strings.Contains(value, vcpProvisionerName) {
			found = true
			break
		}
	}
	return found
}

// waitForCSIMigrationFeatureGatesToggleOnkublet wait for CSIMigration Feature
// Gates toggle result on the csinode.
func waitForCSIMigrationFeatureGatesToggleOnkublet(ctx context.Context,
	client clientset.Interface, nodeName string, added bool) error {
	var found bool
	waitErr := wait.PollImmediate(poll*5, pollTimeout, func() (bool, error) {
		csinode, err := client.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		found = false
		for annotation, value := range csinode.Annotations {
			framework.Logf("Annotation seen on CSI node - %s:%s", annotation, value)
			if annotation == migratedPluginAnnotation && strings.Contains(value, vcpProvisionerName) {
				found = true
				break
			}
		}
		if added && found {
			return true, nil
		}
		if !added && !found {
			return true, nil
		}
		return false, nil
	})
	return waitErr
}

// toggleCSIMigrationFeatureGatesOnkublet adds/remove CSI migration feature
// gates to kubelet config yaml in given k8s node.
func toggleCSIMigrationFeatureGatesOnkublet(ctx context.Context,
	client clientset.Interface, nodeIP string, shouldAdd bool) {
	grepCmd := "grep CSIMigration " + kubeletConfigYaml
	framework.Logf("Invoking command '%v' on host %v", grepCmd, nodeIP)
	sshClientConfig := &ssh.ClientConfig{
		User: "root",
		Auth: []ssh.AuthMethod{
			ssh.Password("ca$hc0w"),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	result, err := sshExec(sshClientConfig, nodeIP, grepCmd)
	if err != nil {
		fssh.LogResult(result)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", grepCmd, nodeIP))
	}

	var sshCmd string
	if result.Code != 0 && shouldAdd {
		// Please don't change alignment in below assignment.
		sshCmd = `echo "featureGates:
  {
    "CSIMigration": true,
	"CSIMigrationvSphere": true
  }" >>` + kubeletConfigYaml
	} else if result.Code == 0 && !shouldAdd {
		sshCmd = fmt.Sprintf("head -n -5 %s > tmp.txt && mv tmp.txt %s", kubeletConfigYaml, kubeletConfigYaml)
	} else {
		return
	}
	framework.Logf("Invoking command '%v' on host %v", sshCmd, nodeIP)
	result, err = sshExec(sshClientConfig, nodeIP, sshCmd)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", sshCmd, nodeIP))
	}
	restartKubeletCmd := "systemctl daemon-reload && systemctl restart kubelet"
	framework.Logf("Invoking command '%v' on host %v", restartKubeletCmd, nodeIP)
	result, err = sshExec(sshClientConfig, nodeIP, restartKubeletCmd)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", restartKubeletCmd, nodeIP))
	}
}

// getK8sNodeIP returns the IP for the given k8s node.
func getK8sNodeIP(node *v1.Node) string {
	var address string
	addrs := node.Status.Addresses
	for _, addr := range addrs {
		if addr.Type == v1.NodeExternalIP && (net.ParseIP(addr.Address)).To4() != nil {
			address = addr.Address
			break
		}
	}
	gomega.Expect(address).NotTo(gomega.BeNil(), "Unable to find IP for node: "+node.Name)
	return address
}

// expectedAnnotation polls for the given annotation in pvc and returns error
// if its not present.
func expectedAnnotation(ctx context.Context, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, annotation string) error {
	framework.Logf("Waiting for health annotation for pvclaim %v", pvclaim.Name)
	waitErr := wait.Poll(healthStatusPollInterval, pollTimeout, func() (bool, error) {
		framework.Logf("wait for next poll %v", healthStatusPollInterval)
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for pvcAnnotation := range pvc.Annotations {
			if pvcAnnotation == annotation {
				return true, nil
			}
		}
		return false, nil
	})
	return waitErr
}

// getRestConfigClient returns  rest config client.
func getRestConfigClient() *rest.Config {
	// Get restConfig.
	var err error
	if restConfig == nil {
		if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
			restConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
	return restConfig
}

// GetResizedStatefulSetFromManifest returns a StatefulSet from a manifest
// stored in fileName by adding namespace and a newSize.
func GetResizedStatefulSetFromManifest(ns string) *appsv1.StatefulSet {
	ssManifestFilePath := filepath.Join(manifestPath, "statefulset.yaml")
	framework.Logf("Parsing statefulset from %v", ssManifestFilePath)
	ss, err := manifest.StatefulSetFromManifest(ssManifestFilePath, ns)
	framework.ExpectNoError(err)
	ss, err = statefulSetFromManifest(ssManifestFilePath, ss)
	framework.ExpectNoError(err)
	return ss
}

// statefulSetFromManifest returns a StatefulSet from a manifest stored in
// fileName in the Namespace indicated by ns.
func statefulSetFromManifest(fileName string, ss *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	currentSize := ss.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage()
	newSize := currentSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	ss.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[v1.ResourceStorage] = newSize

	return ss, nil
}
