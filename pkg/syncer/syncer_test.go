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

package syncer

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/vmware/govmomi/simulator"

	cnstypes "github.com/vmware/govmomi/cns/types"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	testclient "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

const (
	testVolumeName       = "test-pv"
	testPVCName          = "test-pvc"
	testPodName          = "test-pod"
	testClusterName      = "test-cluster"
	gbInMb               = 1024
	testVolumeType       = "BLOCK"
	testPVCLabelName     = "test-PVC-label"
	testPVCLabelValue    = "test-PVC-value"
	testPVLabelName      = "test-PV-label"
	testPVLabelValue     = "test-PV-value"
	testPodLabelName     = "test-Pod-label"
	testPodLabelValue    = "test-Pod-value"
	newTestPVLabelValue  = "new-test-PV-value"
	newTestPVCLabelValue = "new-test-PVC-value"
	PVC                  = "PERSISTENT_VOLUME_CLAIM"
	PV                   = "PERSISTENT_VOLUME"
	POD                  = "POD"
	testNamespace        = "default"
)

var (
	config               *cnsconfig.Config
	ctx                  context.Context
	cnsVCenterConfig     *cnsvsphere.VirtualCenterConfig
	err                  error
	virtualCenterManager cnsvsphere.VirtualCenterManager
	virtualCenter        *cnsvsphere.VirtualCenter
	metadataSyncer       *MetadataSyncInformer
	k8sclient            clientset.Interface
	dc                   []*cnsvsphere.Datacenter
	volumeManager        volume.Manager
	dsList               []vimtypes.ManagedObjectReference
)

func TestSyncerWorkflows(t *testing.T) {
	t.Log("TestSyncerWorkflows: start")
	var cleanup func()
	config, cleanup = cnsconfig.FromEnvOrSim()
	defer cleanup()

	// CNS based CSI requires a valid cluster name
	config.Global.ClusterID = testClusterName

	// Create context
	var cancel func()
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// Init VC configuration
	cnsVCenterConfig, err = cnsvsphere.GetVirtualCenterConfig(config)
	if err != nil {
		t.Errorf("Failed to get virtualCenter. err=%v", err)
		t.Fatal(err)
	}

	virtualCenterManager = cnsvsphere.GetVirtualCenterManager()

	virtualCenter, err = virtualCenterManager.RegisterVirtualCenter(cnsVCenterConfig)
	if err != nil {
		t.Fatal(err)
	}

	err = virtualCenter.ConnectCNS(ctx)
	defer virtualCenter.DisconnectCNS(ctx)
	if err != nil {
		t.Fatal(err)
	}

	volumeManager = volume.GetManager(virtualCenter)

	// Initialize metadata syncer object
	metadataSyncer = &MetadataSyncInformer{
		cfg:                  config,
		vcconfig:             cnsVCenterConfig,
		virtualcentermanager: virtualCenterManager,
		vcenter:              virtualCenter,
	}

	// Create the kubernetes client
	// Here we should use a faked client to avoid test inteference with running
	// metadata syncer pod in real Kubernetes cluster
	k8sclient = testclient.NewSimpleClientset()

	metadataSyncer.k8sInformerManager = k8s.NewInformer(k8sclient)
	metadataSyncer.pvLister = metadataSyncer.k8sInformerManager.GetPVLister()
	metadataSyncer.pvcLister = metadataSyncer.k8sInformerManager.GetPVCLister()
	metadataSyncer.k8sInformerManager.Listen()

	// Initialize maps needed for full sync
	cnsCreationMap = make(map[string]bool)
	cnsDeletionMap = make(map[string]bool)

	runMetadataSyncerTest(t)
	runFullSyncTest(t)
	t.Log("TestSyncerWorkflows: end")
}

/*
	This test verifies the following workflows for metadata syncer:
		1. pv update/delete for dynamically created pv
		2. pv update/delete for statically created pv
		3. pvc update/delete
		4. pod update/delete

	Test Steps:
		1. Dynamically create a test volume on vc
		2. Verify pv update workflow adds pv metadata to vc
		3. Delete test volume with delete disk=false
		4. Statically create test volume on k8s with volumeHandle == recently deleted volume
		5. Verify pv update workflow creates pv metadata on vc
		6. Create pvc on k8s to bound to recently created pv
		7. Verify pvc update workflow adds pvc metadata to vc
		8. Create pod on k8s to bound to recently created pvc
		9. Verify pod update workflow adds pod name to vc
		10. Verify pod delete workflow deletes pod name from vc
		11. Verify pvc delete workflow deletes pvc metadata from vc
		12. Verify pv delete workflow deletes pv metadata from vc
*/

func runMetadataSyncerTest(t *testing.T) {
	t.Log("Begin MetadataSyncer Test")

	// Dynamically create a test volume
	createSpec, err := getCnsCreateSpec(t)
	if err != nil {
		t.Fatal(err)
	}
	volumeID, err := volumeManager.CreateVolume(&createSpec)
	if err != nil {
		t.Fatal(err)
	}

	// Set volume id to be queried
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volumeID.Id,
			},
		},
	}

	// Verify if volume is created
	queryResult, err := metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryResult.Volumes) == 0 || queryResult.Volumes[0].VolumeId.Id != volumeID.Id {
		t.Fatalf("Failed to find the newly created volume with ID: %s", volumeID)
	}

	// Set new PV labels
	newLabel := make(map[string]string)
	newLabel[testPVLabelName] = testPVLabelValue

	// Test pvUpdate workflow for dynamic provisioning of Volume
	oldPv := getPersistentVolumeSpec(volumeID.Id, v1.PersistentVolumeReclaimRetain, nil, v1.VolumeAvailable, "")
	newPv := getPersistentVolumeSpec(volumeID.Id, v1.PersistentVolumeReclaimRetain, newLabel, v1.VolumeAvailable, "")

	pvUpdated(oldPv, newPv, metadataSyncer)

	// Verify pv label of volume matches that of updated metadata
	if queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeID.Id, PV, newPv.Name, testPVLabelValue); err != nil {
		t.Fatal(err)
	}

	// Delete volume with DeleteDisk=false
	err = volumeManager.DeleteVolume(volumeID.Id, false)
	if err != nil {
		t.Fatal(err)
	}

	// Statically create PV on K8S, with VolumeHandle of recently deleted Volume
	pv := getPersistentVolumeSpec(volumeID.Id, v1.PersistentVolumeReclaimRetain, nil, v1.VolumeAvailable, "")
	if pv, err = k8sclient.CoreV1().PersistentVolumes().Create(pv); err != nil {
		t.Fatal(err)
	}

	// Test pvUpdate workflow on VC for static provisioning of Volume
	// pvUpdate should create the volume on vc for static provisioning
	oldPv = getPersistentVolumeSpec(volumeID.Id, v1.PersistentVolumeReclaimRetain, nil, v1.VolumePending, "")
	newPv = getPersistentVolumeSpec(volumeID.Id, v1.PersistentVolumeReclaimRetain, newLabel, v1.VolumeAvailable, "")

	pvUpdated(oldPv, newPv, metadataSyncer)

	// Verify pv label of volume matches that of updated metadata
	if queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeID.Id, PV, newPv.Name, testPVLabelValue); err != nil {
		t.Fatal(err)
	}

	// Create PVC on K8S to bound to recently created PV
	namespace := testNamespace
	newPVCLabel := make(map[string]string)
	newPVCLabel[testPVCLabelName] = testPVCLabelValue
	pvc := getPersistentVolumeClaimSpec(namespace, nil, pv.Name)
	if pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(namespace).Create(pvc); err != nil {
		t.Fatal(err)
	}

	// Test pvcUpdate workflow on VC
	oldPvc := getPersistentVolumeClaimSpec(testNamespace, nil, pv.Name)
	newPvc := getPersistentVolumeClaimSpec(testNamespace, newPVCLabel, pv.Name)
	pvcUpdated(oldPvc, newPvc, metadataSyncer)

	// Verify pvc label of volume matches that of updated metadata
	if queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeID.Id, PVC, newPvc.Name, testPVCLabelValue); err != nil {
		t.Fatal(err)
	}

	// Create Pod on K8S with claim = recently created pvc
	pod := getPodSpec(pvc.Name, v1.PodRunning)
	if pod, err = k8sclient.CoreV1().Pods(namespace).Create(pod); err != nil {
		t.Fatal(err)
	}

	// Test podUpdate workflow on VC
	oldPod := getPodSpec(pvc.Name, v1.PodPending)
	newPod := getPodSpec(pvc.Name, v1.PodRunning)
	podUpdated(oldPod, newPod, metadataSyncer)

	// Verify pod name associated with volume matches updated pod name
	if queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeID.Id, POD, newPod.Name, ""); err != nil {
		t.Fatal(err)
	}

	// Test podDeleted workflow on VC
	podDeleted(newPod, metadataSyncer)
	if queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyDeleteOperation(queryResult, volumeID.Id, POD); err != nil {
		t.Fatal(err)
	}

	// Test pvcDelete workflow
	pvcDeleted(newPvc, metadataSyncer)
	if queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyDeleteOperation(queryResult, volumeID.Id, PVC); err != nil {
		t.Fatal(err)
	}

	// Test pvDelete workflow
	pvDeleted(newPv, metadataSyncer)
	if queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyDeleteOperation(queryResult, volumeID.Id, PV); err != nil {
		t.Fatal(err)
	}

	// Cleanup on K8S
	if err = k8sclient.CoreV1().Pods(namespace).Delete(pod.Name, nil); err != nil {
		t.Fatal(err)
	}
	if err = k8sclient.CoreV1().PersistentVolumeClaims(namespace).Delete(pvc.Name, nil); err != nil {
		t.Fatal(err)
	}
	if err = k8sclient.CoreV1().PersistentVolumes().Delete(pv.Name, nil); err != nil {
		t.Fatal(err)
	}

	t.Log("End MetadataSyncer Test")

}

/*
	This test verifies the fullsync workflow:
		1. PV does not exist in K8S, but exist in CNS cache - fullsync should delete this volume from CNS cache
		2. PV and PVC exist in K8S, but does not exist in CNS cache - fullsync should create this volume in CNS cache
		3. PV and PVC exist in K8S and CNS cache, but the labels dont match - fullsync should update the label in CNS cache
		4. POD is created in K8S with PVC, fullsync should update the POD in CNS cache
*/

func runFullSyncTest(t *testing.T) {
	t.Log("Begin FullSync test")

	// Create spec for new volume
	createSpec, err := getCnsCreateSpec(t)
	if err != nil {
		t.Fatal(err)
	}
	volumeID, err := volumeManager.CreateVolume(&createSpec)
	if err != nil {
		t.Errorf("Failed to create volume. Error: %+v", err)
		t.Fatal(err)
		return
	}

	// Set volume id to be queried
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volumeID.Id,
			},
		},
	}

	// Verify if volume is created
	queryResult, err := metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volumeID.Id {
		t.Fatalf("Failed to find the newly created volume with ID: %s", volumeID)
	}

	// PV does not exist in K8S, but volume exist in CNS cache
	// FullSync should delete this volume from CNS cache after two cycles
	triggerFullSync(k8sclient, metadataSyncer)
	triggerFullSync(k8sclient, metadataSyncer)

	// Verify if volume has been deleted from cache
	queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Full sync failed to remove volume")
	}

	// PV and PVC exist in K8S, but does not exist in CNS cache
	// FullSync should create this volume in CNS cache

	// Create PV in K8S with VolumeHandle of recently deleted Volume
	pvLabel := make(map[string]string)
	pvLabel[testPVLabelName] = testPVLabelValue
	pv := getPersistentVolumeSpec(volumeID.Id, v1.PersistentVolumeReclaimRetain, pvLabel, v1.VolumeAvailable, "")
	if pv, err = k8sclient.CoreV1().PersistentVolumes().Create(pv); err != nil {
		t.Fatal(err)
	}

	// Create PVC in K8S to bound to recently created PV
	pvcLabel := make(map[string]string)
	pvcLabel[testPVCLabelName] = testPVCLabelValue
	pvc := getPersistentVolumeClaimSpec(testNamespace, pvcLabel, pv.Name)
	if pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(testNamespace).Create(pvc); err != nil {
		t.Fatal(err)
	}

	// allocate pvc claimRef for PV spec
	pv = getPersistentVolumeSpec(volumeID.Id, v1.PersistentVolumeReclaimRetain, pvLabel, v1.VolumeBound, pvc.Name)
	if pv, err = k8sclient.CoreV1().PersistentVolumes().Update(pv); err != nil {
		t.Fatal(err)
	}

	triggerFullSync(k8sclient, metadataSyncer)
	triggerFullSync(k8sclient, metadataSyncer)

	// PV, PVC is updated in K8S with new label value, CNS cache still hold the old label value
	// FullSync should update the metadata in CNS cache with new label value

	//  Update pv with new label
	newPVLabel := make(map[string]string)
	newPVLabel[testPVLabelName] = newTestPVLabelValue
	pv.Labels = newPVLabel
	if pv, err = k8sclient.CoreV1().PersistentVolumes().Update(pv); err != nil {
		t.Fatal(err)
	}

	triggerFullSync(k8sclient, metadataSyncer)

	// Verify pv label value has been updated in CNS cache
	if queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeID.Id, PV, pv.Name, newTestPVLabelValue); err != nil {
		t.Fatal(err)
	}

	//update pvc with new label
	newPVCLabel := make(map[string]string)
	newPVCLabel[testPVCLabelName] = newTestPVCLabelValue
	pvc.Labels = newPVCLabel
	if pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(testNamespace).Update(pvc); err != nil {
		t.Fatal(err)
	}

	triggerFullSync(k8sclient, metadataSyncer)

	// Verify pvc label value has been updated in CNS cache
	if queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeID.Id, PVC, pvc.Name, newTestPVCLabelValue); err != nil {
		t.Fatal(err)
	}

	//  POD is created with PVC, CNS cache do not have the POD metadata
	// fullsync should update the POD in CNS cache

	// Create Pod on K8S with claim = recently created pvc
	pod := getPodSpec(pvc.Name, v1.PodRunning)
	if pod, err = k8sclient.CoreV1().Pods(testNamespace).Create(pod); err != nil {
		t.Fatal(err)
	}

	triggerFullSync(k8sclient, metadataSyncer)

	// Verify POD metadata of volume matches that of updated metadata
	if queryResult, err = metadataSyncer.vcenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeID.Id, POD, pod.Name, ""); err != nil {
		t.Fatal(err)
	}

	// Cleanup in K8S
	if err = k8sclient.CoreV1().PersistentVolumes().Delete(pv.Name, nil); err != nil {
		t.Fatal(err)
	}
	if err = k8sclient.CoreV1().PersistentVolumeClaims(testNamespace).Delete(pvc.Name, nil); err != nil {
		t.Fatal(err)
	}
	if err = k8sclient.CoreV1().Pods(testNamespace).Delete(pod.Name, nil); err != nil {
		t.Fatal(err)
	}

	// Cleanup in CNS to delete the volume
	if err = volumeManager.DeleteVolume(volumeID.Id, true); err != nil {
		t.Logf("Failed to delete volume %v from CNS", volumeID.Id)
	}
	t.Log("End FullSync test")
}

// verifyDeleteOperation verifies if a delete operation was successful for the given resource type
// resourceType can be one of PV, PVC or POD
func verifyDeleteOperation(queryResult *cnstypes.CnsQueryResult, volumeID string, resourceType string) error {
	if len(queryResult.Volumes) == 0 && resourceType == PV {
		return nil
	}
	entityMetadata := queryResult.Volumes[0].Metadata.EntityMetadata
	for _, metadata := range entityMetadata {
		if len(metadata.GetCnsEntityMetadata().Labels) == 0 {
			continue
		}
		queryLabel := metadata.GetCnsEntityMetadata().Labels[0].Key
		queryValue := metadata.GetCnsEntityMetadata().Labels[0].Value
		if resourceType == PV && queryLabel == testPVLabelName && queryValue == testPVLabelValue {
			return fmt.Errorf("delete operation failed for volume Id %s and resource type %s ", volumeID, resourceType)
		}
		if resourceType == PVC && queryLabel == testPVCLabelName && queryValue == testPVCLabelValue {
			return fmt.Errorf("delete operation failed for volume Id %s and resource type %s ", volumeID, resourceType)
		}
		if resourceType == POD && queryLabel == testPodLabelName && queryValue == testPodLabelValue {
			return fmt.Errorf("delete operation failed for volume Id %s and resource type %s ", volumeID, resourceType)
		}
	}
	return nil
}

// verifyUpdateOperation verifies if an update operation was successful for the given resource type
// resourceType can be one of PV, PVC or POD
func verifyUpdateOperation(queryResult *cnstypes.CnsQueryResult, volumeID string, resourceType string, resourceName string, resourceNewLabel string) error {
	if len(queryResult.Volumes) == 0 || len(queryResult.Volumes[0].Metadata.EntityMetadata) == 0 {
		return fmt.Errorf("update operation failed for volume Id %s for resource type %s with queryResult: %v", volumeID, resourceType, spew.Sdump(queryResult))
	}
	entityMetadata := queryResult.Volumes[0].Metadata.EntityMetadata
	for _, baseMetadata := range entityMetadata {
		metadata := interface{}(baseMetadata).(*cnstypes.CnsKubernetesEntityMetadata)
		if resourceType == POD && metadata.EntityType == POD && metadata.EntityName == resourceName && len(metadata.Labels) == 0 {
			return nil
		}
		if len(metadata.Labels) == 0 {
			return fmt.Errorf("update operation failed for volume Id %s and resource type %s queryResult: %v", volumeID, metadata.EntityType, spew.Sdump(queryResult))
		}
		queryLabel := metadata.Labels[0].Key
		queryValue := metadata.Labels[0].Value
		if resourceType == PVC && metadata.EntityType == PVC && metadata.EntityName == resourceName && queryLabel == testPVCLabelName && queryValue == resourceNewLabel {
			return nil
		}
		if resourceType == PV && metadata.EntityType == PV && metadata.EntityName == resourceName && queryLabel == testPVLabelName && queryValue == resourceNewLabel {
			return nil
		}
	}
	return fmt.Errorf("update operation failed for volume Id: %s for resource type %s with queryResult: %v", volumeID, resourceType, spew.Sdump(queryResult))
}

// getCnsCreateSpec returns the spec for a create call to cns
func getCnsCreateSpec(t *testing.T) (cnstypes.CnsVolumeCreateSpec, error) {
	var sharedDatastore string
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		sharedDatastore = v
	} else {
		sharedDatastore = simulator.Map.Any("Datastore").(*simulator.Datastore).Info.GetDatastoreInfo().Url
	}
	dc, err = virtualCenter.GetDatacenters(ctx)
	if err != nil || len(dc) == 0 {
		t.Errorf("Failed to get datacenter for the path: %s. Error: %v", cnsVCenterConfig.DatacenterPaths[0], err)
		return cnstypes.CnsVolumeCreateSpec{}, err
	}

	datastoreObj, err := dc[0].GetDatastoreByURL(ctx, sharedDatastore)
	if err != nil {
		t.Errorf("Failed to get datastore with URL: %s. Error: %v", sharedDatastore, err)
		return cnstypes.CnsVolumeCreateSpec{}, err
	}
	dsList = append(dsList, datastoreObj.Reference())
	return cnstypes.CnsVolumeCreateSpec{
		DynamicData: vimtypes.DynamicData{},
		Name:        testVolumeName,
		VolumeType:  testVolumeType,
		Datastores:  dsList,
		Metadata: cnstypes.CnsVolumeMetadata{
			DynamicData: vimtypes.DynamicData{},
			ContainerCluster: cnstypes.CnsContainerCluster{
				ClusterType: string(cnstypes.CnsClusterTypeKubernetes),
				ClusterId:   config.Global.ClusterID,
				VSphereUser: config.VirtualCenter[cnsVCenterConfig.Host].User,
			},
		},
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{
				CapacityInMb: gbInMb,
			},
		},
	}, nil
}

// getPersistentVolumeSpec creates PV volume spec with given Volume Handle, Reclaim Policy, Labels and Phase
func getPersistentVolumeSpec(volumeHandle string, persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy, labels map[string]string, phase v1.PersistentVolumePhase, claimRefName string) *v1.PersistentVolume {
	var pv *v1.PersistentVolume
	var claimRef *v1.ObjectReference
	if claimRefName != "" {
		claimRef = &v1.ObjectReference{
			Name: claimRefName,
		}
	}
	pv = &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: testVolumeName,
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       service.Name,
					VolumeHandle: volumeHandle,
				},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			ClaimRef:                      claimRef,
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	if phase != "" {
		pv.Status.Phase = phase
	}
	return pv
}

// getPersistentVolumeClaimSpec gets vsphere persistent volume spec with given selector labels.
func getPersistentVolumeClaimSpec(namespace string, labels map[string]string, pvName string) *v1.PersistentVolumeClaim {
	var (
		pvc *v1.PersistentVolumeClaim
	)
	sc := ""
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPVCName,
			Namespace: namespace,
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
		Status: v1.PersistentVolumeClaimStatus{
			Phase: "Bound",
		},
	}
	if labels != nil {
		pvc.Labels = labels
		pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}

	return pvc
}

// getPodSpec returns a pod spec with given phase
func getPodSpec(pvcName string, phase v1.PodPhase) *v1.Pod {
	var pod *v1.Pod
	podVolume := []v1.Volume{
		{
			Name: testVolumeName,
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		},
	}

	podContainer := []v1.Container{
		{
			Name:  "my-frontend",
			Image: "busybox",
			Command: []string{
				"sleep",
				"1000000",
			},
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      testVolumeName,
					ReadOnly:  false,
					MountPath: "/data",
				},
			},
		},
	}
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPodName,
			Namespace: testNamespace,
		},
		Spec: v1.PodSpec{
			Volumes:    podVolume,
			Containers: podContainer,
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
	}
	return pod
}
