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
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	cnssim "github.com/vmware/govmomi/cns/simulator"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/simulator"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	testclient "k8s.io/client-go/kubernetes/fake"

	cnsvolumes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

const (
	testVolumeName       = "test-pv"
	testPVCName          = "test-pvc"
	testSCName           = "test-sc"
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
	PVC                  = "PVC"
	PV                   = "PV"
	POD                  = "POD"
	testNamespace        = "default"
)

var (
	csiConfig            *cnsconfig.Config
	ctx                  context.Context
	cnsVCenterConfig     *cnsvsphere.VirtualCenterConfig
	err                  error
	virtualCenterManager cnsvsphere.VirtualCenterManager
	virtualCenter        *cnsvsphere.VirtualCenter
	metadataSyncer       *metadataSyncInformer
	k8sclient            clientset.Interface
	dc                   []*cnsvsphere.Datacenter
	volumeManager        cnsvolumes.Manager
	dsList               []vimtypes.ManagedObjectReference
	cancel               context.CancelFunc
)

// configFromSim starts a vcsim instance and returns config for use against the
// vcsim instance. The vcsim instance is configured with an empty tls.Config.
func configFromSim() (*cnsconfig.Config, func()) {
	return configFromSimWithTLS(new(tls.Config), true)
}

// configFromSimWithTLS starts a vcsim instance and returns config for use
// against the vcsim instance. The vcsim instance is configured with a
// tls.Config. The returned client config can be configured to allow/decline
// insecure connections.
func configFromSimWithTLS(tlsConfig *tls.Config, insecureAllowed bool) (*cnsconfig.Config, func()) {
	cfg := &cnsconfig.Config{}
	model := simulator.VPX()
	defer model.Remove()

	err := model.Create()
	if err != nil {
		log.Fatal(err)
	}

	model.Service.TLS = tlsConfig
	s := model.Service.NewServer()

	// CNS Service simulator.
	model.Service.RegisterSDK(cnssim.New())

	cfg.Global.InsecureFlag = insecureAllowed

	cfg.Global.VCenterIP = s.URL.Hostname()
	cfg.Global.VCenterPort = s.URL.Port()
	cfg.Global.User = s.URL.User.Username()
	cfg.Global.Password, _ = s.URL.User.Password()
	cfg.Global.Datacenters = "DC0"

	// Write values to test_vsphere.conf.
	os.Setenv("VSPHERE_CSI_CONFIG", "test_vsphere.conf")
	conf := []byte(fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\n"+
		"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"",
		cfg.Global.InsecureFlag, cfg.Global.VCenterIP, cfg.Global.User, cfg.Global.Password,
		cfg.Global.Datacenters, cfg.Global.VCenterPort))
	err = ioutil.WriteFile("test_vsphere.conf", conf, 0644)
	if err != nil {
		log.Fatal(err)
	}

	cfg.VirtualCenter = make(map[string]*cnsconfig.VirtualCenterConfig)
	cfg.VirtualCenter[s.URL.Hostname()] = &cnsconfig.VirtualCenterConfig{
		User:         cfg.Global.User,
		Password:     cfg.Global.Password,
		VCenterPort:  cfg.Global.VCenterPort,
		InsecureFlag: cfg.Global.InsecureFlag,
		Datacenters:  cfg.Global.Datacenters,
	}

	return cfg, func() {
		s.Close()
		model.Remove()
	}

}

func configFromEnvOrSim() (*cnsconfig.Config, func()) {
	cfg := &cnsconfig.Config{}
	if err := cnsconfig.FromEnv(ctx, cfg); err != nil {
		return configFromSim()
	}
	return cfg, func() {}
}

func TestSyncerWorkflows(t *testing.T) {
	// Create context.
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	t.Log("TestSyncerWorkflows: start")
	var cleanup func()
	csiConfig, cleanup = configFromEnvOrSim()
	defer cleanup()

	// CNS based CSI requires a valid cluster name.
	csiConfig.Global.ClusterID = testClusterName

	// Init VC configuration.
	cnsVCenterConfig, err = cnsvsphere.GetVirtualCenterConfig(ctx, csiConfig)
	if err != nil {
		t.Errorf("failed to get virtualCenter. err=%v", err)
		t.Fatal(err)
	}

	virtualCenterManager = cnsvsphere.GetVirtualCenterManager(ctx)

	virtualCenter, err = virtualCenterManager.RegisterVirtualCenter(ctx, cnsVCenterConfig)
	if err != nil {
		t.Fatal(err)
	}

	err = virtualCenter.ConnectCns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if virtualCenter != nil {
			err = virtualCenter.Disconnect(ctx)
			if err != nil {
				t.Error(err)
			}
		}
	}()

	volumeManager = cnsvolumes.GetManager(ctx, virtualCenter, nil, false)

	// Initialize metadata syncer object.
	metadataSyncer = &metadataSyncInformer{}
	configInfo := &cnsconfig.ConfigurationInfo{}
	configInfo.Cfg = csiConfig
	metadataSyncer.configInfo = configInfo
	metadataSyncer.volumeManager = cnsvolumes.GetManager(ctx, virtualCenter, nil, false)
	metadataSyncer.host = virtualCenter.Config.Host

	// Create the kubernetes client from config or env.
	// Here we should use a faked client to avoid test inteference with running
	// metadata syncer pod in real Kubernetes cluster.
	k8sclient = testclient.NewSimpleClientset()
	metadataSyncer.k8sInformerManager = k8s.NewInformer(k8sclient)
	metadataSyncer.k8sInformerManager.GetPodLister()
	metadataSyncer.pvLister = metadataSyncer.k8sInformerManager.GetPVLister()
	metadataSyncer.pvcLister = metadataSyncer.k8sInformerManager.GetPVCLister()
	metadataSyncer.podLister = metadataSyncer.k8sInformerManager.GetPodLister()
	metadataSyncer.k8sInformerManager.Listen()

	_, err := unittestcommon.GetFakeVolumeMigrationService(ctx,
		&metadataSyncer.volumeManager, metadataSyncer.configInfo.Cfg)
	if err != nil {
		t.Fatalf("failed to get migration service. Err: %v", err)
	}
	metadataSyncer.coCommonInterface, err = unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("Failed to create co agnostic interface. err=%v", err)
	}

	var sharedDatastore string
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		sharedDatastore = v
	} else {
		sharedDatastore = simulator.Map.Any("Datastore").(*simulator.Datastore).Info.GetDatastoreInfo().Url
	}
	dc, err = virtualCenter.GetDatacenters(ctx)
	if err != nil || len(dc) == 0 {
		t.Errorf("failed to get datacenter for the path: %s. Error: %v", cnsVCenterConfig.DatacenterPaths[0], err)
		t.Fatal(err)
		return
	}

	datastoreObj, err := dc[0].GetDatastoreByURL(ctx, sharedDatastore)
	if err != nil {
		t.Errorf("failed to get datastore with URL: %s. Error: %v", sharedDatastore, err)
		t.Fatal(err)
		return
	}
	dsList = append(dsList, datastoreObj.Reference())

	runTestMetadataSyncInformer(t)
	runTestFullSyncWorkflows(t)
	t.Log("TestSyncerWorkflows: end")
}

// This test verifies the following workflows:
//    1. pv update/delete for dynamically created pv
//    2. pv update/delete for statically created pv
//    3. pvc update/delete
//    4. pod update/delete
//
// Test Steps:
//    1. Setup configuration.
//       a. VC config is either simulated or derived from env.
//       b. k8sclient is simulated or derived from env - based on env varible
//          USE_K8S_CLIENT.
//    2. Dynamically create a test volume on vc.
//    3. Verify pv update workflow.
//    4. Delete test volume with delete disk=false.
//    5. Statically create test volume on k8s with volumeHandle = recently
//       deleted volume.
//    5. Verify pv update workflow.
//    6. Create pvc on k8s to bound to recently created pv.
//    7. Verify pvc update workflow.
//    8. Create pod on k8s to bound to recently created pvc.
//    9. Verify pod update workflow.
//    10. Verify pod delete workflow.
//    11. Verify pvc delete workflow.
//    12. Verify pv delete workflow.

func runTestMetadataSyncInformer(t *testing.T) {

	t.Log("TestMetadataSyncInformer start")
	// Create a test volume.
	createSpec := cnstypes.CnsVolumeCreateSpec{
		DynamicData: vimtypes.DynamicData{},
		Name:        testPVCName,
		VolumeType:  testVolumeType,
		Datastores:  dsList,
		Metadata: cnstypes.CnsVolumeMetadata{
			DynamicData: vimtypes.DynamicData{},
			ContainerCluster: cnstypes.CnsContainerCluster{
				ClusterType: string(cnstypes.CnsClusterTypeKubernetes),
				ClusterId:   metadataSyncer.configInfo.Cfg.Global.ClusterID,
				VSphereUser: metadataSyncer.configInfo.Cfg.VirtualCenter[metadataSyncer.host].User,
			},
		},
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{CapacityInMb: gbInMb},
		},
	}

	volumeInfo, err := volumeManager.CreateVolume(ctx, &createSpec)
	if err != nil {
		t.Fatal(err)
	}

	// Set volume id to be queried.
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volumeInfo.VolumeID.Id,
			},
		},
	}

	// Verify if volume is created.
	queryResult, err := virtualCenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) == 0 || queryResult.Volumes[0].VolumeId.Id != volumeInfo.VolumeID.Id {
		t.Fatalf("failed to find the newly created volume with ID: %s", volumeInfo.VolumeID.Id)
	}

	// Set old and new PV labels.
	var oldLabel map[string]string
	var newLabel map[string]string

	oldLabel = make(map[string]string)
	newLabel = make(map[string]string)
	newLabel[testPVLabelName] = testPVLabelValue

	// Test pvUpdate workflow for dynamic provisioning of Volume.
	pvName := testVolumeName + "-" + uuid.New().String()
	oldPv := getPersistentVolumeSpec(pvName, volumeInfo.VolumeID.Id,
		v1.PersistentVolumeReclaimRetain, oldLabel, v1.VolumeAvailable, "")
	newPv := getPersistentVolumeSpec(pvName, volumeInfo.VolumeID.Id,
		v1.PersistentVolumeReclaimRetain, newLabel, v1.VolumeAvailable, "")

	pvUpdated(oldPv, newPv, metadataSyncer)

	// Verify pv label of volume matches that of updated metadata.
	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeInfo.VolumeID.Id, PV, newPv.Name, testPVLabelValue); err != nil {
		t.Fatal(err)
	}

	// Delete volume with DeleteDisk=false.
	if err = volumeManager.DeleteVolume(ctx, volumeInfo.VolumeID.Id, false); err != nil {
		t.Fatal(err)
	}

	// Create PV on K8S with VolumeHandle of recently deleted Volume.
	pvcName := testPVCName + "-" + uuid.New().String()
	pv := getPersistentVolumeSpec(pvcName, volumeInfo.VolumeID.Id,
		v1.PersistentVolumeReclaimRetain, nil, v1.VolumeAvailable, "")
	if pv, err = k8sclient.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Test pvUpdate workflow on VC for static provisioning of Volume.
	// pvUpdate should create the volume on vc for static provisioning.
	pvName = testVolumeName + "-" + uuid.New().String()
	oldPv = getPersistentVolumeSpec(pvName, volumeInfo.VolumeID.Id,
		v1.PersistentVolumeReclaimRetain, oldLabel, v1.VolumePending, "")
	newPv = getPersistentVolumeSpec(pvName, volumeInfo.VolumeID.Id,
		v1.PersistentVolumeReclaimRetain, newLabel, v1.VolumeAvailable, "")

	pvUpdated(oldPv, newPv, metadataSyncer)

	// Verify pv label of volume matches that of updated metadata.
	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeInfo.VolumeID.Id, PV, newPv.Name, testPVLabelValue); err != nil {
		t.Fatal(err)
	}

	// Create PVC on K8S to bound to recently created PV.
	namespace := testNamespace
	oldPVCLabel := make(map[string]string)
	newPVCLabel := make(map[string]string)
	newPVCLabel[testPVCLabelName] = testPVCLabelValue
	pvcName = testPVCName + "-" + uuid.New().String()
	pvc := getPersistentVolumeClaimSpec(pvcName, namespace, oldPVCLabel, pv.Name, "")
	if pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(namespace).Create(
		ctx, pvc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Test pvcUpdate workflow on VC.
	oldPvc := getPersistentVolumeClaimSpec(pvcName, testNamespace, oldPVCLabel, pv.Name, "")
	newPvc := getPersistentVolumeClaimSpec(pvcName, testNamespace, newPVCLabel, pv.Name, "")
	waitForListerSync()
	pvcUpdated(oldPvc, newPvc, metadataSyncer)

	// Verify pvc label of volume matches that of updated metadata.
	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeInfo.VolumeID.Id, PVC, newPvc.Name, testPVCLabelValue); err != nil {
		t.Fatal(err)
	}

	// Create Pod on K8S with claim = recently created pvc.
	oldPodLabel := make(map[string]string)
	newPodLabel := make(map[string]string)
	newPodLabel[testPodLabelName] = testPodLabelValue
	pod := getPodSpec(testNamespace, oldPodLabel, pvc.Name, v1.PodRunning)
	if pod, err = k8sclient.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Test podUpdate workflow on VC.
	oldPod := getPodSpec(testNamespace, oldPodLabel, pvc.Name, v1.PodPending)
	newPod := getPodSpec(testNamespace, newPodLabel, pvc.Name, v1.PodRunning)
	podUpdated(oldPod, newPod, metadataSyncer)

	// Verify pod label of volume matches that of updated metadata.
	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeInfo.VolumeID.Id, POD, newPod.Name, ""); err != nil {
		t.Fatal(err)
	}

	// Test podDeleted workflow on VC.
	podDeleted(newPod, metadataSyncer)
	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyDeleteOperation(queryResult, volumeInfo.VolumeID.Id, POD); err != nil {
		t.Fatal(err)
	}

	// Test pvcDelete workflow.
	waitForListerSync()
	pvcDeleted(newPvc, metadataSyncer)
	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyDeleteOperation(queryResult, volumeInfo.VolumeID.Id, PVC); err != nil {
		t.Fatal(err)
	}

	// Test pvDelete workflow.
	pvDeleted(newPv, metadataSyncer)
	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyDeleteOperation(queryResult, volumeInfo.VolumeID.Id, PV); err != nil {
		t.Fatal(err)
	}

	// Cleanup on K8S.
	if err = k8sclient.CoreV1().Pods(namespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0)); err != nil {
		t.Fatal(err)
	}
	if err = k8sclient.CoreV1().PersistentVolumeClaims(namespace).
		Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0)); err != nil {
		t.Fatal(err)
	}
	if err = k8sclient.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0)); err != nil {
		t.Fatal(err)
	}

	t.Log("TestMetadataSyncInformer end")

}

// verifyDeleteOperation verifies if a delete operation was successful.
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
		if resourceType == PVC && queryLabel == testPVCLabelName && queryValue == testPVCLabelValue {
			return fmt.Errorf("delete operation failed for volume Id %s and resource type %s ", volumeID, resourceType)
		}
		if resourceType == PV && queryLabel == testPVLabelName && queryValue == testPVLabelValue {
			return fmt.Errorf("delete operation failed for volume Id %s and resource type %s ", volumeID, resourceType)
		}
		if resourceType == POD && queryLabel == testPodLabelName && queryValue == testPodLabelValue {
			return fmt.Errorf("delete operation failed for volume Id %s and resource type %s ", volumeID, resourceType)
		}
	}
	return nil
}

// verifyUpdateOperation verifies if an update operation was successful.
func verifyUpdateOperation(queryResult *cnstypes.CnsQueryResult, volumeID string,
	resourceType string, resourceName string, resourceNewLabel string) error {
	if len(queryResult.Volumes) == 0 || len(queryResult.Volumes[0].Metadata.EntityMetadata) == 0 {
		return fmt.Errorf("update operation failed for volume Id %s for resource type %s with queryResult: %v",
			volumeID, resourceType, spew.Sdump(queryResult))
	}
	entityMetadata := queryResult.Volumes[0].Metadata.EntityMetadata
	for _, baseMetadata := range entityMetadata {
		metadata := interface{}(baseMetadata).(*cnstypes.CnsKubernetesEntityMetadata)
		if resourceType == POD && metadata.EntityType == "POD" && metadata.EntityName == resourceName &&
			len(metadata.Labels) == 0 {
			return nil
		}
		if len(metadata.Labels) == 0 {
			return fmt.Errorf("update operation failed for volume Id %s and resource type %s queryResult: %v",
				volumeID, metadata.EntityType, spew.Sdump(queryResult))
		}
		queryLabel := metadata.Labels[0].Key
		queryValue := metadata.Labels[0].Value
		if resourceType == PVC && metadata.EntityType == "PERSISTENT_VOLUME_CLAIM" &&
			metadata.EntityName == resourceName && queryLabel == testPVCLabelName && queryValue == resourceNewLabel {
			return nil
		}
		if resourceType == PV && metadata.EntityType == "PERSISTENT_VOLUME" &&
			metadata.EntityName == resourceName && queryLabel == testPVLabelName && queryValue == resourceNewLabel {
			return nil
		}
	}
	return fmt.Errorf("update operation failed for volume Id: %s for resource type %s with queryResult: %v",
		volumeID, resourceType, spew.Sdump(queryResult))
}

// getPersistentVolumeSpec creates PV volume spec with given Volume Handle,
// Reclaim Policy, Labels and Phase.
func getPersistentVolumeSpec(volumeName string, volumeHandle string,
	persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy, labels map[string]string,
	phase v1.PersistentVolumePhase, claimRefName string) *v1.PersistentVolume {
	var pv *v1.PersistentVolume
	var claimRef *v1.ObjectReference
	if claimRefName != "" {
		claimRef = &v1.ObjectReference{
			Name:      claimRefName,
			Namespace: testNamespace,
		}
	}
	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PersistentVolume",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              volumeName,
			Generation:        0,
			CreationTimestamp: metav1.Time{},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       csitypes.Name,
					VolumeHandle: volumeHandle,
					ReadOnly:     false,
					FSType:       "ext4",
				},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			ClaimRef:                      claimRef,
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			StorageClassName:              "",
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	pv.Status.Phase = phase
	return pv
}

// getPersistentVolumeClaimSpec gets vsphere persistent volume spec with given
// selector labels.
func getPersistentVolumeClaimSpec(pvcName string, namespace string,
	labels map[string]string, pvName string, scName string) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			Kind:       "",
			APIVersion: "",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              pvcName,
			Namespace:         namespace,
			Generation:        0,
			CreationTimestamp: metav1.Time{},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("1Gi"),
				},
			},
			VolumeName:       pvName,
			StorageClassName: &scName,
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

// This test verifies the fullsync workflow:
//    1. PV does not exist in K8S, but exist in CNS cache, fullsync should
//       delete this volume from CNS cache.
//    2. PV and PVC exist in K8S, but does not exist in CNS cache, fullsync
//       should create this volume in CNS cache.
//    3. PV and PVC exist in K8S and CNS cache, update the label of PV and
//       PVC in K8S, fullsync should update the label in CNS cache.
//    4. Pod is created in K8S with PVC, fullsync should update the Pod in
//       CNS cache.
func runTestFullSyncWorkflows(t *testing.T) {
	t.Log("TestFullSyncWorkflows start")
	// Create spec for new volume.
	createSpec := cnstypes.CnsVolumeCreateSpec{
		DynamicData: vimtypes.DynamicData{},
		Name:        testVolumeName,
		VolumeType:  testVolumeType,
		Datastores:  dsList,
		Metadata: cnstypes.CnsVolumeMetadata{
			DynamicData: vimtypes.DynamicData{},
			ContainerCluster: cnstypes.CnsContainerCluster{
				ClusterType: string(cnstypes.CnsClusterTypeKubernetes),
				ClusterId:   csiConfig.Global.ClusterID,
				VSphereUser: csiConfig.VirtualCenter[cnsVCenterConfig.Host].User,
			},
		},
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{CapacityInMb: gbInMb},
		},
	}
	cnsCreationMap = make(map[string]bool)

	volumeInfo, err := volumeManager.CreateVolume(ctx, &createSpec)
	if err != nil {
		t.Errorf("failed to create volume. Error: %+v", err)
		t.Fatal(err)
		return
	}

	// Set volume id to be queried.
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volumeInfo.VolumeID.Id,
			},
		},
	}

	// Verify if volume is created.
	queryResult, err := virtualCenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volumeInfo.VolumeID.Id {
		t.Fatalf("failed to find the newly created volume with ID: %s", volumeInfo.VolumeID.Id)
	}
	cnsDeletionMap = make(map[string]bool)
	// PV does not exist in K8S, but volume exist in CNS cache.
	// FullSync should delete this volume from CNS cache after two cycles.
	waitForListerSync()
	err = CsiFullSync(ctx, metadataSyncer)
	if err != nil {
		t.Fatal(err)
	}
	err = CsiFullSync(ctx, metadataSyncer)
	if err != nil {
		t.Fatal(err)
	}

	// Verify if volume has been deleted from cache.
	queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Full sync failed to remove volume")
	}

	// PV and PVC exist in K8S, but does not exist in CNS cache.
	// FullSync should create this volume in CNS cache.

	// Create PV in K8S with VolumeHandle of recently deleted Volume.
	pvLabel := make(map[string]string)
	pvLabel[testPVLabelName] = testPVLabelValue
	pvName := testVolumeName + "-" + uuid.New().String()
	pv := getPersistentVolumeSpec(pvName, volumeInfo.VolumeID.Id,
		v1.PersistentVolumeReclaimRetain, pvLabel, v1.VolumeAvailable, "")
	if pv, err = k8sclient.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Create PVC in K8S to bound to recently created PV.
	namespace := testNamespace
	pvcLabel := make(map[string]string)
	pvcLabel[testPVCLabelName] = testPVCLabelValue
	pvcName := testPVCName + "-" + uuid.New().String()
	pvc := getPersistentVolumeClaimSpec(pvcName, namespace, pvcLabel, pv.Name, "")
	if pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(testNamespace).Create(
		ctx, pvc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Allocate pvc claimRef for PV spec.
	pv = getPersistentVolumeSpec(pvName, volumeInfo.VolumeID.Id,
		v1.PersistentVolumeReclaimRetain, pvLabel, v1.VolumeBound, pvc.Name)
	if pv, err = k8sclient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForListerSync()
	err = CsiFullSync(ctx, metadataSyncer)
	if err != nil {
		t.Fatal(err)
	}
	err = CsiFullSync(ctx, metadataSyncer)
	if err != nil {
		t.Fatal(err)
	}

	// Verify pv label of volume matches that of updated metadata.
	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeInfo.VolumeID.Id, PV, pv.Name, testPVLabelValue); err != nil {
		t.Fatal(err)
	}

	if err = verifyUpdateOperation(queryResult, volumeInfo.VolumeID.Id, PVC, pvc.Name, testPVCLabelValue); err != nil {
		t.Fatal(err)
	}

	// PV, PVC is updated in K8S with new label value, CNS cache still hold the
	// old label value. FullSync should update the metadata in CNS cache with
	// new label value.

	// Update pv with new label.
	newPVLabel := make(map[string]string)
	newPVLabel[testPVLabelName] = newTestPVLabelValue
	pv.Labels = newPVLabel
	if pv, err = k8sclient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForListerSync()
	err = CsiFullSync(ctx, metadataSyncer)
	if err != nil {
		t.Fatal(err)
	}
	// Verify pv label value has been updated in CNS cache.

	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeInfo.VolumeID.Id, PV, pv.Name, newTestPVLabelValue); err != nil {
		t.Fatal(err)
	}

	// Update pvc with new label.
	newPVCLabel := make(map[string]string)
	newPVCLabel[testPVCLabelName] = newTestPVCLabelValue
	pvc.Labels = newPVCLabel
	if pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(testNamespace).Update(
		ctx, pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForListerSync()
	err = CsiFullSync(ctx, metadataSyncer)
	if err != nil {
		t.Fatal(err)
	}

	// Verify pvc label value has been updated in CNS cache.

	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeInfo.VolumeID.Id, PVC, pvc.Name, newTestPVCLabelValue); err != nil {
		t.Fatal(err)
	}

	// Pod is created with PVC, CNS cache do not have the Pod metadata.
	// Fullsync should update the Pod in CNS cache.

	// Create Pod on K8S with claim = recently created pvc.
	pod := getPodSpec(testNamespace, nil, pvc.Name, v1.PodRunning)
	if pod, err = k8sclient.CoreV1().Pods(testNamespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForListerSync()
	err = CsiFullSync(ctx, metadataSyncer)
	if err != nil {
		t.Fatal(err)
	}

	// Verify Pod metadata of volume matches that of updated metadata.
	if queryResult, err = virtualCenter.CnsClient.QueryVolume(ctx, queryFilter); err != nil {
		t.Fatal(err)
	}
	if err = verifyUpdateOperation(queryResult, volumeInfo.VolumeID.Id, POD, pod.Name, ""); err != nil {
		t.Fatal(err)
	}

	// Cleanup in K8S.
	if err = k8sclient.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0)); err != nil {
		t.Fatal(err)
	}
	if err = k8sclient.CoreV1().PersistentVolumeClaims(testNamespace).Delete(
		ctx, pvc.Name, *metav1.NewDeleteOptions(0)); err != nil {
		t.Fatal(err)
	}
	if err = k8sclient.CoreV1().Pods(testNamespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0)); err != nil {
		t.Fatal(err)
	}

	// Cleanup in CNS to delete the volume.
	if err = volumeManager.DeleteVolume(ctx, volumeInfo.VolumeID.Id, true); err != nil {
		t.Logf("failed to delete volume %v from CNS", volumeInfo.VolumeID.Id)
	}
	t.Log("TestFullSyncWorkflows end")
}

func getPodSpec(namespace string, labels map[string]string, pvcName string, phase v1.PodPhase) *v1.Pod {
	var pod *v1.Pod
	podVolume := []v1.Volume{
		{
			Name: testVolumeName,
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
					ReadOnly:  false,
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
			Resources: v1.ResourceRequirements{},
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      testVolumeName,
					ReadOnly:  false,
					MountPath: "/data",
				},
			},
			TerminationMessagePath:   "/dev/termination-log",
			TerminationMessagePolicy: "File",
			ImagePullPolicy:          "Always",
		},
	}
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testPodName,
			Namespace:         namespace,
			Generation:        0,
			CreationTimestamp: metav1.Time{},
			Labels:            labels,
		},
		Spec: v1.PodSpec{
			Volumes:       podVolume,
			Containers:    podContainer,
			RestartPolicy: "Always",
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
	}
	return pod
}

// waitForListerSync allow Listers to sync with recently created k8s objects.
// To ensure unit tests are executed successfully for very recently created k8s
// objects, we need to ensure listers used in the metadata syncer are synced.
func waitForListerSync() {
	time.Sleep(1 * time.Second)
}
