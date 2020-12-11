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

package cns

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/pbm"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	testclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog"

	"k8s.io/apimachinery/pkg/api/resource"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

const (
	testVolumeName  = "test-volume"
	testClusterName = "test-cluster"
	testPVName      = "test-pv"
	testPVCName     = "test-pvc"
	testNamespace   = "default"
)

var (
	k8sclient clientset.Interface
)

type FakeNodeManager struct {
	client             *vim25.Client
	sharedDatastoreURL string
	k8sClient          clientset.Interface
}

func (f *FakeNodeManager) Initialize() error {
	return nil
}

func (f *FakeNodeManager) GetSharedDatastoresInK8SCluster(ctx context.Context) ([]*cnsvsphere.DatastoreInfo, error) {
	finder := find.NewFinder(f.client, false)

	var datacenterName string
	if v := os.Getenv("VSPHERE_DATACENTER"); v != "" {
		datacenterName = v
	} else {
		datacenterName = simulator.Map.Any("Datacenter").(*simulator.Datacenter).Name
	}

	dc, _ := finder.Datacenter(ctx, datacenterName)
	finder.SetDatacenter(dc)

	datastores, err := finder.DatastoreList(ctx, "*")
	if err != nil {
		return nil, err
	}
	var dsList []types.ManagedObjectReference
	for _, ds := range datastores {
		dsList = append(dsList, ds.Reference())
	}
	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(dc.Client())
	properties := []string{"info"}
	err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
	if err != nil {
		return nil, err
	}

	var sharedDatastoreManagedObject *mo.Datastore
	for _, dsMo := range dsMoList {
		if dsMo.Info.GetDatastoreInfo().Url == f.sharedDatastoreURL {
			sharedDatastoreManagedObject = &dsMo
			break
		}
	}
	if sharedDatastoreManagedObject == nil {
		return nil, fmt.Errorf("Failed to get shared datastores")
	}
	return []*cnsvsphere.DatastoreInfo{
		{
			Datastore: &cnsvsphere.Datastore{
				Datastore:  object.NewDatastore(nil, sharedDatastoreManagedObject.Reference()),
				Datacenter: nil},
			Info: sharedDatastoreManagedObject.Info.GetDatastoreInfo(),
		},
	}, nil
}

func (f *FakeNodeManager) GetNodeByName(nodeName string) (*cnsvsphere.VirtualMachine, error) {
	var vm *cnsvsphere.VirtualMachine
	if v := os.Getenv("VSPHERE_DATACENTER"); v != "" {
		nodeUUID, err := k8s.GetNodeVMUUID(f.k8sClient, nodeName)
		if err != nil {
			klog.Errorf("Failed to get providerId from node: %q. Err: %v", nodeName, err)
			return nil, err
		}
		vm, err = cnsvsphere.GetVirtualMachineByUUID(nodeUUID, false)
		if err != nil {
			klog.Errorf("Couldn't find VM instance with nodeUUID %s, failed to discover with err: %v", nodeUUID, err)
			return nil, err
		}
	} else {
		obj := simulator.Map.Any("VirtualMachine").(*simulator.VirtualMachine)
		vm = &cnsvsphere.VirtualMachine{
			VirtualMachine: object.NewVirtualMachine(f.client, obj.Reference()),
		}
	}
	return vm, nil
}

func (f *FakeNodeManager) GetSharedDatastoresInTopology(ctx context.Context, topologyRequirement *csi.TopologyRequirement, zoneKey string, regionKey string) ([]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error) {
	return nil, nil, nil
}

type controllerTest struct {
	controller *controller
	config     *config.Config
	vcenter    *cnsvsphere.VirtualCenter
}

var (
	controllerTestInstance *controllerTest
	onceForControllerTest  sync.Once
)

func getControllerTest(t *testing.T) *controllerTest {
	onceForControllerTest.Do(func() {
		config, _ := config.FromEnvOrSim()

		// CNS based CSI requires a valid cluster name
		config.Global.ClusterID = testClusterName

		ctx := context.Background()

		vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(config)
		if err != nil {
			t.Fatal(err)
		}
		vcManager := cnsvsphere.GetVirtualCenterManager()
		vcenter, err := vcManager.RegisterVirtualCenter(vcenterconfig)
		if err != nil {
			t.Fatal(err)
		}

		err = vcenter.ConnectCNS(ctx)
		if err != nil {
			t.Fatal(err)
		}

		manager := &common.Manager{
			VcenterConfig:  vcenterconfig,
			CnsConfig:      config,
			VolumeManager:  cnsvolume.GetManager(vcenter),
			VcenterManager: cnsvsphere.GetVirtualCenterManager(),
		}

		var sharedDatastoreURL string
		if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
			sharedDatastoreURL = v
		} else {
			sharedDatastoreURL = simulator.Map.Any("Datastore").(*simulator.Datastore).Info.GetDatastoreInfo().Url
		}

		if k8senv := os.Getenv("KUBECONFIG"); k8senv != "" {
			k8sclient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			k8sclient = testclient.NewSimpleClientset()
		}

		c := &controller{
			manager: manager,
			nodeMgr: &FakeNodeManager{
				client:             vcenter.Client.Client,
				sharedDatastoreURL: sharedDatastoreURL,
				k8sClient:          k8sclient,
			},
		}
		controllerTestInstance = &controllerTest{
			controller: c,
			config:     config,
			vcenter:    vcenter,
		}
		controllerTestInstance.controller.k8sInformerManager = k8s.NewInformer(k8sclient)
		controllerTestInstance.controller.pvLister = controllerTestInstance.controller.k8sInformerManager.GetPVLister()
		controllerTestInstance.controller.vaLister = controllerTestInstance.controller.k8sInformerManager.GetVALister()
		controllerTestInstance.controller.k8sInformerManager.Listen()
	})
	return controllerTestInstance
}

func TestCreateVolumeWithStoragePolicy(t *testing.T) {
	// Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ct := getControllerTest(t)

	// Create
	params := make(map[string]string)
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		params[common.AttributeDatastoreURL] = v
	}

	// PBM simulator defaults
	params[common.AttributeStoragePolicyName] = "vSAN Default Storage Policy"
	if v := os.Getenv("VSPHERE_STORAGE_POLICY_NAME"); v != "" {
		params[common.AttributeStoragePolicyName] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}

	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId

	// Create objects in k8s
	pvcName := testPVCName + "-" + uuid.New().String()
	pvName := testPVName + "-" + uuid.New().String()
	pvc := getPersistentVolumeClaimSpec(pvcName, testNamespace, nil, pvName)
	if pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(testNamespace).Create(pvc); err != nil {
		t.Fatal(err)
	}
	pv := getPersistentVolumeSpec(pvName, volID, v1.PersistentVolumeReclaimDelete, nil, v1.VolumeBound, "")
	if pv, err = k8sclient.CoreV1().PersistentVolumes().Create(pv); err != nil {
		t.Fatal(err)
	}
	// Verify the volume has been create with corresponding storage policy ID
	pc, err := pbm.NewClient(ctx, ct.vcenter.Client.Client)
	if err != nil {
		t.Fatal(err)
	}

	profileID, err := pc.ProfileIDByName(ctx, params[common.AttributeStoragePolicyName])
	if err != nil {
		t.Fatal(err)
	}

	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("Failed to find the newly created volume with ID: %s", volID)
	}

	if queryResult.Volumes[0].StoragePolicyId != profileID {
		t.Fatalf("Failed to match volume policy ID: %s", profileID)
	}

	// QueryAll
	queryFilter = cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	querySelection := cnstypes.CnsQuerySelection{}
	queryResult, err = ct.vcenter.CnsClient.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("Failed to find the newly created volume with ID: %s", volID)
	}

	// Delete
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}

	// Varify the volume has been deleted
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

func TestCompleteControllerFlow(t *testing.T) {
	// Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ct := getControllerTest(t)

	// Create
	params := make(map[string]string)
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		params[common.AttributeDatastoreURL] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}

	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId

	pvcName := testPVCName + "-" + uuid.New().String()
	pvName := testPVName + "-" + uuid.New().String()
	pvc := getPersistentVolumeClaimSpec(pvcName, testNamespace, nil, pvName)
	if pvc, err = k8sclient.CoreV1().PersistentVolumeClaims(testNamespace).Create(pvc); err != nil {
		t.Fatal(err)
	}
	pv := getPersistentVolumeSpec(pvName, volID, v1.PersistentVolumeReclaimDelete, nil, v1.VolumeBound, "")
	if pv, err = k8sclient.CoreV1().PersistentVolumes().Create(pv); err != nil {
		t.Fatal(err)
	}
	// Varify the volume has been created
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("Failed to find the newly created volume with ID: %s", volID)
	}

	// QueryAll
	queryFilter = cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	querySelection := cnstypes.CnsQuerySelection{}
	queryResult, err = ct.vcenter.CnsClient.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("Failed to find the newly created volume with ID: %s", volID)
	}

	var nodeID string
	if v := os.Getenv("VSPHERE_K8S_NODE"); v != "" {
		nodeID = v
	} else {
		nodeID = simulator.Map.Any("VirtualMachine").(*simulator.VirtualMachine).Name
	}

	// Attach
	reqControllerPublishVolume := &csi.ControllerPublishVolumeRequest{
		VolumeId:         volID,
		NodeId:           nodeID,
		VolumeCapability: capabilities[0],
		Readonly:         false,
	}
	t.Log(fmt.Sprintf("ControllerPublishVolume will be called with req %+v", *reqControllerPublishVolume))
	respControllerPublishVolume, err := ct.controller.ControllerPublishVolume(ctx, reqControllerPublishVolume)
	if err != nil {
		t.Fatal(err)
	}
	diskUUID := respControllerPublishVolume.PublishContext[common.AttributeFirstClassDiskUUID]
	t.Log(fmt.Sprintf("ControllerPublishVolume succeed, diskUUID %s is returned", diskUUID))

	//Detach
	reqControllerUnpublishVolume := &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volID,
		NodeId:   nodeID,
	}
	t.Log(fmt.Sprintf("ControllerUnpublishVolume will be called with req %+v", *reqControllerUnpublishVolume))
	_, err = ct.controller.ControllerUnpublishVolume(ctx, reqControllerUnpublishVolume)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ControllerUnpublishVolume succeed")

	// Delete
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}

	// Varify the volume has been deleted
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

// getPersistentVolumeSpec creates PV volume spec with given Volume Handle, Reclaim Policy, Labels and Phase
func getPersistentVolumeSpec(volumeName string, volumeHandle string, persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy, labels map[string]string, phase v1.PersistentVolumePhase, claimRefName string) *v1.PersistentVolume {
	var pv *v1.PersistentVolume
	var claimRef *v1.ObjectReference
	if claimRefName != "" {
		claimRef = &v1.ObjectReference{
			Name: claimRefName,
		}
	}
	pv = &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeName,
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       "csi.vsphere.vmware.com",
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
	if &phase != nil {
		pv.Status.Phase = phase
	}
	return pv
}

// getPersistentVolumeClaimSpec gets vsphere persistent volume spec with given selector labels.
func getPersistentVolumeClaimSpec(pvcName string, namespace string, labels map[string]string, pvName string) *v1.PersistentVolumeClaim {
	var (
		pvc *v1.PersistentVolumeClaim
	)
	sc := ""
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
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
