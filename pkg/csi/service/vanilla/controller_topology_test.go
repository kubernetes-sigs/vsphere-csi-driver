/*
Copyright 2024 The Kubernetes Authors.

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

package vanilla

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vapi/rest"
	_ "github.com/vmware/govmomi/vapi/simulator"
	"github.com/vmware/govmomi/vapi/tags"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/container-storage-interface/spec/lib/go/csi"
	clientset "k8s.io/client-go/kubernetes"
	testclient "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var (
	ctxtopology                    context.Context
	controllerTestInstanceTopology *controllerTestTopology
	onceForControllerTestTopology  sync.Once
)

type controllerTestTopology struct {
	controller *controller
	config     *config.Config
	vcenter    *cnsvsphere.VirtualCenter
	// Add a VolumeOperationRequest interface to set up certain test scenario
	operationStore cnsvolumeoperationrequest.VolumeOperationRequest
}

type FakeTopologyManager struct {
	nodeLabels map[string]map[string]string
}

type FakeNodeManagerTopology struct {
	cnsNodeManager node.Manager
	k8sClient      clientset.Interface
	vimClient      *vim25.Client
}

func getAllManagedObjects(ctx context.Context, client *vim25.Client, kind string, prop []string, dst any) error {
	m := view.NewManager(client)

	v, err := m.CreateContainerView(ctx, client.ServiceContent.RootFolder, []string{kind}, true)
	if err != nil {
		return err
	}

	defer func() { _ = v.Destroy(ctx) }()

	return v.Retrieve(ctx, []string{kind}, prop, dst)
}

func getAllVirtualMachines(ctx context.Context, client *vim25.Client, props ...string) ([]mo.VirtualMachine, error) {
	var vms []mo.VirtualMachine
	err := getAllManagedObjects(ctx, client, "VirtualMachine", props, &vms)
	if err != nil {
		return nil, err
	}
	if len(vms) == 0 {
		return nil, fmt.Errorf("no VirtualMachines found")
	}
	return vms, nil
}

func (f *FakeNodeManagerTopology) Initialize(ctx context.Context) error {
	f.cnsNodeManager = node.GetManager(ctx)
	f.cnsNodeManager.SetKubernetesClient(f.k8sClient)
	var t *testing.T

	// node.GetManager returns a singleton instance of the Manager interface,
	// so we may have nodes registered as part of previous test from same folder.
	// Unregister all nodes which are already registered with nodeManager.
	err := f.cnsNodeManager.UnregisterAllNodes(ctx)
	if err != nil {
		t.Errorf("Error occurred while unregistering all nodes, err: %v", err)
	}

	objVMs, err := getAllVirtualMachines(ctx, f.vimClient, "config.uuid")
	if err != nil {
		return err
	}
	var i int
	for _, vm := range objVMs {
		i++
		nodeUUID := vm.Config.Uuid
		nodeName := "k8s-node-" + strconv.Itoa(i)
		// Register new node entry in nodeManager
		err := f.cnsNodeManager.RegisterNode(ctx, nodeUUID, nodeName)
		if err != nil {
			t.Errorf("Error occurred while registering a node: %s, nodeUUID: %s, err: %v", nodeName, nodeUUID, err)
			return err
		}
	}
	return nil
}

func (f *FakeNodeManagerTopology) GetSharedDatastoresInK8SCluster(ctx context.Context) ([]*cnsvsphere.DatastoreInfo,
	error) {
	// This function is not required for topology
	return nil, nil
}

func (f *FakeNodeManagerTopology) GetNodeVMByNameAndUpdateCache(ctx context.Context,
	nodeName string) (*cnsvsphere.VirtualMachine, error) {
	return f.cnsNodeManager.GetNodeVMByNameAndUpdateCache(ctx, nodeName)
}

func (f *FakeNodeManagerTopology) GetNodeVMByNameOrUUID(
	ctx context.Context, nodeNameOrUUID string) (*cnsvsphere.VirtualMachine, error) {
	return f.cnsNodeManager.GetNodeVMByNameOrUUID(ctx, nodeNameOrUUID)
}

func (f *FakeNodeManagerTopology) GetNodeNameByUUID(ctx context.Context, nodeUUID string) (string, error) {
	return f.cnsNodeManager.GetNodeNameByUUID(ctx, nodeUUID)
}

func (f *FakeNodeManagerTopology) GetNodeVMByUuid(ctx context.Context, nodeUUID string) (*cnsvsphere.VirtualMachine,
	error) {
	return f.cnsNodeManager.GetNodeVMByUuid(ctx, nodeUUID)
}

func (f *FakeNodeManagerTopology) GetAllNodes(ctx context.Context) ([]*cnsvsphere.VirtualMachine, error) {
	return f.cnsNodeManager.GetAllNodes(ctx)
}

func (f *FakeNodeManagerTopology) GetAllNodesByVC(ctx context.Context, vcHost string) ([]*cnsvsphere.VirtualMachine,
	error) {
	// For topology testing, return all nodes since we're working with a single vCenter
	return f.cnsNodeManager.GetAllNodes(ctx)
}

// generateNodeLabels adds region and zone specific labels on all nodeVMs generated using vcsim
func generateNodeLabels(ctx context.Context, vc *cnsvsphere.VirtualCenter) (map[string]map[string]string, error) {
	nodeLabelsMap := map[string]map[string]string{}
	var i int

	finder := find.NewFinder(vc.Client.Client, false)
	objDCs, err := finder.DatacenterList(ctx, "*")
	if err != nil {
		return nil, err
	}
	for _, dc := range objDCs {
		i++
		dcname := dc.Name()
		fmt.Printf("generateNodeLabels for datacenter=%s\n", dcname)
		datacenter, err := finder.Datacenter(ctx, dcname)
		if err != nil {
			fmt.Printf("Error occurred while getting datacenter using finder\n")
			return nil, err
		}
		finder.SetDatacenter(datacenter)

		vms, err := finder.VirtualMachineList(ctx, "*")
		if err != nil {
			fmt.Printf("Error occurred while getting virtual machine list for datacenter\n")
			return nil, err
		}
		for _, vm := range vms {
			nodeUUID := vm.UUID(ctx)
			nodeLabelsMap[nodeUUID] = make(map[string]string)
			region := "region-" + strconv.Itoa(i)
			zone := "zone-" + strconv.Itoa(i)
			nodeLabelsMap[nodeUUID]["topology.csi.vmware.com/k8s-region"] = region
			nodeLabelsMap[nodeUUID]["topology.csi.vmware.com/k8s-zone"] = zone

			fmt.Printf("Nodename = %s, uuid = %s, region = %s, zone = %s\n", vm, nodeUUID, region, zone)
		}
	}

	return nodeLabelsMap, nil
}

func getSharedDatastoresInTopology(ctx context.Context, topoRequirement []*csi.Topology,
	nodeLabels map[string]map[string]string) ([]*cnsvsphere.DatastoreInfo, error) {
	var (
		t                *testing.T
		err              error
		sharedDatastores []*cnsvsphere.DatastoreInfo
		matchingNodeVMs  []*cnsvsphere.VirtualMachine
	)
	// A topology requirement is an array of topology segments.
	for _, topology := range topoRequirement {
		segments := topology.GetSegments()

		// Get nodes with topology labels matching the topology segments
		matchingNodeVMs, err = getNodesMatchingTopologySegment(ctx, segments, nodeLabels)
		if err != nil {
			t.Errorf("Failed to find nodes in topology segment %+v. Error: %+v", segments, err)
			return nil, err
		}

		if len(matchingNodeVMs) == 0 {
			fmt.Printf("No nodes in the cluster matched the topology requirement provided: %+v\n",
				segments)
			continue
		}

		// Fetch shared datastores for the matching nodeVMs.
		fmt.Printf("Obtained list of nodeVMs %+v\n", matchingNodeVMs)
		sharedDatastoresInTopology, err := cnsvsphere.GetSharedDatastoresForVMs(ctx, matchingNodeVMs)
		if err != nil {
			t.Errorf("failed to get shared datastores for nodes: %+v in topology segment %+v. Error: %+v",
				matchingNodeVMs, segments, err)
			return nil, err
		}

		// Update sharedDatastores with the list of datastores received for current topology segments.
		// Duplicates will not be added.
		for _, ds := range sharedDatastoresInTopology {
			var found bool
			for _, sharedDS := range sharedDatastores {
				if sharedDS.Info.Url == ds.Info.Url {
					found = true
					break
				}
			}
			if !found {
				sharedDatastores = append(sharedDatastores, ds)
			}
		}
	}

	return sharedDatastores, nil
}

func getNodesMatchingTopologySegment(ctx context.Context, segments map[string]string,
	nodeLabels map[string]map[string]string) ([]*cnsvsphere.VirtualMachine, error) {
	var (
		matchingNodeVMs []*cnsvsphere.VirtualMachine
		err             error
		t               *testing.T
	)

	for nodeUUID, labels := range nodeLabels {
		isMatch := true
		for key, value := range segments {
			if labels[key] != value {
				fmt.Printf("Node %q with topology labels %+v did not match the topology requirement - %q:%q\n",
					nodeUUID, labels, key, value)
				isMatch = false
				break
			}
		}
		if isMatch {
			var nodeVM *cnsvsphere.VirtualMachine
			nodeVM, err = cnsvsphere.GetVirtualMachineByUUID(ctx, nodeUUID, false)
			if err != nil {
				t.Errorf("Couldn't find VM instance with nodeUUID %s, err: %v", nodeUUID, err)
				return nil, err
			}

			matchingNodeVMs = append(matchingNodeVMs, nodeVM)
		}
	}

	return matchingNodeVMs, nil
}

// TODO: Currently mocking GetSharedDatastoresInTopology function by adding region and zone labels on nodeVMs
// and returning shared datastores as per topology parameters. We can generate CSI node topology instances in fake
// kubeclient etc. and check if mocking can be avoided.
func (f *FakeTopologyManager) GetSharedDatastoresInTopology(ctx context.Context, topologyFetchDSParams interface{}) (
	[]*cnsvsphere.DatastoreInfo, error) {
	var (
		t                *testing.T
		err              error
		sharedDatastores []*cnsvsphere.DatastoreInfo
		topoRequirement  []*csi.Topology
	)

	params := topologyFetchDSParams.(commoncotypes.VanillaTopologyFetchDSParams)
	fmt.Printf("topologyRequirement is: %+v\n", params.TopologyRequirement)

	// Fetch shared datastores for the preferred topology requirement.
	if params.TopologyRequirement.GetPreferred() != nil {
		fmt.Printf("Using preferred topology to get shared datastores\n")
		topoRequirement = params.TopologyRequirement.GetPreferred()
		sharedDatastores, err = getSharedDatastoresInTopology(ctx, topoRequirement, f.nodeLabels)
		if err != nil {
			t.Errorf("Error finding shared datastores using preferred topology: %+v",
				params.TopologyRequirement.GetPreferred())
			return nil, err
		}
	}

	// If there are no shared datastores for the preferred topology requirement, fetch shared
	// datastores for the requisite topology requirement instead.
	if len(sharedDatastores) == 0 && params.TopologyRequirement.GetRequisite() != nil {
		fmt.Printf("Using requisite topology to get shared datastores\n")
		topoRequirement = params.TopologyRequirement.GetRequisite()
		sharedDatastores, err = getSharedDatastoresInTopology(ctx, topoRequirement, f.nodeLabels)
		if err != nil {
			t.Errorf("Error finding shared datastores using requisite topology: %+v",
				params.TopologyRequirement.GetRequisite())
			return nil, err
		}
	}

	return sharedDatastores, nil
}

func (f *FakeTopologyManager) GetTopologyInfoFromNodes(ctx context.Context, retrieveTopologyInfoParams interface{}) (
	[]map[string]string, error) {
	// This function is not yet implemented
	return nil, nil
}

func (f *FakeTopologyManager) GetAZClustersMap(ctx context.Context) map[string][]string {
	// This function is not yet implemented
	return nil
}

func (f *FakeTopologyManager) ZonesWithMultipleClustersExist(ctx context.Context) bool {
	// This function is not yet implemented
	return false
}

var vcsimParamsTopology = unittestcommon.VcsimParams{
	Datacenters:     2,
	Clusters:        1,
	HostsPerCluster: 1,
	VMsPerCluster:   2,
	StandaloneHosts: 0,
	Datastores:      1,
	Version:         "7.0.3",
	ApiVersion:      "7.0",
}

func getControllerTestWithTopology(t *testing.T) *controllerTestTopology {
	onceForControllerTestTopology.Do(func() {
		// Create context.
		ctxtopology = context.Background()
		config, _ := unittestcommon.ConfigFromEnvOrVCSim(ctxtopology, vcsimParamsTopology, true)

		// CNS based CSI requires a valid cluster name.
		config.Global.ClusterID = testClusterName

		vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(ctxtopology, config)
		if err != nil {
			t.Fatal(err)
		}
		vcManager := cnsvsphere.GetVirtualCenterManager(ctxtopology)
		// GetVirtualCenterManager returns a singleton instance of VirtualCenterManager,
		// so it could have already registered VCs as part of previous unit test run from
		// same folder.
		// Unregister old VCs.
		err = vcManager.UnregisterAllVirtualCenters(ctxtopology)
		if err != nil {
			t.Fatal(err)
		}
		vcenter, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctxtopology, vcenterconfig, false)
		if err != nil {
			t.Fatal(err)
		}

		err = vcenter.ConnectCns(ctxtopology)
		if err != nil {
			t.Fatal(err)
		}

		// Set up real tags in vcsim for topology testing
		err = setupRealTagsInVCSim(ctxtopology, vcenter)
		if err != nil {
			t.Fatal(err)
		}

		fakeOpStore, err := unittestcommon.InitFakeVolumeOperationRequestInterface()
		if err != nil {
			t.Fatal(err)
		}

		volumeManager, err := cnsvolume.GetManager(ctxtopology, vcenter,
			fakeOpStore, true, false,
			false, cnstypes.CnsClusterFlavorVanilla)
		if err != nil {
			t.Fatalf("failed to create an instance of volume manager. err=%v", err)
		}

		// wait till property collector has been started
		err = wait.PollUntilContextTimeout(ctxtopology, 1*time.Second, 10*time.Second, false,
			func(ctx context.Context) (done bool, err error) {
				return volumeManager.IsListViewReady(), nil
			})
		if err != nil {
			t.Fatalf("listview not ready. err=%v", err)
		}

		// GetManager returns a singleton instance of VolumeManager. So, it could be pointing
		// to old VC instance as part of previous unit test run from same folder.
		// Call ResetManager to get new VolumeManager instance with current VC configuration.
		err = volumeManager.ResetManager(ctxtopology, vcenter)
		if err != nil {
			t.Fatalf("failed to reset volume manager with new vcenter. err=%v", err)
		}

		// as per current logic, new vc object will be saved but not immediately used
		// only when we notice an issue adding tasks to listview, we will kill the context to property collector
		// causing the listview to be re-created with the newer credentials
		// this method is called here to explicitly re-create the listview since we changed the config above for
		// topology
		volumeManager.SetListViewNotReady(ctxtopology)

		// wait again for the property collector to be re-created
		err = wait.PollUntilContextTimeout(ctxtopology, 1*time.Second, 10*time.Second, false,
			func(ctx context.Context) (done bool, err error) {
				return volumeManager.IsListViewReady(), nil
			})
		if err != nil {
			t.Fatalf("listview not ready. err=%v", err)
		}

		manager := &common.Manager{
			VcenterConfig:  vcenterconfig,
			CnsConfig:      config,
			VolumeManager:  volumeManager,
			VcenterManager: vcManager,
		}
		managers := &common.Managers{
			VcenterConfigs: make(map[string]*cnsvsphere.VirtualCenterConfig),
			CnsConfig:      config,
			VolumeManagers: make(map[string]cnsvolume.Manager),
			VcenterManager: cnsvsphere.GetVirtualCenterManager(ctx),
		}
		managers.VcenterConfigs[vcenterconfig.Host] = vcenterconfig
		managers.VolumeManagers[vcenterconfig.Host] = volumeManager

		var k8sClient clientset.Interface
		if k8senv := os.Getenv("KUBECONFIG"); k8senv != "" {
			k8sClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			k8sClient = testclient.NewSimpleClientset()
		}

		nodeManager := &FakeNodeManagerTopology{
			k8sClient: k8sClient,
			vimClient: vcenter.Client.Client,
		}
		err = nodeManager.Initialize(ctxtopology)
		if err != nil {
			t.Fatalf("Failed to initialize the node manager, err= =%v", err)
		}

		mockNodeLabels, err := generateNodeLabels(ctxtopology, vcenter)
		if err != nil {
			t.Fatalf("Failed to add mock node labels, err = %v", err)
		}

		fakeAuthMgr := &FakeAuthManager{
			vcenter: vcenter,
		}

		c := &controller{
			manager:  manager,
			managers: managers,
			nodeMgr:  nodeManager,
			authMgr:  fakeAuthMgr,
			authMgrs: make(map[string]*common.AuthManager),
			topologyMgr: &FakeTopologyManager{
				nodeLabels: mockNodeLabels,
			},
			topologyCalc: &defaultTopologyCalculator{}, // Use real topology calculation for topology tests
		}
		c.authMgrs[vcenterconfig.Host], _ =
			common.GetAuthorizationServiceForTesting(ctxtopology,
				vcenter, fakeAuthMgr.GetDatastoreMapForBlockVolumes(ctxtopology), nil)

		commonco.ContainerOrchestratorUtility, err =
			unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
		if err != nil {
			t.Fatalf("Failed to create co agnostic interface. err=%v", err)
		}

		// Create CSINodeTopology instances for topology testing
		err = createCSINodeTopologyInstances(ctxtopology, mockNodeLabels, nodeManager, commonco.ContainerOrchestratorUtility)
		if err != nil {
			t.Fatalf("Failed to create CSINodeTopology instances. err=%v", err)
		}
		controllerTestInstanceTopology = &controllerTestTopology{
			controller:     c,
			config:         config,
			vcenter:        vcenter,
			operationStore: fakeOpStore,
		}
	})
	return controllerTestInstanceTopology
}

// Test CreateVolume functionality with specific AccessibilityRequirements
func TestCreateVolumeWithAccessibilityRequirements(t *testing.T) {
	// Create context.
	ct := getControllerTestWithTopology(t)

	// Create volume
	params := make(map[string]string)
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
		AccessibilityRequirements: &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{
					Segments: map[string]string{
						"topology.csi.vmware.com/k8s-zone": "zone-1",
					},
				},
			},
			Preferred: []*csi.Topology{
				{
					Segments: map[string]string{
						"topology.csi.vmware.com/k8s-zone": "zone-1",
					},
				},
			},
		},
	}

	respCreate, err := ct.controller.CreateVolume(ctxtopology, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId

	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctxtopology, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	// Delete volume
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctxtopology, reqDelete)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the volume has been deleted.
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctxtopology, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

// createCSINodeTopologyInstances creates CSINodeTopology instances based on the node labels
func createCSINodeTopologyInstances(ctx context.Context, nodeLabels map[string]map[string]string,
	nodeManager *FakeNodeManagerTopology, co commonco.COCommonInterface) error {

	var instances []interface{}
	nodeIndex := 1

	for nodeUUID, labels := range nodeLabels {
		// Get the node name from the node manager
		nodeName := fmt.Sprintf("k8s-node-%d", nodeIndex)
		nodeIndex++

		// Create topology labels array
		var topologyLabels []csinodetopologyv1alpha1.TopologyLabel
		for key, value := range labels {
			topologyLabels = append(topologyLabels, csinodetopologyv1alpha1.TopologyLabel{
				Key:   key,
				Value: value,
			})
		}

		// Create CSINodeTopology instance
		instance := &csinodetopologyv1alpha1.CSINodeTopology{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "cns.vmware.com/v1alpha1",
				Kind:       "CSINodeTopology",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
			},
			Spec: csinodetopologyv1alpha1.CSINodeTopologySpec{
				NodeID:   nodeName,
				NodeUUID: nodeUUID,
			},
			Status: csinodetopologyv1alpha1.CSINodeTopologyStatus{
				Status:         csinodetopologyv1alpha1.CSINodeTopologySuccess,
				TopologyLabels: topologyLabels,
				ErrorMessage:   "",
			},
		}

		// Convert to unstructured for storage
		unstructuredObj := &unstructured.Unstructured{}
		unstructuredMap, err := convertToUnstructured(instance)
		if err != nil {
			return fmt.Errorf("failed to convert CSINodeTopology to unstructured: %v", err)
		}
		unstructuredObj.Object = unstructuredMap

		instances = append(instances, unstructuredObj)
	}

	// Set the instances in the fake container orchestrator
	if fakeOrchestrator, ok := co.(*unittestcommon.FakeK8SOrchestrator); ok {
		fakeOrchestrator.SetCSINodeTopologyInstances(instances)
	} else {
		return fmt.Errorf("container orchestrator is not a FakeK8SOrchestrator")
	}

	return nil
}

// convertToUnstructured converts a typed object to an unstructured map
func convertToUnstructured(obj interface{}) (map[string]interface{}, error) {
	// This is a simplified conversion - in a real implementation, you might use
	// runtime.DefaultUnstructuredConverter.ToUnstructured
	switch v := obj.(type) {
	case *csinodetopologyv1alpha1.CSINodeTopology:
		return map[string]interface{}{
			"apiVersion": v.APIVersion,
			"kind":       v.Kind,
			"metadata": map[string]interface{}{
				"name": v.Name,
			},
			"spec": map[string]interface{}{
				"nodeID":   v.Spec.NodeID,
				"nodeuuid": v.Spec.NodeUUID,
			},
			"status": map[string]interface{}{
				"status":         string(v.Status.Status),
				"topologyLabels": convertTopologyLabelsToUnstructured(v.Status.TopologyLabels),
				"errorMessage":   v.Status.ErrorMessage,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", obj)
	}
}

// convertTopologyLabelsToUnstructured converts topology labels to unstructured format
func convertTopologyLabelsToUnstructured(labels []csinodetopologyv1alpha1.TopologyLabel) []interface{} {
	var result []interface{}
	for _, label := range labels {
		result = append(result, map[string]interface{}{
			"key":   label.Key,
			"value": label.Value,
		})
	}
	return result
}

// setupRealTagsInVCSim creates real vSphere tags and categories in vcsim instead of using mocks
func setupRealTagsInVCSim(ctx context.Context, vcenter *cnsvsphere.VirtualCenter) error {
	log := logger.GetLogger(ctx)

	// Create REST client for tag management
	restClient := rest.NewClient(vcenter.Client.Client)
	err := restClient.Login(ctx, simulator.DefaultLogin)
	if err != nil {
		return fmt.Errorf("failed to login to REST client: %v", err)
	}

	// Create tag manager
	tagManager := tags.NewManager(restClient)
	if tagManager == nil {
		return fmt.Errorf("failed to create tag manager")
	}

	log.Infof("Created tag manager with useragent: %s", tagManager.UserAgent)

	// Create k8s-region category
	regionCategoryID, err := cnsvsphere.CreateNewCategory(ctx, "k8s-region", "SINGLE", tagManager)
	if err != nil {
		return fmt.Errorf("failed to create k8s-region category: %v", err)
	}

	// Create k8s-zone category
	zoneCategoryID, err := cnsvsphere.CreateNewCategory(ctx, "k8s-zone", "SINGLE", tagManager)
	if err != nil {
		return fmt.Errorf("failed to create k8s-zone category: %v", err)
	}

	// Create region tags
	region1TagID, err := cnsvsphere.CreateNewTag(ctx, regionCategoryID, "region-1", "Region 1 tag", tagManager)
	if err != nil {
		return fmt.Errorf("failed to create region-1 tag: %v", err)
	}

	region2TagID, err := cnsvsphere.CreateNewTag(ctx, regionCategoryID, "region-2", "Region 2 tag", tagManager)
	if err != nil {
		return fmt.Errorf("failed to create region-2 tag: %v", err)
	}

	// Create zone tags
	zone1TagID, err := cnsvsphere.CreateNewTag(ctx, zoneCategoryID, "zone-1", "Zone 1 tag", tagManager)
	if err != nil {
		return fmt.Errorf("failed to create zone-1 tag: %v", err)
	}

	zone2TagID, err := cnsvsphere.CreateNewTag(ctx, zoneCategoryID, "zone-2", "Zone 2 tag", tagManager)
	if err != nil {
		return fmt.Errorf("failed to create zone-2 tag: %v", err)
	}

	// Get datacenter references to attach tags
	datacenters, err := vcenter.GetDatacenters(ctx)
	if err != nil {
		return fmt.Errorf("failed to get datacenters: %v", err)
	}

	// Attach tags to datacenters
	// DC0 gets region-1 and zone-1
	// DC1 gets region-2 and zone-2
	for i, datacenter := range datacenters {
		dcRef := datacenter.Datacenter.Reference()

		if i == 0 {
			// DC0 - region-1, zone-1
			err = cnsvsphere.AttachTag(ctx, region1TagID, dcRef, tagManager)
			if err != nil {
				return fmt.Errorf("failed to attach region-1 tag to DC0: %v", err)
			}
			err = cnsvsphere.AttachTag(ctx, zone1TagID, dcRef, tagManager)
			if err != nil {
				return fmt.Errorf("failed to attach zone-1 tag to DC0: %v", err)
			}
			log.Infof("Attached region-1 and zone-1 tags to DC0")
		} else if i == 1 {
			// DC1 - region-2, zone-2
			err = cnsvsphere.AttachTag(ctx, region2TagID, dcRef, tagManager)
			if err != nil {
				return fmt.Errorf("failed to attach region-2 tag to DC1: %v", err)
			}
			err = cnsvsphere.AttachTag(ctx, zone2TagID, dcRef, tagManager)
			if err != nil {
				return fmt.Errorf("failed to attach zone-2 tag to DC1: %v", err)
			}
			log.Infof("Attached region-2 and zone-2 tags to DC1")
		}
	}

	log.Infof("Successfully set up real tags in vcsim")
	return nil
}
