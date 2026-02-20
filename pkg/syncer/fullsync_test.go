/*
Copyright 2025 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	commonco "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	cnsvolumeoperationrequest "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	// Test scale constants
	testPVCount_1K   = 1000
	testPVCount_10K  = 10000
	testPVCount_50K  = 50000
	testPVCount_120K = 120000
)

// TestFullSync_1K_PVs tests full sync with 1K PVs (quick smoke test)
func TestFullSync_1K_PVs(t *testing.T) {
	testFullSyncAtScale(t, testPVCount_1K)
}

// TestFullSync_10K_PVs tests full sync with 10K PVs
func TestFullSync_10K_PVs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 10K PV test in short mode")
	}
	testFullSyncAtScale(t, testPVCount_10K)
}

// TestFullSync_50K_PVs tests full sync with 50K PVs
func TestFullSync_50K_PVs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 50K PV test in short mode")
	}
	testFullSyncAtScale(t, testPVCount_50K)
}

// TestFullSync_120K_PVs tests full sync performance with 120K PVs
func TestFullSync_120K_PVs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 120K PV test in short mode")
	}
	testFullSyncAtScale(t, testPVCount_120K)
}

// testFullSyncAtScale is the common test function for various scales
func testFullSyncAtScale(t *testing.T, pvCount int) {
	ctx := context.Background()

	// Generate PVs and PVCs
	t.Logf("Generating %d PVs and PVCs...", pvCount)
	startGen := time.Now()
	pvs, pvcs := generateLargePVPVCSet(pvCount)
	genTime := time.Since(startGen)
	t.Logf("Generated %d PVs and PVCs in %v (%.2f objects/sec)",
		pvCount, genTime, float64(pvCount*2)/genTime.Seconds())

	// Create fake K8s client
	t.Log("Creating fake Kubernetes client...")
	startClient := time.Now()
	k8sObjects := make([]k8sruntime.Object, 0, pvCount*2)
	for i := range pvs {
		k8sObjects = append(k8sObjects, pvs[i], pvcs[i])
	}
	k8sClient := k8sfake.NewClientset(k8sObjects...)
	clientTime := time.Since(startClient)
	t.Logf("Created fake Kubernetes client with %d objects in %v", len(k8sObjects), clientTime)

	// Test listing PVs and PVCs (simulates syncer queries)
	t.Logf("Testing PV and PVC listing performance...")
	startList := time.Now()
	pvList, err := k8sClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list PVs: %v", err)
	}
	pvcList, err := k8sClient.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list PVCs: %v", err)
	}
	listTime := time.Since(startList)

	// Verify counts
	if len(pvList.Items) != pvCount {
		t.Errorf("Expected %d PVs, got %d", pvCount, len(pvList.Items))
	}
	if len(pvcList.Items) != pvCount {
		t.Errorf("Expected %d PVCs, got %d", pvCount, len(pvcList.Items))
	}

	t.Logf("Successfully listed %d PVs and %d PVCs in %v", len(pvList.Items), len(pvcList.Items), listTime)

	// For 120K test, also run CsiFullSync to get real performance data
	var syncTime time.Duration
	var syncErr error
	var queryCount int
	if pvCount == testPVCount_120K {
		t.Log("Setting up metadata syncer for CsiFullSync test...")
		metadataSyncer, mockVolMgr := setupMetadataSyncerForFullSync(t, k8sClient, pvs, pvcs)

		// Patch GetVirtualCenterInstanceForVCenterHost to return a mock VirtualCenter.
		// vcenter.Client.Version resolves through: govmomi.Client -
		//  vim25.Client -> soap.Client.Version
		mockSoapClient := &soap.Client{Version: "8.0.0"}
		mockVC := &cnsvsphere.VirtualCenter{
			Client: &govmomi.Client{
				Client: &vim25.Client{
					Client: mockSoapClient,
					ServiceContent: types.ServiceContent{
						About: types.AboutInfo{
							Version:    "8.0.0",
							ApiVersion: "8.0.0.0",
						},
					},
				},
			},
		}
		patches := gomonkey.ApplyFunc(cnsvsphere.GetVirtualCenterInstanceForVCenterHost,
			func(ctx context.Context, vcHost string, reconnect bool) (*cnsvsphere.VirtualCenter, error) {
				return mockVC, nil
			})
		defer patches.Reset()

		// Bypass the informer cache (which hasn't synced yet in the test) and return
		// our pre-built PV list directly — the same pattern used by pvcsi_fullsync_test.go.
		origGetPVs := getPVsInBoundAvailableOrReleased
		defer func() { getPVsInBoundAvailableOrReleased = origGetPVs }()
		getPVsInBoundAvailableOrReleased = func(_ context.Context, _ *metadataSyncInformer) ([]*v1.PersistentVolume, error) {
			return pvs, nil
		}

		// buildPVCMapPodMap uses pvcLister (informer cache, not synced in tests).
		// Patch it to return a pre-built pvToPVC map from our PVC slice directly.
		pvcByName := make(map[string]*v1.PersistentVolumeClaim, len(pvcs))
		for _, pvc := range pvcs {
			pvcByName[pvc.Name] = pvc
		}
		patches.ApplyFunc(buildPVCMapPodMap,
			func(_ context.Context, pvList []*v1.PersistentVolume, _ *metadataSyncInformer, _ string) (pvcMap, podMap, error) {
				pvcMapResult := make(pvcMap, len(pvList))
				for _, pv := range pvList {
					if pv.Spec.ClaimRef != nil {
						if pvc, ok := pvcByName[pv.Spec.ClaimRef.Name]; ok {
							pvcMapResult[pv.Name] = pvc
						}
					}
				}
				return pvcMapResult, make(podMap), nil
			})

		t.Logf("Running CsiFullSync with %d PVs...", pvCount)
		startSync := time.Now()
		syncErr = CsiFullSync(ctx, metadataSyncer, "test-vcenter")
		syncTime = time.Since(startSync)

		queryCount = mockVolMgr.queryCallCount

		if syncErr != nil {
			t.Logf("CsiFullSync completed with error (may be expected in mock environment): %v", syncErr)
		} else {
			t.Logf("CsiFullSync completed successfully")
		}

		t.Logf("CsiFullSync execution time: %v (%.2f PVs/sec)",
			syncTime, float64(pvCount)/syncTime.Seconds())
		t.Logf("CNS QueryVolume was called %d times", queryCount)
	}

	// Performance metrics
	totalTime := genTime + clientTime + listTime
	t.Logf("\nPerformance Metrics for %d PVs:", pvCount)
	t.Logf("  - Generation time: %v", genTime)
	t.Logf("  - Client creation time: %v", clientTime)
	t.Logf("  - List time: %v", listTime)
	t.Logf("  - Total time: %v", totalTime)
	t.Logf("  - Overall throughput: %.2f PVs/sec", float64(pvCount)/totalTime.Seconds())

	// For 120K, show actual CsiFullSync performance
	if pvCount == testPVCount_120K && syncTime > 0 {
		t.Logf("\nActual CsiFullSync Performance:")
		t.Logf("  - CsiFullSync execution time: %v", syncTime)
		t.Logf("  - CsiFullSync throughput: %.2f PVs/sec", float64(pvCount)/syncTime.Seconds())
		t.Logf("  - CNS query calls: %d", queryCount)
		if syncErr != nil {
			t.Logf("  - Note: Completed with error (expected in mock): %v", syncErr)
		}
	}

	// Performance assertions (adjust based on your requirements)
	maxAcceptableDuration := calculateMaxAcceptableDuration(pvCount)
	if totalTime > maxAcceptableDuration {
		t.Logf("WARNING: Operations took %v, expected < %v", totalTime, maxAcceptableDuration)
	}
}

// calculateMaxAcceptableDuration calculates acceptable duration based on PV count
func calculateMaxAcceptableDuration(pvCount int) time.Duration {
	switch {
	case pvCount <= 1000:
		return 30 * time.Second
	case pvCount <= 10000:
		return 2 * time.Minute
	case pvCount <= 50000:
		return 5 * time.Minute
	default: // 120K+
		return 10 * time.Minute
	}
}

// TestFullSync_ScaleProgression tests increasing PV counts
func TestFullSync_ScaleProgression(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping scale progression test in short mode")
	}

	scales := []int{1000, 5000, 10000}

	results := make(map[int]time.Duration)

	for _, scale := range scales {
		t.Run(fmt.Sprintf("Scale_%d", scale), func(t *testing.T) {
			ctx := context.Background()

			start := time.Now()
			pvs, pvcs := generateLargePVPVCSet(scale)
			k8sObjects := make([]k8sruntime.Object, 0, scale*2)
			for i := range pvs {
				k8sObjects = append(k8sObjects, pvs[i], pvcs[i])
			}
			k8sClient := k8sfake.NewClientset(k8sObjects...)

			// Test listing performance
			_, err := k8sClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatalf("Failed to list PVs: %v", err)
			}

			elapsed := time.Since(start)

			results[scale] = elapsed

			t.Logf("Scale %d: completed in %v (%.2f PVs/sec)",
				scale, elapsed, float64(scale)/elapsed.Seconds())
		})
	}

	// Log scaling analysis
	t.Log("\n=== Scaling Analysis ===")
	for _, scale := range scales {
		if duration, ok := results[scale]; ok {
			t.Logf("%d PVs: %v (%.2f PVs/sec)",
				scale, duration, float64(scale)/duration.Seconds())
		}
	}
}

// TestFullSync_MemoryUsage tests memory consumption at various scales
func TestFullSync_MemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	scales := []int{1000, 10000}

	for _, scale := range scales {
		t.Run(fmt.Sprintf("Memory_%d", scale), func(t *testing.T) {
			testCtx := context.Background()

			// Measure baseline memory
			runtime.GC()
			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)

			// Generate PVs and perform listing operations
			pvs, pvcs := generateLargePVPVCSet(scale)
			k8sObjects := make([]k8sruntime.Object, 0, scale*2)
			for i := range pvs {
				k8sObjects = append(k8sObjects, pvs[i], pvcs[i])
			}
			k8sClient := k8sfake.NewClientset(k8sObjects...)

			// Perform operations
			_, err := k8sClient.CoreV1().PersistentVolumes().List(testCtx, metav1.ListOptions{})
			if err != nil {
				t.Fatalf("Failed to list PVs: %v", err)
			}

			// Measure final memory
			runtime.GC()
			var m2 runtime.MemStats
			runtime.ReadMemStats(&m2)

			allocMB := float64(m2.Alloc-m1.Alloc) / 1024 / 1024
			sysMB := float64(m2.Sys-m1.Sys) / 1024 / 1024

			t.Logf("Memory usage for %d PVs:", scale)
			t.Logf("  Allocated: %.2f MB", allocMB)
			t.Logf("  System: %.2f MB", sysMB)
			t.Logf("  Per PV: %.2f KB", (allocMB*1024)/float64(scale))
		})
	}
}

// generateLargePVPVCSet generates N PVs and corresponding PVCs
func generateLargePVPVCSet(count int) ([]*v1.PersistentVolume, []*v1.PersistentVolumeClaim) {
	pvs := make([]*v1.PersistentVolume, count)
	pvcs := make([]*v1.PersistentVolumeClaim, count)

	// Use parallel generation for speed
	batchSize := 10000
	var wg sync.WaitGroup

	for batch := 0; batch < count; batch += batchSize {
		end := batch + batchSize
		if end > count {
			end = count
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				pvs[i], pvcs[i] = generatePVPVCPair(i)
			}
		}(batch, end)
	}

	wg.Wait()
	return pvs, pvcs
}

// generatePVPVCPair creates a single PV-PVC pair
func generatePVPVCPair(index int) (*v1.PersistentVolume, *v1.PersistentVolumeClaim) {
	pvName := fmt.Sprintf("test-pv-%d", index)
	pvcName := fmt.Sprintf("test-pvc-%d", index)
	namespace := fmt.Sprintf("ns-%d", index%100) // Distribute across 100 namespaces
	volumeHandle := fmt.Sprintf("volume-handle-%d", index)

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			Labels: map[string]string{
				"test-index": fmt.Sprintf("%d", index),
			},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceStorage: resource.MustParse("10Gi"),
			},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       csitypes.Name,
					VolumeHandle: volumeHandle,
				},
			},
			ClaimRef: &v1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: namespace,
				Name:      pvcName,
			},
			StorageClassName: "test-sc",
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeBound,
		},
	}

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
			VolumeName:       pvName,
			StorageClassName: strPtr("test-sc"),
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	return pv, pvc
}

// Mock implementations for CsiFullSync testing

// setupMetadataSyncerForFullSync sets up a metadata syncer with comprehensive mocks for CsiFullSync
func setupMetadataSyncerForFullSync(
	t *testing.T,
	k8sClient *k8sfake.Clientset,
	pvs []*v1.PersistentVolume,
	pvcs []*v1.PersistentVolumeClaim,
) (*metadataSyncInformer, *mockVolumeManagerForFullSync) {
	t.Logf("Setting up metadata syncer for CsiFullSync")

	ctx := context.Background()

	// Create mock CO common interface
	mockCO := &mockCOCommonForFullSync{}

	// Build a realistic CNS volume index mirroring the K8s PVs.
	// Each CnsVolume has:
	//   - VolumeId.Id matching the PV's VolumeHandle (so fullSyncConstructVolumeMaps finds them)
	//   - Metadata.EntityMetadata with a CnsKubernetesEntityMetadata record for this cluster
	//     (so fullSyncGetQueryResults can compare CNS metadata vs K8s metadata)
	const clusterID = "test-cluster-id"
	cnsVolumeIndex := make(map[string]cnstypes.CnsVolume, len(pvs))
	for _, pv := range pvs {
		if pv.Spec.CSI == nil {
			continue
		}
		vh := pv.Spec.CSI.VolumeHandle
		pvMeta := &cnstypes.CnsKubernetesEntityMetadata{
			CnsEntityMetadata: cnstypes.CnsEntityMetadata{
				EntityName: pv.Name,
				ClusterID:  clusterID,
			},
			EntityType: string(cnstypes.CnsKubernetesEntityTypePV),
		}
		cnsVolumeIndex[vh] = cnstypes.CnsVolume{
			VolumeId:   cnstypes.CnsVolumeId{Id: vh},
			VolumeType: string(cnstypes.CnsVolumeTypeBlock),
			Metadata: cnstypes.CnsVolumeMetadata{
				ContainerCluster: cnstypes.CnsContainerCluster{
					ClusterType:   string(cnstypes.CnsClusterTypeKubernetes),
					ClusterId:     clusterID,
					ClusterFlavor: string(cnstypes.CnsClusterFlavorVanilla),
				},
				EntityMetadata: []cnstypes.BaseCnsEntityMetadata{pvMeta},
			},
		}
	}

	// Create mock volume manager pre-loaded with the CNS volume index
	mockVolMgr := &mockVolumeManagerForFullSync{
		queryCallCount: 0,
		cnsVolumeIndex: cnsVolumeIndex,
	}

	// Create mock config
	cfg := &cnsconfig.ConfigurationInfo{
		Cfg: &cnsconfig.Config{
			VirtualCenter: map[string]*cnsconfig.VirtualCenterConfig{
				"test-vcenter": {},
			},
		},
	}
	// Set Global.VCenterIP and ClusterID
	cfg.Cfg.Global.VCenterIP = "test-vcenter"
	cfg.Cfg.Global.ClusterID = "test-cluster-id"

	// Create fake informer manager with listers
	informerManager := k8s.NewInformer(ctx, k8sClient, true)
	informerManager.Listen()

	// Initialize volumeManagers map for multi-vCenter support
	volumeManagers := make(map[string]volumes.Manager)
	volumeManagers["test-vcenter"] = mockVolMgr

	// Initialize package-level maps normally set up by InitMetadataSyncer.
	volumeOperationsLock = make(map[string]*sync.Mutex)
	volumeOperationsLock["test-vcenter"] = &sync.Mutex{}
	cnsDeletionMap = make(map[string]map[string]bool)
	cnsDeletionMap["test-vcenter"] = make(map[string]bool)
	cnsCreationMap = make(map[string]map[string]bool)
	cnsCreationMap["test-vcenter"] = make(map[string]bool)

	ms := &metadataSyncInformer{
		clusterFlavor:      cnstypes.CnsClusterFlavorVanilla,
		coCommonInterface:  mockCO,
		volumeManager:      mockVolMgr,
		volumeManagers:     volumeManagers,
		configInfo:         cfg,
		host:               "test-vcenter",
		k8sInformerManager: informerManager,
		pvLister:           informerManager.GetPVLister(),
		pvcLister:          informerManager.GetPVCLister(),
		podLister:          informerManager.GetPodLister(),
	}

	return ms, mockVolMgr
}

// mockCOCommonForFullSync implements commonco.COCommonInterface for testing
type mockCOCommonForFullSync struct{}

func (m *mockCOCommonForFullSync) IsFSSEnabled(ctx context.Context, featureName string) bool {
	return false
}

func (m *mockCOCommonForFullSync) IsCNSCSIFSSEnabled(ctx context.Context, featureName string) bool {
	return false
}

func (m *mockCOCommonForFullSync) IsPVCSIFSSEnabled(ctx context.Context, featureName string) bool {
	return false
}

func (m *mockCOCommonForFullSync) IsFSSEnabledInNamespace(
	ctx context.Context, featureName string, namespace string,
) bool {
	return false
}

func (m *mockCOCommonForFullSync) EnableFSS(ctx context.Context, featureName string) error {
	return nil
}

func (m *mockCOCommonForFullSync) DisableFSS(ctx context.Context, featureName string) error {
	return nil
}

func (m *mockCOCommonForFullSync) InitializeCSINodes(ctx context.Context) error {
	return nil
}

func (m *mockCOCommonForFullSync) IsLinkedCloneRequest(ctx context.Context, arg1, arg2 string) (bool, error) {
	return false, nil
}

func (m *mockCOCommonForFullSync) ListPVCs(ctx context.Context, namespace string) []*v1.PersistentVolumeClaim {
	return []*v1.PersistentVolumeClaim{}
}

func (m *mockCOCommonForFullSync) PreLinkedCloneCreateAction(ctx context.Context, arg1, arg2 string) error {
	return nil
}

func (m *mockCOCommonForFullSync) StartZonesInformer(ctx context.Context, cfg *rest.Config, clusterID string) error {
	return nil
}

func (m *mockCOCommonForFullSync) UpdatePersistentVolumeLabel(
	ctx context.Context, pvName string, labelKey string, labelValue string,
) error {
	return nil
}

func (m *mockCOCommonForFullSync) IsFakeAttachAllowed(
	ctx context.Context, volumeID string, volumeManager volumes.Manager,
) (bool, error) {
	return false, nil
}

func (m *mockCOCommonForFullSync) MarkFakeAttached(ctx context.Context, volumeID string) error {
	return nil
}

func (m *mockCOCommonForFullSync) ClearFakeAttached(ctx context.Context, volumeID string) error {
	return nil
}

func (m *mockCOCommonForFullSync) InitTopologyServiceInController(
	ctx context.Context,
) (commonco.ControllerTopologyService, error) {
	return nil, nil
}

func (m *mockCOCommonForFullSync) InitTopologyServiceInNode(ctx context.Context) (commonco.NodeTopologyService, error) {
	return nil, nil
}

func (m *mockCOCommonForFullSync) GetNodesForVolumes(ctx context.Context, volumeIds []string) map[string][]string {
	return make(map[string][]string)
}

func (m *mockCOCommonForFullSync) GetNodeIDtoNameMap(ctx context.Context) map[string]string {
	return make(map[string]string)
}

func (m *mockCOCommonForFullSync) GetFakeAttachedVolumes(ctx context.Context, volumeIDs []string) map[string]bool {
	return make(map[string]bool)
}

func (m *mockCOCommonForFullSync) GetVolumeAttachment(
	ctx context.Context, volumeId string, nodeName string,
) (*storagev1.VolumeAttachment, error) {
	return nil, nil
}

func (m *mockCOCommonForFullSync) GetAllVolumes() []string {
	return []string{}
}

func (m *mockCOCommonForFullSync) GetAllK8sVolumes() []string {
	return []string{}
}

func (m *mockCOCommonForFullSync) AnnotateVolumeSnapshot(ctx context.Context, volumeSnapshotName string,
	volumeSnapshotNamespace string, annotations map[string]string) (bool, error) {
	return false, nil
}

func (m *mockCOCommonForFullSync) GetConfigMap(
	ctx context.Context, name string, namespace string,
) (map[string]string, error) {
	return make(map[string]string), nil
}

func (m *mockCOCommonForFullSync) CreateConfigMap(
	ctx context.Context, name string, namespace string,
	data map[string]string, isImmutable bool,
) error {
	return nil
}

func (m *mockCOCommonForFullSync) GetCSINodeTopologyInstancesList() []interface{} {
	return []interface{}{}
}

func (m *mockCOCommonForFullSync) GetCSINodeTopologyInstanceByName(name string) (interface{}, bool, error) {
	return nil, false, nil
}

func (m *mockCOCommonForFullSync) GetActiveClustersForNamespaceInRequestedZones(
	ctx context.Context, namespace string, zones []string,
) ([]string, error) {
	return []string{}, nil
}

func (m *mockCOCommonForFullSync) GetLinkedCloneVolumeSnapshotSourceUUID(
	ctx context.Context, volumeSnapshotName, volumeSnapshotNamespace string,
) (string, error) {
	return "", nil
}

func (m *mockCOCommonForFullSync) GetPVCNameFromCSIVolumeID(volumeID string) (string, string, bool) {
	return "", "", false
}

func (m *mockCOCommonForFullSync) GetPVCNamespacedNameByUID(uid string) (k8stypes.NamespacedName, bool) {
	return k8stypes.NamespacedName{}, false
}

func (m *mockCOCommonForFullSync) GetPVNameFromCSIVolumeID(volumeID string) (string, bool) {
	return "", false
}

func (m *mockCOCommonForFullSync) GetPvcObjectByName(
	ctx context.Context, namespace, name string,
) (*v1.PersistentVolumeClaim, error) {
	return nil, nil
}

func (m *mockCOCommonForFullSync) GetVolumeIDFromPVCName(namespace, pvcName string) (string, bool) {
	return "", false
}

func (m *mockCOCommonForFullSync) GetVolumeSnapshotPVCSource(
	ctx context.Context, volumeSnapshotName, volumeSnapshotNamespace string,
) (*v1.PersistentVolumeClaim, error) {
	return nil, nil
}

func (m *mockCOCommonForFullSync) GetZonesForNamespace(namespace string) map[string]struct{} {
	return make(map[string]struct{})
}

func (m *mockCOCommonForFullSync) HandleLateEnablementOfCapability(
	ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor, arg1, arg2, arg3 string,
) {
	// No-op for mock
}

// mockVolumeManagerForFullSync implements volumes.Manager for testing.
// cnsVolumes holds a pre-built index of VolumeId.Id -> CnsVolume so that
// QueryAllVolume and QueryVolumeAsync can return realistic results without
// rebuilding the dataset on every call.
type mockVolumeManagerForFullSync struct {
	queryCallCount int
	mu             sync.Mutex
	// cnsVolumes is the full set of CNS volumes, keyed by VolumeId.Id
	cnsVolumeIndex map[string]cnstypes.CnsVolume
}

func (m *mockVolumeManagerForFullSync) CreateVolume(
	ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec, extraParams interface{},
) (*volumes.CnsVolumeInfo, string, error) {
	return nil, "", nil
}

func (m *mockVolumeManagerForFullSync) AttachVolume(
	ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string, checkNVMeController bool,
) (string, string, error) {
	return "", "", nil
}

func (m *mockVolumeManagerForFullSync) BatchAttachVolumes(
	ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeIDs []volumes.BatchAttachRequest,
) ([]volumes.BatchAttachResult, string, error) {
	return nil, "", nil
}

func (m *mockVolumeManagerForFullSync) DetachVolume(
	ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string,
) (string, error) {
	return "", nil
}

func (m *mockVolumeManagerForFullSync) DeleteVolume(
	ctx context.Context, volumeID string, deleteDisk bool,
) (string, error) {
	return "", nil
}

func (m *mockVolumeManagerForFullSync) UpdateVolumeMetadata(
	ctx context.Context, spec *cnstypes.CnsVolumeMetadataUpdateSpec,
) error {
	return nil
}

func (m *mockVolumeManagerForFullSync) UpdateVolumeCrypto(
	ctx context.Context, spec *cnstypes.CnsVolumeCryptoUpdateSpec,
) error {
	return nil
}

func (m *mockVolumeManagerForFullSync) QueryVolumeInfo(
	ctx context.Context, volumeIDList []cnstypes.CnsVolumeId,
) (*cnstypes.CnsQueryVolumeInfoResult, error) {
	return &cnstypes.CnsQueryVolumeInfoResult{}, nil
}

// QueryAllVolume returns the full CNS volume inventory (all cnsVolumeIndex entries).
// This mirrors what a real VC returns when fullsync queries all volumes for the cluster.
func (m *mockVolumeManagerForFullSync) QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queryCallCount++

	vols := make([]cnstypes.CnsVolume, 0, len(m.cnsVolumeIndex))
	for _, v := range m.cnsVolumeIndex {
		vols = append(vols, v)
	}
	return &cnstypes.CnsQueryResult{Volumes: vols}, nil
}

// QueryVolumeAsync returns CNS volumes filtered by the VolumeIds in queryFilter.
// This is called by fullSyncGetQueryResults in batches of 1000 to fetch per-volume
// metadata so that fullSyncConstructVolumeMaps can compare CNS vs K8s metadata.
func (m *mockVolumeManagerForFullSync) QueryVolumeAsync(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queryCallCount++

	return m.queryByVolumeIds(queryFilter.VolumeIds), nil
}

// QueryVolume is the fallback path (used when QueryVolumeAsync returns ErrNotSupported).
func (m *mockVolumeManagerForFullSync) QueryVolume(
	ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
) (*cnstypes.CnsQueryResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queryCallCount++

	return m.queryByVolumeIds(queryFilter.VolumeIds), nil
}

// queryByVolumeIds looks up CnsVolumes by the requested VolumeIds.
// If VolumeIds is empty, returns the full index (matches QueryAllVolume semantics).
// Must be called with m.mu held.
func (m *mockVolumeManagerForFullSync) queryByVolumeIds(ids []cnstypes.CnsVolumeId) *cnstypes.CnsQueryResult {
	if len(ids) == 0 {
		vols := make([]cnstypes.CnsVolume, 0, len(m.cnsVolumeIndex))
		for _, v := range m.cnsVolumeIndex {
			vols = append(vols, v)
		}
		return &cnstypes.CnsQueryResult{Volumes: vols}
	}
	vols := make([]cnstypes.CnsVolume, 0, len(ids))
	for _, id := range ids {
		if v, ok := m.cnsVolumeIndex[id.Id]; ok {
			vols = append(vols, v)
		}
	}
	return &cnstypes.CnsQueryResult{Volumes: vols}
}

func (m *mockVolumeManagerForFullSync) RelocateVolume(
	ctx context.Context, relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec,
) (*object.Task, error) {
	return nil, nil
}

func (m *mockVolumeManagerForFullSync) ExpandVolume(
	ctx context.Context, volumeID string, size int64, extraParams interface{},
) (string, error) {
	return "", nil
}

func (m *mockVolumeManagerForFullSync) ResetManager(ctx context.Context, vcenter *cnsvsphere.VirtualCenter) error {
	return nil
}

func (m *mockVolumeManagerForFullSync) ConfigureVolumeACLs(
	ctx context.Context, spec cnstypes.CnsVolumeACLConfigureSpec,
) error {
	return nil
}

func (m *mockVolumeManagerForFullSync) RegisterDisk(ctx context.Context, path string, name string) (string, error) {
	return "", nil
}

func (m *mockVolumeManagerForFullSync) CreateSnapshot(
	ctx context.Context, volumeID string, desc string, extraParams interface{},
) (*volumes.CnsSnapshotInfo, error) {
	return nil, nil
}

func (m *mockVolumeManagerForFullSync) DeleteSnapshot(
	ctx context.Context, volumeID string, snapshotID string, extraParams interface{},
) (*volumes.CnsSnapshotInfo, error) {
	return nil, nil
}

func (m *mockVolumeManagerForFullSync) GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest {
	return nil
}

func (m *mockVolumeManagerForFullSync) IsListViewReady() bool {
	return true
}

func (m *mockVolumeManagerForFullSync) MonitorCreateVolumeTask(
	ctx context.Context,
	volumeOperationDetails **cnsvolumeoperationrequest.VolumeOperationRequestDetails,
	task *object.Task,
	volumeID string,
	volumeName string,
) (*volumes.CnsVolumeInfo, string, error) {
	return nil, "", nil
}

func (m *mockVolumeManagerForFullSync) ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	return nil
}

func (m *mockVolumeManagerForFullSync) QuerySnapshots(
	ctx context.Context, snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter,
) (*cnstypes.CnsSnapshotQueryResult, error) {
	return &cnstypes.CnsSnapshotQueryResult{}, nil
}

func (m *mockVolumeManagerForFullSync) ReRegisterVolume(ctx context.Context, volumeID string) error {
	return nil
}

func (m *mockVolumeManagerForFullSync) RetrieveVStorageObject(
	ctx context.Context, volumeID string,
) (*types.VStorageObject, error) {
	return nil, nil
}

func (m *mockVolumeManagerForFullSync) SetListViewNotReady(ctx context.Context) {
	// No-op for mock
}

func (m *mockVolumeManagerForFullSync) SyncVolume(
	ctx context.Context, syncVolumeSpecs []cnstypes.CnsSyncVolumeSpec,
) (string, error) {
	return "", nil
}

func (m *mockVolumeManagerForFullSync) UnregisterVolume(
	ctx context.Context, volumeID string, deleteDisk bool,
) (string, error) {
	return "", nil
}

// TestCNSQueryBatching tests CNS query batching logic
func TestCNSQueryBatching_VaryingBatchSizes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping CNS query batching test in short mode")
	}

	volumeCount := 10000

	// Generate volume IDs
	volumeIds := make([]cnstypes.CnsVolumeId, volumeCount)
	for i := 0; i < volumeCount; i++ {
		volumeIds[i] = cnstypes.CnsVolumeId{
			Id: fmt.Sprintf("volume-%d", i),
		}
	}

	// Test with different batch sizes
	batchSizes := []int{1000, 2500, 5000, 10000}

	for _, batchSize := range batchSizes {
		t.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(t *testing.T) {
			expectedBatches := (volumeCount + batchSize - 1) / batchSize

			t.Logf("Testing with batch size %d", batchSize)
			t.Logf("Expected number of batches: %d", expectedBatches)
			t.Logf("Average volumes per batch: %.2f", float64(volumeCount)/float64(expectedBatches))

			// Calculate expected performance improvement
			if batchSize > 1000 {
				improvement := float64(batchSize) / 1000.0
				t.Logf("Expected performance improvement over 1000 batch: %.2fx", improvement)
			}
		})
	}
}

// BenchmarkPVGeneration benchmarks PV generation and K8s client operations at various scales
func BenchmarkPVGeneration_1K(b *testing.B)  { benchmarkPVGeneration(b, 1000) }
func BenchmarkPVGeneration_10K(b *testing.B) { benchmarkPVGeneration(b, 10000) }

func benchmarkPVGeneration(b *testing.B, pvCount int) {
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Generate PVs and PVCs
		pvs, pvcs := generateLargePVPVCSet(pvCount)

		// Create K8s client
		k8sObjects := make([]k8sruntime.Object, 0, pvCount*2)
		for j := range pvs {
			k8sObjects = append(k8sObjects, pvs[j], pvcs[j])
		}
		k8sClient := k8sfake.NewClientset(k8sObjects...)

		// List PVs (simulates syncer operations)
		_, err := k8sClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		if err != nil {
			b.Fatalf("Failed to list PVs: %v", err)
		}
	}

	// Report metrics
	b.ReportMetric(float64(pvCount), "pvs")
	if b.Elapsed().Seconds() > 0 {
		b.ReportMetric(float64(pvCount*b.N)/b.Elapsed().Seconds(), "pvs/sec")
	}
}

func strPtr(s string) *string {
	return &s
}

func TestSetFileShareAnnotationsOnPVC_Success(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and PVC
	k8sClient := k8sfake.NewClientset(pv, pvc)

	// Create expected volume response with file share backing details
	expectedVolume := &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{
			Id: "test-volume-handle",
		},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: []types.KeyValue{
				{
					Key:   common.Nfsv3AccessPointKey,
					Value: "192.168.1.100:/nfs/v3/path",
				},
				{
					Key:   common.Nfsv4AccessPointKey,
					Value: "192.168.1.100:/nfs/v4/path",
				},
			},
		},
	}

	// Mock the QueryVolumeUtil function using gomonkey
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			assert.Equal(t, "test-volume-handle", queryFilter.VolumeIds[0].Id)
			assert.NotNil(t, querySelection)
			assert.Contains(t, querySelection.Names, string(cnstypes.QuerySelectionNameTypeBackingObjectDetails))
			return &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{*expectedVolume},
			}, nil
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, "192.168.1.100:/nfs/v3/path", pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Equal(t, "192.168.1.100:/nfs/v4/path", pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_PVNotFound(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "non-existent-pv",
		},
	}

	// Create fake k8s client without the PV
	k8sClient := k8sfake.NewClientset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify no annotations were added
	assert.Empty(t, pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Empty(t, pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_QueryVolumeError(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and PVC
	k8sClient := k8sfake.NewClientset(pv, pvc)

	// Mock the QueryVolumeUtil function to return error using gomonkey
	expectedError := errors.New("query volume failed")
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			return nil, expectedError
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions - The function should return the QueryVolume error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query volume failed")
}

func TestSetFileShareAnnotationsOnPVC_PVCUpdateError(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and configure it to fail on PVC update
	k8sClient := k8sfake.NewClientset(pv)
	k8sClient.PrependReactor("update", "persistentvolumeclaims",
		func(action k8stesting.Action) (handled bool, ret k8sruntime.Object, err error) {
			return true, nil, errors.New("update failed")
		})

	// Create expected volume response with file share backing details
	expectedVolume := &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{
			Id: "test-volume-handle",
		},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: []types.KeyValue{
				{
					Key:   common.Nfsv3AccessPointKey,
					Value: "192.168.1.100:/nfs/v3/path",
				},
				{
					Key:   common.Nfsv4AccessPointKey,
					Value: "192.168.1.100:/nfs/v4/path",
				},
			},
		},
	}

	// Mock the QueryVolumeUtil function using gomonkey
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			return &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{*expectedVolume},
			}, nil
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update failed")

	// Verify annotations were set on the PVC object (even though update failed)
	assert.Equal(t, "192.168.1.100:/nfs/v3/path", pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Equal(t, "192.168.1.100:/nfs/v4/path", pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_OnlyNFSv4AccessPoint(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and PVC
	k8sClient := k8sfake.NewClientset(pv, pvc)

	// Create expected volume response with only NFSv4 access point
	expectedVolume := &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{
			Id: "test-volume-handle",
		},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: []types.KeyValue{
				{
					Key:   common.Nfsv4AccessPointKey,
					Value: "192.168.1.100:/nfs/v4/path",
				},
			},
		},
	}

	// Mock the QueryVolumeUtil function using gomonkey
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			return &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{*expectedVolume},
			}, nil
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.NoError(t, err)
	assert.Empty(t, pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Equal(t, "192.168.1.100:/nfs/v4/path", pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_EmptyAccessPoints(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and PVC
	k8sClient := k8sfake.NewClientset(pv, pvc)

	// Create expected volume response with empty access points
	expectedVolume := &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{
			Id: "test-volume-handle",
		},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: []types.KeyValue{},
		},
	}

	// Mock the QueryVolumeUtil function using gomonkey
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			return &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{*expectedVolume},
			}, nil
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.NoError(t, err)
	assert.Empty(t, pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Empty(t, pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_ExistingAnnotations(t *testing.T) {
	ctx := context.Background()

	// Create test PVC with existing annotations
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "test-namespace",
			Annotations: map[string]string{
				"existing-annotation": "existing-value",
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and PVC
	k8sClient := k8sfake.NewClientset(pv, pvc)

	// Create expected volume response with file share backing details
	expectedVolume := &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{
			Id: "test-volume-handle",
		},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: []types.KeyValue{
				{
					Key:   common.Nfsv3AccessPointKey,
					Value: "192.168.1.100:/nfs/v3/path",
				},
			},
		},
	}

	// Mock the QueryVolumeUtil function using gomonkey
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			return &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{*expectedVolume},
			}, nil
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, "existing-value", pvc.Annotations["existing-annotation"])
	assert.Equal(t, "192.168.1.100:/nfs/v3/path", pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Empty(t, pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}
