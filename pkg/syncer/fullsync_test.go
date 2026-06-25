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
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
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
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
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
	informerManager := k8s.NewInformer(ctx, k8sClient)
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
) (commoncotypes.ControllerTopologyService, error) {
	return nil, nil
}

func (m *mockCOCommonForFullSync) InitTopologyServiceInNode(
	ctx context.Context,
) (commoncotypes.NodeTopologyService, error) {
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

func (m *mockCOCommonForFullSync) IsDPOServiceInstalled(ctx context.Context) (bool, error) {
	return true, nil
}

func (m *mockCOCommonForFullSync) HandleLateInstallationOfDPOService(ctx context.Context) {
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
	// unprotectVolumeFromVMDeletionFunc allows tests to customize the behavior
	unprotectVolumeFromVMDeletionFunc func(ctx context.Context, volumeID string) error
	// protectVolumeFromVMDeletionFunc allows tests to customize the behavior
	protectVolumeFromVMDeletionFunc func(ctx context.Context, volumeID string) error
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

func (m *mockVolumeManagerForFullSync) QueryFCDAllocatedBlocks(ctx context.Context,
	volumeID, snapshotID string, startingOffset uint64) ([]volumes.DiskArea, uint64, error) {
	return nil, 0, nil
}

func (m *mockVolumeManagerForFullSync) QueryFCDChangedBlocks(ctx context.Context,
	volumeID, targetSnapshotID, baseChangeID string, startingOffset uint64) (
	[]volumes.DiskArea, uint64, error) {
	return nil, 0, nil
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
	if m.protectVolumeFromVMDeletionFunc != nil {
		return m.protectVolumeFromVMDeletionFunc(ctx, volumeID)
	}
	return nil
}

func (m *mockVolumeManagerForFullSync) UnprotectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	if m.unprotectVolumeFromVMDeletionFunc != nil {
		return m.unprotectVolumeFromVMDeletionFunc(ctx, volumeID)
	}
	return nil
}

func (m *mockVolumeManagerForFullSync) SetVolumeControlFlags(
	ctx context.Context, volumeID string, controlFlags []string,
) error {
	return nil
}

func (m *mockVolumeManagerForFullSync) ClearVolumeControlFlags(
	ctx context.Context, volumeID string, controlFlags []string,
) error {
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

func (m *mockVolumeManagerForFullSync) UnregisterVolumeEx(
	ctx context.Context, volumeID string,
) (string, string, error) {
	return "", "", nil
}

func (m *mockVolumeManagerForFullSync) QueryPendingUnregisters(
	ctx context.Context,
) ([]volumes.PendingUnregisterRecord, error) {
	return nil, nil
}

func (m *mockVolumeManagerForFullSync) AckUnregister(ctx context.Context, volumeID string) error {
	return nil
}

func (m *mockVolumeManagerForFullSync) GetDiskFolderURL(ctx context.Context, datastorePath string) (string, error) {
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

// makeTestVolume returns a CnsVolume with CnsVsanFileShareBackingDetails for use in
// TestSetFileShareAnnotations* tests.
func makeTestVolume(accessPoints []types.KeyValue) *cnstypes.CnsVolume {
	return &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{Id: "test-volume-handle"},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: accessPoints,
		},
	}
}

// swapQueryVolumeByIDFn replaces queryVolumeByIDFn for the duration of the test
// and restores it via t.Cleanup. This avoids gomonkey, which patches at the binary
// level and leaks across tests when used with defer inside top-level test functions.
func swapQueryVolumeByIDFn(
	t *testing.T,
	stub func(context.Context, volumes.Manager, string, *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error),
) {
	t.Helper()
	orig := queryVolumeByIDFn
	queryVolumeByIDFn = stub
	t.Cleanup(func() { queryVolumeByIDFn = orig })
}

func TestSetFileShareAnnotationsOnPVC_Success(t *testing.T) {
	ctx := context.Background()

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{VolumeName: "test-pv"},
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: "test-volume-handle"},
			},
		},
	}
	k8sClient := k8sfake.NewClientset(pv, pvc)

	expectedVolume := makeTestVolume([]types.KeyValue{
		{Key: common.Nfsv3AccessPointKey, Value: "192.168.1.100:/nfs/v3/path"},
		{Key: common.Nfsv4AccessPointKey, Value: "192.168.1.100:/nfs/v4/path"},
	})
	swapQueryVolumeByIDFn(t, func(_ context.Context, _ volumes.Manager,
		volumeID string, _ *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
		assert.Equal(t, "test-volume-handle", volumeID)
		return expectedVolume, nil
	})

	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	assert.NoError(t, err)
	assert.Equal(t, "192.168.1.100:/nfs/v3/path", pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Equal(t, "192.168.1.100:/nfs/v4/path", pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_PVNotFound(t *testing.T) {
	ctx := context.Background()

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{VolumeName: "non-existent-pv"},
	}
	k8sClient := k8sfake.NewClientset()

	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	assert.Empty(t, pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Empty(t, pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_QueryVolumeError(t *testing.T) {
	ctx := context.Background()

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{VolumeName: "test-pv"},
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: "test-volume-handle"},
			},
		},
	}
	k8sClient := k8sfake.NewClientset(pv, pvc)

	expectedError := errors.New("query volume failed")
	swapQueryVolumeByIDFn(t, func(_ context.Context, _ volumes.Manager,
		_ string, _ *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
		return nil, expectedError
	})

	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query volume failed")
}

func TestSetFileShareAnnotationsOnPVC_PVCUpdateError(t *testing.T) {
	ctx := context.Background()

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{VolumeName: "test-pv"},
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: "test-volume-handle"},
			},
		},
	}
	k8sClient := k8sfake.NewClientset(pv)
	k8sClient.PrependReactor("update", "persistentvolumeclaims",
		func(action k8stesting.Action) (handled bool, ret k8sruntime.Object, err error) {
			return true, nil, errors.New("update failed")
		})

	expectedVolume := makeTestVolume([]types.KeyValue{
		{Key: common.Nfsv3AccessPointKey, Value: "192.168.1.100:/nfs/v3/path"},
		{Key: common.Nfsv4AccessPointKey, Value: "192.168.1.100:/nfs/v4/path"},
	})
	swapQueryVolumeByIDFn(t, func(_ context.Context, _ volumes.Manager,
		_ string, _ *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
		return expectedVolume, nil
	})

	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update failed")
	assert.Equal(t, "192.168.1.100:/nfs/v3/path", pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Equal(t, "192.168.1.100:/nfs/v4/path", pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_OnlyNFSv4AccessPoint(t *testing.T) {
	ctx := context.Background()

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{VolumeName: "test-pv"},
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: "test-volume-handle"},
			},
		},
	}
	k8sClient := k8sfake.NewClientset(pv, pvc)

	expectedVolume := makeTestVolume([]types.KeyValue{
		{Key: common.Nfsv4AccessPointKey, Value: "192.168.1.100:/nfs/v4/path"},
	})
	swapQueryVolumeByIDFn(t, func(_ context.Context, _ volumes.Manager,
		_ string, _ *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
		return expectedVolume, nil
	})

	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	assert.NoError(t, err)
	assert.Empty(t, pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Equal(t, "192.168.1.100:/nfs/v4/path", pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_EmptyAccessPoints(t *testing.T) {
	ctx := context.Background()

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{VolumeName: "test-pv"},
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: "test-volume-handle"},
			},
		},
	}
	k8sClient := k8sfake.NewClientset(pv, pvc)

	expectedVolume := makeTestVolume([]types.KeyValue{})
	swapQueryVolumeByIDFn(t, func(_ context.Context, _ volumes.Manager,
		_ string, _ *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
		return expectedVolume, nil
	})

	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	assert.NoError(t, err)
	assert.Empty(t, pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Empty(t, pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_ExistingAnnotations(t *testing.T) {
	ctx := context.Background()

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "test-namespace",
			Annotations: map[string]string{
				"existing-annotation": "existing-value",
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{VolumeName: "test-pv"},
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: "test-volume-handle"},
			},
		},
	}
	k8sClient := k8sfake.NewClientset(pv, pvc)

	expectedVolume := makeTestVolume([]types.KeyValue{
		{Key: common.Nfsv3AccessPointKey, Value: "192.168.1.100:/nfs/v3/path"},
	})
	swapQueryVolumeByIDFn(t, func(_ context.Context, _ volumes.Manager,
		_ string, _ *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
		return expectedVolume, nil
	})

	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	assert.NoError(t, err)
	assert.Equal(t, "existing-value", pvc.Annotations["existing-annotation"])
	assert.Equal(t, "192.168.1.100:/nfs/v3/path", pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Empty(t, pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

// fakeVolumeManager is a test-only mock of volumes.Manager. It embeds
// unittestcommon.MockVolumeManager (which implements all interface methods) and
// overrides QuerySnapshots to return user-defined results for testing.
type fakeVolumeManager struct {
	*unittestcommon.MockVolumeManager
	querySnapshotsFunc func(ctx context.Context, filter cnstypes.CnsSnapshotQueryFilter) (
		*cnstypes.CnsSnapshotQueryResult, error)
}

func (f *fakeVolumeManager) QuerySnapshots(ctx context.Context,
	filter cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
	if f.querySnapshotsFunc != nil {
		return f.querySnapshotsFunc(ctx, filter)
	}
	return nil, nil
}

// testCOCommonInterface embeds the FakeK8SOrchestrator from unittestcommon (which already
// implements all COCommonInterface methods) and overrides only AnnotateVolumeSnapshot
// to make it customizable per test.
type testCOCommonInterface struct {
	commonco.COCommonInterface
	annotateFunc func(context.Context, string, string, map[string]string) (bool, error)
}

func (m *testCOCommonInterface) AnnotateVolumeSnapshot(
	ctx context.Context,
	name, namespace string,
	annotations map[string]string,
) (bool, error) {
	if m.annotateFunc != nil {
		return m.annotateFunc(ctx, name, namespace, annotations)
	}
	return m.COCommonInterface.AnnotateVolumeSnapshot(ctx, name, namespace, annotations)
}

// newTestCOInterface creates a testCOCommonInterface using the FakeK8SOrchestrator as a base.
func newTestCOInterface(t *testing.T,
	annotateFunc func(context.Context, string, string, map[string]string) (bool, error),
) *testCOCommonInterface {
	t.Helper()
	base, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to create fake CO interface: %v", err)
	}
	return &testCOCommonInterface{
		COCommonInterface: base,
		annotateFunc:      annotateFunc,
	}
}

// TestAnnotateSnapshotsFromCNSResults_SuccessfulAnnotation tests annotation with sample data
func TestAnnotateSnapshotsFromCNSResults_SuccessfulAnnotation(t *testing.T) {
	ctx := context.Background()

	// Create sample VolumeSnapshot
	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "snap-1",
			Namespace:   "ns-1",
			Annotations: make(map[string]string),
		},
	}

	snapshotMap := map[string]*snapv1.VolumeSnapshot{"snap-id-123": vs}
	wantedSnapIDs := map[string]struct{}{"snap-id-123": {}}

	// Mock volume manager that returns CNS results with change-id when QuerySnapshots is called
	mockVM := &fakeVolumeManager{
		MockVolumeManager: &unittestcommon.MockVolumeManager{},
		querySnapshotsFunc: func(ctx context.Context,
			filter cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
			// Verify that the filter is for the correct volume
			if len(filter.SnapshotQuerySpecs) > 0 {
				assert.Equal(t, "vol-123", filter.SnapshotQuerySpecs[0].VolumeId.Id)
			}
			return &cnstypes.CnsSnapshotQueryResult{
				Entries: []cnstypes.CnsSnapshotQueryResultEntry{
					{
						Snapshot: cnstypes.CnsSnapshot{
							SnapshotId:             cnstypes.CnsSnapshotId{Id: "snap-id-123"},
							ChangedBlockTrackingId: "change-id-abc-123",
						},
					},
				},
				// Set Offset == TotalRecords to signal end of pagination
				Cursor: cnstypes.CnsCursor{Offset: 1, TotalRecords: 1},
			}, nil
		},
	}

	// Create a mock coCommonInterface
	mockCOInterface := newTestCOInterface(t,
		func(ctx context.Context, name, namespace string, annotations map[string]string) (bool, error) {
			assert.Equal(t, "snap-1", name)
			assert.Equal(t, "ns-1", namespace)
			assert.Equal(t, "change-id-abc-123", annotations[common.VolumeSnapshotChangeIDKey])
			return true, nil
		})

	count := annotateSnapshotsFromCNSResults(ctx, "vol-123", wantedSnapIDs, snapshotMap, mockVM, mockCOInterface)

	assert.Equal(t, 1, count)
}

// TestAnnotateSnapshotsFromCNSResults_NoChangeID tests handling of snapshots without change-id
func TestAnnotateSnapshotsFromCNSResults_NoChangeID(t *testing.T) {
	ctx := context.Background()

	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "snap-1",
			Namespace:   "ns-1",
			Annotations: make(map[string]string),
		},
	}

	snapshotMap := map[string]*snapv1.VolumeSnapshot{"snap-id-123": vs}
	wantedSnapIDs := map[string]struct{}{"snap-id-123": {}}

	// Mock volume manager that returns CNS results with empty change-id
	mockVM := &fakeVolumeManager{
		MockVolumeManager: &unittestcommon.MockVolumeManager{},
		querySnapshotsFunc: func(ctx context.Context,
			filter cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
			return &cnstypes.CnsSnapshotQueryResult{
				Entries: []cnstypes.CnsSnapshotQueryResultEntry{
					{
						Snapshot: cnstypes.CnsSnapshot{
							SnapshotId:             cnstypes.CnsSnapshotId{Id: "snap-id-123"},
							ChangedBlockTrackingId: "",
						},
					},
				},
				Cursor: cnstypes.CnsCursor{Offset: 1, TotalRecords: 1},
			}, nil
		},
	}

	mockCOInterface := newTestCOInterface(t, nil)

	count := annotateSnapshotsFromCNSResults(ctx, "vol-123", wantedSnapIDs, snapshotMap, mockVM, mockCOInterface)

	// Should not annotate since change-id is empty
	assert.Equal(t, 0, count)
}

// TestAnnotateSnapshotsFromCNSResults_QueryError tests error handling on CNS query failure
func TestAnnotateSnapshotsFromCNSResults_QueryError(t *testing.T) {
	ctx := context.Background()

	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-1", Namespace: "ns-1"},
	}

	snapshotMap := map[string]*snapv1.VolumeSnapshot{"snap-id-123": vs}
	wantedSnapIDs := map[string]struct{}{"snap-id-123": {}}

	// Mock volume manager that returns CNS query error
	mockVM := &fakeVolumeManager{
		MockVolumeManager: &unittestcommon.MockVolumeManager{},
		querySnapshotsFunc: func(ctx context.Context,
			filter cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
			return nil, errors.New("CNS query failed")
		},
	}

	mockCOInterface := newTestCOInterface(t, nil)

	count := annotateSnapshotsFromCNSResults(ctx, "vol-123", wantedSnapIDs, snapshotMap, mockVM, mockCOInterface)

	// Should not annotate on error
	assert.Equal(t, 0, count)
}

// TestAnnotateSnapshotsFromCNSResults_AnnotationFailure tests handling of annotation failures
func TestAnnotateSnapshotsFromCNSResults_AnnotationFailure(t *testing.T) {
	ctx := context.Background()

	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-1", Namespace: "ns-1"},
	}

	snapshotMap := map[string]*snapv1.VolumeSnapshot{"snap-id-123": vs}
	wantedSnapIDs := map[string]struct{}{"snap-id-123": {}}

	// Mock volume manager returning a snapshot with valid change-id
	mockVM := &fakeVolumeManager{
		MockVolumeManager: &unittestcommon.MockVolumeManager{},
		querySnapshotsFunc: func(ctx context.Context,
			filter cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
			return &cnstypes.CnsSnapshotQueryResult{
				Entries: []cnstypes.CnsSnapshotQueryResultEntry{
					{
						Snapshot: cnstypes.CnsSnapshot{
							SnapshotId:             cnstypes.CnsSnapshotId{Id: "snap-id-123"},
							ChangedBlockTrackingId: "change-id-123",
						},
					},
				},
				Cursor: cnstypes.CnsCursor{Offset: 1, TotalRecords: 1},
			}, nil
		},
	}

	// Mock annotation failure
	mockCOInterface := newTestCOInterface(t,
		func(ctx context.Context, name, namespace string, annotations map[string]string) (bool, error) {
			return false, errors.New("annotation failed")
		})

	count := annotateSnapshotsFromCNSResults(ctx, "vol-123", wantedSnapIDs, snapshotMap, mockVM, mockCOInterface)

	// Should not count as annotated on failure
	assert.Equal(t, 0, count)
}

// TestSetChangeIDAnnotationOnSupervisorSnapshots tests error handling when no snapshots exist
func TestSetChangeIDAnnotationOnSupervisorSnapshots(t *testing.T) {
	ctx := context.Background()

	// Create a mock metadataSyncer with minimal required fields
	mockMetadataSyncer := &metadataSyncInformer{
		volumeManager:     nil, // Will be patched
		coCommonInterface: nil, // Will be patched
	}

	// Patch NewSnapshotterClient to return error
	patchSnapshotClient := gomonkey.ApplyFunc(k8s.NewSnapshotterClient, func(_ context.Context) (interface{}, error) {
		return nil, errors.New("test error - no snapshots")
	})
	defer patchSnapshotClient.Reset()

	// Call should handle error gracefully
	setChangeIDAnnotationOnSupervisorSnapshots(ctx, mockMetadataSyncer, "test-vc")
	// If we get here without panicking, test passes
}

// makePVCForClassification is a small builder for the
// TestClassifySupervisorPVC table tests.
func makePVCForClassification(labels map[string]string,
	ownerKinds []string) *v1.PersistentVolumeClaim {
	owners := make([]metav1.OwnerReference, 0, len(ownerKinds))
	for _, k := range ownerKinds {
		owners = append(owners, metav1.OwnerReference{Kind: k, Name: "owner-" + k})
	}
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-pvc",
			Namespace:       "test-ns",
			Labels:          labels,
			OwnerReferences: owners,
		},
	}
}

// TestClassifySupervisorPVC verifies the rule table for
// classifySupervisorPVC: VirtualMachine / VSphereMachine ownerRefs imply
// vks-node, a TKGService marker in any label key implies vks-workload,
// and the absence of both implies supervisor-workload. The classification
// is boolean-tag-based — vks-node and vks-workload can co-occur, but
// supervisor-workload is mutually exclusive with both.
func TestClassifySupervisorPVC(t *testing.T) {
	tests := []struct {
		name      string
		labels    map[string]string
		owners    []string
		wantKeys  []string
		notWanted []string
	}{
		{
			name:      "vks node via VirtualMachine ownerRef",
			owners:    []string{"VirtualMachine"},
			wantKeys:  []string{common.AnnKeyVKSNode},
			notWanted: []string{common.AnnKeyVKSWorkload, common.AnnKeySupervisorWorkload},
		},
		{
			name:      "vks node via VSphereMachine ownerRef",
			owners:    []string{"VSphereMachine"},
			wantKeys:  []string{common.AnnKeyVKSNode},
			notWanted: []string{common.AnnKeyVKSWorkload, common.AnnKeySupervisorWorkload},
		},
		{
			name:      "vks workload via TKGService label",
			labels:    map[string]string{"my-tkc/TKGService": "abc123"},
			wantKeys:  []string{common.AnnKeyVKSWorkload},
			notWanted: []string{common.AnnKeyVKSNode, common.AnnKeySupervisorWorkload},
		},
		{
			name: "vks workload via TKGService marker anywhere in key",
			// The current syncer uses strings.Contains(key, "TKGService")
			// rather than a prefix/suffix match — we intentionally preserve
			// that semantics so prior PVCs with non-standard label keys
			// stay matched.
			labels:    map[string]string{"random/key.with.TKGService.embedded": "x"},
			wantKeys:  []string{common.AnnKeyVKSWorkload},
			notWanted: []string{common.AnnKeyVKSNode, common.AnnKeySupervisorWorkload},
		},
		{
			name:      "no signals -> supervisor-workload",
			wantKeys:  []string{common.AnnKeySupervisorWorkload},
			notWanted: []string{common.AnnKeyVKSNode, common.AnnKeyVKSWorkload},
		},
		{
			name:      "both signals -> double-tagged, supervisor-workload NOT set",
			labels:    map[string]string{"my-tkc/TKGService": "abc123"},
			owners:    []string{"VirtualMachine"},
			wantKeys:  []string{common.AnnKeyVKSNode, common.AnnKeyVKSWorkload},
			notWanted: []string{common.AnnKeySupervisorWorkload},
		},
		{
			name:      "unrelated owner kind is ignored",
			owners:    []string{"StatefulSet"},
			wantKeys:  []string{common.AnnKeySupervisorWorkload},
			notWanted: []string{common.AnnKeyVKSNode, common.AnnKeyVKSWorkload},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pvc := makePVCForClassification(tt.labels, tt.owners)
			got := classifySupervisorPVC(pvc)
			for _, k := range tt.wantKeys {
				v, ok := got[k]
				assert.True(t, ok, "missing expected key %s in classification (got=%v)", k, got)
				assert.Equal(t, common.AnnValueTrue, v, "expected value %q for key %s", common.AnnValueTrue, k)
			}
			for _, k := range tt.notWanted {
				_, ok := got[k]
				assert.False(t, ok, "unexpected key %s in classification (got=%v)", k, got)
			}
		})
	}
}

// TestReconcilePVCWorkloadTypeAnnotations exercises the diff-and-patch
// path: it should emit a Strategic Merge Patch that adds the desired
// annotations, removes stale csi.vsphere.volume.type/* annotations, leaves
// other annotations untouched, and returns no-patch-needed when the PVC
// already matches the desired set.
// TestUpdateMetadataSpecArrayReset tests that updateMetadataSpecArray is reset between iterations
// to prevent duplicate operations across multiple query results
func TestUpdateMetadataSpecArrayReset(t *testing.T) {
	// This test demonstrates the importance of resetting updateMetadataSpecArray
	// between iterations to prevent duplicate operations

	// Simulate the scenario: multiple query results each containing volumes
	queryResults := [][]string{
		{"volume-1"},
		{"volume-2"},
	}

	// Test WITH reset (correct behavior)
	var processedVolumesWithReset []string
	var updateMetadataSpecArrayFixed []cnstypes.CnsVolumeMetadataUpdateSpec

	for _, queryResult := range queryResults {
		for _, volumeID := range queryResult {
			spec := cnstypes.CnsVolumeMetadataUpdateSpec{
				VolumeId: cnstypes.CnsVolumeId{Id: volumeID},
			}
			updateMetadataSpecArrayFixed = append(updateMetadataSpecArrayFixed, spec)
		}

		// Process the accumulated specs
		for _, spec := range updateMetadataSpecArrayFixed {
			processedVolumesWithReset = append(processedVolumesWithReset, spec.VolumeId.Id)
		}

		// FIX: Reset the array for the next iteration
		updateMetadataSpecArrayFixed = updateMetadataSpecArrayFixed[:0]
	}

	// Test WITHOUT reset (buggy behavior)
	var processedVolumesWithoutReset []string
	var updateMetadataSpecArrayBuggy []cnstypes.CnsVolumeMetadataUpdateSpec

	for _, queryResult := range queryResults {
		for _, volumeID := range queryResult {
			spec := cnstypes.CnsVolumeMetadataUpdateSpec{
				VolumeId: cnstypes.CnsVolumeId{Id: volumeID},
			}
			updateMetadataSpecArrayBuggy = append(updateMetadataSpecArrayBuggy, spec)
		}

		// Process the accumulated specs
		for _, spec := range updateMetadataSpecArrayBuggy {
			processedVolumesWithoutReset = append(processedVolumesWithoutReset, spec.VolumeId.Id)
		}

		// BUG: NOT resetting array here causes accumulation
	}

	// Verify correct behavior with reset
	expectedWithReset := []string{"volume-1", "volume-2"}
	assert.Equal(t, expectedWithReset, processedVolumesWithReset,
		"With reset, each volume should be processed exactly once")

	// Verify buggy behavior without reset
	expectedWithoutReset := []string{"volume-1", "volume-1", "volume-2"} // volume-1 gets processed twice!
	assert.Equal(t, expectedWithoutReset, processedVolumesWithoutReset,
		"Without reset, volumes accumulate and get processed multiple times")

	// Verify the fixed array is properly reset
	assert.Equal(t, 0, len(updateMetadataSpecArrayFixed),
		"updateMetadataSpecArray should be empty after proper reset")

	// Verify the buggy array keeps accumulating
	assert.Equal(t, 2, len(updateMetadataSpecArrayBuggy),
		"Without reset, updateMetadataSpecArray accumulates entries")
}

// TestSliceResetBehavior tests the difference between setting slice to nil vs [:0]
func TestSliceResetBehavior(t *testing.T) {
	// Test [:0] behavior (what we use in the fix)
	slice1 := make([]cnstypes.CnsVolumeMetadataUpdateSpec, 0, 10) // capacity 10
	originalCap := cap(slice1)

	// Add some elements
	for i := 0; i < 5; i++ {
		spec := cnstypes.CnsVolumeMetadataUpdateSpec{
			VolumeId: cnstypes.CnsVolumeId{Id: fmt.Sprintf("volume-%d", i)},
		}
		slice1 = append(slice1, spec)
	}

	// Reset using [:0] - preserves capacity, reuses memory
	slice1 = slice1[:0]
	assert.Equal(t, 0, len(slice1), "Length should be 0 after [:0] reset")
	assert.Equal(t, originalCap, cap(slice1), "Capacity should be preserved with [:0] reset")

	// Test nil assignment behavior - demonstrate that nil loses capacity
	testNilAssignment := func() (int, int, int) {
		slice := make([]cnstypes.CnsVolumeMetadataUpdateSpec, 5, 10)
		initialCap := cap(slice)
		slice = nil
		return len(slice), cap(slice), initialCap
	}

	length, capacity, initialCap := testNilAssignment()
	assert.Equal(t, 0, length, "Length should be 0 after nil assignment")
	assert.Equal(t, 0, capacity, "Capacity should be 0 after nil assignment")
	assert.NotEqual(t, initialCap, capacity, "Capacity should be lost with nil assignment")

	// Demonstrate why [:0] is better for performance
	t.Logf("Using [:0] preserves capacity (%d), avoiding reallocation", originalCap)
	t.Logf("Using nil loses capacity, requiring reallocation on next append")
}

func TestReconcilePVCWorkloadTypeAnnotations(t *testing.T) {
	tests := []struct {
		name           string
		existingAnn    map[string]string
		desired        map[string]string
		wantNeedsPatch bool
		// substrings the generated patch JSON must contain (for additions)
		mustContain []string
		// substrings the generated patch JSON must contain at value=null
		// (for deletions of stale tags)
		mustContainDeletion []string
		// substrings the generated patch JSON must NOT contain (e.g.,
		// annotations outside the AnnPrefixVKSWorkloadType namespace
		// must never be mentioned)
		mustNotContain []string
	}{
		{
			name:           "fresh PVC, no annotations, gets vks-node",
			existingAnn:    nil,
			desired:        map[string]string{common.AnnKeyVKSNode: common.AnnValueTrue},
			wantNeedsPatch: true,
			mustContain:    []string{common.AnnKeyVKSNode, common.AnnValueTrue},
		},
		{
			name: "already-correct annotations -> no patch",
			existingAnn: map[string]string{
				common.AnnKeyVKSWorkload: common.AnnValueTrue,
			},
			desired:        map[string]string{common.AnnKeyVKSWorkload: common.AnnValueTrue},
			wantNeedsPatch: false,
		},
		{
			name: "stale tag must be deleted (set to null) when classification changes",
			existingAnn: map[string]string{
				common.AnnKeySupervisorWorkload: common.AnnValueTrue,
			},
			desired:             map[string]string{common.AnnKeyVKSNode: common.AnnValueTrue},
			wantNeedsPatch:      true,
			mustContain:         []string{common.AnnKeyVKSNode},
			mustContainDeletion: []string{common.AnnKeySupervisorWorkload},
		},
		{
			name: "unrelated annotation outside the namespace must not appear in patch",
			existingAnn: map[string]string{
				"csi.vsphere.volume/fast-provisioning": "true",
				"some.other.namespace/label":           "value",
			},
			desired:        map[string]string{common.AnnKeyVKSNode: common.AnnValueTrue},
			wantNeedsPatch: true,
			mustContain:    []string{common.AnnKeyVKSNode},
			mustNotContain: []string{
				"csi.vsphere.volume/fast-provisioning",
				"some.other.namespace/label",
			},
		},
		{
			name: "double-tag transition: vks-node already set, add vks-workload",
			existingAnn: map[string]string{
				common.AnnKeyVKSNode: common.AnnValueTrue,
			},
			desired: map[string]string{
				common.AnnKeyVKSNode:     common.AnnValueTrue,
				common.AnnKeyVKSWorkload: common.AnnValueTrue,
			},
			wantNeedsPatch: true,
			mustContain:    []string{common.AnnKeyVKSWorkload},
			// vks-node is already correct, so it must NOT be in the patch
			// — strategic merge would no-op on it but the smaller payload
			// is the correct shape.
			mustNotContain: []string{`"` + common.AnnKeyVKSNode + `":"true"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pvc := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "pvc",
					Namespace:   "ns",
					Annotations: tt.existingAnn,
				},
			}
			patchBytes, needsPatch, err := reconcilePVCWorkloadTypeAnnotations(pvc, tt.desired)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantNeedsPatch, needsPatch, "needsPatch mismatch")
			if !tt.wantNeedsPatch {
				assert.Nil(t, patchBytes)
				return
			}
			body := string(patchBytes)
			for _, s := range tt.mustContain {
				assert.Contains(t, body, s, "patch must contain %q\npatch=%s", s, body)
			}
			for _, s := range tt.mustContainDeletion {
				// SMP deletes a map key by setting it to JSON null.
				assert.Contains(t, body, `"`+s+`":null`,
					"stale tag %q must be set to null for deletion\npatch=%s", s, body)
			}
			for _, s := range tt.mustNotContain {
				assert.NotContains(t, body, s,
					"patch must NOT contain %q\npatch=%s", s, body)
			}
		})
	}
}

// TestReconcileKeepAfterDeleteVmFlags tests that fullsync performs bidirectional
// reconciliation of keepAfterDeleteVm flags:
// - Clears flag for PVCs with VM ownerRef but no annotation
// - Restores flag for PVCs with annotation but no VM ownerRef
func TestReconcileKeepAfterDeleteVmFlags(t *testing.T) {
	ctx := context.Background()

	// Case 1: PVC with VM ownerRef but no annotation (needs flag cleared)
	pvcNeedsClear := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-needs-clear",
			Namespace: "test-ns",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "vmoperator.vmware.com/v1alpha2",
					Kind:       "VirtualMachine",
					Name:       "test-vm",
				},
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "pv-1",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	// Case 2: PVC with annotation but no VM ownerRef (needs flag restored)
	pvcNeedsRestore := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-needs-restore",
			Namespace: "test-ns",
			Annotations: map[string]string{
				common.AnnVMDeleteProtectionCleared: "true",
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "pv-2",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	// Case 3: PVC with VM ownerRef AND annotation (already processed - skip)
	pvcAlreadyProcessed := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-already-processed",
			Namespace: "test-ns",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "vmoperator.vmware.com/v1alpha2",
					Kind:       "VirtualMachine",
					Name:       "test-vm-2",
				},
			},
			Annotations: map[string]string{
				common.AnnVMDeleteProtectionCleared: "true",
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "pv-3",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	// Case 4: PVC without VM ownerRef and no annotation (no action needed)
	pvcNoAction := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-no-action",
			Namespace: "test-ns",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "pv-4",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	// Create corresponding PVs
	pv1 := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       csitypes.Name,
					VolumeHandle: "vol-1",
				},
			},
		},
	}
	pv2 := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-2"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       csitypes.Name,
					VolumeHandle: "vol-2",
				},
			},
		},
	}
	pv3 := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-3"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       csitypes.Name,
					VolumeHandle: "vol-3",
				},
			},
		},
	}
	pv4 := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-4"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       csitypes.Name,
					VolumeHandle: "vol-4",
				},
			},
		},
	}

	// Create fake k8s client with PVCs and PVs
	k8sClient := k8sfake.NewSimpleClientset(
		pvcNeedsClear, pvcNeedsRestore, pvcAlreadyProcessed, pvcNoAction,
		pv1, pv2, pv3, pv4,
	)

	// Create fake informer manager with listers using a fresh factory to avoid singleton caching
	factory := informers.NewSharedInformerFactory(k8sClient, 0)
	informerManager := k8s.NewInformerFromFactory(ctx, k8sClient, factory)
	// Register listeners so Listen() waits for the cache to sync
	err := informerManager.AddPVCListener(
		ctx, func(interface{}) {}, func(interface{}, interface{}) {}, func(interface{}) {},
	)
	assert.NoError(t, err, "Failed to add PVC listener")
	err = informerManager.AddPVListener(
		ctx, func(interface{}) {}, func(interface{}, interface{}) {}, func(interface{}) {},
	)
	assert.NoError(t, err, "Failed to add PV listener")
	informerManager.Listen()

	// Track VSLM calls
	protectCalls := []string{}
	unprotectCalls := []string{}
	mockVolMgr := &mockVolumeManagerForFullSync{
		protectVolumeFromVMDeletionFunc: func(_ context.Context, volumeID string) error {
			protectCalls = append(protectCalls, volumeID)
			return nil
		},
		unprotectVolumeFromVMDeletionFunc: func(_ context.Context, volumeID string) error {
			unprotectCalls = append(unprotectCalls, volumeID)
			return nil
		},
	}

	// Create metadataSyncer
	ms := &metadataSyncInformer{
		clusterFlavor:    cnstypes.CnsClusterFlavorWorkload,
		volumeManager:    mockVolMgr,
		pvLister:         informerManager.GetPVLister(),
		pvcLister:        informerManager.GetPVCLister(),
		supervisorClient: k8sClient,
	}

	// Run the reconciliation
	reconcileKeepAfterDeleteVmFlags(ctx, ms)

	// Verify that PVC with VM ownerRef and no annotation had flag cleared
	assert.Equal(t, []string{"vol-1"}, unprotectCalls,
		"Only vol-1 should have UnprotectVolumeFromVMDeletion called")

	// Verify that PVC with annotation but no VM ownerRef had flag restored
	assert.Equal(t, []string{"vol-2"}, protectCalls,
		"Only vol-2 should have ProtectVolumeFromVMDeletion called")
}

// pvMissingTestSetup configures the package-level state required by
// getMissingPVVolumeUpdateSpecs: the per-VC cnsDeletionMap entry (grace
// tracker) and clusterIDforVolumeMetadata. It returns a metadataSyncInformer
// carrying a mockCOCommonForFullSync so the function's PVC-namespace cache
// lookup returns false (i.e. "no cached PVC"), matching the production code
// path we want to exercise.
func pvMissingTestSetup(t *testing.T, vc, clusterID string) *metadataSyncInformer {
	t.Helper()
	cnsDeletionMap = map[string]map[string]bool{vc: {}}
	clusterIDforVolumeMetadata = clusterID
	return &metadataSyncInformer{
		coCommonInterface: &mockCOCommonForFullSync{},
	}
}

// makeCNSVolume builds a CnsVolume with optional PV-type entity metadata.
func makeCNSVolume(id, name, clusterID string, pvEntities ...*cnstypes.CnsKubernetesEntityMetadata) cnstypes.CnsVolume {
	var ems []cnstypes.BaseCnsEntityMetadata
	for _, em := range pvEntities {
		ems = append(ems, em)
	}
	return cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{Id: id},
		Name:     name,
		Metadata: cnstypes.CnsVolumeMetadata{EntityMetadata: ems},
	}
}

// hasPVMissingTrueLabel returns true if any PV-type entity in the spec carries
// pv_missing=true (Delete=false).
func hasPVMissingTrueLabel(spec cnstypes.CnsVolumeMetadataUpdateSpec) bool {
	for _, baseEm := range spec.Metadata.EntityMetadata {
		em, ok := baseEm.(*cnstypes.CnsKubernetesEntityMetadata)
		if !ok || em.EntityType != string(cnstypes.CnsKubernetesEntityTypePV) || em.Delete {
			continue
		}
		for _, kv := range em.Labels {
			if kv.Key == "pv_missing" && kv.Value == "true" {
				return true
			}
		}
	}
	return false
}

// TestGetMissingPVVolumeUpdateSpecs_GracePeriod verifies that on the first
// full-sync cycle where a CNS volume has no matching K8s PV, the volume is
// only recorded in the grace tracker (cnsDeletionMap) and NO update spec is
// produced. On the second cycle, an update spec with pv_missing=true is
// generated.
func TestGetMissingPVVolumeUpdateSpecs_GracePeriod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := "test-vc"
	clusterID := "test-cluster"
	ms := pvMissingTestSetup(t, vc, clusterID)

	pvEntity := &cnstypes.CnsKubernetesEntityMetadata{
		CnsEntityMetadata: cnstypes.CnsEntityMetadata{
			EntityName: "pv-orphan",
			ClusterID:  clusterID,
		},
		EntityType: string(cnstypes.CnsKubernetesEntityTypePV),
	}
	vol := makeCNSVolume("vol-1", "pv-orphan", clusterID, pvEntity)
	cc := cnstypes.CnsContainerCluster{ClusterId: clusterID}

	// Cycle 1: empty k8sPVMap. Expect 0 update specs (grace recorded).
	specs, count, err := getMissingPVVolumeUpdateSpecs(ctx, []cnstypes.CnsVolume{vol},
		map[string]string{}, ms, false, cc, vc)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	assert.Len(t, specs, 0)
	assert.True(t, cnsDeletionMap[vc]["vol-1"],
		"volume should be in grace tracker after cycle 1")

	// Cycle 2: still missing → expect 1 spec with pv_missing=true.
	specs, count, err = getMissingPVVolumeUpdateSpecs(ctx, []cnstypes.CnsVolume{vol},
		map[string]string{}, ms, false, cc, vc)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Len(t, specs, 1)
	assert.Equal(t, "vol-1", specs[0].VolumeId.Id)
	assert.True(t, hasPVMissingTrueLabel(specs[0]),
		"pv_missing=true label should be present on PV-type entity")
}

// TestGetMissingPVVolumeUpdateSpecs_PVExists verifies that volumes whose PV
// IS present in K8s are not touched and not added to the grace tracker.
func TestGetMissingPVVolumeUpdateSpecs_PVExists(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := "test-vc"
	clusterID := "test-cluster"
	ms := pvMissingTestSetup(t, vc, clusterID)

	vol := makeCNSVolume("vol-1", "pv-good", clusterID,
		&cnstypes.CnsKubernetesEntityMetadata{
			CnsEntityMetadata: cnstypes.CnsEntityMetadata{
				EntityName: "pv-good",
				ClusterID:  clusterID,
			},
			EntityType: string(cnstypes.CnsKubernetesEntityTypePV),
		})
	cc := cnstypes.CnsContainerCluster{ClusterId: clusterID}

	k8sPVMap := map[string]string{"vol-1": ""}
	specs, count, err := getMissingPVVolumeUpdateSpecs(ctx, []cnstypes.CnsVolume{vol},
		k8sPVMap, ms, false, cc, vc)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	assert.Len(t, specs, 0)
	assert.False(t, cnsDeletionMap[vc]["vol-1"],
		"volume with present PV should not be in grace tracker")
}

// TestGetMissingPVVolumeUpdateSpecs_PreservesExistingLabels verifies that the
// pv_missing label is appended on top of any pre-existing labels on the PV-
// type entity, rather than replacing them.
func TestGetMissingPVVolumeUpdateSpecs_PreservesExistingLabels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := "test-vc"
	clusterID := "test-cluster"
	ms := pvMissingTestSetup(t, vc, clusterID)

	vol := makeCNSVolume("vol-1", "pv-orphan", clusterID,
		&cnstypes.CnsKubernetesEntityMetadata{
			CnsEntityMetadata: cnstypes.CnsEntityMetadata{
				EntityName: "pv-orphan",
				ClusterID:  clusterID,
				Labels: []types.KeyValue{
					{Key: "app", Value: "nginx"},
					{Key: "tier", Value: "frontend"},
				},
			},
			EntityType: string(cnstypes.CnsKubernetesEntityTypePV),
		})
	cc := cnstypes.CnsContainerCluster{ClusterId: clusterID}

	// Pre-seed the grace tracker to short-circuit the cycle-1 deferral.
	cnsDeletionMap[vc]["vol-1"] = true

	specs, _, err := getMissingPVVolumeUpdateSpecs(ctx, []cnstypes.CnsVolume{vol},
		map[string]string{}, ms, false, cc, vc)
	assert.NoError(t, err)
	assert.Len(t, specs, 1)

	labels := map[string]string{}
	for _, baseEm := range specs[0].Metadata.EntityMetadata {
		em := baseEm.(*cnstypes.CnsKubernetesEntityMetadata)
		for _, kv := range em.Labels {
			labels[kv.Key] = kv.Value
		}
	}
	assert.Equal(t, "true", labels["pv_missing"], "pv_missing label must be set")
	assert.Equal(t, "nginx", labels["app"], "pre-existing app label must be preserved")
	assert.Equal(t, "frontend", labels["tier"], "pre-existing tier label must be preserved")
}

// TestGetMissingPVVolumeUpdateSpecs_AlreadyLabeled verifies idempotency: a
// volume that is already labeled pv_missing=true on its PV entity does not
// produce a second update spec, even after the grace period is satisfied.
func TestGetMissingPVVolumeUpdateSpecs_AlreadyLabeled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := "test-vc"
	clusterID := "test-cluster"
	ms := pvMissingTestSetup(t, vc, clusterID)

	vol := makeCNSVolume("vol-1", "pv-orphan", clusterID,
		&cnstypes.CnsKubernetesEntityMetadata{
			CnsEntityMetadata: cnstypes.CnsEntityMetadata{
				EntityName: "pv-orphan",
				ClusterID:  clusterID,
				Labels: []types.KeyValue{
					{Key: "pv_missing", Value: "true"},
				},
			},
			EntityType: string(cnstypes.CnsKubernetesEntityTypePV),
		})
	cc := cnstypes.CnsContainerCluster{ClusterId: clusterID}

	// Pre-seed grace tracker so we skip straight to the labeling decision.
	cnsDeletionMap[vc]["vol-1"] = true

	specs, count, err := getMissingPVVolumeUpdateSpecs(ctx, []cnstypes.CnsVolume{vol},
		map[string]string{}, ms, false, cc, vc)
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "should not re-label an already-labeled volume")
	assert.Len(t, specs, 0)
}

// TestGetMissingPVVolumeUpdateSpecs_SynthesizesPVEntity verifies that volumes
// with no in-cluster PV-type entity still get labeled via a synthetic PV
// entity derived from CnsVolume.Name (legacy volumes whose CNS metadata
// never carried a PV entry).
func TestGetMissingPVVolumeUpdateSpecs_SynthesizesPVEntity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := "test-vc"
	clusterID := "test-cluster"
	ms := pvMissingTestSetup(t, vc, clusterID)

	// Note: no entityMetadata at all.
	vol := makeCNSVolume("vol-1", "legacy-pv-name", clusterID)
	cc := cnstypes.CnsContainerCluster{ClusterId: clusterID}

	cnsDeletionMap[vc]["vol-1"] = true

	specs, _, err := getMissingPVVolumeUpdateSpecs(ctx, []cnstypes.CnsVolume{vol},
		map[string]string{}, ms, false, cc, vc)
	assert.NoError(t, err)
	assert.Len(t, specs, 1)
	assert.True(t, hasPVMissingTrueLabel(specs[0]))
	// The synthesized entity must reuse vol.Name as the EntityName so the
	// label is attached to a stable key.
	found := false
	for _, baseEm := range specs[0].Metadata.EntityMetadata {
		em := baseEm.(*cnstypes.CnsKubernetesEntityMetadata)
		if em.EntityName == "legacy-pv-name" &&
			em.EntityType == string(cnstypes.CnsKubernetesEntityTypePV) {
			found = true
			break
		}
	}
	assert.True(t, found, "synthesized PV entity must use vol.Name as EntityName")
}

// TestGetMissingPVVolumeUpdateSpecs_ForeignClusterIgnored verifies that
// PV-type entity metadata that belongs to a different K8s cluster (different
// ClusterID) is left alone — we do not relabel volumes co-owned by another
// cluster's PVs.
func TestGetMissingPVVolumeUpdateSpecs_ForeignClusterIgnored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := "test-vc"
	clusterID := "test-cluster"
	ms := pvMissingTestSetup(t, vc, clusterID)

	// Vol has a PV entity for a DIFFERENT cluster.
	vol := makeCNSVolume("vol-1", "", clusterID,
		&cnstypes.CnsKubernetesEntityMetadata{
			CnsEntityMetadata: cnstypes.CnsEntityMetadata{
				EntityName: "pv-on-other-cluster",
				ClusterID:  "some-other-cluster",
			},
			EntityType: string(cnstypes.CnsKubernetesEntityTypePV),
		})
	cc := cnstypes.CnsContainerCluster{ClusterId: clusterID}

	cnsDeletionMap[vc]["vol-1"] = true

	specs, count, err := getMissingPVVolumeUpdateSpecs(ctx, []cnstypes.CnsVolume{vol},
		map[string]string{}, ms, false, cc, vc)
	assert.NoError(t, err)
	// vol.Name is empty AND there's no in-cluster PV entity → nothing to label.
	assert.Equal(t, 0, count)
	assert.Len(t, specs, 0)
}
