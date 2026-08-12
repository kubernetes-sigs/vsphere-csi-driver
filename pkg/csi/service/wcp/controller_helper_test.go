package wcp

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

func TestGetPodVMUUID(t *testing.T) {
	containerOrchOriginal := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	newK8sClientOriginal := newK8sClient
	defer func() {
		newK8sClient = newK8sClientOriginal
		commonco.ContainerOrchestratorUtility = containerOrchOriginal
	}()

	t.Run("WhenPVCDoesNotExist", func(t *testing.T) {
		// Execute
		_, err := getPodVMUUID(context.Background(), "invalid-mock-volume", "")

		// Verify
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "failed to get PVC name")
	})

	t.Run("WhenCreatingK8sClientFails", func(t *testing.T) {
		// Setup
		newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
			return nil, assert.AnError
		}

		// Execute
		_, err := getPodVMUUID(context.Background(), "mock-volume-id", "mock-node-name")

		// Verify
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "failed to create kubernetes client")
	})

	t.Run("WhenListingPodsFails", func(t *testing.T) {
		// Setup
		newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
			c := fake.Clientset{}
			c.PrependReactor("list", "pods",
				func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, assert.AnError
				},
			)
			return &c, nil
		}

		// Assert
		_, err := getPodVMUUID(context.Background(), "mock-volume-id", "mock-node-name")

		// Verify
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "listing pods in the namespace \"mock-namespace\" failed")
	})

	t.Run("WhenPodNotFound", func(t *testing.T) {
		// Setup
		newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
			c := fake.Clientset{}
			c.PrependReactor("list", "pods",
				func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, nil // No pods found
				},
			)
			return &c, nil
		}

		// Execute
		_, err := getPodVMUUID(context.Background(), "mock-volume-id", "mock-node-name")

		// Verify
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "failed to find pod for pvc")
	})

	t.Run("WhenPodDoesNotHaveVMUUIDAnn", func(t *testing.T) {
		// Setup
		newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
			// register a few pods
			p1 := newMockPod("mock-pod", "mock-namespace", "mock-node-name",
				[]string{"mock-pvc"}, nil, v1.PodPending)
			p2 := newMockPod("mock-pod-2", "mock-namespace", "mock-node-name-2",
				nil, nil, v1.PodRunning)
			p3 := newMockPod("mock-pod-3", "mock-namespace", "mock-node-name-3",
				[]string{"mock-pvc2"}, map[string]string{"vmUUID": "mock-vm-uuid-2"}, v1.PodRunning)
			return fake.NewClientset(p1, p2, p3), nil
		}

		// Execute
		_, err := getPodVMUUID(context.Background(), "mock-volume-id", "mock-node-name")

		// Verify
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "\"vmware-system-vm-uuid\" annotation not found on pod \"mock-pod\"")
	})

	t.Run("WhenPodFoundWithVMUUID", func(t *testing.T) {
		// Setup
		newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
			// register a few pods
			p1 := newMockPod("mock-pod", "mock-namespace", "mock-node-name",
				[]string{"mock-pvc"}, map[string]string{"vmware-system-vm-uuid": "mock-vm-uuid"}, v1.PodPending)
			p2 := newMockPod("mock-pod-2", "mock-namespace", "mock-node-name-2",
				nil, nil, v1.PodRunning)
			p3 := newMockPod("mock-pod-3", "mock-namespace", "mock-node-name-3",
				[]string{"mock-pvc2"}, map[string]string{"vmUUID": "mock-vm-uuid-2"}, v1.PodRunning)
			return fake.NewClientset(p1, p2, p3), nil
		}

		// Execute
		vmUUID, err := getPodVMUUID(context.Background(), "mock-volume-id", "mock-node-name")

		// Verify
		assert.Nil(t, err)
		assert.Equal(t, "mock-vm-uuid", vmUUID)
	})
}

func newMockPod(name, namespace, nodeName string, volumes []string,
	annotations map[string]string, phase v1.PodPhase) *v1.Pod {
	vols := make([]v1.Volume, len(volumes))
	for i, vol := range volumes {
		vols[i] = v1.Volume{
			Name: vol,
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: vol,
				},
			},
		}
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotations,
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Volumes:  vols,
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
	}
}

func TestGetSnapshotLimitForNamespace(t *testing.T) {
	t.Run("WhenConfigMapExists_ValidValue", func(t *testing.T) {
		// Setup
		cm := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      common.ConfigMapCSILimits,
				Namespace: "test-namespace",
			},
			Data: map[string]string{
				common.ConfigMapKeyMaxSnapshotsPerVolume: "5",
			},
		}
		fakeClient := fake.NewClientset(cm)

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), fakeClient, "test-namespace")

		// Verify
		assert.Nil(t, err)
		assert.Equal(t, 5, limit)
	})

	t.Run("WhenConfigMapExists_ValueEqualsMax", func(t *testing.T) {
		// Setup
		cm := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      common.ConfigMapCSILimits,
				Namespace: "test-namespace",
			},
			Data: map[string]string{
				common.ConfigMapKeyMaxSnapshotsPerVolume: "32",
			},
		}
		fakeClient := fake.NewClientset(cm)

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), fakeClient, "test-namespace")

		// Verify
		assert.Nil(t, err)
		assert.Equal(t, 32, limit)
	})

	t.Run("WhenConfigMapExists_ValueExceedsMax", func(t *testing.T) {
		// Setup
		cm := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      common.ConfigMapCSILimits,
				Namespace: "test-namespace",
			},
			Data: map[string]string{
				common.ConfigMapKeyMaxSnapshotsPerVolume: "50",
			},
		}
		fakeClient := fake.NewClientset(cm)

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), fakeClient, "test-namespace")

		// Verify
		assert.Nil(t, err)
		assert.Equal(t, 32, limit) // Should be capped to absolute max
	})

	t.Run("WhenConfigMapExists_ValueIsZero", func(t *testing.T) {
		// Setup
		cm := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      common.ConfigMapCSILimits,
				Namespace: "test-namespace",
			},
			Data: map[string]string{
				common.ConfigMapKeyMaxSnapshotsPerVolume: "0",
			},
		}
		fakeClient := fake.NewClientset(cm)

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), fakeClient, "test-namespace")

		// Verify
		assert.Nil(t, err)
		assert.Equal(t, 0, limit) // 0 means block all snapshots
	})

	t.Run("WhenConfigMapExists_ValueIsNegative", func(t *testing.T) {
		// Setup
		cm := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      common.ConfigMapCSILimits,
				Namespace: "test-namespace",
			},
			Data: map[string]string{
				common.ConfigMapKeyMaxSnapshotsPerVolume: "-5",
			},
		}
		fakeClient := fake.NewClientset(cm)

		// Execute
		_, err := getSnapshotLimitForNamespace(context.Background(), fakeClient, "test-namespace")

		// Verify
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "invalid value")
		assert.Contains(t, err.Error(), "must be a non-negative integer")
	})

	t.Run("WhenConfigMapExists_InvalidFormat", func(t *testing.T) {
		// Setup
		cm := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      common.ConfigMapCSILimits,
				Namespace: "test-namespace",
			},
			Data: map[string]string{
				common.ConfigMapKeyMaxSnapshotsPerVolume: "abc",
			},
		}
		fakeClient := fake.NewClientset(cm)

		// Execute
		_, err := getSnapshotLimitForNamespace(context.Background(), fakeClient, "test-namespace")

		// Verify
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "invalid value")
		assert.Contains(t, err.Error(), "must be a non-negative integer")
	})

	t.Run("WhenConfigMapExists_MissingKey", func(t *testing.T) {
		// Setup
		cm := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      common.ConfigMapCSILimits,
				Namespace: "test-namespace",
			},
			Data: map[string]string{}, // ConfigMap exists but key is missing
		}
		fakeClient := fake.NewClientset(cm)

		// Execute
		_, err := getSnapshotLimitForNamespace(context.Background(), fakeClient, "test-namespace")

		// Verify
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "missing required key")
	})

	t.Run("WhenConfigMapNotFound", func(t *testing.T) {
		// Setup
		fakeClient := fake.NewClientset() // Empty clientset

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), fakeClient, "test-namespace")

		// Verify
		assert.Nil(t, err)
		assert.Equal(t, common.DefaultMaxSnapshotsPerVolume, limit) // Should return default (4)
	})
}

// TestValidateCreateBlockReqParamHostLocalPolicy verifies the WCP block-volume parameter validator
// accepts the host-local policy marker only when its value is "true", and continues to accept the
// existing block-volume parameters while rejecting unknown ones.
func TestValidateCreateBlockReqParamHostLocalPolicy(t *testing.T) {
	// The constant is already lowercase; the caller lowercases incoming parameter names too.
	hostLocalParam := common.AttributeHostLocalPolicy
	tests := []struct {
		name      string
		paramName string
		value     string
		want      bool
	}{
		{"hostLocalPolicy true", hostLocalParam, "true", true},
		{"hostLocalPolicy True mixed case", hostLocalParam, "True", true},
		{"hostLocalPolicy false", hostLocalParam, "false", false},
		{"hostLocalPolicy empty", hostLocalParam, "", false},
		{"storagePolicyID accepted", common.AttributeStoragePolicyID, "policy-1", true},
		{"unknown param rejected", "someunknownparam", "true", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, validateCreateBlockReqParam(tt.paramName, tt.value))
		})
	}
}

// TestGetHostMoRefsForHostLocalVolume verifies that the candidate host set for a host-local volume
// is built from the kubernetes.io/hostname values in the accessibility requirement's Preferred
// segments: a single host for Case A (WFFC) and the full set for Case B (Immediate).
func TestGetHostMoRefsForHostLocalVolume(t *testing.T) {
	ctx := context.Background()
	// Reverse map maintained by the orchestrator: node name -> ESX host MoID.
	nodeNameToID := map[string]string{
		"node-a": "host-1",
		"node-b": "host-2",
	}
	// makeReq builds a topology requirement whose Preferred segments carry both the zone and the
	// hostname for each node (zone defaults to zone-1, overridable via zoneFor).
	makeReqZoned := func(zoneFor map[string]string, hostnames ...string) *csi.TopologyRequirement {
		var prefs []*csi.Topology
		for _, h := range hostnames {
			zone := "zone-1"
			if z, ok := zoneFor[h]; ok {
				zone = z
			}
			prefs = append(prefs, &csi.Topology{Segments: map[string]string{
				v1.LabelTopologyZone: zone,
				v1.LabelHostname:     h,
			}})
		}
		return &csi.TopologyRequirement{Preferred: prefs}
	}
	makeReq := func(hostnames ...string) *csi.TopologyRequirement {
		return makeReqZoned(nil, hostnames...)
	}

	t.Run("CaseA_single_host", func(t *testing.T) {
		refs, hostTopo, err := getHostMoRefsForHostLocalVolume(ctx, makeReq("node-a"), nodeNameToID)
		assert.NoError(t, err)
		assert.Equal(t, []vimtypes.ManagedObjectReference{{Type: "HostSystem", Value: "host-1"}}, refs)
		assert.Equal(t, map[string]map[string]string{
			"host-1": {v1.LabelHostname: "node-a", v1.LabelTopologyZone: "zone-1"},
		}, hostTopo)
	})

	t.Run("CaseB_multiple_hosts_distinct_zones", func(t *testing.T) {
		refs, hostTopo, err := getHostMoRefsForHostLocalVolume(ctx,
			makeReqZoned(map[string]string{"node-b": "zone-2"}, "node-a", "node-b"), nodeNameToID)
		assert.NoError(t, err)
		assert.Len(t, refs, 2)
		values := []string{refs[0].Value, refs[1].Value}
		assert.ElementsMatch(t, []string{"host-1", "host-2"}, values)
		assert.Equal(t, "zone-1", hostTopo["host-1"][v1.LabelTopologyZone])
		assert.Equal(t, "zone-2", hostTopo["host-2"][v1.LabelTopologyZone])
	})

	t.Run("duplicate_hostnames_deduped", func(t *testing.T) {
		refs, hostTopo, err := getHostMoRefsForHostLocalVolume(ctx, makeReq("node-a", "node-a"), nodeNameToID)
		assert.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.Len(t, hostTopo, 1)
	})

	t.Run("unresolvable_hostname_errors", func(t *testing.T) {
		_, _, err := getHostMoRefsForHostLocalVolume(ctx, makeReq("node-x"), nodeNameToID)
		assert.Error(t, err)
	})

	t.Run("nil_requirement_errors", func(t *testing.T) {
		_, _, err := getHostMoRefsForHostLocalVolume(ctx, nil, nodeNameToID)
		assert.Error(t, err)
	})

	t.Run("no_hostname_segment_errors", func(t *testing.T) {
		req := &csi.TopologyRequirement{Preferred: []*csi.Topology{
			{Segments: map[string]string{v1.LabelTopologyZone: "zone-1"}},
		}}
		_, _, err := getHostMoRefsForHostLocalVolume(ctx, req, nodeNameToID)
		assert.Error(t, err)
	})
}

// TestGetHostLocalAccessibleTopology verifies the pure lookup of the CNS-selected host's zone +
// hostname from the request-scoped host MoID -> topology map (no vCenter call).
func TestGetHostLocalAccessibleTopology(t *testing.T) {
	ctx := context.Background()
	hostTopo := map[string]map[string]string{
		"host-1": {v1.LabelHostname: "node-a", v1.LabelTopologyZone: "zone-1"},
		"host-2": {v1.LabelHostname: "node-b"}, // incomplete: zone missing
	}

	t.Run("resolved", func(t *testing.T) {
		segments, err := getHostLocalAccessibleTopology(ctx,
			vimtypes.ManagedObjectReference{Type: "HostSystem", Value: "host-1"}, hostTopo)
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{v1.LabelHostname: "node-a", v1.LabelTopologyZone: "zone-1"}, segments)
	})

	t.Run("host_not_in_candidate_set_errors", func(t *testing.T) {
		_, err := getHostLocalAccessibleTopology(ctx,
			vimtypes.ManagedObjectReference{Type: "HostSystem", Value: "host-99"}, hostTopo)
		assert.Error(t, err)
	})

	t.Run("incomplete_topology_errors", func(t *testing.T) {
		_, err := getHostLocalAccessibleTopology(ctx,
			vimtypes.ManagedObjectReference{Type: "HostSystem", Value: "host-2"}, hostTopo)
		assert.Error(t, err)
	})
}

// TestIsHostExclusiveDatastore verifies the host-mount count check used to tell a host-exclusive
// datastore (the backing of a host-local storage policy) apart from a datastore shared across the
// hosts of a cluster.
func TestIsHostExclusiveDatastore(t *testing.T) {
	ctx := context.Background()
	dsInfo := &cnsvsphere.DatastoreInfo{
		Datastore: &cnsvsphere.Datastore{},
		Info:      &vimtypes.DatastoreInfo{Url: "ds:///vmfs/volumes/host-local-1/"},
	}
	hostMount := func(id string) vimtypes.DatastoreHostMount {
		return vimtypes.DatastoreHostMount{
			Key: vimtypes.ManagedObjectReference{Type: "HostSystem", Value: id},
		}
	}

	original := datastoreHostMounts
	t.Cleanup(func() { datastoreHostMounts = original })

	t.Run("single_host_mount_is_host_exclusive", func(t *testing.T) {
		datastoreHostMounts = func(_ context.Context,
			_ *cnsvsphere.DatastoreInfo) ([]vimtypes.DatastoreHostMount, error) {
			return []vimtypes.DatastoreHostMount{hostMount("host-1")}, nil
		}
		hostExclusive, err := isHostExclusiveDatastore(ctx, dsInfo)
		assert.NoError(t, err)
		assert.True(t, hostExclusive)
	})

	t.Run("multiple_host_mounts_is_not_host_exclusive", func(t *testing.T) {
		datastoreHostMounts = func(_ context.Context,
			_ *cnsvsphere.DatastoreInfo) ([]vimtypes.DatastoreHostMount, error) {
			return []vimtypes.DatastoreHostMount{hostMount("host-1"), hostMount("host-2")}, nil
		}
		hostExclusive, err := isHostExclusiveDatastore(ctx, dsInfo)
		assert.NoError(t, err)
		assert.False(t, hostExclusive)
	})

	t.Run("no_host_mounts_errors", func(t *testing.T) {
		datastoreHostMounts = func(_ context.Context,
			_ *cnsvsphere.DatastoreInfo) ([]vimtypes.DatastoreHostMount, error) {
			return nil, nil
		}
		_, err := isHostExclusiveDatastore(ctx, dsInfo)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no host mounts")
	})

	t.Run("property_fetch_error_is_propagated", func(t *testing.T) {
		datastoreHostMounts = func(_ context.Context,
			_ *cnsvsphere.DatastoreInfo) ([]vimtypes.DatastoreHostMount, error) {
			return nil, assert.AnError
		}
		_, err := isHostExclusiveDatastore(ctx, dsInfo)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to retrieve host mounts")
	})
}

// TestNarrowToSnapshotSourceHost verifies that a host-local restore is pinned to the host owning
// the snapshot's source volume. That host mounts the host-exclusive source datastore, so it is the
// only one that can run the restore copy; leaving the full candidate set lets CNS pick a host that
// cannot see the source datastore, which vpxd rejects with "No common host found between source
// and target datastores".
func TestNarrowToSnapshotSourceHost(t *testing.T) {
	ctx := context.Background()
	hostRef := func(id string) vimtypes.ManagedObjectReference {
		return vimtypes.ManagedObjectReference{Type: "HostSystem", Value: id}
	}
	candidates := []vimtypes.ManagedObjectReference{hostRef("host-1"), hostRef("host-2"), hostRef("host-3")}

	t.Run("narrows_to_the_source_host", func(t *testing.T) {
		narrowed, err := narrowToSnapshotSourceHost(ctx, candidates, hostRef("host-2"))
		assert.NoError(t, err)
		assert.Equal(t, []vimtypes.ManagedObjectReference{hostRef("host-2")}, narrowed)
	})

	t.Run("source_host_outside_the_candidate_set_errors", func(t *testing.T) {
		_, err := narrowToSnapshotSourceHost(ctx, candidates, hostRef("host-99"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "host-99")
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("empty_candidate_set_errors", func(t *testing.T) {
		_, err := narrowToSnapshotSourceHost(ctx, nil, hostRef("host-1"))
		assert.Error(t, err)
	})
}

// TestResolveHostLocalAccessibleTopologySegments verifies that the PV node-affinity segments for a
// host-local volume are derived correctly for both the ordinary case (from the host CNS reports in
// the placement result) and the linked-clone case, where CNS never reports a selected host because
// only `datastores` (not `hosts`) is supplied for a linked clone create request - the segments must
// instead come from the single candidate host already known from the accessibility requirement.
func TestResolveHostLocalAccessibleTopologySegments(t *testing.T) {
	ctx := context.Background()
	hostTopo := map[string]map[string]string{
		"host-1": {v1.LabelHostname: "node-a", v1.LabelTopologyZone: "zone-1"},
		"host-2": {v1.LabelHostname: "node-b", v1.LabelTopologyZone: "zone-2"},
	}
	hostRef1 := vimtypes.ManagedObjectReference{Type: "HostSystem", Value: "host-1"}
	hostRef2 := vimtypes.ManagedObjectReference{Type: "HostSystem", Value: "host-2"}

	t.Run("ordinary_host_local_uses_selected_host", func(t *testing.T) {
		segments, err := resolveHostLocalAccessibleTopologySegments(ctx, false,
			[]vimtypes.ManagedObjectReference{hostRef1, hostRef2}, hostTopo, &hostRef2, "vol-1")
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{v1.LabelHostname: "node-b", v1.LabelTopologyZone: "zone-2"}, segments)
	})

	t.Run("ordinary_host_local_no_selected_host_errors", func(t *testing.T) {
		_, err := resolveHostLocalAccessibleTopologySegments(ctx, false,
			[]vimtypes.ManagedObjectReference{hostRef1}, hostTopo, nil, "vol-1")
		assert.Error(t, err)
	})

	t.Run("linked_clone_uses_single_candidate_host_ignoring_selected_host", func(t *testing.T) {
		// Linked clone requests never get a selected host back from CNS (nil here), but the segments
		// should still resolve from the sole candidate host.
		segments, err := resolveHostLocalAccessibleTopologySegments(ctx, true,
			[]vimtypes.ManagedObjectReference{hostRef1}, hostTopo, nil, "vol-1")
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{v1.LabelHostname: "node-a", v1.LabelTopologyZone: "zone-1"}, segments)
	})

	t.Run("linked_clone_with_no_candidate_hosts_errors", func(t *testing.T) {
		_, err := resolveHostLocalAccessibleTopologySegments(ctx, true,
			nil, hostTopo, nil, "vol-1")
		assert.Error(t, err)
	})

	t.Run("linked_clone_with_multiple_candidate_hosts_errors", func(t *testing.T) {
		_, err := resolveHostLocalAccessibleTopologySegments(ctx, true,
			[]vimtypes.ManagedObjectReference{hostRef1, hostRef2}, hostTopo, nil, "vol-1")
		assert.Error(t, err)
	})
}
