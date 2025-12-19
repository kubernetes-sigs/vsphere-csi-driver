package wcp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
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
	// Save original functions and restore after tests
	originalGetConfig := getK8sConfig
	originalNewK8sClientFromConfig := newK8sClientFromConfig
	defer func() {
		getK8sConfig = originalGetConfig
		newK8sClientFromConfig = originalNewK8sClientFromConfig
	}()

	// Mock getK8sConfig to return a fake config
	getK8sConfig = func() (*restclient.Config, error) {
		return &restclient.Config{}, nil
	}

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
		newK8sClientFromConfig = func(c *restclient.Config) (kubernetes.Interface, error) {
			return fakeClient, nil
		}

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), "test-namespace")

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
		newK8sClientFromConfig = func(c *restclient.Config) (kubernetes.Interface, error) {
			return fakeClient, nil
		}

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), "test-namespace")

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
		newK8sClientFromConfig = func(c *restclient.Config) (kubernetes.Interface, error) {
			return fakeClient, nil
		}

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), "test-namespace")

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
		newK8sClientFromConfig = func(c *restclient.Config) (kubernetes.Interface, error) {
			return fakeClient, nil
		}

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), "test-namespace")

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
		newK8sClientFromConfig = func(c *restclient.Config) (kubernetes.Interface, error) {
			return fakeClient, nil
		}

		// Execute
		_, err := getSnapshotLimitForNamespace(context.Background(), "test-namespace")

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
		newK8sClientFromConfig = func(c *restclient.Config) (kubernetes.Interface, error) {
			return fakeClient, nil
		}

		// Execute
		_, err := getSnapshotLimitForNamespace(context.Background(), "test-namespace")

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
		newK8sClientFromConfig = func(c *restclient.Config) (kubernetes.Interface, error) {
			return fakeClient, nil
		}

		// Execute
		_, err := getSnapshotLimitForNamespace(context.Background(), "test-namespace")

		// Verify
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "missing required key")
	})

	t.Run("WhenConfigMapNotFound", func(t *testing.T) {
		// Setup
		fakeClient := fake.NewClientset() // Empty clientset
		newK8sClientFromConfig = func(c *restclient.Config) (kubernetes.Interface, error) {
			return fakeClient, nil
		}

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), "test-namespace")

		// Verify
		assert.Nil(t, err)
		assert.Equal(t, common.DefaultMaxSnapshotsPerVolume, limit) // Should return default (4)
	})

	t.Run("WhenK8sClientCreationFails", func(t *testing.T) {
		// Setup
		newK8sClientFromConfig = func(c *restclient.Config) (kubernetes.Interface, error) {
			return nil, assert.AnError
		}

		// Execute
		limit, err := getSnapshotLimitForNamespace(context.Background(), "test-namespace")

		// Verify - should return default instead of error
		assert.Nil(t, err)
		assert.Equal(t, common.DefaultMaxSnapshotsPerVolume, limit)
	})
}
