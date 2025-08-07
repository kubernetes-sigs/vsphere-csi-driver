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
	k8stesting "k8s.io/client-go/testing"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func TestGetPodVMUUID(t *testing.T) {
	logger.SetLoggerLevel(logger.DevelopmentLogLevel)
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

func TestVerifyStoragePolicyForVmfsWithEageredZeroThick(t *testing.T) {
	ctx := context.TODO()

	t.Run("Valid VMFS EZT policy", func(t *testing.T) {
		policyList := []cnsvsphere.SpbmPolicyContent{
			{
				Profiles: []cnsvsphere.SpbmPolicySubProfile{
					{
						Rules: []cnsvsphere.SpbmPolicyRule{
							{Ns: vmfsNamespace, Value: vmfsNamespaceEztValue},
						},
					},
				},
			},
		}

		err := verifyStoragePolicyForVmfsWithEageredZeroThick(ctx, policyList, "valid-policy-id")
		assert.NoError(t, err)
	})

	t.Run("Invalid VMFS policy - not EZT", func(t *testing.T) {
		policyList := []cnsvsphere.SpbmPolicyContent{
			{
				Profiles: []cnsvsphere.SpbmPolicySubProfile{
					{
						Rules: []cnsvsphere.SpbmPolicyRule{
							{Ns: vmfsNamespace, Value: "Thin"},
						},
					},
				},
			},
		}

		err := verifyStoragePolicyForVmfsWithEageredZeroThick(ctx, policyList, "invalid-policy-id")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be fully initialized")
	})

	t.Run("Policy with no VMFS rule", func(t *testing.T) {
		policyList := []cnsvsphere.SpbmPolicyContent{
			{
				Profiles: []cnsvsphere.SpbmPolicySubProfile{
					{
						Rules: []cnsvsphere.SpbmPolicyRule{
							{Ns: "VSAN", Value: "Whatever"},
						},
					},
				},
			},
		}

		err := verifyStoragePolicyForVmfsWithEageredZeroThick(ctx, policyList, "no-vmfs-rule-policy")
		assert.NoError(t, err)
	})

	t.Run("Empty policy list", func(t *testing.T) {
		var policyList []cnsvsphere.SpbmPolicyContent
		err := verifyStoragePolicyForVmfsWithEageredZeroThick(ctx, policyList, "empty-policy")
		assert.NoError(t, err)
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
