package cnsvolumeattachment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubectl/pkg/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsvolumeattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/cnsvolumeattachment/v1alpha1"
)

var (
	testCnsvolumeAttachmentName = "testvol"
	testNamespace               = "test-ns"
)

func setupTestCnsNodeVmBatchAttachment() cnsvolumeattachmentv1alpha1.CnsVolumeAttachment {

	return cnsvolumeattachmentv1alpha1.CnsVolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCnsvolumeAttachmentName,
			Namespace: testNamespace,
		},
		Spec: cnsvolumeattachmentv1alpha1.CnsVolumeAttachmentSpec{
			AttachedVms: []string{
				"vm-1",
				"vm-2",
			},
		},
	}

}

func setTestEnvironment(testCnsVolumeAttachment cnsvolumeattachmentv1alpha1.CnsVolumeAttachment) client.WithWatch {
	cnsVolumeAttachment := testCnsVolumeAttachment.DeepCopy()
	objs := []runtime.Object{cnsVolumeAttachment}

	SchemeGroupVersion := schema.GroupVersion{
		Group:   "cns.vmware.com",
		Version: "v1alpha1",
	}

	// Register operator types with the runtime scheme.
	s := scheme.Scheme
	s.AddKnownTypes(SchemeGroupVersion, cnsVolumeAttachment)

	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(objs...).
		Build()

	return fakeClient

}
func TestAddVmToAttachedList(t *testing.T) {
	testCnsvolumeAttachment := setupTestCnsNodeVmBatchAttachment()
	fakeClient := setTestEnvironment(testCnsvolumeAttachment)

	t.Run("TestAddVmToAttachedList", func(t *testing.T) {
		cnsAttachmentInstance := &cnsVolumeAttachment{
			client: fakeClient,
		}

		volumeName := testNamespace + "/" + testCnsvolumeAttachmentName
		addVmToAttachedList := "vm-3"
		err := cnsAttachmentInstance.AddVmToAttachedList(context.Background(), volumeName, addVmToAttachedList)
		assert.NoError(t, err)

		namespacedName := types.NamespacedName{
			Name:      testCnsvolumeAttachmentName,
			Namespace: testNamespace,
		}

		updatedCnsVolumeAttachment := &cnsvolumeattachmentv1alpha1.CnsVolumeAttachment{}
		if err := fakeClient.Get(context.TODO(), namespacedName, updatedCnsVolumeAttachment); err != nil {
			t.Fatalf("get updatedCnsVolumeAttachment: %v", err)
		}

		exepectedAttachedVmsList := []string{"vm-1", "vm-2", "vm-3"}

		assert.Equal(t, exepectedAttachedVmsList, updatedCnsVolumeAttachment.Spec.AttachedVms)
	})

}

func TestRemoveVmFromAttachedList(t *testing.T) {
	testCnsvolumeAttachment := setupTestCnsNodeVmBatchAttachment()
	fakeClient := setTestEnvironment(testCnsvolumeAttachment)

	t.Run("TestRemoveVmFromAttachedList", func(t *testing.T) {
		cnsAttachmentInstance := &cnsVolumeAttachment{
			client: fakeClient,
		}

		// Remove VM-2
		volumeName := testNamespace + "/" + testCnsvolumeAttachmentName
		removeVmFromAttachedList := "vm-2"
		err, isLastAttachedVm := cnsAttachmentInstance.RemoveVmFromAttachedList(context.Background(),
			volumeName, removeVmFromAttachedList)
		assert.NoError(t, err)
		assert.Equal(t, false, isLastAttachedVm)

		namespacedName := types.NamespacedName{
			Name:      testCnsvolumeAttachmentName,
			Namespace: testNamespace,
		}

		updatedCnsVolumeAttachment := &cnsvolumeattachmentv1alpha1.CnsVolumeAttachment{}
		if err := fakeClient.Get(context.TODO(), namespacedName, updatedCnsVolumeAttachment); err != nil {
			t.Fatalf("get updatedCnsVolumeAttachment: %v", err)
		}

		exepectedAttachedVmsList := []string{"vm-1"}
		assert.Equal(t, exepectedAttachedVmsList, updatedCnsVolumeAttachment.Spec.AttachedVms)

		// Remove VM-1 also
		removeVmFromAttachedList = "vm-1"
		err, isLastAttachedVm = cnsAttachmentInstance.RemoveVmFromAttachedList(context.Background(),
			volumeName, removeVmFromAttachedList)
		assert.NoError(t, err)
		assert.Equal(t, true, isLastAttachedVm)

		updatedCnsVolumeAttachment = &cnsvolumeattachmentv1alpha1.CnsVolumeAttachment{}
		err = fakeClient.Get(context.TODO(), namespacedName, updatedCnsVolumeAttachment)
		assert.Error(t, err)

	})
}
