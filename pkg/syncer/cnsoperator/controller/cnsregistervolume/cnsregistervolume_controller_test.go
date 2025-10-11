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

package cnsregistervolume

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

type mockVolumeManager struct {
	createVolumeFunc func(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
		ctxParams interface{}) (*cnsvolume.CnsVolumeInfo, string, error)
}

func (m *mockVolumeManager) UnregisterVolume(ctx context.Context, volumeID string, unregisterDisk bool) error {
	//TODO implement me
	return nil
}

func (m *mockVolumeManager) AttachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string, checkNVMeController bool) (string, string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) DeleteVolume(ctx context.Context, volumeID string, deleteDisk bool) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) UpdateVolumeMetadata(ctx context.Context,
	spec *cnstypes.CnsVolumeMetadataUpdateSpec) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) UpdateVolumeCrypto(ctx context.Context, spec *cnstypes.CnsVolumeCryptoUpdateSpec) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) QueryVolumeInfo(ctx context.Context,
	volumeIDList []cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) QueryVolume(ctx context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) RelocateVolume(ctx context.Context,
	relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) ExpandVolume(ctx context.Context, volumeID string,
	size int64, extraParams interface{}) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) ResetManager(ctx context.Context, vcenter *cnsvsphere.VirtualCenter) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) ConfigureVolumeACLs(ctx context.Context, spec cnstypes.CnsVolumeACLConfigureSpec) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) RegisterDisk(ctx context.Context, path string, name string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) RetrieveVStorageObject(ctx context.Context,
	volumeID string) (*vim25types.VStorageObject, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) CreateSnapshot(ctx context.Context, volumeID string,
	desc string, extraParams interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) DeleteSnapshot(ctx context.Context, volumeID string,
	snapshotID string, extraParams interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) QuerySnapshots(ctx context.Context,
	snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) MonitorCreateVolumeTask(ctx context.Context,
	volumeOperationDetails **cnsvolumeoperationrequest.VolumeOperationRequestDetails,
	task *object.Task, volNameFromInputSpec, clusterID string) (*cnsvolume.CnsVolumeInfo, string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) IsListViewReady() bool {
	//TODO implement me
	panic("implement me")
}

func (m *mockVolumeManager) SetListViewNotReady(ctx context.Context) {

}

func (m *mockVolumeManager) BatchAttachVolumes(ctx context.Context,
	vm *cnsvsphere.VirtualMachine,
	volumeIDs []cnsvolume.BatchAttachRequest) ([]cnsvolume.BatchAttachResult, string, error) {
	return []cnsvolume.BatchAttachResult{}, "", nil
}

func (m *mockVolumeManager) QueryVolumeAsync(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	return &cnstypes.CnsQueryResult{
		Volumes: []cnstypes.CnsVolume{
			{
				VolumeId:        cnstypes.CnsVolumeId{Id: "dummy-volume-id"},
				DatastoreUrl:    "dummy-ds-url",
				StoragePolicyId: "dummy-storage-policy-id",
			},
		},
	}, nil

}

func (m *mockVolumeManager) CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
	ctxParams interface{}) (*cnsvolume.CnsVolumeInfo, string, error) {
	if m.createVolumeFunc != nil {
		return m.createVolumeFunc(ctx, spec, ctxParams)
	}
	return nil, "", nil
}

func (m *mockVolumeManager) SyncVolume(ctx context.Context,
	syncVolumeSpecs []cnstypes.CnsSyncVolumeSpec) (string, error) {
	return "", nil
}

type mockCOCommon struct{}

func (m *mockCOCommon) GetPVCNamespacedNameByUID(uid string) (types.NamespacedName, bool) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) EnableFSS(ctx context.Context, featureName string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) DisableFSS(ctx context.Context, featureName string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) HandleLateEnablementOfCapability(ctx context.Context,
	clusterFlavor cnstypes.CnsClusterFlavor, capability, gcPort, gcEndpoint string) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) IsFSSEnabled(ctx context.Context, featureName string) bool {
	return true
}

func (m *mockCOCommon) IsCNSCSIFSSEnabled(ctx context.Context, featureName string) bool {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) IsPVCSIFSSEnabled(ctx context.Context, featureName string) bool {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) IsFakeAttachAllowed(ctx context.Context, volumeID string,
	volumeManager cnsvolume.Manager) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) MarkFakeAttached(ctx context.Context, volumeID string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) ClearFakeAttached(ctx context.Context, volumeID string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) InitTopologyServiceInController(ctx context.Context) (commoncotypes.ControllerTopologyService,
	error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) InitTopologyServiceInNode(ctx context.Context) (commoncotypes.NodeTopologyService, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetNodesForVolumes(ctx context.Context, volumeIds []string) map[string][]string {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetNodeIDtoNameMap(ctx context.Context) map[string]string {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetFakeAttachedVolumes(ctx context.Context, volumeIDs []string) map[string]bool {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetVolumeAttachment(ctx context.Context,
	volumeId string, nodeName string) (*storagev1.VolumeAttachment, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetAllVolumes() []string {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetAllK8sVolumes() []string {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) AnnotateVolumeSnapshot(ctx context.Context, volumeSnapshotName string,
	volumeSnapshotNamespace string, annotations map[string]string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetConfigMap(ctx context.Context, name string, namespace string) (map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) CreateConfigMap(ctx context.Context, name string, namespace string,
	data map[string]string, isImmutable bool) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetCSINodeTopologyInstancesList() []interface{} {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetCSINodeTopologyInstanceByName(nodeName string) (item interface{}, exists bool, err error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetPVNameFromCSIVolumeID(volumeID string) (string, bool) {
	return "", false
}

func (m *mockCOCommon) GetPVCNameFromCSIVolumeID(volumeID string) (string, string, bool) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) InitializeCSINodes(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) StartZonesInformer(ctx context.Context,
	restClientConfig *restclient.Config, namespace string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) PreLinkedCloneCreateAction(ctx context.Context, pvcNamespace string,
	pvcName string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetLinkedCloneVolumeSnapshotSourceUUID(ctx context.Context,
	pvcName string, pvcNamespace string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetVolumeSnapshotPVCSource(ctx context.Context, volumeSnapshotNamespace string,
	volumeSnapshotName string) (*corev1.PersistentVolumeClaim, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) IsLinkedCloneRequest(ctx context.Context, pvcName string, pvcNamespace string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) UpdatePersistentVolumeLabel(ctx context.Context, pvName string, key string, value string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockCOCommon) GetZonesForNamespace(ns string) map[string]struct{} {
	return map[string]struct{}{"zone-a": {}}
}

func (m *mockCOCommon) GetActiveClustersForNamespaceInRequestedZones(ctx context.Context,
	ns string, zones []string) ([]string, error) {
	return []string{"cluster-a"}, nil
}

func (m *mockCOCommon) GetPvcObjectByName(ctx context.Context, pvcName string,
	namespace string) (*corev1.PersistentVolumeClaim, error) {
	return nil, nil
}

func (m *mockCOCommon) GetVolumeIDFromPVCName(pvcName string) (string, bool) {
	return "vol-1", true
}

var _ = Describe("Reconcile Accessibility Logic", func() {
	var (
		scheme   *runtime.Scheme
		r        *ReconcileCnsRegisterVolume
		ctx      context.Context
		patches  *gomonkey.Patches
		instance cnsregistervolumev1alpha1.CnsRegisterVolume
	)

	commonco.ContainerOrchestratorUtility = &mockCOCommon{}

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		scheme.AddKnownTypes(schema.GroupVersion{
			Group:   "cnsoperator.vmware.com",
			Version: "v1alpha1",
		}, &cnsregistervolumev1alpha1.CnsRegisterVolume{}, &cnsregistervolumev1alpha1.CnsRegisterVolumeList{})

		ctx = context.Background()

		crv := &cnsregistervolumev1alpha1.CnsRegisterVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-volume",
				Namespace: "test-ns",
			},
			Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
				PvcName:    "test-pvc",
				VolumeID:   "dummy-volume-id",
				AccessMode: "ReadWriteOnce",
			},
		}
		// Create fake client with instance preloaded
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(&instance).
			Build()
		_ = fakeClient.Create(ctx, crv)

		mockVolMgr := &mockVolumeManager{
			createVolumeFunc: func(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
				ctxParams interface{}) (*cnsvolume.CnsVolumeInfo, string, error) {
				return &cnsvolume.CnsVolumeInfo{
					VolumeID: cnstypes.CnsVolumeId{Id: "fake-volume-id"},
				}, "", nil
			},
		}
		r = &ReconcileCnsRegisterVolume{
			client:        fakeClient,
			scheme:        scheme,
			volumeManager: mockVolMgr,
		}
		patches = gomonkey.NewPatches()
	})

	AfterEach(func() {
		patches.Reset()
	})

	It("should disallow registering volume not accessible to active clusters on namespace", func() {
		patches.ApplyFunc(cnsvsphere.GetVirtualCenterInstance, func(ctx context.Context,
			config *config.ConfigurationInfo, reinitialize bool) (*cnsvsphere.VirtualCenter, error) {
			return &cnsvsphere.VirtualCenter{}, nil
		})

		patches.ApplyFunc(common.QueryVolumeByID, func(ctx context.Context, vm cnsvolume.Manager,
			volumeID string, querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsVolume, error) {
			return &cnstypes.CnsVolume{
					VolumeId:        cnstypes.CnsVolumeId{Id: volumeID},
					DatastoreUrl:    "dummy-ds-url",
					StoragePolicyId: "dummy-storage-policy-id",
					BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
						CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{
							CapacityInMb: 1024,
						},
					},
				},
				nil
		})

		patches.ApplyFunc(common.GetClusterComputeResourceMoIds, func(ctx context.Context) ([]string, bool, error) {
			return []string{"cluster-a", "cluster-b"}, true, nil
		})

		patches.ApplyMethod(
			reflect.TypeOf(&mockCOCommon{}), // concrete type pointer
			"GetZonesForNamespace",
			func(_ *mockCOCommon, ns string) map[string]struct{} {
				return map[string]struct{}{"zone-b": {}}
			})

		patches.ApplyMethod(reflect.TypeOf(&mockCOCommon{}), "GetActiveClustersForNamespaceInRequestedZones",
			func(_ commonco.COCommonInterface, ctx context.Context, ns string, zones []string) ([]string, error) {
				return []string{"cluster-a"}, nil
			})

		patches.ApplyFunc(isDatastoreAccessibleToAZClusters,
			func(ctx context.Context, vc *cnsvsphere.VirtualCenter, azToClusters map[string][]string, ds string) bool {
				return false
			})

		patches.ApplyFunc(setInstanceError, func(ctx context.Context, r *ReconcileCnsRegisterVolume,
			instance *cnsregistervolumev1alpha1.CnsRegisterVolume, msg string) {
			// no-op
		})

		patches.ApplyFunc(cnsvsphere.GetVirtualCenterInstance, func(ctx context.Context,
			config *config.ConfigurationInfo, reinitialize bool) (*cnsvsphere.VirtualCenter, error) {
			return &cnsvsphere.VirtualCenter{
				Config: &cnsvsphere.VirtualCenterConfig{
					Host: "dummy-vcenter",
				},
			}, nil
		})

		patches.ApplyFunc(constructCreateSpecForInstance, func(
			ctx context.Context,
			r *ReconcileCnsRegisterVolume,
			instance *cnsregistervolumev1alpha1.CnsRegisterVolume,
			host string,
			isTKGSHAEnabled bool,
		) *cnstypes.CnsVolumeCreateSpec {
			return &cnstypes.CnsVolumeCreateSpec{
				Name:       "fake-volume",
				VolumeType: "BLOCK",
			}
		})

		patches.ApplyFunc(
			common.DeleteVolumeUtil,
			func(ctx context.Context, volumeManager cnsvolume.Manager, volumeID string,
				forceDelete bool) (string, error) {
				return "", nil
			},
		)

		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      "test-volume",
				Namespace: "test-ns",
			},
		}

		result, err := r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(reconcile.Result{RequeueAfter: 0}))
	})

	It("should add vm name and storage policy reservation labels to PVC when both present on CR", func() {
		// Test the getPersistentVolumeClaimSpec function directly
		instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-volume-with-labels",
				Namespace: "test-ns",
				Labels: map[string]string{
					cnsoperatortypes.LabelVirtualMachineName:           "test-vm-name",
					cnsoperatortypes.LabelStoragePolicyReservationName: "test-storage-policy-reservation-name",
				},
			},
			Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
				PvcName:    "test-pvc-with-labels",
				VolumeID:   "dummy-volume-id",
				AccessMode: "ReadWriteOnce",
			},
		}

		// Test getPersistentVolumeClaimSpec function directly
		pvcSpec, err := getPersistentVolumeClaimSpec(ctx, "test-pvc", "test-ns", 1024,
			"test-storage-class", corev1.ReadWriteOnce, corev1.PersistentVolumeFilesystem, "test-pv", nil, instance)

		Expect(err).NotTo(HaveOccurred())
		Expect(pvcSpec).NotTo(BeNil())
		Expect(pvcSpec.Labels).To(HaveKeyWithValue(cnsoperatortypes.LabelVirtualMachineName, "test-vm-name"))
		Expect(pvcSpec.Labels).To(HaveKeyWithValue(cnsoperatortypes.LabelStoragePolicyReservationName,
			"test-storage-policy-reservation-name"))
	})

	It("should not add labels to PVC when not present on CR", func() {
		// Test the getPersistentVolumeClaimSpec function directly without labels
		instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-volume-without-labels",
				Namespace: "test-ns",
				// No labels set
			},
			Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
				PvcName:    "test-pvc-without-labels",
				VolumeID:   "dummy-volume-id",
				AccessMode: "ReadWriteOnce",
			},
		}

		// Test getPersistentVolumeClaimSpec function directly
		pvcSpec, err := getPersistentVolumeClaimSpec(ctx, "test-pvc", "test-ns", 1024,
			"test-storage-class", corev1.ReadWriteOnce, corev1.PersistentVolumeFilesystem, "test-pv", nil, instance)

		Expect(err).NotTo(HaveOccurred())
		Expect(pvcSpec).NotTo(BeNil())
		Expect(pvcSpec.Labels).NotTo(HaveKey(cnsoperatortypes.LabelVirtualMachineName))
		Expect(pvcSpec.Labels).NotTo(HaveKey(cnsoperatortypes.LabelStoragePolicyReservationName))
	})

	It("should not add vm name label when only that label is present on CR", func() {
		// Test the getPersistentVolumeClaimSpec function directly with only vm name label
		instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-volume-with-vm-label",
				Namespace: "test-ns",
				Labels: map[string]string{
					cnsoperatortypes.LabelVirtualMachineName: "test-vm-name",
					// Missing storage policy reservation label
				},
			},
			Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
				PvcName:    "test-pvc-with-vm-label",
				VolumeID:   "dummy-volume-id",
				AccessMode: "ReadWriteOnce",
			},
		}

		// Test getPersistentVolumeClaimSpec function directly
		pvcSpec, err := getPersistentVolumeClaimSpec(ctx, "test-pvc", "test-ns", 1024,
			"test-storage-class", corev1.ReadWriteOnce, corev1.PersistentVolumeFilesystem, "test-pv", nil, instance)

		Expect(err).NotTo(HaveOccurred())
		Expect(pvcSpec).NotTo(BeNil())
		Expect(pvcSpec.Labels).NotTo(HaveKey(cnsoperatortypes.LabelVirtualMachineName))
		Expect(pvcSpec.Labels).NotTo(HaveKey(cnsoperatortypes.LabelStoragePolicyReservationName))
	})
})

var _ = Describe("checkExistingPVCDataSourceRef", func() {
	var (
		ctx       context.Context
		k8sclient *k8sfake.Clientset
		namespace string
		pvcName   string
	)

	BeforeEach(func() {
		ctx = context.Background()
		k8sclient = k8sfake.NewSimpleClientset()
		namespace = "test-namespace"
		pvcName = "test-pvc"
	})

	Context("when PVC does not exist", func() {
		It("should return nil without error", func() {
			pvc, err := checkExistingPVCDataSourceRef(ctx, k8sclient, pvcName, namespace)
			Expect(err).To(BeNil())
			Expect(pvc).To(BeNil())
		})
	})

	Context("when PVC exists without DataSourceRef", func() {
		BeforeEach(func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: namespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					DataSourceRef: nil,
				},
			}
			_, err := k8sclient.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).To(BeNil())
		})

		It("should return the PVC without error", func() {
			pvc, err := checkExistingPVCDataSourceRef(ctx, k8sclient, pvcName, namespace)
			Expect(err).To(BeNil())
			Expect(pvc).ToNot(BeNil())
			Expect(pvc.Name).To(Equal(pvcName))
			Expect(pvc.Spec.DataSourceRef).To(BeNil())
		})
	})

	Context("when PVC exists with VolumeSnapshot DataSourceRef", func() {
		BeforeEach(func() {
			apiGroup := "snapshot.storage.k8s.io"
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: namespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					DataSourceRef: &corev1.TypedObjectReference{
						APIGroup: &apiGroup,
						Kind:     "VolumeSnapshot",
						Name:     "test-snapshot",
					},
				},
			}
			_, err := k8sclient.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).To(BeNil())
		})

		It("should return an error since VolumeSnapshots are not supported for CNSRegisterVolume", func() {
			pvc, err := checkExistingPVCDataSourceRef(ctx, k8sclient, pvcName, namespace)
			Expect(err).ToNot(BeNil())
			Expect(pvc).To(BeNil())
		})
	})

	Context("when PVC exists with supported DataSourceRef", func() {
		BeforeEach(func() {
			apiGroup := "vmoperator.vmware.com"
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: namespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					DataSourceRef: &corev1.TypedObjectReference{
						APIGroup: &apiGroup,
						Kind:     "VirtualMachine",
						Name:     "test-vm",
					},
				},
			}
			_, err := k8sclient.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).To(BeNil())
		})

		It("should return the PVC without error", func() {
			pvc, err := checkExistingPVCDataSourceRef(ctx, k8sclient, pvcName, namespace)
			Expect(err).To(BeNil())
			Expect(pvc).ToNot(BeNil())
			Expect(pvc.Name).To(Equal(pvcName))
			Expect(pvc.Spec.DataSourceRef.Kind).To(Equal("VirtualMachine"))
			Expect(*pvc.Spec.DataSourceRef.APIGroup).To(Equal("vmoperator.vmware.com"))
		})
	})

	Context("when PVC exists with unsupported DataSourceRef", func() {
		BeforeEach(func() {
			apiGroup := "unsupported.example.com"
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: namespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					DataSourceRef: &corev1.TypedObjectReference{
						APIGroup: &apiGroup,
						Kind:     "UnsupportedKind",
						Name:     "test-resource",
					},
				},
			}
			_, err := k8sclient.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).To(BeNil())
		})

		It("should return an error", func() {
			pvc, err := checkExistingPVCDataSourceRef(ctx, k8sclient, pvcName, namespace)
			Expect(err).ToNot(BeNil())
			Expect(pvc).To(BeNil())
		})
	})

	Context("when PVC exists with empty APIGroup DataSourceRef", func() {
		BeforeEach(func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: namespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					DataSourceRef: &corev1.TypedObjectReference{
						APIGroup: nil,
						Kind:     "SomeKind",
						Name:     "test-resource",
					},
				},
			}
			_, err := k8sclient.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).To(BeNil())
		})

		It("should return an error for unsupported empty APIGroup", func() {
			pvc, err := checkExistingPVCDataSourceRef(ctx, k8sclient, pvcName, namespace)
			Expect(err).ToNot(BeNil())
			Expect(pvc).To(BeNil())
		})
	})
})

var _ = Describe("validatePVCTopologyCompatibility", func() {
	var (
		ctx                         context.Context
		pvc                         *corev1.PersistentVolumeClaim
		volumeDatastoreURL          string
		mockTopologyMgr             *mockTopologyService
		mockVC                      *cnsvsphere.VirtualCenter
		datastoreAccessibleTopology []map[string]string
		mockK8sClient               *k8sfake.Clientset
	)

	BeforeEach(func() {
		ctx = context.Background()
		volumeDatastoreURL = "dummy-datastore-url"
		mockTopologyMgr = &mockTopologyService{}
		mockVC = &cnsvsphere.VirtualCenter{}
		datastoreAccessibleTopology = []map[string]string{
			{"topology.kubernetes.io/zone": "zone-1"},
		}

		pvc = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pvc",
				Namespace: "test-namespace",
			},
		}

		// Create a fake Kubernetes client
		mockK8sClient = k8sfake.NewSimpleClientset()
	})

	Context("when PVC has no topology annotation", func() {
		It("should return nil without error", func() {
			// Add the PVC to the fake client so it can be updated
			_, err := mockK8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).To(BeNil())

			err = validatePVCTopologyCompatibility(ctx, mockK8sClient, pvc, volumeDatastoreURL, mockTopologyMgr, mockVC,
				datastoreAccessibleTopology)
			Expect(err).To(BeNil())
		})
	})

	Context("when PVC has empty topology annotation", func() {
		BeforeEach(func() {
			pvc.Annotations = map[string]string{
				"csi.vsphere.volume-accessible-topology": "",
			}
		})

		It("should return nil without error", func() {
			// Add the PVC to the fake client so it can be updated
			_, err := mockK8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).To(BeNil())

			err = validatePVCTopologyCompatibility(ctx, mockK8sClient, pvc, volumeDatastoreURL, mockTopologyMgr, mockVC,
				datastoreAccessibleTopology)
			Expect(err).To(BeNil())
		})
	})

	Context("when PVC has invalid topology annotation", func() {
		BeforeEach(func() {
			pvc.Annotations = map[string]string{
				"csi.vsphere.volume-accessible-topology": "invalid-json",
			}
		})

		It("should return error for invalid JSON", func() {
			err := validatePVCTopologyCompatibility(ctx, mockK8sClient, pvc, volumeDatastoreURL, mockTopologyMgr, mockVC,
				datastoreAccessibleTopology)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("failed to parse topology annotation"))
		})
	})

	Context("when topology manager fails", func() {
		BeforeEach(func() {
			pvc.Annotations = map[string]string{
				"csi.vsphere.volume-accessible-topology": `[{"topology.kubernetes.io/zone":"zone-a"}]`,
			}
			mockTopologyMgr.shouldFail = true
		})

		It("should return error from topology manager", func() {
			err := validatePVCTopologyCompatibility(ctx, mockK8sClient, pvc, volumeDatastoreURL, mockTopologyMgr, mockVC,
				datastoreAccessibleTopology)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("failed to get topology for volume datastore"))
		})
	})

	Context("when topologies are compatible", func() {
		BeforeEach(func() {
			pvc.Annotations = map[string]string{
				"csi.vsphere.volume-accessible-topology": `[{"topology.kubernetes.io/zone":"zone-a"}]`,
			}
			mockTopologyMgr.returnTopology = []map[string]string{
				{"topology.kubernetes.io/zone": "zone-a"},
			}
		})

		It("should return nil without error", func() {
			err := validatePVCTopologyCompatibility(ctx, mockK8sClient, pvc, volumeDatastoreURL, mockTopologyMgr, mockVC,
				datastoreAccessibleTopology)
			Expect(err).To(BeNil())
		})
	})

	Context("when topologies are incompatible", func() {
		BeforeEach(func() {
			pvc.Annotations = map[string]string{
				"csi.vsphere.volume-accessible-topology": `[{"topology.kubernetes.io/zone":"zone-a"}]`,
			}
			mockTopologyMgr.returnTopology = []map[string]string{
				{"topology.kubernetes.io/zone": "zone-b"},
			}
		})

		It("should return error for incompatible zones", func() {
			err := validatePVCTopologyCompatibility(ctx, mockK8sClient, pvc, volumeDatastoreURL, mockTopologyMgr, mockVC,
				datastoreAccessibleTopology)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("is not compatible with volume placement"))
		})
	})

	Context("when PVC exists without topology annotation and annotation needs to be added", func() {
		var originalPVC *corev1.PersistentVolumeClaim

		BeforeEach(func() {
			// Create a PVC with some existing annotations but no topology annotation
			originalPVC = &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-pvc",
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"some.other/annotation": "existing-value",
						"another/annotation":    "another-value",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			}
			pvc = originalPVC
		})

		It("should add topology annotation to existing PVC and return nil", func() {
			// Verify PVC initially has no topology annotation
			_, exists := pvc.Annotations["csi.vsphere.volume-accessible-topology"]
			Expect(exists).To(BeFalse())

			// Add the PVC to the fake client so it can be updated
			_, err := mockK8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).To(BeNil())

			// Call the function
			err = validatePVCTopologyCompatibility(ctx, mockK8sClient, pvc, volumeDatastoreURL, mockTopologyMgr, mockVC,
				datastoreAccessibleTopology)
			Expect(err).To(BeNil())

			// Verify topology annotation was added
			topologyAnnotation, exists := pvc.Annotations["csi.vsphere.volume-accessible-topology"]
			Expect(exists).To(BeTrue())
			Expect(topologyAnnotation).ToNot(BeEmpty())

			// Verify the annotation contains the expected topology data
			expectedAnnotation := `[{"topology.kubernetes.io/zone":"zone-1"}]`
			Expect(topologyAnnotation).To(Equal(expectedAnnotation))

			// Verify existing annotations are preserved
			Expect(pvc.Annotations["some.other/annotation"]).To(Equal("existing-value"))
			Expect(pvc.Annotations["another/annotation"]).To(Equal("another-value"))
		})

		It("should handle PVC with nil annotations map", func() {
			// Create PVC with nil annotations
			pvcWithNilAnnotations := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "pvc-nil-annotations",
					Namespace:   "test-namespace",
					Annotations: nil,
				},
			}

			// Add the PVC to the fake client so it can be updated
			_, err := mockK8sClient.CoreV1().PersistentVolumeClaims(pvcWithNilAnnotations.Namespace).Create(ctx,
				pvcWithNilAnnotations, metav1.CreateOptions{})
			Expect(err).To(BeNil())

			// Call the function
			err = validatePVCTopologyCompatibility(ctx, mockK8sClient, pvcWithNilAnnotations, volumeDatastoreURL,
				mockTopologyMgr, mockVC, datastoreAccessibleTopology)
			Expect(err).To(BeNil())

			// Verify annotations map was created and topology annotation was added
			Expect(pvcWithNilAnnotations.Annotations).ToNot(BeNil())
			topologyAnnotation, exists := pvcWithNilAnnotations.Annotations["csi.vsphere.volume-accessible-topology"]
			Expect(exists).To(BeTrue())
			Expect(topologyAnnotation).To(Equal(`[{"topology.kubernetes.io/zone":"zone-1"}]`))
		})

		It("should handle complex topology data with multiple zones", func() {
			// Use more complex topology data
			complexTopology := []map[string]string{
				{"topology.kubernetes.io/zone": "zone-a", "topology.kubernetes.io/region": "us-west"},
				{"topology.kubernetes.io/zone": "zone-b", "topology.kubernetes.io/region": "us-west"},
			}

			// Add the PVC to the fake client so it can be updated
			_, err := mockK8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).To(BeNil())

			err = validatePVCTopologyCompatibility(ctx, mockK8sClient, pvc, volumeDatastoreURL, mockTopologyMgr, mockVC,
				complexTopology)
			Expect(err).To(BeNil())

			// Verify the complex topology was properly serialized
			topologyAnnotation := pvc.Annotations["csi.vsphere.volume-accessible-topology"]
			Expect(topologyAnnotation).ToNot(BeEmpty())

			// Parse the annotation to verify it contains both topology segments
			var parsedTopology []map[string]string
			err = json.Unmarshal([]byte(topologyAnnotation), &parsedTopology)
			Expect(err).To(BeNil())
			Expect(len(parsedTopology)).To(Equal(2))
			Expect(parsedTopology[0]).To(Equal(map[string]string{
				"topology.kubernetes.io/zone":   "zone-a",
				"topology.kubernetes.io/region": "us-west",
			}))
			Expect(parsedTopology[1]).To(Equal(map[string]string{
				"topology.kubernetes.io/zone":   "zone-b",
				"topology.kubernetes.io/region": "us-west",
			}))
		})

	})
})

var _ = Describe("isTopologyCompatible", func() {
	It("should return true when all volume segments exist in PVC segments", func() {
		pvcTopologySegments := []map[string]string{
			{"topology.kubernetes.io/zone": "zone-a"},
			{"topology.kubernetes.io/zone": "zone-b"},
		}
		volumeTopologySegments := []map[string]string{
			{"topology.kubernetes.io/zone": "zone-a"},
		}
		Expect(isTopologyCompatible(pvcTopologySegments, volumeTopologySegments)).To(BeTrue())
	})

	It("should return true when volume has multiple segments all existing in PVC", func() {
		pvcTopologySegments := []map[string]string{
			{"topology.kubernetes.io/zone": "zone-a", "topology.kubernetes.io/region": "us-west"},
			{"topology.kubernetes.io/zone": "zone-b", "topology.kubernetes.io/region": "us-west"},
		}
		volumeTopologySegments := []map[string]string{
			{"topology.kubernetes.io/zone": "zone-a"},
			{"topology.kubernetes.io/zone": "zone-b"},
		}
		Expect(isTopologyCompatible(pvcTopologySegments, volumeTopologySegments)).To(BeTrue())
	})

	It("should return false when volume segment does not exist in any PVC segment", func() {
		pvcTopologySegments := []map[string]string{
			{"topology.kubernetes.io/zone": "zone-a"},
		}
		volumeTopologySegments := []map[string]string{
			{"topology.kubernetes.io/zone": "zone-b"},
		}
		Expect(isTopologyCompatible(pvcTopologySegments, volumeTopologySegments)).To(BeFalse())
	})

	It("should return true when some volume segments exist in PVC segments", func() {
		pvcTopologySegments := []map[string]string{
			{"topology.kubernetes.io/zone": "zone-a"},
		}
		volumeTopologySegments := []map[string]string{
			{"topology.kubernetes.io/zone": "zone-a"},
			{"topology.kubernetes.io/zone": "zone-b"}, // This doesn't exist in PVC
		}
		Expect(isTopologyCompatible(pvcTopologySegments, volumeTopologySegments)).To(BeTrue())
	})

	It("should return true for empty PVC segments with non-empty volume segments", func() {
		pvcTopologySegments := []map[string]string{}
		volumeTopologySegments := []map[string]string{
			{"topology.kubernetes.io/zone": "zone-a"},
		}
		Expect(isTopologyCompatible(pvcTopologySegments, volumeTopologySegments)).To(BeTrue())
	})
})

// validatePVCStorageClass is a helper function to test the storage class validation logic
func validatePVCStorageClass(pvc *corev1.PersistentVolumeClaim, expectedStorageClassName,
	storagePolicyID string) error {
	if pvc == nil {
		return nil // No PVC to validate
	}

	if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != expectedStorageClassName {
		return fmt.Errorf("existing PVC %s has storage class %s, but storage policy %s maps to storage class %s",
			pvc.Name, *pvc.Spec.StorageClassName, storagePolicyID, expectedStorageClassName)
	} else if pvc.Spec.StorageClassName == nil {
		return fmt.Errorf("existing PVC %s has no storage class specified, but storage policy %s requires storage class %s",
			pvc.Name, storagePolicyID, expectedStorageClassName)
	}

	return nil
}

var _ = Describe("PVC Storage Class Validation", func() {
	var (
		expectedSCName  string
		storagePolicyID string
		pvcName         string
	)

	BeforeEach(func() {
		expectedSCName = "expected-storage-class"
		storagePolicyID = "test-storage-policy-id"
		pvcName = "test-pvc"
	})

	Context("when PVC exists with matching storage class", func() {
		It("should pass validation without error", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: pvcName,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: &expectedSCName,
				},
			}

			err := validatePVCStorageClass(pvc, expectedSCName, storagePolicyID)
			Expect(err).To(BeNil())
		})
	})

	Context("when PVC exists with different storage class", func() {
		It("should return error for storage class mismatch", func() {
			differentSCName := "different-storage-class"
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: pvcName,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: &differentSCName,
				},
			}

			err := validatePVCStorageClass(pvc, expectedSCName, storagePolicyID)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("has storage class different-storage-class"))
			Expect(err.Error()).To(ContainSubstring(
				"but storage policy test-storage-policy-id maps to storage class expected-storage-class"))
		})
	})

	Context("when PVC exists with nil storage class", func() {
		It("should return error for missing storage class", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: pvcName,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: nil,
				},
			}

			err := validatePVCStorageClass(pvc, expectedSCName, storagePolicyID)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("has no storage class specified"))
			Expect(err.Error()).To(ContainSubstring(
				"but storage policy test-storage-policy-id requires storage class expected-storage-class"))
		})
	})

	Context("when PVC does not exist", func() {
		It("should skip storage class validation and return nil", func() {
			err := validatePVCStorageClass(nil, expectedSCName, storagePolicyID)
			Expect(err).To(BeNil())
		})
	})
})

// mockTopologyService implements commoncotypes.ControllerTopologyService for testing
type mockTopologyService struct {
	shouldFail     bool
	returnTopology []map[string]string
}

func (m *mockTopologyService) GetTopologyInfoFromNodes(ctx context.Context,
	reqParams interface{}) ([]map[string]string, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock topology service failure")
	}
	if m.returnTopology != nil {
		return m.returnTopology, nil
	}
	return []map[string]string{
		{"topology.kubernetes.io/zone": "default-zone"},
	}, nil
}

func (m *mockTopologyService) GetSharedDatastoresInTopology(ctx context.Context,
	reqParams interface{}) ([]*cnsvsphere.DatastoreInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockTopologyService) GetAZClustersMap(ctx context.Context) map[string][]string {
	return map[string][]string{
		"zone-a": {"cluster-1"},
		"zone-b": {"cluster-2"},
	}
}

func (m *mockTopologyService) ZonesWithMultipleClustersExist(ctx context.Context) bool {
	return false
}

func TestCnsRegisterVolumeController(t *testing.T) {
	backOffDuration = make(map[types.NamespacedName]time.Duration)

	// Set required FSS to true for unit test
	workloadDomainIsolationEnabled = true
	syncer.IsPodVMOnStretchSupervisorFSSEnabled = true
	isMultipleClustersPerVsphereZoneEnabled = true

	RegisterFailHandler(Fail)
	RunSpecs(t, "CnsRegisterVolumeController Suite")
}

func TestValidateCnsRegisterVolumeSpecWithDiskUrlPath(t *testing.T) {
	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "register-vol",
			Namespace: "test-ns",
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:     "pvc-1",
			DiskURLPath: "som-url",
			AccessMode:  corev1.ReadWriteMany,
			VolumeMode:  corev1.PersistentVolumeFilesystem,
		},
	}

	isSharedDiskEnabled = true
	err := validateCnsRegisterVolumeSpec(context.TODO(), instance)
	assert.Error(t, err)
	assert.Equal(t, "DiskURLPath cannot be used with accessMode: ReadWriteMany and volumeMode: Filesystem", err.Error())
}

func TestValidateCnsRegisterVolumeSpecWithVolumeIdAndNoAccessMode(t *testing.T) {
	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "register-vol",
			Namespace: "test-ns",
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:    "pvc-1",
			VolumeID:   "123456",
			VolumeMode: corev1.PersistentVolumeFilesystem,
		},
	}

	isSharedDiskEnabled = true
	err := validateCnsRegisterVolumeSpec(context.TODO(), instance)
	assert.Error(t, err)
	assert.Equal(t, "AccessMode cannot be empty when volumeID is specified", err.Error())
}

func TestValidateCnsRegisterVolumeSpecWithVolumeIdAndAccessMode(t *testing.T) {
	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "register-vol",
			Namespace: "test-ns",
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:    "pvc-1",
			VolumeID:   "123456",
			AccessMode: corev1.ReadWriteMany,
			VolumeMode: corev1.PersistentVolumeFilesystem,
		},
	}

	isSharedDiskEnabled = true
	commonco.ContainerOrchestratorUtility = &mockCOCommon{}
	err := validateCnsRegisterVolumeSpec(context.TODO(), instance)
	assert.NoError(t, err)
}

func TestIsBlockVolumeRegisterRequestWithSharedBlockVolume(t *testing.T) {
	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "register-vol",
			Namespace: "test-ns",
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:    "pvc-1",
			VolumeID:   "123456",
			VolumeMode: corev1.PersistentVolumeBlock,
			AccessMode: corev1.ReadWriteMany,
		},
	}

	isSharedDiskEnabled = true
	isBlockVolume := isBlockVolumeRegisterRequest(t.Context(), instance)
	assert.Equal(t, true, isBlockVolume)
}

func TestIsBlockVolumeRegisterRequestWithFileVolume(t *testing.T) {
	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "register-vol",
			Namespace: "test-ns",
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:    "pvc-1",
			VolumeID:   "123456",
			AccessMode: corev1.ReadWriteMany,
			VolumeMode: corev1.PersistentVolumeFilesystem,
		},
	}

	isSharedDiskEnabled = true
	isBlockVolume := isBlockVolumeRegisterRequest(t.Context(), instance)
	assert.Equal(t, false, isBlockVolume)
}

func TestGetPersistentVolumeSpecWhenVolumeModeIsEmpty(t *testing.T) {
	var (
		volumeName = "vol-1"
		volumeID   = "123456"
		capacity   = 256
		accessMode = corev1.ReadWriteMany
		scName     = "testsc"
	)

	isSharedDiskEnabled = true
	commonco.ContainerOrchestratorUtility = &mockCOCommon{}
	pv := getPersistentVolumeSpec(volumeName, volumeID, int64(capacity), accessMode, "", scName, nil)
	assert.Equal(t, corev1.PersistentVolumeFilesystem, *pv.Spec.VolumeMode)
}

func TestGetPersistentVolumeSpecWithVolumeMode(t *testing.T) {
	var (
		volumeName = "vol-1"
		volumeID   = "123456"
		capacity   = 256
		accessMode = corev1.ReadWriteMany
		scName     = "testsc"
		volumeMode = corev1.PersistentVolumeBlock
	)

	isSharedDiskEnabled = true
	pv := getPersistentVolumeSpec(volumeName, volumeID,
		int64(capacity), accessMode, volumeMode, scName, nil)
	assert.Equal(t, volumeMode, *pv.Spec.VolumeMode)
}
