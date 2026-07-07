/*
Copyright 2021 The Kubernetes Authors.

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

package csinodetopology

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	vmoperatorv1alpha2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
)

func boolPtr(b bool) *bool { return &b }

func TestCSINodeTopologyControllerForTKGSHA(t *testing.T) {
	var (
		testNodeName            = "test-node-name"
		testNodeUID             = uuid.New().String()
		testCSINodeTopologyName = "test-csinodetopology-name"
		testUnexpectedVmName    = "test-unexpected-vm-name"
		testNodeIDInSpec        = "test-node-id"
		testSupervisorNamespace = "test-supervisor-namespace"
		testPVCName             = "test-hostlocalvm-pvc"
		expectedZoneKey         = corev1.LabelTopologyZone
		expectedZoneValue       = "zone-1"
		expectedHostKey         = common.GuestClusterTopologyLabelHost
		expectedHostValue       = "lvn-dvm-10-161-28-187.dvm.lvn.broadcom.net"
		testCSINodeTopology     = &csinodetopologyv1alpha1.CSINodeTopology{
			ObjectMeta: metav1.ObjectMeta{
				Name: testCSINodeTopologyName,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "Node",
						Name:       testNodeName,
						UID:        types.UID(testNodeUID),
					},
				},
			},
			Spec: csinodetopologyv1alpha1.CSINodeTopologySpec{
				NodeID: testNodeIDInSpec,
			},
		}
		testVMwithNonRemovableVolume = &vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testCSINodeTopologyName,
				Namespace: testSupervisorNamespace,
			},
			Spec: vmoperatortypes.VirtualMachineSpec{
				Volumes: []vmoperatortypes.VirtualMachineVolume{
					{
						Name:      testPVCName,
						Removable: boolPtr(false),
						VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
							PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
								PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: testPVCName,
								},
							},
						},
					},
				},
			},
			Status: vmoperatortypes.VirtualMachineStatus{
				Zone: expectedZoneValue,
			},
		}
		testVMwithRemovableVolumeOnly = &vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testCSINodeTopologyName,
				Namespace: testSupervisorNamespace,
			},
			Spec: vmoperatortypes.VirtualMachineSpec{
				Volumes: []vmoperatortypes.VirtualMachineVolume{
					{
						Name:      testPVCName,
						Removable: boolPtr(true),
						VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
							PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
								PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: testPVCName,
								},
							},
						},
					},
				},
			},
			Status: vmoperatortypes.VirtualMachineStatus{
				Zone: expectedZoneValue,
			},
		}
		// v1alpha2 fixtures: production code only fetches v1alpha5 when
		// enableHostLocalSupportInGuest is true. When it is false, it falls
		// back to the same utils.GetVirtualMachine (v1alpha2) path used
		// elsewhere in the codebase, so those test cases need a v1alpha2 VM.
		testVMwithZoneV1alpha2 = &vmoperatorv1alpha2.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testCSINodeTopologyName,
				Namespace: testSupervisorNamespace,
			},
			Status: vmoperatorv1alpha2.VirtualMachineStatus{
				Zone: expectedZoneValue,
			},
		}
		testVMwithoutZoneV1alpha2 = &vmoperatorv1alpha2.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testCSINodeTopologyName,
				Namespace: testSupervisorNamespace,
			},
		}
		testUnexpectedVMv1alpha2 = &vmoperatorv1alpha2.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testUnexpectedVmName,
				Namespace: testSupervisorNamespace,
			},
		}
		testPVCWithTopologyAnnotation = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testPVCName,
				Namespace: testSupervisorNamespace,
				Annotations: map[string]string{
					common.AnnVolumeAccessibleTopology: `[{"kubernetes.io/hostname":"` + expectedHostValue +
						`","topology.kubernetes.io/zone":"` + expectedZoneValue + `"}]`,
				},
			},
		}
		testPVCWithoutTopologyAnnotation = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testPVCName,
				Namespace: testSupervisorNamespace,
			},
		}
		testBufferSize = 1024
	)

	tests := []struct {
		name                          string
		csiNodeTopology               *csinodetopologyv1alpha1.CSINodeTopology
		vm                            *vmoperatortypes.VirtualMachine
		vmV1alpha2                    *vmoperatorv1alpha2.VirtualMachine
		enableHostLocalSupportInGuest bool
		existingPVC                   *corev1.PersistentVolumeClaim
		expectedTopologyLabels        []csinodetopologyv1alpha1.TopologyLabel
		expectedCRDStatus             csinodetopologyv1alpha1.CRDStatus
		expectedReconcileResult       reconcile.Result
		expectedReconcileError        error
	}{
		{
			name:            "TestWithVmStatusZonePopulated",
			csiNodeTopology: testCSINodeTopology.DeepCopy(),
			vmV1alpha2:      testVMwithZoneV1alpha2.DeepCopy(),
			expectedTopologyLabels: []csinodetopologyv1alpha1.TopologyLabel{
				{
					Key:   expectedZoneKey,
					Value: expectedZoneValue,
				},
			},
			expectedCRDStatus:       csinodetopologyv1alpha1.CSINodeTopologySuccess,
			expectedReconcileResult: reconcile.Result{},
			expectedReconcileError:  nil,
		},
		{
			name:                          "TestWithHostLocalSupportEnabled_PVCHasHostnameAnnotation",
			csiNodeTopology:               testCSINodeTopology.DeepCopy(),
			vm:                            testVMwithNonRemovableVolume.DeepCopy(),
			enableHostLocalSupportInGuest: true,
			existingPVC:                   testPVCWithTopologyAnnotation.DeepCopy(),
			expectedTopologyLabels: []csinodetopologyv1alpha1.TopologyLabel{
				{
					Key:   expectedZoneKey,
					Value: expectedZoneValue,
				},
				{
					Key:   expectedHostKey,
					Value: expectedHostValue,
				},
			},
			expectedCRDStatus:       csinodetopologyv1alpha1.CSINodeTopologySuccess,
			expectedReconcileResult: reconcile.Result{},
			expectedReconcileError:  nil,
		},
		{
			name:                          "TestWithHostLocalSupportEnabled_PVCMissingAnnotation",
			csiNodeTopology:               testCSINodeTopology.DeepCopy(),
			vm:                            testVMwithNonRemovableVolume.DeepCopy(),
			enableHostLocalSupportInGuest: true,
			existingPVC:                   testPVCWithoutTopologyAnnotation.DeepCopy(),
			expectedTopologyLabels: []csinodetopologyv1alpha1.TopologyLabel{
				{
					Key:   expectedZoneKey,
					Value: expectedZoneValue,
				},
			},
			expectedCRDStatus:       csinodetopologyv1alpha1.CSINodeTopologySuccess,
			expectedReconcileResult: reconcile.Result{},
			expectedReconcileError:  nil,
		},
		{
			name:                          "TestWithHostLocalSupportEnabled_NoNonRemovableVolume",
			csiNodeTopology:               testCSINodeTopology.DeepCopy(),
			vm:                            testVMwithRemovableVolumeOnly.DeepCopy(),
			enableHostLocalSupportInGuest: true,
			expectedTopologyLabels: []csinodetopologyv1alpha1.TopologyLabel{
				{
					Key:   expectedZoneKey,
					Value: expectedZoneValue,
				},
			},
			expectedCRDStatus:       csinodetopologyv1alpha1.CSINodeTopologySuccess,
			expectedReconcileResult: reconcile.Result{},
			expectedReconcileError:  nil,
		},
		{
			name:                          "TestWithHostLocalSupportDisabled",
			csiNodeTopology:               testCSINodeTopology.DeepCopy(),
			vmV1alpha2:                    testVMwithZoneV1alpha2.DeepCopy(),
			enableHostLocalSupportInGuest: false,
			expectedTopologyLabels: []csinodetopologyv1alpha1.TopologyLabel{
				{
					Key:   expectedZoneKey,
					Value: expectedZoneValue,
				},
			},
			expectedCRDStatus:       csinodetopologyv1alpha1.CSINodeTopologySuccess,
			expectedReconcileResult: reconcile.Result{},
			expectedReconcileError:  nil,
		},
		{
			name:                    "TestWithVmStatusZoneEmpty",
			csiNodeTopology:         testCSINodeTopology.DeepCopy(),
			vmV1alpha2:              testVMwithoutZoneV1alpha2.DeepCopy(),
			expectedTopologyLabels:  nil,
			expectedCRDStatus:       csinodetopologyv1alpha1.CSINodeTopologySuccess,
			expectedReconcileResult: reconcile.Result{},
			expectedReconcileError:  nil,
		},
		{
			name:                    "TestWithGetVmFailure",
			csiNodeTopology:         testCSINodeTopology.DeepCopy(),
			vmV1alpha2:              testUnexpectedVMv1alpha2.DeepCopy(),
			expectedTopologyLabels:  nil,
			expectedCRDStatus:       csinodetopologyv1alpha1.CSINodeTopologyError,
			expectedReconcileResult: reconcile.Result{RequeueAfter: time.Second},
			expectedReconcileError:  nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// objects to track in the fake client.
			objs := []runtime.Object{test.csiNodeTopology}

			// Register operator types with the runtime scheme.
			s := scheme.Scheme
			s.AddKnownTypes(csinodetopologyv1alpha1.SchemeGroupVersion, test.csiNodeTopology)

			fakeClient := ctrlfake.NewClientBuilder().
				WithScheme(s).
				WithRuntimeObjects(objs...).
				Build()

			var supervisorObjs []runtime.Object
			supervisor_scheme := scheme.Scheme
			if test.vm != nil {
				supervisorObjs = append(supervisorObjs, test.vm)
				supervisor_scheme.AddKnownTypes(vmoperatortypes.GroupVersion, test.vm)
			}
			if test.vmV1alpha2 != nil {
				supervisorObjs = append(supervisorObjs, test.vmV1alpha2)
				supervisor_scheme.AddKnownTypes(vmoperatorv1alpha2.GroupVersion, test.vmV1alpha2)
			}

			fakeVmOperatorClient := ctrlfake.NewClientBuilder().
				WithScheme(supervisor_scheme).
				WithRuntimeObjects(supervisorObjs...).
				Build()

			var supervisorClientsetObjs []runtime.Object
			if test.existingPVC != nil {
				supervisorClientsetObjs = append(supervisorClientsetObjs, test.existingPVC)
			}
			supervisorClientset := fake.NewSimpleClientset(supervisorClientsetObjs...)

			r := &ReconcileCSINodeTopology{
				client:                        fakeClient,
				scheme:                        s,
				configInfo:                    &cnsconfig.ConfigurationInfo{},
				recorder:                      record.NewFakeRecorder(testBufferSize),
				enableTKGsHAinGuest:           true,
				enableHostLocalSupportInGuest: test.enableHostLocalSupportInGuest,
				vmOperatorClient:              fakeVmOperatorClient,
				supervisorNamespace:           testSupervisorNamespace,
				supervisorClientset:           supervisorClientset,
			}

			backOffDuration = make(map[types.NamespacedName]time.Duration)

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      testCSINodeTopologyName,
					Namespace: "",
				},
			}

			res, err := r.Reconcile(context.TODO(), req)
			if err != nil {
				t.Fatal("Unexpected reconcile error")
			}

			assert.Equal(t, test.expectedReconcileResult, res)
			assert.Equal(t, test.expectedReconcileError, err)

			updatedCSINodeTopology := &csinodetopologyv1alpha1.CSINodeTopology{}
			if err := r.client.Get(context.TODO(), req.NamespacedName, updatedCSINodeTopology); err != nil {
				t.Fatalf("get updatedCSINodeTopology: %v", err)
			}

			assert.Equal(t, test.expectedCRDStatus, updatedCSINodeTopology.Status.Status)
			assert.Equal(t, test.expectedTopologyLabels, updatedCSINodeTopology.Status.TopologyLabels)
		})
	}
}
