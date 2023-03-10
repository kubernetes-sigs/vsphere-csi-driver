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
	vmoperatortypes "github.com/vmware-tanzu/vm-operator-api/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
)

func TestCSINodeTopologyControllerForTKGSHA(t *testing.T) {
	var (
		testNodeName            = "test-node-name"
		testNodeUID             = uuid.New().String()
		testCSINodeTopologyName = "test-csinodetopology-name"
		testUnexpectedVmName    = "test-unexpected-vm-name"
		testNodeIDInSpec        = "test-node-id"
		testSupervisorNamespace = "test-supervisor-namespace"
		expectedZoneKey         = corev1.LabelZoneFailureDomainStable
		expectedZoneValue       = "zone-1"
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
		testVMwithZone = &vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testCSINodeTopologyName,
				Namespace: testSupervisorNamespace,
			},
			Status: vmoperatortypes.VirtualMachineStatus{
				Zone: expectedZoneValue,
			},
		}
		testVMwithoutZone = &vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testCSINodeTopologyName,
				Namespace: testSupervisorNamespace,
			},
		}
		testUnexpectedVM = &vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testUnexpectedVmName,
				Namespace: testSupervisorNamespace,
			},
		}
		testBufferSize = 1024
	)

	tests := []struct {
		name                    string
		csiNodeTopology         *csinodetopologyv1alpha1.CSINodeTopology
		vm                      *vmoperatortypes.VirtualMachine
		expectedTopologyLabels  []csinodetopologyv1alpha1.TopologyLabel
		expectedCRDStatus       csinodetopologyv1alpha1.CRDStatus
		expectedReconcileResult reconcile.Result
		expectedReconcileError  error
	}{
		{
			name:            "TestWithVmStatusZonePopulated",
			csiNodeTopology: testCSINodeTopology.DeepCopy(),
			vm:              testVMwithZone.DeepCopy(),
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
			vm:                      testVMwithoutZone.DeepCopy(),
			expectedTopologyLabels:  nil,
			expectedCRDStatus:       csinodetopologyv1alpha1.CSINodeTopologySuccess,
			expectedReconcileResult: reconcile.Result{},
			expectedReconcileError:  nil,
		},
		{
			name:                    "TestWithGetVmFailure",
			csiNodeTopology:         testCSINodeTopology.DeepCopy(),
			vm:                      testUnexpectedVM.DeepCopy(),
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

			fakeClient := fake.NewClientBuilder().
				WithScheme(s).
				WithRuntimeObjects(objs...).
				Build()

			supervisorObjs := []runtime.Object{test.vm}

			supervisor_scheme := scheme.Scheme
			supervisor_scheme.AddKnownTypes(vmoperatortypes.SchemeGroupVersion, test.vm)

			fakeVmOperatorClient := fake.NewClientBuilder().
				WithScheme(supervisor_scheme).
				WithRuntimeObjects(supervisorObjs...).
				Build()

			r := &ReconcileCSINodeTopology{
				client:              fakeClient,
				scheme:              s,
				configInfo:          &cnsconfig.ConfigurationInfo{},
				recorder:            record.NewFakeRecorder(testBufferSize),
				useNodeUuid:         true,
				enableTKGsHAinGuest: true,
				vmOperatorClient:    fakeVmOperatorClient,
				supervisorNamespace: testSupervisorNamespace,
			}

			backOffDuration = make(map[string]time.Duration)

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
