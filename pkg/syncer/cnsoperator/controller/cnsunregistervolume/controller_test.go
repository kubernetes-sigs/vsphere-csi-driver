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

package cnsunregistervolume

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	v1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsunregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	cnsoptypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

func TestReconciler_Reconcile(t *testing.T) {
	newK8sClientOriginal := newK8sClient
	getValidatedParamsOriginal := getValidatedParams
	getVolumeUsageInfoOriginal := getVolumeUsageInfo
	retainPVOriginal := retainPV
	deletePVCOriginal := deletePVC
	deletePVOriginal := deletePV
	defer func() {
		newK8sClient = newK8sClientOriginal
		getValidatedParams = getValidatedParamsOriginal
		getVolumeUsageInfo = getVolumeUsageInfoOriginal
		retainPV = retainPVOriginal
		deletePVC = deletePVCOriginal
		deletePV = deletePVOriginal
	}()

	request := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "mock-instance",
			Namespace: "mock-namespace",
		},
	}

	t.Run("WhenGettingInstanceFails", func(tt *testing.T) {
		tt.Run("WhenNotFound", func(tt *testing.T) {
			cb := fake.NewClientBuilder().WithInterceptorFuncs(
				interceptor.Funcs{
					Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey,
						obj client.Object, opts ...client.GetOption) error {
						return apierrors.NewNotFound(
							schema.GroupResource{
								Group:    "cns.vmware.com",
								Resource: "cnsunregistervolume",
							}, key.Name)
					},
				},
			)
			reconciler := &Reconciler{
				client: cb.Build(),
			}
			ctx := context.Background()
			res, err := reconciler.Reconcile(ctx, request)
			assert.Nil(tt, err)
			assert.True(tt, res.IsZero(), "Expected no requeue")
		})
		tt.Run("WhenOtherError", func(tt *testing.T) {
			cb := fake.NewClientBuilder().WithInterceptorFuncs(
				interceptor.Funcs{
					Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey,
						obj client.Object, opts ...client.GetOption) error {
						return apierrors.NewInternalError(errors.New("other error"))
					},
				},
			)
			reconciler := &Reconciler{
				client: cb.Build(),
			}
			ctx := context.Background()
			res, err := reconciler.Reconcile(ctx, request)
			assert.NotNil(tt, err)
			assert.True(tt, res.IsZero(), "Expected no requeue")
		})
	})

	t.Run("WhenInstanceIsBeingDeleted", func(tt *testing.T) {
		// Setup
		cb := fake.NewClientBuilder()
		registerSchemes(tt, cb)
		ctx := context.Background()
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "", []string{cnsoptypes.CNSUnregisterVolumeFinalizer}, false, false, false, true)
		registerRuntimeObjects(tt, cb, instance)
		reconciler := &Reconciler{
			client: cb.Build(),
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, request)

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.True(tt, res.IsZero(), "Expected no requeue")
		assert.NotContains(tt, backOffDuration, request, "Expected no backoff duration for instance being deleted")
	})

	t.Run("WhenInstanceIsUnregistered", func(tt *testing.T) {
		// Setup
		cb := fake.NewClientBuilder()
		registerSchemes(tt, cb)
		ctx := context.Background()
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "", nil, false, false, true, false)
		registerRuntimeObjects(tt, cb, instance)
		reconciler := &Reconciler{
			client: cb.Build(),
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, request)

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.True(tt, res.IsZero(), "Expected no requeue")
		assert.NotContains(tt, backOffDuration, request, "Expected no backoff duration for unregistered instance")
	})

	cb := fake.NewClientBuilder()
	registerSchemes(t, cb)
	ctx := context.Background()
	instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "", nil, false, false, false, false)
	registerRuntimeObjects(t, cb, instance)

	t.Run("WhenAddingFinalizerFails", func(tt *testing.T) {
		// Setup
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		cb.WithInterceptorFuncs(
			interceptor.Funcs{
				Update: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					return apierrors.NewInternalError(errors.New("failed to add finalizer"))
				},
			},
		)
		reconciler := &Reconciler{
			client:   cb.Build(),
			recorder: record.NewFakeRecorder(10),
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, request)

		// Assert
		assert.NotNil(tt, err, "Expected an error")
		assert.True(tt, res.IsZero(), "Expected to use default requeue")
	})

	// Reset interceptor funcs to avoid affecting other tests
	cb.WithInterceptorFuncs(interceptor.Funcs{})

	t.Run("WhenParamsAreInvalid", func(tt *testing.T) {
		// Setup
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		getValidatedParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) (*params, error) {
			return nil, errors.New("invalid parameters")
		}
		reconciler := &Reconciler{
			client:   cb.Build(),
			recorder: record.NewFakeRecorder(10),
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      "mock-instance",
				Namespace: "mock-namespace",
			},
		})

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.Equal(tt, res.RequeueAfter, time.Second, "Expected requeue after 1 second")
		assert.Equal(tt, 2*time.Second, backOffDuration[types.NamespacedName{
			Name:      "mock-instance",
			Namespace: "mock-namespace",
		}], "Expected backoff duration to be 2 seconds")
	})

	getValidatedParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) (*params, error) {
		return &params{
			retainFCD: false,
			force:     false,
			namespace: "mock-namespace",
			volumeID:  "mock-volume-id",
			pvcName:   "mock-pvc-name",
			pvName:    "mock-pv-name",
		}, nil
	}

	t.Run("WhenCreatingK8sClientFails", func(tt *testing.T) {
		// Setup
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
			return nil, errors.New("failed to create k8s client")
		}
		reconciler := &Reconciler{
			client:   cb.Build(),
			recorder: record.NewFakeRecorder(10),
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      "mock-instance",
				Namespace: "mock-namespace",
			},
		})

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.Equal(tt, res.RequeueAfter, time.Second, "Expected requeue after 1 second")
		assert.Equal(tt, 2*time.Second, backOffDuration[types.NamespacedName{
			Name:      "mock-instance",
			Namespace: "mock-namespace",
		}], "Expected backoff duration to be 2 seconds")
	})

	newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
		return fakeclientset.NewClientset(), nil
	}

	t.Run("WhenGettingVolumeUsageInfoFails", func(tt *testing.T) {
		// Setup
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		getVolumeUsageInfo = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName string,
			pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
			return nil, errors.New("failed to get volume usage info")
		}
		reconciler := &Reconciler{
			client:   cb.Build(),
			recorder: record.NewFakeRecorder(10),
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      "mock-instance",
				Namespace: "mock-namespace",
			},
		})

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.Equal(tt, res.RequeueAfter, time.Second, "Expected requeue after 1 second")
		assert.Equal(tt, 2*time.Second, backOffDuration[types.NamespacedName{
			Name:      "mock-instance",
			Namespace: "mock-namespace",
		}], "Expected backoff duration to be 2 seconds")
	})

	t.Run("WhenVolumeIsInUse", func(tt *testing.T) {
		// Setup
		getVolumeUsageInfo = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName string,
			pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
			return &volumeUsageInfo{
				isInUse: true,
				pods:    []string{"pod1", "pod2"},
			}, nil
		}
		newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
			return fakeclientset.NewClientset(), nil
		}
		reconciler := &Reconciler{
			client:   cb.Build(),
			recorder: record.NewFakeRecorder(10),
		}
		ctx := context.Background()
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

		// Execute
		res, err := reconciler.Reconcile(ctx, request)

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.Equal(tt, res.RequeueAfter, time.Second, "Expected requeue after 1 second")
		assert.Equal(tt, 2*time.Second, backOffDuration[request.NamespacedName],
			"Expected backoff duration to be 2 seconds")
	})

	getVolumeUsageInfo = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName string,
		pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
		return &volumeUsageInfo{
			isInUse: false,
		}, nil
	}

	t.Run("WhenRetainingPVFails", func(tt *testing.T) {
		// Setup
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		retainPV = func(ctx context.Context, k8sClient kubernetes.Interface, pvName string) error {
			return errors.New("failed to retain PV")
		}
		reconciler := &Reconciler{
			client:   cb.Build(),
			recorder: record.NewFakeRecorder(10),
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, request)

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.Equal(tt, res.RequeueAfter, time.Second, "Expected requeue after 1 second")
		assert.Equal(tt, 2*time.Second, backOffDuration[request.NamespacedName],
			"Expected backoff duration to be 2 seconds")
	})

	retainPV = func(ctx context.Context, k8sClient kubernetes.Interface, pvName string) error {
		return nil
	}

	t.Run("WhenDeletingPVCFails", func(tt *testing.T) {
		// Setup
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		deletePVC = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName string, pvcNamespace string) error {
			return errors.New("failed to delete PVC")
		}
		reconciler := &Reconciler{
			client:   cb.Build(),
			recorder: record.NewFakeRecorder(10),
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, request)

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.Equal(tt, res.RequeueAfter, time.Second, "Expected requeue after 1 second")
		assert.Equal(tt, 2*time.Second, backOffDuration[request.NamespacedName],
			"Expected backoff duration to be 2 seconds")
	})

	deletePVC = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName string, pvcNamespace string) error {
		return nil
	}

	t.Run("WhenDeletingPVFails", func(tt *testing.T) {
		// Setup
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		deletePV = func(ctx context.Context, k8sClient kubernetes.Interface, pvName string) error {
			return errors.New("failed to delete PV")
		}
		reconciler := &Reconciler{
			client:   cb.Build(),
			recorder: record.NewFakeRecorder(10),
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, request)

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.Equal(tt, res.RequeueAfter, time.Second, "Expected requeue after 1 second")
		assert.Equal(tt, 2*time.Second, backOffDuration[request.NamespacedName],
			"Expected backoff duration to be 2 seconds")
	})

	deletePV = func(ctx context.Context, k8sClient kubernetes.Interface, pvName string) error {
		return nil
	}

	t.Run("WhenUnregisteringVolumeFails", func(tt *testing.T) {
		// Setup
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		mockVolManager := volume.NewMockManager(true, errors.New("failed to unregister volume"))
		reconciler := &Reconciler{
			client:        cb.Build(),
			recorder:      record.NewFakeRecorder(10),
			volumeManager: mockVolManager,
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, request)

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.Equal(tt, res.RequeueAfter, time.Second, "Expected requeue after 1 second")
		assert.Equal(tt, 2*time.Second, backOffDuration[request.NamespacedName],
			"Expected backoff duration to be 2 seconds")
	})

	mockVolManager := volume.NewMockManager(false, nil)

	t.Run("WhenUpdatingStatusFails", func(tt *testing.T) {
		// Setup
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		cb.WithInterceptorFuncs(
			interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, client client.Client, subResourceName string,
					obj client.Object, opts ...client.SubResourceUpdateOption) error {
					return apierrors.NewInternalError(errors.New("failed to update status"))
				},
			},
		)
		reconciler := &Reconciler{
			client:        cb.Build(),
			recorder:      record.NewFakeRecorder(10),
			volumeManager: mockVolManager,
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, request)

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.Equal(tt, res.RequeueAfter, time.Second, "Expected requeue after 1 second")
		assert.Equal(tt, 2*time.Second, backOffDuration[request.NamespacedName],
			"Expected backoff duration to be 2 seconds")
	})

	cb.WithInterceptorFuncs(
		interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, client client.Client, subResourceName string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				return nil // Simulate successful update
			},
		},
	)

	t.Run("WhenReconcileSucceeds", func(tt *testing.T) {
		// Setup
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		reconciler := &Reconciler{
			client:        cb.Build(),
			recorder:      record.NewFakeRecorder(10),
			volumeManager: mockVolManager,
		}

		// Execute
		res, err := reconciler.Reconcile(ctx, request)

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.True(tt, res.IsZero(), "Expected no requeue")
		assert.NotContains(tt, backOffDuration, request.NamespacedName,
			"Expected no backoff duration for successful reconciliation")
	})
}

func TestGetValidatedParams(t *testing.T) {
	getPVCNameOriginal := getPVCName
	getPVNameOriginal := getPVName
	getVolumeIDOriginal := getVolumeID
	defer func() {
		getPVCName = getPVCNameOriginal
		getPVName = getPVNameOriginal
		getVolumeID = getVolumeIDOriginal
	}()

	t.Run("WhenVolumeIDAndPVCNameAreEmpty", func(t *testing.T) {
		// Setup
		instance := newInstance(t, "mock-instance", "mock-namespace", "", "", "", nil, false, false, false, false)

		// Execute
		params, err := getValidatedParams(context.Background(), *instance)

		// Assert
		assert.Nil(t, params, "Expected params to be nil")
		assert.NotNil(t, err, "Expected an error")
	})

	t.Run("WhenVolumeIDAndPVCNameAreBothSet", func(t *testing.T) {
		// Setup
		instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "mock-pvc-name", nil, false, false, false, false)

		// Execute
		params, err := getValidatedParams(context.Background(), *instance)

		// Assert
		assert.Nil(t, params, "Expected params to be nil")
		assert.NotNil(t, err, "Expected an error")
	})

	t.Run("WhenVolumeIDIsSet", func(t *testing.T) {
		t.Run("WhenPVCNotFound", func(t *testing.T) {
			// Setup
			getPVCName = func(ctx context.Context, volumeID string) (string, string, error) {
				return "", "", errors.New("PVC not found")
			}
			getPVName = func(ctx context.Context, volumeID string) (string, error) {
				return "mock-pv", nil
			}
			instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "", nil, false, false, false, false)
			exp := params{
				retainFCD: false,
				force:     false,
				namespace: "mock-namespace",
				volumeID:  "mock-volume-id",
				pvcName:   "",
				pvName:    "mock-pv",
			}

			// Execute
			params, err := getValidatedParams(context.Background(), *instance)

			// Assert
			assert.Nil(t, err, "Expected no error")
			assert.Equal(t, exp, *params, "Expected params to match")
		})
		t.Run("WhenPVNotFound", func(t *testing.T) {
			// Setup
			getPVCName = func(ctx context.Context, volumeID string) (string, string, error) {
				return "mock-pvc", "mock-namespace", nil
			}
			getPVName = func(ctx context.Context, volumeID string) (string, error) {
				return "", errors.New("PV not found")
			}
			instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "", nil, false, false, false, false)
			exp := params{
				retainFCD: false,
				force:     false,
				namespace: "mock-namespace",
				volumeID:  "mock-volume-id",
				pvcName:   "mock-pvc",
				pvName:    "",
			}

			// Execute
			params, err := getValidatedParams(context.Background(), *instance)

			// Assert
			assert.Nil(t, err, "Expected no error")
			assert.Equal(t, exp, *params, "Expected params to match")
		})
	})

	t.Run("WhenPVCNameIsSet", func(t *testing.T) {
		t.Run("WhenVolumeIDNotFound", func(t *testing.T) {
			// Setup
			getVolumeID = func(ctx context.Context, namespace string, pvcName string) (string, error) {
				return "", errors.New("VolumeID not found")
			}
			getPVName = func(ctx context.Context, volumeID string) (string, error) {
				return "mock-pv", nil
			}
			instance := newInstance(t, "mock-instance", "mock-namespace", "", "", "mock-pvc-name", nil, false, false, false, false)
			exp := params{
				retainFCD: false,
				force:     false,
				namespace: "mock-namespace",
				volumeID:  "",
				pvcName:   "mock-pvc-name",
				pvName:    "mock-pv",
			}

			// Execute
			params, err := getValidatedParams(context.Background(), *instance)

			// Assert
			assert.Nil(t, err, "Expected no error")
			assert.Equal(t, exp, *params, "Expected params to match")
		})
		t.Run("WhenPVNotFound", func(t *testing.T) {
			// Setup
			getVolumeID = func(ctx context.Context, namespace string, pvcName string) (string, error) {
				return "mock-volume-id", nil
			}
			getPVName = func(ctx context.Context, volumeID string) (string, error) {
				return "", errors.New("PV not found")
			}
			instance := newInstance(t, "mock-instance", "mock-namespace", "", "", "mock-pvc-name", nil, false, false, false, false)
			exp := params{
				retainFCD: false,
				force:     false,
				namespace: "mock-namespace",
				volumeID:  "mock-volume-id",
				pvcName:   "mock-pvc-name",
				pvName:    "",
			}

			// Execute
			params, err := getValidatedParams(context.Background(), *instance)

			// Assert
			assert.Nil(t, err, "Expected no error")
			assert.Equal(t, exp, *params, "Expected params to match")
		})
	})
}

func registerSchemes(t *testing.T, cb *fake.ClientBuilder) {
	t.Helper()
	scheme := runtime.NewScheme()
	schemeBuilder := runtime.NewSchemeBuilder(
		apis.AddToScheme,
		v1.AddToScheme,
	)
	err := schemeBuilder.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	cb.WithScheme(scheme)
}

func registerRuntimeObjects(t *testing.T, cb *fake.ClientBuilder, objs ...client.Object) {
	t.Helper()
	cb.WithObjects(objs...)
	cb.WithStatusSubresource(objs...)
}

func newInstance(t *testing.T, name, namespace, volumeID, errMsg, pvcName string, finalizers []string,
	retainFCD, forceUnregister, unregistered, withDeletionTS bool) *v1a1.CnsUnregisterVolume {
	t.Helper()
	instance := &v1a1.CnsUnregisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Finalizers: finalizers,
		},
		Spec: v1a1.CnsUnregisterVolumeSpec{
			VolumeID:        volumeID,
			PVCName:         pvcName,
			RetainFCD:       retainFCD,
			ForceUnregister: forceUnregister,
		},
		Status: v1a1.CnsUnregisterVolumeStatus{
			Unregistered: unregistered,
			Error:        errMsg,
		},
	}

	if withDeletionTS {
		instance.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	}
	return instance
}
