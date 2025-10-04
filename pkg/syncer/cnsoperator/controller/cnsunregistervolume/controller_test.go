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
	"fmt"
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
	cnsoptypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

func TestReconciler_Reconcile(t *testing.T) {
	// function overrides
	newK8sClientOriginal := newK8sClient
	getValidatedParamsOriginal := getValidatedParams
	getVolumeUsageInfoOriginal := getVolumeUsageInfo
	protectPVCOriginal := protectPVC
	deletePVCOriginal := deletePVC
	removeFinalizerFromPVCOriginal := removeFinalizerFromPVC
	backOffDurationOriginal := backOffDuration
	defer func() {
		newK8sClient = newK8sClientOriginal
		getValidatedParams = getValidatedParamsOriginal
		getVolumeUsageInfo = getVolumeUsageInfoOriginal
		protectPVC = protectPVCOriginal
		deletePVC = deletePVCOriginal
		removeFinalizerFromPVC = removeFinalizerFromPVCOriginal
		backOffDuration = backOffDurationOriginal
	}()

	setup := func(t *testing.T, runtimeObjs []client.Object, interceptorFuncs interceptor.Funcs,
		volumeManager volume.Manager) *Reconciler {
		t.Helper()
		c := newClient(t, runtimeObjs, interceptorFuncs)
		backOffDuration = make(map[types.NamespacedName]time.Duration)
		return &Reconciler{
			client:        c,
			recorder:      record.NewFakeRecorder(10),
			volumeManager: volumeManager,
		}
	}
	assertUtil := func(t *testing.T, r *Reconciler, req reconcile.Request, res reconcile.Result, err error,
		expRequeue bool, expRequeueAfter time.Duration, expBackoff bool, expErr, expEvent, expStatus string) {
		t.Helper()

		if expErr == "" {
			assert.Nil(t, err, "Expected no error")
		} else {
			assert.NotNil(t, err, "Expected an error")
		}

		if expRequeue {
			assert.Equal(t, expRequeueAfter, res.RequeueAfter, "Expected requeue after duration")
		} else {
			assert.True(t, res.IsZero(), "Expected no requeue")
		}

		if expBackoff {
			assert.Equal(t, expRequeueAfter*2, backOffDuration[req.NamespacedName], "Expected backoff duration to be doubled")
		} else {
			assert.NotContains(t, backOffDuration, req.NamespacedName, "Expected no backoff duration")
		}

		if expEvent != "" {
			if r.recorder == nil {
				t.Fatal("Expected recorder to be initialized")
			}

			event, ok := <-r.recorder.(*record.FakeRecorder).Events
			if !ok {
				t.Fatal("Expected an event but none found")
			}

			assert.Contains(t, event, expEvent)
		}

		if expStatus != "" {
			var updatedInstance v1a1.CnsUnregisterVolume
			err := r.client.Get(context.Background(), req.NamespacedName, &updatedInstance)
			if err != nil {
				t.Fatalf("Failed to get updated instance: %v", err)
			}

			assert.Equal(t, expStatus, updatedInstance.Status.Error, "Expected status error to match")
		}
	}

	request := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "mock-instance",
			Namespace: "mock-namespace",
		},
	}

	t.Run("WhenGettingInstanceFails", func(tt *testing.T) {
		tt.Run("WhenNotFound", func(tt *testing.T) {
			// Setup
			reconciler := setup(tt, nil, interceptor.Funcs{
				Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey,
					obj client.Object, opts ...client.GetOption) error {
					return apierrors.NewNotFound(
						schema.GroupResource{
							Group:    "cns.vmware.com",
							Resource: "cnsunregistervolume",
						}, key.Name)
				},
			}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, false, 0, false, "", "", "")
		})
		tt.Run("WhenOtherError", func(tt *testing.T) {
			// Setup
			reconciler := setup(tt, nil, interceptor.Funcs{
				Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey,
					obj client.Object, opts ...client.GetOption) error {
					return apierrors.NewInternalError(errors.New("other error"))
				},
			}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, false, time.Second, false, "other error", "", "")
		})
	})

	t.Run("WhenInstanceIsBeingDeleted", func(tt *testing.T) {
		tt.Run("WhenRemovingFinalizerFails", func(tt *testing.T) {
			// Setup
			errMsg := "failed to remove finalizer"
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, true)
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{
				Update: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					return apierrors.NewInternalError(errors.New("failed to remove finalizer"))
				},
			}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, false, time.Second,
				false, errMsg, "", "")
		})
		tt.Run("WhenRemovingFinalizerSucceeds", func(tt *testing.T) {
			// Setup
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, false, false, false, true)
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, false, 0, false, "", "", "")
		})
	})

	t.Run("WhenInstanceIsUnregistered", func(tt *testing.T) {
		// Setup
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, true, false)
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assertUtil(tt, reconciler, request, res, err, false, 0, false, "", "", "")
	})

	t.Run("WhenAddingFinalizerFails", func(tt *testing.T) {
		// Setup
		instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{
			Update: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				return apierrors.NewInternalError(errors.New("failed to add finalizer"))
			},
		}, nil)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assertUtil(tt, reconciler, request, res, err, false, 0, false, "failed to add finalizer", "", "")
	})

	t.Run("WhenParamsAreInvalid", func(tt *testing.T) {
		// Setup
		errMsg := "invalid parameters"
		instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		getValidatedParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) (*params, error) {
			return nil, errors.New(errMsg)
		}
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", errMsg, errMsg)
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
		errMsg := "failed to init K8s client for volume unregistration"
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
			return nil, errors.New(errMsg)
		}
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", errMsg, errMsg)
	})

	newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
		return fakeclientset.NewClientset(), nil
	}

	t.Run("WhenGettingVolumeUsageInfoFails", func(tt *testing.T) {
		// Setup
		errMsg := "failed to get volume usage info"
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		getVolumeUsageInfo = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName string,
			pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
			return nil, errors.New(errMsg)
		}
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      "mock-instance",
				Namespace: "mock-namespace",
			},
		})

		// Assert
		assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", errMsg, errMsg)
	})

	t.Run("WhenVolumeIsInUse", func(tt *testing.T) {
		// Setup
		usageInfo := &volumeUsageInfo{
			isInUse: true,
			pods:    []string{"pod1", "pod2"},
		}
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		getVolumeUsageInfo = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName string,
			pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
			return usageInfo, nil
		}
		newK8sClient = func(ctx context.Context) (kubernetes.Interface, error) {
			return fakeclientset.NewClientset(), nil
		}
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		expStatus := fmt.Sprintf("volume %s cannot be unregistered because %s", instance.Spec.VolumeID, usageInfo)
		assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", usageInfo.String(), expStatus)
	})

	getVolumeUsageInfo = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName string,
		pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
		return &volumeUsageInfo{}, nil
	}

	t.Run("WhenAddingFinalizerOnPVCFails", func(tt *testing.T) {
		// Setup
		errMsg := "failed to add finalizer on PVC"
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		protectPVC = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName, pvcNamespace,
			finalizer string) error {
			return errors.New("failed to add finalizer on PVC")
		}
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assertUtil(tt, reconciler, request, res, err, true, time.Second,
			true, "", errMsg, errMsg)
	})

	protectPVC = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName, pvcNamespace, finalizer string) error {
		return nil
	}

	t.Run("WhenDeletingPVCFails", func(tt *testing.T) {
		// Setup
		errMsg := "failed to delete PVC"
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		deletePVC = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName string, pvcNamespace string) error {
			return errors.New(errMsg)
		}
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assert.Nil(tt, err, "Expected no error")
		assert.Equal(tt, res.RequeueAfter, time.Second, "Expected requeue after 1 second")
		assert.Equal(tt, 2*time.Second, backOffDuration[request.NamespacedName],
			"Expected backoff duration to be 2 seconds")

		assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", errMsg, errMsg)
	})

	deletePVC = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName string, pvcNamespace string) error {
		return nil
	}

	t.Run("WhenDeletingPVFails", func(tt *testing.T) {
		// Setup
		errMsg := "failed to delete PV"
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		deletePV = func(ctx context.Context, k8sClient kubernetes.Interface, pvName string) error {
			return errors.New(errMsg)
		}
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", errMsg, errMsg)
	})

	deletePV = func(ctx context.Context, k8sClient kubernetes.Interface, pvName string) error {
		return nil
	}

	t.Run("WhenUnregisteringVolumeFails", func(tt *testing.T) {
		// Setup
		errMsg := "failed to unregister volume"
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		mockVolManager := volume.NewMockManager(true, errors.New(errMsg))
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, mockVolManager)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", errMsg, errMsg)
	})

	mockVolManager := volume.NewMockManager(false, nil)

	t.Run("WhenRemovingFinalizerFromPVCFails", func(tt *testing.T) {
		// Setup
		errMsg := "failed to remove finalizer from PVC"
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		removeFinalizerFromPVC = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName, pvcNamespace,
			finalizer string) error {
			return errors.New(errMsg)
		}
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, mockVolManager)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assertUtil(tt, reconciler, request, res, err, true, time.Second,
			true, "", errMsg, errMsg)
	})

	removeFinalizerFromPVC = func(ctx context.Context, k8sClient kubernetes.Interface, pvcName, pvcNamespace,
		finalizer string) error {
		return nil
	}

	t.Run("WhenUpdatingStatusFails", func(tt *testing.T) {
		// Setup
		errMsg := "failed to update status"
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, client client.Client, subResourceName string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				return apierrors.NewInternalError(errors.New(errMsg))
			},
		}, mockVolManager)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", "", "")
	})

	t.Run("WhenReconcileSucceeds", func(tt *testing.T) {
		// Setup
		instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, client client.Client, subResourceName string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				return nil // Simulate successful update
			},
		}, mockVolManager)

		// Execute
		res, err := reconciler.Reconcile(context.Background(), request)

		// Assert
		assertUtil(tt, reconciler, request, res, err, false, 0, false, "", "", "")
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
		instance := newInstance(t, "mock-instance", "mock-namespace", "", "", "",
			nil, false, false, false, false)

		// Execute
		params, err := getValidatedParams(context.Background(), *instance)

		// Assert
		assert.Nil(t, params, "Expected params to be nil")
		assert.NotNil(t, err, "Expected an error")
	})

	t.Run("WhenVolumeIDAndPVCNameAreBothSet", func(t *testing.T) {
		// Setup
		instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "mock-pvc-name",
			nil, false, false, false, false)

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
			instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				nil, false, false, false, false)
			expParams := params{
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
			assert.Equal(t, expParams, *params, "Expected params to match")
		})
		t.Run("WhenPVNotFound", func(t *testing.T) {
			// Setup
			cb := fake.NewClientBuilder()
			registerSchemes(t, cb)
			getPVCName = func(ctx context.Context, volumeID string) (string, string, error) {
				return "mock-pvc", "mock-namespace", nil
			}
			getPVName = func(ctx context.Context, volumeID string) (string, error) {
				return "", errors.New("PV not found")
			}
			instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				nil, false, false, false, false)
			expParams := params{
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
			assert.Equal(t, expParams, *params, "Expected params to match")
		})
	})

	t.Run("WhenPVCNameIsSet", func(t *testing.T) {
		t.Run("WhenVolumeIDNotFound", func(t *testing.T) {
			// Setup
			getVolumeID = func(ctx context.Context, pvcName, namespace string) (string, error) {
				return "", errors.New("VolumeID not found")
			}
			getPVName = func(ctx context.Context, volumeID string) (string, error) {
				return "mock-pv", nil
			}
			instance := newInstance(t, "mock-instance", "mock-namespace", "", "", "mock-pvc-name",
				nil, false, false, false, false)
			expParams := params{
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
			assert.Equal(t, expParams, *params, "Expected params to match")
		})
		t.Run("WhenPVNotFound", func(t *testing.T) {
			// Setup
			getVolumeID = func(ctx context.Context, pvcName, namespace string) (string, error) {
				return "mock-volume-id", nil
			}
			getPVName = func(ctx context.Context, volumeID string) (string, error) {
				return "", errors.New("PV not found")
			}
			instance := newInstance(t, "mock-instance", "mock-namespace", "", "", "mock-pvc-name",
				nil, false, false, false, false)
			expParams := params{
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
			assert.Equal(t, expParams, *params, "Expected params to match")
		})
	})
}

func newClient(t *testing.T, runtimeObjs []client.Object, interceptorFuncs interceptor.Funcs) client.Client {
	t.Helper()
	cb := fake.NewClientBuilder()
	registerSchemes(t, cb)
	registerRuntimeObjects(t, cb, runtimeObjs...)
	cb.WithInterceptorFuncs(interceptorFuncs)
	return cb.Build()
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
