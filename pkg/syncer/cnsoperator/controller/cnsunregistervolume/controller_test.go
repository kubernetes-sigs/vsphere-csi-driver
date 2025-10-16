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
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	v1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsunregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	cnsoptypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

func TestReconciler_Reconcile(t *testing.T) {
	// function overrides
	getValidatedParamsOriginal := getParams
	getVolumeUsageInfoOriginal := getVolumeUsageInfo
	unregisterVolumeOriginal := unregisterVolume
	backOffDurationOriginal := backOffDuration
	defer func() {
		getParams = getValidatedParamsOriginal
		getVolumeUsageInfo = getVolumeUsageInfoOriginal
		unregisterVolume = unregisterVolumeOriginal
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
			assert.Equal(t, expRequeueAfter*2, backOffDuration[req.NamespacedName],
				"Expected backoff duration to be doubled")
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
		tt.Run("WhenVolumeIsAlreadyUnregistered", func(tt *testing.T) {
			// Setup
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, true, true)
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, false, 0, false, "", "", "")
		})
		tt.Run("WhenGettingVolumeUsageInfoFails", func(tt *testing.T) {
			// Setup
			getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
				return params{}
			}
			getVolumeUsageInfo = func(ctx context.Context,
				pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
				return nil, errors.New("internal error")
			}
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, true)
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, true,
				time.Second, true, "", "", "")
		})
		tt.Run("WhenVolumeIsInUse", func(tt *testing.T) {
			// Setup
			usageInfo := &volumeUsageInfo{
				pods:    []string{"pod"},
				isInUse: true,
			}
			getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
				return params{}
			}
			getVolumeUsageInfo = func(ctx context.Context,
				pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
				return usageInfo, nil
			}
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, true)
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, false, 0, false, "", "", "")
		})
		tt.Run("WhenUnregisteringVolumeFails", func(tt *testing.T) {
			tt.Run("WhenVolumeNotFoundOrOperationNotSupported", func(tt *testing.T) {
				// Setup
				errMsg := "internal server error"
				getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
					return params{}
				}
				getVolumeUsageInfo = func(ctx context.Context,
					pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
					return &volumeUsageInfo{}, nil
				}
				instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
					[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, true)
				unregisterVolume = func(ctx context.Context, volMgr volume.Manager, request reconcile.Request,
					params params) (string, error) {
					return fault.VimFaultNotSupported, errors.New(errMsg)
				}
				reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

				// Execute
				res, err := reconciler.Reconcile(context.Background(), request)

				// Assert
				assertUtil(tt, reconciler, request, res, err, false, 0, false, "", "", "")
			})
			tt.Run("WhenOtherError", func(tt *testing.T) {
				// Setup
				errMsg := "internal server error"
				getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
					return params{}
				}
				getVolumeUsageInfo = func(ctx context.Context,
					pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
					return &volumeUsageInfo{}, nil
				}
				instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
					[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, true)
				unregisterVolume = func(ctx context.Context, volMgr volume.Manager, request reconcile.Request,
					params params) (string, error) {
					return "", errors.New(errMsg)
				}
				reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

				// Execute
				res, err := reconciler.Reconcile(context.Background(), request)

				// Assert
				assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", errMsg, errMsg)
			})
		})
		tt.Run("WhenRemovingFinalizerFails", func(tt *testing.T) {
			// Setup
			errMsg := "internal server error"
			getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
				return params{}
			}
			getVolumeUsageInfo = func(ctx context.Context,
				pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
				return &volumeUsageInfo{}, nil
			}
			unregisterVolume = func(ctx context.Context, volMgr volume.Manager, request reconcile.Request,
				params params) (string, error) {
				return "", nil
			}
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, true)
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{
				Update: func(ctx context.Context, client client.WithWatch,
					obj client.Object, opts ...client.UpdateOption) error {
					return apierrors.NewInternalError(errors.New(errMsg))
				},
			}, nil)
			expStatus := "failed to remove finalizer from the instance"
			expErrMsg := expStatus

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, true, time.Second, true,
				"", expErrMsg, expStatus)
		})
		tt.Run("WhenSuccessful", func(tt *testing.T) {
			// Setup
			getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
				return params{}
			}
			getVolumeUsageInfo = func(ctx context.Context,
				pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
				return &volumeUsageInfo{}, nil
			}
			unregisterVolume = func(ctx context.Context, volMgr volume.Manager, request reconcile.Request,
				params params) (string, error) {
				return "", nil
			}
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
	t.Run("WhenNormalReconcile", func(tt *testing.T) {
		tt.Run("WhenProtectingInstanceFails", func(tt *testing.T) {
			// Setup
			errMsg := "failed to add finalizer"
			getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
				return params{}
			}
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				nil, true, false, false, false)
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{
				Update: func(ctx context.Context, client client.WithWatch,
					obj client.Object, opts ...client.UpdateOption) error {
					return errors.New(errMsg)
				},
			}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", errMsg, errMsg)
		})
		tt.Run("WhenGettingVolumeUsageInfoFails", func(tt *testing.T) {
			// Setup
			errMsg := "failed to get volume usage info"
			getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
				return params{}
			}
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, false)
			getVolumeUsageInfo = func(ctx context.Context,
				pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
				return nil, errors.New(errMsg)
			}
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", errMsg, errMsg)
		})
		tt.Run("WhenVolumeIsInUse", func(tt *testing.T) {
			// Setup
			usageInfo := &volumeUsageInfo{
				pods:    []string{"pod"},
				isInUse: true,
			}
			getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
				return params{volumeID: "mock-volume-id"}
			}
			getVolumeUsageInfo = func(ctx context.Context,
				pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
				return usageInfo, nil
			}
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, false)
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)
			expErr := fmt.Sprintf("volume %s cannot be unregistered because %s", instance.Spec.VolumeID, usageInfo)
			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", expErr, expErr)
		})
		tt.Run("WhenUnregisteringVolumeFails", func(tt *testing.T) {
			tt.Run("WhenVolumeNotFound", func(tt *testing.T) {
				// Setup
				errMsg := "internal server error"
				getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
					return params{}
				}
				getVolumeUsageInfo = func(ctx context.Context,
					pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
					return &volumeUsageInfo{}, nil
				}
				instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
					[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, true)
				unregisterVolume = func(ctx context.Context, volMgr volume.Manager, request reconcile.Request,
					params params) (string, error) {
					return fault.VimFaultNotFound, errors.New(errMsg)
				}
				reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

				// Execute
				res, err := reconciler.Reconcile(context.Background(), request)

				// Assert
				assertUtil(tt, reconciler, request, res, err, false, 0, false, "", "", "")
			})
			tt.Run("WhenOtherError", func(tt *testing.T) {

				// Setup
				errMsg := "internal server error"
				getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
					return params{}
				}
				getVolumeUsageInfo = func(ctx context.Context,
					pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
					return &volumeUsageInfo{}, nil
				}
				instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
					[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, false)
				unregisterVolume = func(ctx context.Context, volMgr volume.Manager, request reconcile.Request,
					params params) (string, error) {
					return "", errors.New(errMsg)
				}
				reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

				// Execute
				res, err := reconciler.Reconcile(context.Background(), request)

				// Assert
				assertUtil(tt, reconciler, request, res, err, true, time.Second, true, "", errMsg, errMsg)
			})
		})
		tt.Run("WhenUpdatingStatusFails", func(tt *testing.T) {
			// Setup
			errMsg := "internal server error"
			getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
				return params{}
			}
			getVolumeUsageInfo = func(ctx context.Context,
				pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
				return &volumeUsageInfo{}, nil
			}
			unregisterVolume = func(ctx context.Context, volMgr volume.Manager, request reconcile.Request,
				params params) (string, error) {
				return "", nil
			}
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, false)
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, client client.Client, subResourceName string,
					obj client.Object, opts ...client.SubResourceUpdateOption) error {
					return apierrors.NewInternalError(errors.New(errMsg))
				},
			}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, true, time.Second, true,
				"", "", "")
		})
		tt.Run("WhenSuccessful", func(tt *testing.T) {
			// Setup
			getParams = func(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
				return params{}
			}
			getVolumeUsageInfo = func(ctx context.Context,
				pvcName, pvcNamespace string, ignoreVMUsage bool) (*volumeUsageInfo, error) {
				return &volumeUsageInfo{}, nil
			}
			unregisterVolume = func(ctx context.Context, volMgr volume.Manager, request reconcile.Request,
				params params) (string, error) {
				return "", nil
			}
			instance := newInstance(tt, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
				[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, true, false, false, false)
			reconciler := setup(tt, []client.Object{instance}, interceptor.Funcs{}, nil)

			// Execute
			res, err := reconciler.Reconcile(context.Background(), request)

			// Assert
			assertUtil(tt, reconciler, request, res, err, false, 0, false, "", "", "")
		})
	})
}

func TestUnregisterVolume(t *testing.T) {
	// function overrides
	newK8sClientOriginal := newK8sClient
	protectPVCOriginal := protectPVC
	retainPVOriginal := retainPV
	deletePVCOriginal := deletePVC
	deletePVOriginal := deletePV
	removeFinalizerFromPVCOriginal := removeFinalizerFromPVC
	defer func() {
		newK8sClient = newK8sClientOriginal
		protectPVC = protectPVCOriginal
		retainPV = retainPVOriginal
		deletePVC = deletePVCOriginal
		deletePV = deletePVOriginal
		removeFinalizerFromPVC = removeFinalizerFromPVCOriginal
	}()

	request := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "mock-instance",
			Namespace: "mock-namespace",
		},
	}

	t.Run("WhenGettingK8sClientFails", func(tt *testing.T) {
		// Setup
		errMsg := "internal server error"
		newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
			return nil, errors.New(errMsg)
		}
		expErr := "failed to init K8s client for volume unregistration"

		// Execute
		_, err := unregisterVolume(context.Background(), nil, request, params{})

		// Assert
		assert.Equal(tt, expErr, err.Error(), "Expected error to match")
	})
	t.Run("WhenProtectingPVCFails", func(tt *testing.T) {
		// Setup
		errMsg := "internal server error"
		newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
			return &clientset.Clientset{}, nil
		}
		protectPVC = func(ctx context.Context, k8sClient clientset.Interface, pvcName, pvcNamespace,
			finalizer string) error {
			return errors.New(errMsg)
		}
		params := params{
			pvcName:   "mock-pvc",
			namespace: "mock-namespace",
		}
		expErr := fmt.Sprintf("failed to protect associated PVC %s/%s", params.namespace, params.pvcName)

		// Execute
		_, err := unregisterVolume(context.Background(), nil, request, params)

		// Assert
		assert.Equal(tt, expErr, err.Error(), "Expected error to match")
	})
	t.Run("WhenRetainingPVFails", func(tt *testing.T) {
		// Setup
		errMsg := "internal server error"
		newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
			return &clientset.Clientset{}, nil
		}
		retainPV = func(ctx context.Context, k8sClient clientset.Interface, pvName string) error {
			return errors.New(errMsg)
		}
		params := params{pvName: "mock-pv"}
		expErr := fmt.Sprintf("failed to set reclaim policy to Retain on associated PV %s", params.pvName)

		// Execute
		_, err := unregisterVolume(context.Background(), nil, request, params)

		// Assert
		assert.Equal(tt, expErr, err.Error(), "Expected error to match")
	})
	t.Run("WhenDeletingPVCFails", func(tt *testing.T) {
		// Setup
		errMsg := "internal server error"
		newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
			return &clientset.Clientset{}, nil
		}
		protectPVC = func(ctx context.Context, k8sClient clientset.Interface, pvcName, pvcNamespace,
			finalizer string) error {
			return nil
		}
		deletePVC = func(ctx context.Context, k8sClient clientset.Interface, pvcName, pvcNamespace string) error {
			return errors.New(errMsg)
		}
		params := params{
			pvcName:   "mock-pvc",
			namespace: "mock-namespace",
		}
		expErr := fmt.Sprintf("failed to delete associated PVC %s/%s", params.namespace, params.pvcName)

		// Execute
		_, err := unregisterVolume(context.Background(), nil, request, params)

		// Assert
		assert.Equal(tt, expErr, err.Error(), "Expected error to match")
	})
	t.Run("WhenDeletingPVFails", func(tt *testing.T) {
		// Setup
		errMsg := "internal server error"
		newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
			return &clientset.Clientset{}, nil
		}
		retainPV = func(ctx context.Context, k8sClient clientset.Interface, pvName string) error {
			return nil
		}
		deletePV = func(ctx context.Context, k8sClient clientset.Interface, pvName string) error {
			return errors.New(errMsg)
		}
		params := params{pvName: "mock-pv"}
		expErr := fmt.Sprintf("failed to delete associated PV %s", params.pvName)

		// Execute
		_, err := unregisterVolume(context.Background(), nil, request, params)

		// Assert
		assert.Equal(tt, expErr, err.Error(), "Expected error to match")
	})
	t.Run("WhenUnregisteringVolumeFromCNSFails", func(tt *testing.T) {
		// Setup
		errMsg := "internal server error"
		newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
			return &clientset.Clientset{}, nil
		}
		mockMgr := volume.NewMockManager(true, errors.New(errMsg), fault.VimFaultCNSFault)
		params := params{volumeID: "mock-volume-id"}
		expErr := fmt.Sprintf("failed to unregister associated volume %s", params.volumeID)

		// Execute
		_, err := unregisterVolume(context.Background(), mockMgr, request, params)

		// Assert
		assert.Equal(tt, expErr, err.Error(), "Expected error to match")
	})
	t.Run("WhenRemovingFinalizerFromPVCFails", func(tt *testing.T) {
		// Setup
		errMsg := "internal server error"
		newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
			return &clientset.Clientset{}, nil
		}
		protectPVC = func(ctx context.Context, k8sClient clientset.Interface, pvcName, pvcNamespace,
			finalizer string) error {
			return nil
		}
		deletePVC = func(ctx context.Context, k8sClient clientset.Interface, pvcName, pvcNamespace string) error {
			return nil
		}
		removeFinalizerFromPVC = func(ctx context.Context, k8sClient clientset.Interface,
			pvcName, pvcNamespace, finalizer string) error {
			return errors.New(errMsg)
		}
		mockMgr := volume.NewMockManager(true, nil, "")

		params := params{pvcName: "mock-pvc", namespace: "mock-namespace"}
		expErr := fmt.Sprintf("failed to remove finalizer from associated PVC %s/%s", params.namespace, params.pvcName)

		// Execute
		_, err := unregisterVolume(context.Background(), mockMgr, request, params)

		// Assert
		assert.Equal(tt, expErr, err.Error(), "Expected error to match")
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		// Setup
		newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
			return &clientset.Clientset{}, nil
		}
		protectPVC = func(ctx context.Context, k8sClient clientset.Interface, pvcName, pvcNamespace,
			finalizer string) error {
			return nil
		}
		retainPV = func(ctx context.Context, k8sClient clientset.Interface, pvName string) error {
			return nil
		}
		deletePVC = func(ctx context.Context, k8sClient clientset.Interface, pvcName, pvcNamespace string) error {
			return nil
		}
		deletePV = func(ctx context.Context, k8sClient clientset.Interface, pvName string) error {
			return nil
		}
		removeFinalizerFromPVC = func(ctx context.Context, k8sClient clientset.Interface,
			pvcName, pvcNamespace, finalizer string) error {
			return nil
		}
		mockMgr := volume.NewMockManager(false, nil, "")
		params := params{
			pvcName:   "mock-pvc",
			pvName:    "mock-pv",
			namespace: "mock-namespace",
			volumeID:  "mock-volume-id",
		}

		// Execute
		_, err := unregisterVolume(context.Background(), mockMgr, request, params)

		// Assert
		assert.Nil(tt, err, "Expected no error")
	})
}

func TestGetParams(t *testing.T) {
	// function overrides
	getPVCNameOriginal := getPVCName
	getPVNameOriginal := getPVName
	getVolumeIDOriginal := getVolumeID
	defer func() {
		getPVCName = getPVCNameOriginal
		getPVName = getPVNameOriginal
		getVolumeID = getVolumeIDOriginal
	}()

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
			params := getParams(context.Background(), *instance)

			// Assert
			assert.Equal(t, expParams, params, "Expected params to match")
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
			params := getParams(context.Background(), *instance)

			// Assert
			assert.Equal(t, expParams, params, "Expected params to match")
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
			params := getParams(context.Background(), *instance)

			// Assert
			assert.Equal(t, expParams, params, "Expected params to match")
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
			params := getParams(context.Background(), *instance)

			// Assert
			assert.Equal(t, expParams, params, "Expected params to match")
		})
	})
}

func TestProtectInstance(t *testing.T) {
	t.Run("WhenFinalizerAlreadyPresent", func(t *testing.T) {
		// Setup
		instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, false, false, false, false)
		c := newClient(t, []client.Object{instance}, interceptor.Funcs{})

		// Execute
		err := protectInstance(context.Background(), c, instance)

		// Assert
		assert.Nil(t, err, "Expected no error")
		assert.Contains(t, instance.Finalizers, cnsoptypes.CNSUnregisterVolumeFinalizer,
			"Expected finalizer to be present")
	})
	t.Run("WhenAddingFinalizerFails", func(t *testing.T) {
		// Setup
		errMsg := "internal server error"
		instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		c := newClient(t, []client.Object{instance}, interceptor.Funcs{
			Update: func(ctx context.Context, client client.WithWatch,
				obj client.Object, opts ...client.UpdateOption) error {
				instance.Finalizers = []string{} // reset finalizers to simulate failure
				return errors.New(errMsg)
			},
		})

		// Execute
		err := protectInstance(context.Background(), c, instance)

		// Assert
		assert.NotNil(t, err, "Expected an error")
		assert.Equal(t, errMsg, err.Error(), "Expected error to match")
		assert.NotContains(t, instance.Finalizers, cnsoptypes.CNSUnregisterVolumeFinalizer,
			"Expected finalizer to not be present")
	})
	t.Run("WhenSuccessful", func(t *testing.T) {
		// Setup
		instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		c := newClient(t, []client.Object{instance}, interceptor.Funcs{})

		// Execute
		err := protectInstance(context.Background(), c, instance)

		// Assert
		assert.Nil(t, err, "Expected no error")
		assert.Contains(t, instance.Finalizers, cnsoptypes.CNSUnregisterVolumeFinalizer,
			"Expected finalizer to be added")
	})
}

func TestRemoveFinalizer(t *testing.T) {
	t.Run("WhenFinalizerNotPresent", func(t *testing.T) {
		// Setup
		instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			nil, false, false, false, false)
		c := newClient(t, []client.Object{instance}, interceptor.Funcs{})

		// Execute
		err := removeFinalizer(context.Background(), c, instance)

		// Assert
		assert.Nil(t, err, "Expected no error")
		assert.NotContains(t, instance.Finalizers, cnsoptypes.CNSUnregisterVolumeFinalizer,
			"Expected finalizer to not be present")
	})
	t.Run("WhenRemovingFinalizerFails", func(t *testing.T) {
		// Setup
		errMsg := "internal server error"
		instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, false, false, false, false)
		c := newClient(t, []client.Object{instance}, interceptor.Funcs{
			Update: func(ctx context.Context, client client.WithWatch,
				obj client.Object, opts ...client.UpdateOption) error {
				// reset finalizers to simulate failure
				instance.Finalizers = []string{cnsoptypes.CNSUnregisterVolumeFinalizer}
				return errors.New(errMsg)
			},
		})

		// Execute
		err := removeFinalizer(context.Background(), c, instance)

		// Assert
		assert.NotNil(t, err, "Expected an error")
		assert.Equal(t, errMsg, err.Error(), "Expected error to match")
		assert.Contains(t, instance.Finalizers, cnsoptypes.CNSUnregisterVolumeFinalizer,
			"Expected finalizer to be present")
	})
	t.Run("WhenSuccessful", func(t *testing.T) {
		// Setup
		instance := newInstance(t, "mock-instance", "mock-namespace", "mock-volume-id", "", "",
			[]string{cnsoptypes.CNSUnregisterVolumeFinalizer}, false, false, false, false)
		c := newClient(t, []client.Object{instance}, interceptor.Funcs{})

		// Execute
		err := removeFinalizer(context.Background(), c, instance)

		// Assert
		assert.Nil(t, err, "Expected no error")
		assert.NotContains(t, instance.Finalizers, cnsoptypes.CNSUnregisterVolumeFinalizer,
			"Expected finalizer to be removed")
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
