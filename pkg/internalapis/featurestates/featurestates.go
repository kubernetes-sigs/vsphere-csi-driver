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

package featurestates

import (
	"context"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis"
	featurestatesv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/featurestates/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

const (
	// CRDName represent the name of cnscsisvfeaturestate CRD.
	CRDName = "cnscsisvfeaturestates.cns.vmware.com"
	// CRDGroupName represent the group of cnscsisvfeaturestate CRD.
	CRDGroupName = "cns.vmware.com"
	// CRDSingular represent the singular name of cnscsisvfeaturestate CRD.
	CRDSingular = "cnscsisvfeaturestate"
	// CRDPlural represent the plural name of cnscsisvfeaturestates CRD.
	CRDPlural = "cnscsisvfeaturestates"
	// WorkLoadNamespaceLabelKey is the label key found on the workload namespace
	// in the supervisor k8s cluster.
	WorkLoadNamespaceLabelKey = "vSphereClusterID"
	// SVFeatureStateCRName to be used for CR in workload namespaces.
	SVFeatureStateCRName = "svfeaturestates"
	// crUpdateRetryInterval is the interval at which pending CR update/create
	// tasks are executed.
	crUpdateRetryInterval = 30 * time.Second
)

// pendingCRUpdates holds latest states of the namespaces to push CR update,
// latest feature states and the lock to operate on namespaceUpdateMap and
// latestFeatureStates.
type pendingCRUpdates struct {
	// lock for updating namespaceUpdateMap and latestFeatureStates.
	lock *sync.RWMutex
	// Holds namespace name and status to update CR in the namespace.
	// True means update is pending, false means no update is required.
	namespaceUpdateMap map[string]bool
	// Latest featureStates.
	latestFeatureStates []featurestatesv1alpha1.FeatureState
}

var (
	supervisorFeatureStatConfigMapName       string
	supervisorFeatureStateConfigMapNamespace string
	pendingCRUpdatesObj                      *pendingCRUpdates
	k8sClient                                clientset.Interface
	controllerRuntimeClient                  client.Client
)

// StartSvFSSReplicationService Starts SvFSSReplicationService.
func StartSvFSSReplicationService(ctx context.Context, svFeatureStatConfigMapName string,
	svFeatureStateConfigMapNamespace string) error {
	log := logger.GetLogger(ctx)
	log.Info("Starting SvFSSReplicationService")

	supervisorFeatureStatConfigMapName = svFeatureStatConfigMapName
	supervisorFeatureStateConfigMapNamespace = svFeatureStateConfigMapNamespace
	pendingCRUpdatesObj = &pendingCRUpdates{
		lock:                &sync.RWMutex{},
		namespaceUpdateMap:  make(map[string]bool),
		latestFeatureStates: make([]featurestatesv1alpha1.FeatureState, 0),
	}
	var err error
	// This is idempotent if CRD is pre-created then we continue with
	// initialization of svFSSReplicationService.
	err = k8s.CreateCustomResourceDefinitionFromSpec(ctx, CRDName, CRDSingular, CRDPlural,
		reflect.TypeOf(featurestatesv1alpha1.CnsCsiSvFeatureStates{}).Name(), CRDGroupName,
		internalapis.SchemeGroupVersion.Version, apiextensionsv1beta1.NamespaceScoped)
	if err != nil {
		log.Errorf("failed to create CnsCsiSvFeatureStates CRD. Error: %v", err)
		return err
	}

	// Create the kubernetes client.
	k8sClient, err = k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("create k8s client failed. Err: %v", err)
		return err
	}
	// Get kube config.
	config, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("failed to get kubeconfig. Error: %v", err)
		return err
	}
	// Create controller runtime client.
	controllerRuntimeClient, err = k8s.NewClientForGroup(ctx, config, CRDGroupName)
	if err != nil {
		log.Errorf("failed to create controllerRuntimeClient. Err: %v", err)
		return err
	}

	// Create k8s Informer and watch on configmaps and namespaces.
	informer := k8s.NewInformer(k8sClient)
	// Configmap informer to watch on SV featurestate config-map.
	informer.AddConfigMapListener(ctx, k8sClient, svFeatureStateConfigMapNamespace,
		// Add.
		func(Obj interface{}) {
			configMapAdded(Obj)
		},
		// Update.
		func(oldObj interface{}, newObj interface{}) {
			configMapUpdated(oldObj, newObj)
		},
		// Delete.
		func(obj interface{}) {
			configMapDeleted(obj)
		})

	// Namespace informer to watch on namespaces.
	informer.AddNamespaceListener(
		// Add.
		func(obj interface{}) {
			namespaceAdded(obj)
		},
		// Update.
		func(oldObj interface{}, newObj interface{}) {
			namespaceUpdated(oldObj, newObj)
		},
		// Delete.
		func(obj interface{}) {
			namespaceDeleted(obj)
		})
	informer.Listen()
	log.Infof("Informer on config-map and namespaces started")

	// Create a dynamic informer for the cnscsisvfeaturestates CR.
	dynInformer, err := k8s.GetDynamicInformer(ctx, CRDGroupName, internalapis.SchemeGroupVersion.Version,
		CRDPlural, metav1.NamespaceAll, config, true)
	if err != nil {
		log.Errorf("failed to create dynamic informer for %s CR. Error: %+v", CRDPlural, err)
		return err
	}
	dynInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		// Add.
		AddFunc: nil,
		// Update.
		UpdateFunc: nil,
		// Delete.
		DeleteFunc: func(obj interface{}) {
			fssCRDeleted(obj)
		},
	})
	go func() {
		log.Infof("Informer to watch on %s CR starting..", CRDPlural)
		dynInformer.Informer().Run(make(chan struct{}))
	}()

	// Start routine to process pending feature state updates.
	go pendingCRUpdatesObj.processPendingCRUpdates()
	log.Infof("Started background routine to process pending feature state updates at regular interval")
	log.Infof("SvFSSReplicationService is running")
	var stopCh = make(chan bool)
	<-stopCh
	return nil
}

// processPendingCRUpdates helps process pending CR updates at regular interval.
func (pendingCRUpdatesObj *pendingCRUpdates) processPendingCRUpdates() {
	ticker := time.NewTicker(crUpdateRetryInterval)
	for range ticker.C {
		func() {
			pendingCRUpdatesObj.lock.Lock()
			defer pendingCRUpdatesObj.lock.Unlock()

			ctx, log := logger.GetNewContextWithLogger()
			var err error
			if len(pendingCRUpdatesObj.latestFeatureStates) == 0 {
				log.Warn("empty feature states observed. feature state config-map might be deleted. " +
					"Trying to obtain latest feature states.")
				pendingCRUpdatesObj.latestFeatureStates, err = getFeatureStates(ctx)
				if err != nil {
					log.Errorf("failed to get feature states. error: %v", err)
					return
				}
			}
			for namespace, updateRequired := range pendingCRUpdatesObj.namespaceUpdateMap {
				if !updateRequired {
					log.Debugf("CR update is not required for the namespace: %q", namespace)
					continue
				}
				log.Infof("Feature state update is required for namespace: %q", namespace)
				// Check if CR is present on the namespace.
				featurestateCR := &featurestatesv1alpha1.CnsCsiSvFeatureStates{}
				err := controllerRuntimeClient.Get(ctx, client.ObjectKey{Name: SVFeatureStateCRName,
					Namespace: namespace}, featurestateCR)
				if err == nil {
					// Attempt to Update CR.
					featurestateCR.Spec.FeatureStates = pendingCRUpdatesObj.latestFeatureStates
					err = controllerRuntimeClient.Update(ctx, featurestateCR)
					if err != nil {
						log.Errorf("failed to update cnsCsiSvFeatureStates CR instance in the namespace: %q, Err: %v",
							namespace, err)
						continue
					}
					pendingCRUpdatesObj.namespaceUpdateMap[namespace] = false
					log.Infof("Updated cnsCsiSvFeatureStates CR instance in the namespace: %q", namespace)
				} else {
					if apierrors.IsNotFound(err) {
						log.Infof("cnsCsiSvFeatureStates CR instance is not present in the namespace: %q. "+
							"Creating CR with latest feature switch state, Err: %v", namespace, err)
						// Attempt to Create the CR.
						cnsCsiSvFeatureStates := &featurestatesv1alpha1.CnsCsiSvFeatureStates{
							ObjectMeta: metav1.ObjectMeta{Name: SVFeatureStateCRName, Namespace: namespace},
							Spec: featurestatesv1alpha1.CnsCsiSvFeatureStatesSpec{
								FeatureStates: pendingCRUpdatesObj.latestFeatureStates,
							},
						}
						err = controllerRuntimeClient.Create(ctx, cnsCsiSvFeatureStates)
						if err != nil {
							log.Errorf("failed to create cnsCsiSvFeatureStates CR instance in the "+
								"namespace: %q. Continuing the FSS replication to other namespaces.., Err: %v",
								namespace, err)
							continue
						}
						pendingCRUpdatesObj.namespaceUpdateMap[namespace] = false
						log.Infof("Created cnsCsiSvFeatureStates CR instance in the namespace: %q", namespace)
					} else {
						log.Errorf("failed to check if cnsCsiSvFeatureStates CR is present in the namespace :%q, err: %v",
							namespace, err)
					}
				}
			}
		}()
	}
}

// enqueueFeatureStateUpdatesForAllWorkloadNamespaces helps enqueue
// featurestates updates for all workload namespaces.
func (pendingCRUpdatesObj *pendingCRUpdates) enqueueFeatureStateUpdatesForAllWorkloadNamespaces(
	ctx context.Context, featurestates []featurestatesv1alpha1.FeatureState) {
	pendingCRUpdatesObj.lock.Lock()
	defer pendingCRUpdatesObj.lock.Unlock()

	log := logger.GetLogger(ctx)
	pendingCRUpdatesObj.latestFeatureStates = featurestates
	for namespace := range pendingCRUpdatesObj.namespaceUpdateMap {
		pendingCRUpdatesObj.namespaceUpdateMap[namespace] = true
	}
	log.Infof("Enqueued CR updates for all workload namespaces")
}

// enqueueFeatureStateUpdatesForWorkloadNamespace enqueues CR updates for
// specified workload namespaces.
func (pendingCRUpdatesObj *pendingCRUpdates) enqueueFeatureStateUpdatesForWorkloadNamespace(
	ctx context.Context, namespace string) {
	pendingCRUpdatesObj.lock.Lock()
	defer pendingCRUpdatesObj.lock.Unlock()

	log := logger.GetLogger(ctx)
	pendingCRUpdatesObj.namespaceUpdateMap[namespace] = true
	log.Infof("Enqueued CR updates for workload namespace: %q", namespace)
}

// configMapAdded is called when configmap is created.
func configMapAdded(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	fssConfigMap, ok := obj.(*v1.ConfigMap)
	if fssConfigMap == nil || !ok {
		log.Warnf("configMapAdded: unrecognized object %+v", obj)
		return
	}
	if fssConfigMap.Name == supervisorFeatureStatConfigMapName &&
		fssConfigMap.Namespace == supervisorFeatureStateConfigMapNamespace {
		log.Infof("Observed fss add: fss: %+v", fssConfigMap.Data)
		var err error
		var featureStates []featurestatesv1alpha1.FeatureState
		for feature, state := range fssConfigMap.Data {
			var featureState featurestatesv1alpha1.FeatureState
			featureState.Name = feature
			featureState.Enabled, err = strconv.ParseBool(state)
			if err != nil {
				log.Errorf("failed to parse feature state value: %v for feature: %q", state, feature)
				os.Exit(1)
			}
			featureStates = append(featureStates, featureState)
		}
		pendingCRUpdatesObj.enqueueFeatureStateUpdatesForAllWorkloadNamespaces(ctx, featureStates)
	}
}

// configMapUpdated is called when configmap is updated.
func configMapUpdated(oldObj, newObj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	newfssConfigMap, ok := newObj.(*v1.ConfigMap)
	if newfssConfigMap == nil || !ok {
		log.Warnf("configMapUpdated: unrecognized new object %+v", newObj)
		return
	}
	oldfssConfigMap, ok := oldObj.(*v1.ConfigMap)
	if oldfssConfigMap == nil || !ok {
		log.Warnf("configMapUpdated: unrecognized old object %+v", newObj)
		return
	}

	if newfssConfigMap.Name == supervisorFeatureStatConfigMapName &&
		newfssConfigMap.Namespace == supervisorFeatureStateConfigMapNamespace {
		if reflect.DeepEqual(newfssConfigMap.Data, oldfssConfigMap.Data) {
			return
		}
		log.Infof("Observed fss update: oldfss: %+v newfss: %+v", oldfssConfigMap.Data, newfssConfigMap.Data)
		var err error
		var featureStates []featurestatesv1alpha1.FeatureState
		for feature, state := range newfssConfigMap.Data {
			var featureState featurestatesv1alpha1.FeatureState
			featureState.Name = feature
			featureState.Enabled, err = strconv.ParseBool(state)
			if err != nil {
				log.Errorf("failed to parse feature state value: %v for feature: %q", state, feature)
				os.Exit(1)
			}
			featureStates = append(featureStates, featureState)
		}
		pendingCRUpdatesObj.enqueueFeatureStateUpdatesForAllWorkloadNamespaces(ctx, featureStates)
	}
}

// configMapDeleted is called when config-map is deleted.
func configMapDeleted(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	fssConfigMap, ok := obj.(*v1.ConfigMap)
	if fssConfigMap == nil || !ok {
		log.Warnf("configMapDeleted: unrecognized object %+v", obj)
		return
	}
	if fssConfigMap.Name == supervisorFeatureStatConfigMapName &&
		fssConfigMap.Namespace == supervisorFeatureStateConfigMapNamespace {
		log.Errorf("supervisor feature switch state configmap %q from the namespace: %q is deleted",
			supervisorFeatureStatConfigMapName, supervisorFeatureStateConfigMapNamespace)
		os.Exit(1)
	}
}

// namespaceAdded adds is called when new namespace is added on the k8s cluster.
func namespaceAdded(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	namespace, ok := obj.(*v1.Namespace)
	if namespace == nil || !ok {
		log.Warnf("namespaceAdded: unrecognized object %+v", obj)
		return
	}
	if _, ok = namespace.Labels[WorkLoadNamespaceLabelKey]; ok {
		log.Infof("Observed new workload namespace: %v", namespace.Name)
		pendingCRUpdatesObj.enqueueFeatureStateUpdatesForWorkloadNamespace(ctx, namespace.Name)
	}
}

// namespaceUpdated is called when namespace is updated.
func namespaceUpdated(oldObj, newObj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	oldNamespace, ok := oldObj.(*v1.Namespace)
	if oldNamespace == nil || !ok {
		log.Warnf("namespaceUpdated: unrecognized object %+v", oldObj)
		return
	}
	newNamespace, ok := newObj.(*v1.Namespace)
	if newNamespace == nil || !ok {
		log.Warnf("namespaceUpdated: unrecognized object %+v", oldObj)
		return
	}

	_, labelPresentInOldNamespace := oldNamespace.Labels[WorkLoadNamespaceLabelKey]
	_, labelPresentInNewNamespace := newNamespace.Labels[WorkLoadNamespaceLabelKey]

	if !labelPresentInOldNamespace && labelPresentInNewNamespace {
		// Label with Key vSphereClusterID is added on the namespace.
		log.Infof("Observed new workload namespace: %v", newNamespace.Name)
		pendingCRUpdatesObj.enqueueFeatureStateUpdatesForWorkloadNamespace(ctx, newNamespace.Name)
	} else if labelPresentInOldNamespace && !labelPresentInNewNamespace {
		// Label with Key vSphereClusterID is removed from the namespace.
		log.Infof("label vSphereClusterID is removed from namespace: %q "+
			"Removing namespace from listing of further feature states CR updates",
			newNamespace.Name)
		pendingCRUpdatesObj.lock.Lock()
		defer pendingCRUpdatesObj.lock.Unlock()
		delete(pendingCRUpdatesObj.namespaceUpdateMap, newNamespace.Name)
	}
}

// namespaceDeleted is called when namespace is deleted.
func namespaceDeleted(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	namespace, ok := obj.(*v1.Namespace)
	if namespace == nil || !ok {
		log.Warnf("namespaceDeleted: unrecognized object %+v", obj)
		return
	}
	log.Infof("Namespace: %q is deleted", namespace.Name)

	pendingCRUpdatesObj.lock.Lock()
	defer pendingCRUpdatesObj.lock.Unlock()
	delete(pendingCRUpdatesObj.namespaceUpdateMap, namespace.Name)
}

// getFeatureStates returns latest feature states from supervisor config-map
// if failed to retrieve feature states, func returns error with empty array
// of FeatureState.
func getFeatureStates(ctx context.Context) ([]featurestatesv1alpha1.FeatureState, error) {
	log := logger.GetLogger(ctx)
	//  Retrieve SV FeatureStates configmap.
	fssConfigMap, err := k8sClient.CoreV1().ConfigMaps(supervisorFeatureStateConfigMapNamespace).Get(ctx,
		supervisorFeatureStatConfigMapName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Errorf("supervisor feature switch state with name: %q not found in the namespace: %q",
				supervisorFeatureStatConfigMapName, supervisorFeatureStateConfigMapNamespace)
			os.Exit(1)
		}
		log.Errorf("failed to retrieve SV feature switch state from namespace:%q with name: %q",
			supervisorFeatureStateConfigMapNamespace, supervisorFeatureStatConfigMapName)
		return nil, err
	}

	log.Infof("Successfully retrieved SV feature switch state from namespace:%q with name: %q",
		supervisorFeatureStateConfigMapNamespace, supervisorFeatureStatConfigMapName)
	var featureStates []featurestatesv1alpha1.FeatureState
	for feature, state := range fssConfigMap.Data {
		var featureState featurestatesv1alpha1.FeatureState
		featureState.Name = feature
		featureState.Enabled, err = strconv.ParseBool(state)
		if err != nil {
			log.Errorf("failed to parse feature state value: %v for feature: %q.", state, feature)
			os.Exit(1)
		}
		featureStates = append(featureStates, featureState)
	}
	return featureStates, nil
}

// fssCRDeleted is called when cnscsisvfeaturestates is deleted.
func fssCRDeleted(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	var fssObj featurestatesv1alpha1.CnsCsiSvFeatureStates
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &fssObj)
	if err != nil {
		log.Errorf("fssCRDeleted: failed to cast object to %s. err: %v", CRDPlural, err)
		return
	}
	if fssObj.Name != SVFeatureStateCRName {
		log.Warnf("fssCRDeleted: Ignoring %s CR object with name %q", CRDPlural, fssObj.Name)
		return
	}
	log.Infof("fssCRDeleted: cnscsisvfeaturestates with name: %q is deleted from namespace: %q",
		fssObj.Name, fssObj.Namespace)
	if isNamespaceDeleted(ctx, fssObj.Namespace) {
		return
	}
	log.Infof("Namespace: %q is not being deleted. putting back cnscsisvfeaturestates CR on the namespace",
		fssObj.Namespace)
	pendingCRUpdatesObj.enqueueFeatureStateUpdatesForWorkloadNamespace(ctx, fssObj.Namespace)
}

// isNamespaceDeleted return true if namespace is deleted or DeletionTimestamp
// is present on the namespace.
func isNamespaceDeleted(ctx context.Context, namespace string) bool {
	log := logger.GetLogger(ctx)
	for {
		namespaceObj, err := k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				log.Infof("namespace: %q not found", namespace)
				return true
			}
			log.Errorf("failed to get namespace %q object. err: %v", namespace, err)
		} else {
			if namespaceObj.DeletionTimestamp != nil {
				log.Infof("namespace: %q is being deleted", namespace)
				return true
			}
			return false
		}
		time.Sleep(1 * time.Second)
	}
}
