/*
Copyright 2020 The Kubernetes Authors.

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

package k8sorchestrator

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	v1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/featurestates"
	featurestatesv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/featurestates/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

const informerCreateRetryInterval = 5 * time.Minute

var (
	k8sOrchestratorInstance            *K8sOrchestrator
	k8sOrchestratorInstanceInitialized uint32
	// doesSvFssCRExist is set only in Guest cluster flavor if
	// the cnscsisvfeaturestate CR exists in the supervisor namespace
	// of the TKG cluster.
	doesSvFssCRExist         bool
	serviceMode              string
	svFssCRMutex             = &sync.RWMutex{}
	k8sOrchestratorInitMutex = &sync.RWMutex{}
)

// FSSConfigMapInfo contains details about the FSS configmap(s) present in
// all flavors.
type FSSConfigMapInfo struct {
	featureStates      map[string]string
	configMapName      string
	configMapNamespace string
}

// Map of volume handles to the pvc it is bound to.
// Key is the volume handle ID and value is the namespaced name of the pvc.
// The methods to add, remove and get entries from the map in a threadsafe
// manner are defined.
type volumeIDToPvcMap struct {
	*sync.RWMutex
	items map[string]string
}

// Adds an entry to volumeIDToPvcMap in a thread safe manner.
func (m *volumeIDToPvcMap) add(volumeHandle, pvcName string) {
	m.Lock()
	defer m.Unlock()
	m.items[volumeHandle] = pvcName
}

// Removes a volume handle from volumeIDToPvcMap in a thread safe manner.
func (m *volumeIDToPvcMap) remove(volumeHandle string) {
	m.Lock()
	defer m.Unlock()
	delete(m.items, volumeHandle)
}

// Returns the namespaced pvc name corresponding to volumeHandle.
func (m *volumeIDToPvcMap) get(volumeHandle string) string {
	m.RLock()
	defer m.RUnlock()
	return m.items[volumeHandle]
}

// K8sOrchestrator defines set of properties specific to K8s.
type K8sOrchestrator struct {
	supervisorFSS    FSSConfigMapInfo
	internalFSS      FSSConfigMapInfo
	informerManager  *k8s.InformerManager
	clusterFlavor    cnstypes.CnsClusterFlavor
	volumeIDToPvcMap *volumeIDToPvcMap
	k8sClient        clientset.Interface
}

// K8sGuestInitParams lists the set of parameters required to run the init for
// K8sOrchestrator in Guest cluster.
type K8sGuestInitParams struct {
	InternalFeatureStatesConfigInfo   cnsconfig.FeatureStatesConfigInfo
	SupervisorFeatureStatesConfigInfo cnsconfig.FeatureStatesConfigInfo
	ServiceMode                       string
}

// K8sSupervisorInitParams lists the set of parameters required to run the init
// for K8sOrchestrator in Supervisor cluster.
type K8sSupervisorInitParams struct {
	SupervisorFeatureStatesConfigInfo cnsconfig.FeatureStatesConfigInfo
	ServiceMode                       string
}

// K8sVanillaInitParams lists the set of parameters required to run the init for
// K8sOrchestrator in Vanilla cluster.
type K8sVanillaInitParams struct {
	InternalFeatureStatesConfigInfo cnsconfig.FeatureStatesConfigInfo
	ServiceMode                     string
}

// Newk8sOrchestrator instantiates K8sOrchestrator object and returns this
// object. NOTE: As Newk8sOrchestrator is created in the init of the driver and
// syncer components, raise an error only if it is of utmost importance.
func Newk8sOrchestrator(ctx context.Context, controllerClusterFlavor cnstypes.CnsClusterFlavor,
	params interface{}) (*K8sOrchestrator, error) {
	var (
		coInstanceErr error
		k8sClient     clientset.Interface
	)
	if atomic.LoadUint32(&k8sOrchestratorInstanceInitialized) == 0 {
		k8sOrchestratorInitMutex.Lock()
		defer k8sOrchestratorInitMutex.Unlock()
		if k8sOrchestratorInstanceInitialized == 0 {
			log := logger.GetLogger(ctx)
			log.Info("Initializing k8sOrchestratorInstance")

			// Create a K8s client
			k8sClient, coInstanceErr = k8s.NewClient(ctx)
			if coInstanceErr != nil {
				log.Errorf("Creating Kubernetes client failed. Err: %v", coInstanceErr)
				return nil, coInstanceErr
			}

			k8sOrchestratorInstance = &K8sOrchestrator{}
			k8sOrchestratorInstance.clusterFlavor = controllerClusterFlavor
			k8sOrchestratorInstance.k8sClient = k8sClient
			k8sOrchestratorInstance.informerManager = k8s.NewInformer(k8sClient)
			coInstanceErr = initFSS(ctx, k8sClient, controllerClusterFlavor, params)
			if coInstanceErr != nil {
				log.Errorf("Failed to initialize the orchestrator. Error: %v", coInstanceErr)
				return nil, coInstanceErr
			}

			if controllerClusterFlavor == cnstypes.CnsClusterFlavorWorkload &&
				k8sOrchestratorInstance.IsFSSEnabled(ctx, common.FakeAttach) {

				initVolumeHandleToPvcMap(ctx)
			}
			k8sOrchestratorInstance.informerManager.Listen()
			atomic.StoreUint32(&k8sOrchestratorInstanceInitialized, 1)
			log.Info("k8sOrchestratorInstance initialized")
		}
	}
	return k8sOrchestratorInstance, nil
}

// initFSS performs all the operations required to initialize the Feature
// states map and keep a watch on it. NOTE: As initFSS is called during the
// init of the driver and syncer components, raise an error only if the
// containers need to crash.
func initFSS(ctx context.Context, k8sClient clientset.Interface,
	controllerClusterFlavor cnstypes.CnsClusterFlavor, params interface{}) error {
	log := logger.GetLogger(ctx)
	var (
		fssConfigMap               *v1.ConfigMap
		err                        error
		configMapNamespaceToListen string
	)
	// Store configmap info in global variables to access later.
	if controllerClusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		k8sOrchestratorInstance.supervisorFSS.featureStates = make(map[string]string)
		// Validate init params
		svInitParams, ok := params.(K8sSupervisorInitParams)
		if !ok {
			return fmt.Errorf("expected orchestrator params of type K8sSupervisorInitParams, got %T instead", params)
		}
		k8sOrchestratorInstance.supervisorFSS.configMapName = svInitParams.SupervisorFeatureStatesConfigInfo.Name
		k8sOrchestratorInstance.supervisorFSS.configMapNamespace = svInitParams.SupervisorFeatureStatesConfigInfo.Namespace
		configMapNamespaceToListen = k8sOrchestratorInstance.supervisorFSS.configMapNamespace
		serviceMode = svInitParams.ServiceMode
	}
	if controllerClusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		k8sOrchestratorInstance.internalFSS.featureStates = make(map[string]string)
		// Validate init params.
		vanillaInitParams, ok := params.(K8sVanillaInitParams)
		if !ok {
			return fmt.Errorf("expected orchestrator params of type K8sVanillaInitParams, got %T instead", params)
		}
		k8sOrchestratorInstance.internalFSS.configMapName = vanillaInitParams.InternalFeatureStatesConfigInfo.Name
		k8sOrchestratorInstance.internalFSS.configMapNamespace = vanillaInitParams.InternalFeatureStatesConfigInfo.Namespace
		configMapNamespaceToListen = k8sOrchestratorInstance.internalFSS.configMapNamespace
		serviceMode = vanillaInitParams.ServiceMode
	}
	if controllerClusterFlavor == cnstypes.CnsClusterFlavorGuest {
		k8sOrchestratorInstance.supervisorFSS.featureStates = make(map[string]string)
		k8sOrchestratorInstance.internalFSS.featureStates = make(map[string]string)
		// Validate init params.
		guestInitParams, ok := params.(K8sGuestInitParams)
		if !ok {
			return fmt.Errorf("expected orchestrator params of type K8sGuestInitParams, got %T instead", params)
		}
		k8sOrchestratorInstance.internalFSS.configMapName = guestInitParams.InternalFeatureStatesConfigInfo.Name
		k8sOrchestratorInstance.internalFSS.configMapNamespace = guestInitParams.InternalFeatureStatesConfigInfo.Namespace
		k8sOrchestratorInstance.supervisorFSS.configMapName = guestInitParams.SupervisorFeatureStatesConfigInfo.Name
		k8sOrchestratorInstance.supervisorFSS.configMapNamespace = guestInitParams.SupervisorFeatureStatesConfigInfo.Namespace
		// As of now, TKGS is having both supervisor FSS and internal FSS in the
		// same namespace. If the configmap's namespaces change in future, we may
		// need listeners on different namespaces. Until then, we will initialize
		// configMapNamespaceToListen to internalFSS.configMapNamespace.
		configMapNamespaceToListen = k8sOrchestratorInstance.internalFSS.configMapNamespace
		serviceMode = guestInitParams.ServiceMode
	}

	// Initialize internal FSS map values.
	if controllerClusterFlavor == cnstypes.CnsClusterFlavorGuest ||
		controllerClusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		if k8sOrchestratorInstance.internalFSS.configMapName != "" &&
			k8sOrchestratorInstance.internalFSS.configMapNamespace != "" {
			// Retrieve configmap.
			fssConfigMap, err = k8sClient.CoreV1().ConfigMaps(k8sOrchestratorInstance.internalFSS.configMapNamespace).Get(
				ctx, k8sOrchestratorInstance.internalFSS.configMapName, metav1.GetOptions{})
			if err != nil {
				// return error as we cannot init containers without this info.
				log.Errorf("failed to fetch configmap %s from namespace %s. Error: %v",
					k8sOrchestratorInstance.internalFSS.configMapName,
					k8sOrchestratorInstance.internalFSS.configMapNamespace, err)
				return err
			}
			// Update values.
			k8sOrchestratorInstance.internalFSS.featureStates = fssConfigMap.Data
			log.Infof("New internal feature states values stored successfully: %v",
				k8sOrchestratorInstance.internalFSS.featureStates)
		}
	}

	if controllerClusterFlavor == cnstypes.CnsClusterFlavorGuest && serviceMode != "node" {
		var isFSSCREnabled bool
		// Check if csi-sv-feature-states-replication FSS exists and is enabled.
		if val, ok := k8sOrchestratorInstance.internalFSS.featureStates[common.CSISVFeatureStateReplication]; ok {
			isFSSCREnabled, err = strconv.ParseBool(val)
			if err != nil {
				log.Errorf("unable to convert %v to bool. csi-sv-feature-states-replication FSS disabled. Error: %v",
					val, err)
				return err
			}
		} else {
			return logger.LogNewError(log, "csi-sv-feature-states-replication FSS not present")
		}

		// Initialize supervisor FSS map values in GC using the
		// cnscsisvfeaturestate CR if csi-sv-feature-states-replication FSS
		// is enabled.
		if isFSSCREnabled {
			svNamespace, err := cnsconfig.GetSupervisorNamespace(ctx)
			if err != nil {
				log.Errorf("failed to retrieve supervisor cluster namespace from config. Error: %+v", err)
				return err
			}
			cfg, err := common.GetConfig(ctx)
			if err != nil {
				log.Errorf("failed to read config. Error: %+v", err)
				return err
			}
			// Get rest client config for supervisor.
			restClientConfig := k8s.GetRestClientConfigForSupervisor(ctx, cfg.GC.Endpoint, cfg.GC.Port)

			// Attempt to fetch the cnscsisvfeaturestate CR from the supervisor
			// namespace of the TKG cluster.
			svFssCR, err := getSVFssCR(ctx, restClientConfig)
			if err != nil {
				// If the cnscsisvfeaturestate CR is not yet registered in the
				// supervisor cluster, we receive NoKindMatchError. In such cases
				// log an info message and fallback to GCM replicated configmap
				// approach.
				_, ok := err.(*apiMeta.NoKindMatchError)
				if ok {
					log.Infof("%s CR not found in supervisor namespace. Defaulting to the %q FSS configmap "+
						"in %q namespace. Error: %+v",
						featurestates.CRDSingular, k8sOrchestratorInstance.supervisorFSS.configMapName,
						k8sOrchestratorInstance.supervisorFSS.configMapNamespace, err)
				} else {
					log.Errorf("failed to get %s CR from supervisor namespace %q. Error: %+v",
						featurestates.CRDSingular, svNamespace, err)
					return err
				}
			} else {
				setSvFssCRAvailability(true)
				// Store supervisor FSS values in cache.
				for _, svFSS := range svFssCR.Spec.FeatureStates {
					k8sOrchestratorInstance.supervisorFSS.featureStates[svFSS.Name] = strconv.FormatBool(svFSS.Enabled)
				}
				log.Infof("New supervisor feature states values stored successfully from %s CR object: %v",
					featurestates.SVFeatureStateCRName, k8sOrchestratorInstance.supervisorFSS.featureStates)
			}

			// Create an informer to watch on the cnscsisvfeaturestate CR.
			go func() {
				// Ideally if a resource is not yet registered on a cluster and we
				// try to create an informer to watch it, the informer creation will
				// not fail. But, the informer starts emitting error messages like
				// `Failed to list X: the server could not find the requested resource`.
				// To avoid this, we attempt to fetch the cnscsisvfeaturestate CR
				// first and retry if we receive an error. This is required in cases
				// where TKG cluster is on a newer build and supervisor is at an
				// older version.
				ticker := time.NewTicker(informerCreateRetryInterval)
				var dynInformer informers.GenericInformer
				for range ticker.C {
					// Check if cnscsisvfeaturestate CR exists, if not keep retrying.
					_, err = getSVFssCR(ctx, restClientConfig)
					if err != nil {
						continue
					}
					// Create a dynamic informer for the cnscsisvfeaturestate CR.
					dynInformer, err = k8s.GetDynamicInformer(ctx, featurestates.CRDGroupName,
						internalapis.Version, featurestates.CRDPlural, svNamespace, restClientConfig, false)
					if err != nil {
						log.Errorf("failed to create dynamic informer for %s CR. Error: %+v", featurestates.CRDSingular, err)
						continue
					}
					break
				}
				// Set up namespaced listener for cnscsisvfeaturestate CR.
				dynInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
					// Add.
					AddFunc: func(obj interface{}) {
						fssCRAdded(obj)
					},
					// Update.
					UpdateFunc: func(oldObj interface{}, newObj interface{}) {
						fssCRUpdated(oldObj, newObj)
					},
					// Delete.
					DeleteFunc: func(obj interface{}) {
						fssCRDeleted(obj)
					},
				})
				stopCh := make(chan struct{})
				log.Infof("Informer to watch on %s CR starting..", featurestates.CRDSingular)
				dynInformer.Informer().Run(stopCh)
			}()
		}
	}
	// Initialize supervisor FSS map values using configmap in Supervisor
	// cluster flavor or in guest cluster flavor only if cnscsisvfeaturestate
	// CR is not registered yet.
	if controllerClusterFlavor == cnstypes.CnsClusterFlavorWorkload ||
		(controllerClusterFlavor == cnstypes.CnsClusterFlavorGuest &&
			!getSvFssCRAvailability() && serviceMode != "node") {
		if k8sOrchestratorInstance.supervisorFSS.configMapName != "" &&
			k8sOrchestratorInstance.supervisorFSS.configMapNamespace != "" {
			// Retrieve configmap.
			fssConfigMap, err = k8sClient.CoreV1().ConfigMaps(k8sOrchestratorInstance.supervisorFSS.configMapNamespace).Get(
				ctx, k8sOrchestratorInstance.supervisorFSS.configMapName, metav1.GetOptions{})
			if err != nil {
				log.Errorf("failed to fetch configmap %s from namespace %s. Error: %v",
					k8sOrchestratorInstance.supervisorFSS.configMapName,
					k8sOrchestratorInstance.supervisorFSS.configMapNamespace, err)
				return err
			}
			// Update values.
			k8sOrchestratorInstance.supervisorFSS.featureStates = fssConfigMap.Data
			log.Infof("New supervisor feature states values stored successfully: %v",
				k8sOrchestratorInstance.supervisorFSS.featureStates)
		}
	}
	// Set up kubernetes configmap listener for CSI namespace.
	k8sOrchestratorInstance.informerManager.AddConfigMapListener(ctx, k8sClient, configMapNamespaceToListen,
		// Add.
		func(obj interface{}) {
			configMapAdded(obj)
		},
		// Update.
		func(oldObj interface{}, newObj interface{}) {
			configMapUpdated(oldObj, newObj)
		},
		// Delete.
		func(obj interface{}) {
			configMapDeleted(obj)
		})
	return nil
}

func setSvFssCRAvailability(exists bool) {
	svFssCRMutex.Lock()
	defer svFssCRMutex.Unlock()
	doesSvFssCRExist = exists
}

func getSvFssCRAvailability() bool {
	svFssCRMutex.RLock()
	defer svFssCRMutex.RUnlock()
	return doesSvFssCRExist
}

// getSVFssCR retrieves the cnscsisvfeaturestate CR from the supervisor
// namespace in the TKG cluster using the supervisor client.
// It takes the REST config to the cluster and creates a client using the config
// to returns the svFssCR object.
func getSVFssCR(ctx context.Context, restClientConfig *restclient.Config) (
	*featurestatesv1alpha1.CnsCsiSvFeatureStates, error) {
	log := logger.GetLogger(ctx)

	// Get CNS operator client.
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("failed to create CnsOperator client. Err: %+v", err)
		return nil, err
	}
	svNamespace, err := cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("failed to retrieve supervisor cluster namespace from config. Error: %+v", err)
		return nil, err
	}
	// Fetch cnscsisvfeaturestate CR.
	svFssCR := &featurestatesv1alpha1.CnsCsiSvFeatureStates{}
	err = cnsOperatorClient.Get(ctx, client.ObjectKey{Name: featurestates.SVFeatureStateCRName,
		Namespace: svNamespace}, svFssCR)
	if err != nil {
		log.Debugf("failed to get %s CR from supervisor namespace %q. Error: %+v",
			featurestates.CRDSingular, svNamespace, err)
		return nil, err
	}

	return svFssCR, nil
}

// configMapAdded adds feature state switch values from configmap that has been
// created on K8s cluster.
func configMapAdded(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	fssConfigMap, ok := obj.(*v1.ConfigMap)
	if fssConfigMap == nil || !ok {
		log.Warnf("configMapAdded: unrecognized object %+v", obj)
		return
	}

	if fssConfigMap.Name == k8sOrchestratorInstance.supervisorFSS.configMapName &&
		fssConfigMap.Namespace == k8sOrchestratorInstance.supervisorFSS.configMapNamespace {
		if serviceMode == "node" {
			log.Debug("configMapAdded: Ignoring supervisor FSS configmap add event in the nodes")
			return
		}
		if getSvFssCRAvailability() {
			log.Debugf("configMapAdded: Ignoring supervisor FSS configmap add event as %q CR is present",
				featurestates.CRDSingular)
			return
		}
		// Update supervisor FSS.
		k8sOrchestratorInstance.supervisorFSS.featureStates = fssConfigMap.Data
		log.Infof("configMapAdded: Supervisor feature state values from %q stored successfully: %v",
			fssConfigMap.Name, k8sOrchestratorInstance.supervisorFSS.featureStates)
	} else if fssConfigMap.Name == k8sOrchestratorInstance.internalFSS.configMapName &&
		fssConfigMap.Namespace == k8sOrchestratorInstance.internalFSS.configMapNamespace {
		// Update internal FSS.
		k8sOrchestratorInstance.internalFSS.featureStates = fssConfigMap.Data
		log.Infof("configMapAdded: Internal feature state values from %q stored successfully: %v",
			fssConfigMap.Name, k8sOrchestratorInstance.internalFSS.featureStates)
	}
}

// configMapUpdated updates feature state switch values from configmap that
// has been created on K8s cluster.
func configMapUpdated(oldObj, newObj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	oldFssConfigMap, ok := oldObj.(*v1.ConfigMap)
	if oldFssConfigMap == nil || !ok {
		log.Warnf("configMapUpdated: unrecognized old object %+v", oldObj)
		return
	}
	newFssConfigMap, ok := newObj.(*v1.ConfigMap)
	if newFssConfigMap == nil || !ok {
		log.Warnf("configMapUpdated: unrecognized new object %+v", newObj)
		return
	}
	// Check if there are updates to configmap data.
	if reflect.DeepEqual(newFssConfigMap.Data, oldFssConfigMap.Data) {
		log.Debug("configMapUpdated: No change in configmap data. Ignoring the event")
		return
	}

	if newFssConfigMap.Name == k8sOrchestratorInstance.supervisorFSS.configMapName &&
		newFssConfigMap.Namespace == k8sOrchestratorInstance.supervisorFSS.configMapNamespace {
		// The controller in nodes is not dependent on the supervisor FSS updates.
		if serviceMode == "node" {
			log.Debug("configMapUpdated: Ignoring supervisor FSS configmap update event in the nodes")
			return
		}
		// Ignore configmap updates if the cnscsisvfeaturestate CR is present in
		// supervisor namespace.
		if getSvFssCRAvailability() {
			log.Debugf("configMapUpdated: Ignoring supervisor FSS configmap update event as %q CR is present",
				featurestates.CRDSingular)
			return
		}
		// Update supervisor FSS.
		k8sOrchestratorInstance.supervisorFSS.featureStates = newFssConfigMap.Data
		log.Warnf("configMapUpdated: Supervisor feature state values from %q stored successfully: %v",
			newFssConfigMap.Name, k8sOrchestratorInstance.supervisorFSS.featureStates)
	} else if newFssConfigMap.Name == k8sOrchestratorInstance.internalFSS.configMapName &&
		newFssConfigMap.Namespace == k8sOrchestratorInstance.internalFSS.configMapNamespace {
		// Update internal FSS.
		k8sOrchestratorInstance.internalFSS.featureStates = newFssConfigMap.Data
		log.Warnf("configMapUpdated: Internal feature state values from %q stored successfully: %v",
			newFssConfigMap.Name, k8sOrchestratorInstance.internalFSS.featureStates)
	}
}

// configMapDeleted clears the feature state switch values from the feature
// states map.
func configMapDeleted(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	fssConfigMap, ok := obj.(*v1.ConfigMap)
	if fssConfigMap == nil || !ok {
		log.Warnf("configMapDeleted: unrecognized object %+v", obj)
		return
	}
	// Check if it is either internal or supervisor FSS configmap.
	if fssConfigMap.Name == k8sOrchestratorInstance.supervisorFSS.configMapName &&
		fssConfigMap.Namespace == k8sOrchestratorInstance.supervisorFSS.configMapNamespace {
		if serviceMode == "node" {
			log.Debug("configMapDeleted: Ignoring supervisor FSS configmap delete event in the nodes")
			return
		}
		if getSvFssCRAvailability() {
			log.Debugf("configMapDeleted: Ignoring supervisor FSS configmap delete event as %q CR is present",
				featurestates.CRDSingular)
			return
		}
		log.Errorf("configMapDeleted: configMap %q in namespace %q deleted. "+
			"This is a system resource, kindly restore it.", fssConfigMap.Name, fssConfigMap.Namespace)
		os.Exit(1)
	} else if fssConfigMap.Name == k8sOrchestratorInstance.internalFSS.configMapName &&
		fssConfigMap.Namespace == k8sOrchestratorInstance.internalFSS.configMapNamespace {
		log.Errorf("configMapDeleted: configMap %q in namespace %q deleted. "+
			"This is a system resource, kindly restore it.", fssConfigMap.Name, fssConfigMap.Namespace)
		os.Exit(1)
	}
}

// fssCRAdded adds supervisor feature state switch values from the
// cnscsisvfeaturestate CR.
func fssCRAdded(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	var svFSSObject featurestatesv1alpha1.CnsCsiSvFeatureStates
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &svFSSObject)
	if err != nil {
		log.Errorf("fssCRAdded: failed to cast object to %s. err: %v", featurestates.CRDSingular, err)
		return
	}
	if svFSSObject.Name != featurestates.SVFeatureStateCRName {
		log.Warnf("fssCRAdded: Ignoring %s CR object with name %q", featurestates.CRDSingular, svFSSObject.Name)
		return
	}
	setSvFssCRAvailability(true)
	for _, fss := range svFSSObject.Spec.FeatureStates {
		k8sOrchestratorInstance.supervisorFSS.featureStates[fss.Name] = strconv.FormatBool(fss.Enabled)
	}
	log.Infof("fssCRAdded: New supervisor feature states values stored successfully from %s CR object: %v",
		featurestates.SVFeatureStateCRName, k8sOrchestratorInstance.supervisorFSS.featureStates)
}

// fssCRUpdated updates supervisor feature state switch values from the
// cnscsisvfeaturestate CR.
func fssCRUpdated(oldObj, newObj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	var (
		newSvFSSObject featurestatesv1alpha1.CnsCsiSvFeatureStates
		oldSvFSSObject featurestatesv1alpha1.CnsCsiSvFeatureStates
	)
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		newObj.(*unstructured.Unstructured).Object, &newSvFSSObject)
	if err != nil {
		log.Errorf("fssCRUpdated: failed to cast new object to %s. err: %v", featurestates.CRDSingular, err)
		return
	}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(
		oldObj.(*unstructured.Unstructured).Object, &oldSvFSSObject)
	if err != nil {
		log.Errorf("fssCRUpdated: failed to cast old object to %s. err: %v", featurestates.CRDSingular, err)
		return
	}
	// Check if there are updates to the feature states in the
	// cnscsisvfeaturestate CR.
	if reflect.DeepEqual(oldSvFSSObject.Spec.FeatureStates, newSvFSSObject.Spec.FeatureStates) {
		log.Debug("fssCRUpdated: No change in %s CR data. Ignoring the event", featurestates.CRDSingular)
		return
	}

	if newSvFSSObject.Name != featurestates.SVFeatureStateCRName {
		log.Warnf("fssCRUpdated: Ignoring %s CR object with name %q", featurestates.CRDSingular, newSvFSSObject.Name)
		return
	}
	for _, fss := range newSvFSSObject.Spec.FeatureStates {
		k8sOrchestratorInstance.supervisorFSS.featureStates[fss.Name] = strconv.FormatBool(fss.Enabled)
	}
	log.Warnf("fssCRUpdated: New supervisor feature states values stored successfully from %s CR object: %v",
		featurestates.SVFeatureStateCRName, k8sOrchestratorInstance.supervisorFSS.featureStates)
}

// fssCRDeleted crashes the container if the cnscsisvfeaturestate CR object
// with name svfeaturestates is deleted.
func fssCRDeleted(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	var svFSSObject featurestatesv1alpha1.CnsCsiSvFeatureStates
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, &svFSSObject)
	if err != nil {
		log.Errorf("fssCRDeleted: failed to cast object to %s. err: %v", featurestates.CRDSingular, err)
		return
	}
	if svFSSObject.Name != featurestates.SVFeatureStateCRName {
		log.Warnf("fssCRDeleted: Ignoring %s CR object with name %q", featurestates.CRDSingular, svFSSObject.Name)
		return
	}
	setSvFssCRAvailability(false)
	// Logging an error here because cnscsisvfeaturestate CR should not be
	// deleted.
	log.Errorf("fssCRDeleted: %s CR object with name %q in namespace %q deleted. "+
		"This is a system resource, kindly restore it.",
		featurestates.CRDSingular, svFSSObject.Name, svFSSObject.Namespace)
	os.Exit(1)
}

// initVolumeHandleToPvcMap performs all the operations required to initialize
// the volume id to PVC name map. It also watches for PV update & delete
// operations, and updates the map accordingly.
func initVolumeHandleToPvcMap(ctx context.Context) {
	log := logger.GetLogger(ctx)
	log.Debugf("Initializing volume ID to PVC name map")
	k8sOrchestratorInstance.volumeIDToPvcMap = &volumeIDToPvcMap{
		RWMutex: &sync.RWMutex{},
		items:   make(map[string]string),
	}

	// Set up kubernetes resource listener to listen events on PersistentVolumes
	// and PersistentVolumeClaims.
	k8sOrchestratorInstance.informerManager.AddPVListener(
		func(obj interface{}) { // Add.
			pvAdded(obj)
		},
		func(oldObj interface{}, newObj interface{}) { // Update.
			pvUpdated(oldObj, newObj)
		},
		func(obj interface{}) { // Delete.
			pvDeleted(obj)
		})

	k8sOrchestratorInstance.informerManager.AddPVCListener(
		func(obj interface{}) { // Add.
			pvcAdded(obj)
		},
		nil, // Update.
		nil, // Delete.
	)
}

// Since informerManager's sharedInformerFactory is started with no resync
// period, it never syncs the existing cluster objects to its Store when
// it's started. pvcAdded provides no additional handling but it ensures that
// existing PVCs in the cluster gets added to sharedInformerFactory's Store
// before it's started. Then using informerManager's PVCLister should find
// the existing PVCs as well.
func pvcAdded(obj interface{}) {}

// pvAdded adds a volume to the volumeIDToPvcMap if it's already in Bound phase.
// This ensures that all existing PVs in the cluster are added to the map, even
// across container restarts.
func pvAdded(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	pv, ok := obj.(*v1.PersistentVolume)
	if pv == nil || !ok {
		log.Warnf("pvAdded: unrecognized object %+v", obj)
		return
	}

	if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csitypes.Name &&
		pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound &&
		!isFileVolume(pv) { // We should not be caching file volumes to the map.

		// Add volume handle to PVC mapping.
		objKey := pv.Spec.CSI.VolumeHandle
		objVal := pv.Spec.ClaimRef.Namespace + "/" + pv.Spec.ClaimRef.Name

		k8sOrchestratorInstance.volumeIDToPvcMap.add(objKey, objVal)
		log.Debugf("pvAdded: Added '%s -> %s' pair to volumeIDToPvcMap", objKey, objVal)
	}
}

// pvUpdated updates the volumeIDToPvcMap when a PV goes to Bound phase.
func pvUpdated(oldObj, newObj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	// Get old and new PV objects.
	oldPv, ok := oldObj.(*v1.PersistentVolume)
	if oldPv == nil || !ok {
		log.Warnf("PVUpdated: unrecognized old object %+v", oldObj)
		return
	}

	newPv, ok := newObj.(*v1.PersistentVolume)
	if newPv == nil || !ok {
		log.Warnf("PVUpdated: unrecognized new object %+v", newObj)
		return
	}

	// PV goes into Bound phase.
	if oldPv.Status.Phase != v1.VolumeBound && newPv.Status.Phase == v1.VolumeBound {
		if newPv.Spec.CSI != nil && newPv.Spec.CSI.Driver == csitypes.Name &&
			newPv.Spec.ClaimRef != nil && !isFileVolume(newPv) {

			log.Debugf("pvUpdated: PV %s went to Bound phase", newPv.Name)
			// Add volume handle to PVC mapping.
			objKey := newPv.Spec.CSI.VolumeHandle
			objVal := newPv.Spec.ClaimRef.Namespace + "/" + newPv.Spec.ClaimRef.Name

			k8sOrchestratorInstance.volumeIDToPvcMap.add(objKey, objVal)
			log.Debugf("pvUpdated: Added '%s -> %s' pair to volumeIDToPvcMap", objKey, objVal)
		}
	}
}

// pvDeleted deletes an entry from volumeIDToPvcMap when a PV gets deleted.
func pvDeleted(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	pv, ok := obj.(*v1.PersistentVolume)
	if pv == nil || !ok {
		log.Warnf("PVDeleted: unrecognized object %+v", obj)
		return
	}
	log.Debugf("PV: %s deleted. Removing entry from volumeIDToPvcMap", pv.Name)

	if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csitypes.Name {
		k8sOrchestratorInstance.volumeIDToPvcMap.remove(pv.Spec.CSI.VolumeHandle)
		log.Debugf("k8sorchestrator: Deleted key %s from volumeIDToPvcMap", pv.Spec.CSI.VolumeHandle)
	}
}

// IsFSSEnabled utilises the cluster flavor to check their corresponding FSS
// maps and returns if the feature state switch is enabled for the given feature
// indicated by featureName.
func (c *K8sOrchestrator) IsFSSEnabled(ctx context.Context, featureName string) bool {
	log := logger.GetLogger(ctx)
	var (
		internalFeatureState   bool
		supervisorFeatureState bool
		err                    error
	)
	if c.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		// Check internal FSS map.
		if flag, ok := c.internalFSS.featureStates[featureName]; ok {
			internalFeatureState, err = strconv.ParseBool(flag)
			if err != nil {
				log.Errorf("Error while converting %v feature state value: %v to boolean. "+
					"Setting the feature state to false", featureName, internalFeatureState)
				return false
			}
			return internalFeatureState
		}
		log.Debugf("Could not find the %s feature state in ConfigMap %s. "+
			"Setting the feature state to false", featureName, c.internalFSS.configMapName)
		return false
	} else if c.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		// Check SV FSS map.
		if flag, ok := c.supervisorFSS.featureStates[featureName]; ok {
			supervisorFeatureState, err = strconv.ParseBool(flag)
			if err != nil {
				log.Errorf("Error while converting %v feature state value: %v to boolean. "+
					"Setting the feature state to false", featureName, supervisorFeatureState)
				return false
			}
			return supervisorFeatureState
		}
		log.Debugf("Could not find the %s feature state in ConfigMap %s. "+
			"Setting the feature state to false", featureName, c.supervisorFSS.configMapName)
		return false
	} else if c.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Check internal FSS map.
		if flag, ok := c.internalFSS.featureStates[featureName]; ok {
			internalFeatureState, err := strconv.ParseBool(flag)
			if err != nil {
				log.Errorf("Error while converting %v feature state value: %v to boolean. "+
					"Setting the feature state to false", featureName, internalFeatureState)
				return false
			}
			if !internalFeatureState {
				// If FSS set to false, return.
				log.Infof("%s feature state set to false in %s ConfigMap", featureName, c.internalFSS.configMapName)
				return internalFeatureState
			}
		} else {
			log.Debugf("Could not find the %s feature state in ConfigMap %s. Setting the feature state to false",
				featureName, c.internalFSS.configMapName)
			return false
		}
		if serviceMode != "node" {
			// Check SV FSS map.
			if flag, ok := c.supervisorFSS.featureStates[featureName]; ok {
				supervisorFeatureState, err := strconv.ParseBool(flag)
				if err != nil {
					log.Errorf("Error while converting %v feature state value: %v to boolean. "+
						"Setting the feature state to false", featureName, supervisorFeatureState)
					return false
				}
				if !supervisorFeatureState {
					// If FSS set to false, return.
					log.Infof("%s feature state set to false in %s ConfigMap", featureName, c.supervisorFSS.configMapName)
					return supervisorFeatureState
				}
			} else {
				log.Debugf("Could not find the %s feature state in ConfigMap %s. Setting the feature state to false",
					featureName, c.supervisorFSS.configMapName)
				return false
			}
		}
		return true
	}
	log.Debugf("cluster flavor %q not recognised. Defaulting to false", c.clusterFlavor)
	return false
}

// IsFakeAttachAllowed checks if the volume is eligible to be fake attached
// and returns a bool value.
func (c *K8sOrchestrator) IsFakeAttachAllowed(ctx context.Context, volumeID string,
	volumeManager cnsvolume.Manager) (bool, error) {
	log := logger.GetLogger(ctx)
	// Check pvc annotations.
	pvcAnn, err := c.getPVCAnnotations(ctx, volumeID)
	if err != nil {
		log.Errorf("IsFakeAttachAllowed: failed to get pvc annotations for volume ID %s "+
			"while checking eligibility for fake attach", volumeID)
		return false, err
	}

	if val, found := pvcAnn[common.AnnIgnoreInaccessiblePV]; found && val == "yes" {
		log.Debugf("Found %s annotation on pvc set to yes for volume: %s. Checking volume health on CNS volume.",
			common.AnnIgnoreInaccessiblePV, volumeID)
		// Check if volume is inaccessible.
		vol, err := common.QueryVolumeByID(ctx, volumeManager, volumeID)
		if err != nil {
			log.Errorf("failed to query CNS for volume ID %s while checking eligibility for fake attach", volumeID)
			return false, err
		}

		if vol.HealthStatus != string(pbmtypes.PbmHealthStatusForEntityUnknown) {
			volHealthStatus, err := common.ConvertVolumeHealthStatus(ctx, vol.VolumeId.Id, vol.HealthStatus)
			if err != nil {
				log.Errorf("invalid health status: %s for volume: %s", vol.HealthStatus, vol.VolumeId.Id)
				return false, err
			}
			log.Debugf("CNS volume health is: %s", volHealthStatus)

			// If volume is inaccessible, it can be fake attached.
			if volHealthStatus == common.VolHealthStatusInaccessible {
				log.Infof("Volume: %s is eligible to be fake attached", volumeID)
				return true, nil
			}
		}
		// For all other cases, return false.
		return false, nil
	}
	// Annotation is not found or not set to true, return false.
	log.Debugf("Annotation %s not found or not set to true on pvc for volume %s",
		common.AnnIgnoreInaccessiblePV, volumeID)
	return false, nil
}

// MarkFakeAttached updates the pvc corresponding to volume to have a fake
// attach annotation.
func (c *K8sOrchestrator) MarkFakeAttached(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)
	annotations := make(map[string]string)
	annotations[common.AnnVolumeHealth] = common.VolHealthStatusInaccessible
	annotations[common.AnnFakeAttached] = "yes"

	// Update annotations.
	// Along with updating fake attach annotation on pvc, also update the volume
	// health on pvc as inaccessible, as that's one of the conditions for volume
	// to be fake attached.
	if err := c.updatePVCAnnotations(ctx, volumeID, annotations); err != nil {
		log.Errorf("failed to mark fake attach annotation on the pvc for volume %s. Error:%+v", volumeID, err)
		return err
	}

	return nil
}

// ClearFakeAttached checks if pvc corresponding to the volume has fake
// annotations, and unmark it as not fake attached.
func (c *K8sOrchestrator) ClearFakeAttached(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)
	// Check pvc annotations.
	pvcAnn, err := c.getPVCAnnotations(ctx, volumeID)
	if err != nil {
		if err.Error() == common.ErrNotFound.Error() {
			// PVC not found, which means PVC could have been deleted. No need to proceed.
			return nil
		}
		log.Errorf("ClearFakeAttached: failed to get pvc annotations for volume ID %s "+
			"while checking if it was fake attached", volumeID)
		return err
	}
	val, found := pvcAnn[common.AnnFakeAttached]
	if found && val == "yes" {
		log.Debugf("Volume: %s was fake attached", volumeID)
		// Clear the fake attach annotation.
		annotations := make(map[string]string)
		annotations[common.AnnFakeAttached] = ""
		if err := c.updatePVCAnnotations(ctx, volumeID, annotations); err != nil {
			if err.Error() == common.ErrNotFound.Error() {
				// PVC not found, which means PVC could have been deleted.
				return nil
			}
			log.Errorf("failed to clear fake attach annotation on the pvc for volume %s. Error:%+v", volumeID, err)
			return err
		}
	}
	return nil
}
