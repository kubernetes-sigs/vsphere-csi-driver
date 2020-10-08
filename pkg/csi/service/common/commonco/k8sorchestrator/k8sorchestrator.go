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
	"strconv"
	"sync"
	"sync/atomic"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var (
	k8sOrchestratorInstance            *K8sOrchestrator
	k8sOrchestratorInstanceInitialized uint32
)

// FSSConfigMapInfo contains details about the FSS configmap(s) present in all flavors
type FSSConfigMapInfo struct {
	featureStates      map[string]string
	configMapName      string
	configMapNamespace string
}

// K8sOrchestrator defines set of properties specific to K8s
type K8sOrchestrator struct {
	supervisorFSS   FSSConfigMapInfo
	internalFSS     FSSConfigMapInfo
	informerManager *k8s.InformerManager
	clusterFlavor   cnstypes.CnsClusterFlavor
}

// K8sGuestInitParams lists the set of parameters required to run the init for K8sOrchestrator in Guest cluster
type K8sGuestInitParams struct {
	InternalFeatureStatesConfigInfo   config.FeatureStatesConfigInfo
	SupervisorFeatureStatesConfigInfo config.FeatureStatesConfigInfo
}

// K8sSupervisorInitParams lists the set of parameters required to run the init for K8sOrchestrator in Supervisor cluster
type K8sSupervisorInitParams struct {
	SupervisorFeatureStatesConfigInfo config.FeatureStatesConfigInfo
}

// K8sVanillaInitParams lists the set of parameters required to run the init for K8sOrchestrator in Vanilla cluster
type K8sVanillaInitParams struct {
	InternalFeatureStatesConfigInfo config.FeatureStatesConfigInfo
}

var mutex = &sync.RWMutex{}

// Newk8sOrchestrator instantiates K8sOrchestrator object and returns this object
// NOTE: As Newk8sOrchestrator is created in the init of the driver and syncer components,
// raise an error only if it is of utmost importance
func Newk8sOrchestrator(ctx context.Context, controllerClusterFlavor cnstypes.CnsClusterFlavor, params interface{}) (*K8sOrchestrator, error) {
	var (
		coInstanceErr error
		k8sClient     clientset.Interface
	)
	if atomic.LoadUint32(&k8sOrchestratorInstanceInitialized) == 0 {
		mutex.Lock()
		defer mutex.Unlock()
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
			coInstanceErr = initFSS(ctx, k8sClient, controllerClusterFlavor, params)
			if coInstanceErr != nil {
				log.Errorf("Failed to initialize the orchestrator. Error: %v", coInstanceErr)
				return nil, coInstanceErr
			}

			atomic.StoreUint32(&k8sOrchestratorInstanceInitialized, 1)
			log.Info("k8sOrchestratorInstance initialized")
		}
	}
	return k8sOrchestratorInstance, nil
}

// initFSS performs all the operations required to initialize the Feature states map and keep a watch on it
func initFSS(ctx context.Context, k8sClient clientset.Interface, controllerClusterFlavor cnstypes.CnsClusterFlavor, params interface{}) error {
	log := logger.GetLogger(ctx)
	var (
		fssConfigMap *v1.ConfigMap
		err          error
	)

	// Store configmap info in global variables to access later
	if controllerClusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		k8sOrchestratorInstance.supervisorFSS.featureStates = make(map[string]string)
		// Validate init params
		svInitParams, ok := params.(K8sSupervisorInitParams)
		if !ok {
			return fmt.Errorf("expected orchestrator params of type K8sSupervisorInitParams, got %T instead", params)
		}
		k8sOrchestratorInstance.supervisorFSS.configMapName = svInitParams.SupervisorFeatureStatesConfigInfo.Name
		k8sOrchestratorInstance.supervisorFSS.configMapNamespace = svInitParams.SupervisorFeatureStatesConfigInfo.Namespace
	}
	if controllerClusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		k8sOrchestratorInstance.internalFSS.featureStates = make(map[string]string)
		// Validate init params
		vanillaInitParams, ok := params.(K8sVanillaInitParams)
		if !ok {
			return fmt.Errorf("expected orchestrator params of type K8sVanillaInitParams, got %T instead", params)
		}
		k8sOrchestratorInstance.internalFSS.configMapName = vanillaInitParams.InternalFeatureStatesConfigInfo.Name
		k8sOrchestratorInstance.internalFSS.configMapNamespace = vanillaInitParams.InternalFeatureStatesConfigInfo.Namespace
	}
	if controllerClusterFlavor == cnstypes.CnsClusterFlavorGuest {
		k8sOrchestratorInstance.supervisorFSS.featureStates = make(map[string]string)
		k8sOrchestratorInstance.internalFSS.featureStates = make(map[string]string)
		// Validate init params
		guestInitParams, ok := params.(K8sGuestInitParams)
		if !ok {
			return fmt.Errorf("expected orchestrator params of type K8sGuestInitParams, got %T instead", params)
		}
		k8sOrchestratorInstance.internalFSS.configMapName = guestInitParams.InternalFeatureStatesConfigInfo.Name
		k8sOrchestratorInstance.internalFSS.configMapNamespace = guestInitParams.InternalFeatureStatesConfigInfo.Namespace
		k8sOrchestratorInstance.supervisorFSS.configMapName = guestInitParams.SupervisorFeatureStatesConfigInfo.Name
		k8sOrchestratorInstance.supervisorFSS.configMapNamespace = guestInitParams.SupervisorFeatureStatesConfigInfo.Namespace
	}

	// Initialize supervisor FSS map values
	if controllerClusterFlavor == cnstypes.CnsClusterFlavorGuest || controllerClusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		if k8sOrchestratorInstance.supervisorFSS.configMapName != "" && k8sOrchestratorInstance.supervisorFSS.configMapNamespace != "" {
			// Retrieve configmap
			fssConfigMap, err = k8sClient.CoreV1().ConfigMaps(k8sOrchestratorInstance.supervisorFSS.configMapNamespace).Get(
				ctx, k8sOrchestratorInstance.supervisorFSS.configMapName, metav1.GetOptions{})
			if err != nil {
				log.Errorf("failed to fetch configmap %s from namespace %s. Setting the supervisor feature states to default values: %v. Error: %v",
					k8sOrchestratorInstance.supervisorFSS.configMapName, k8sOrchestratorInstance.supervisorFSS.configMapNamespace,
					k8sOrchestratorInstance.supervisorFSS, err)
			} else {
				// Update values
				k8sOrchestratorInstance.supervisorFSS.featureStates = fssConfigMap.Data
				log.Infof("New supervisor feature states values stored successfully: %v", k8sOrchestratorInstance.supervisorFSS.featureStates)
			}
		}
	}

	// Initialize internal FSS map values
	if controllerClusterFlavor == cnstypes.CnsClusterFlavorGuest || controllerClusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		if k8sOrchestratorInstance.internalFSS.configMapName != "" && k8sOrchestratorInstance.internalFSS.configMapNamespace != "" {
			// Retrieve configmap
			fssConfigMap, err = k8sClient.CoreV1().ConfigMaps(k8sOrchestratorInstance.internalFSS.configMapNamespace).Get(
				ctx, k8sOrchestratorInstance.internalFSS.configMapName, metav1.GetOptions{})
			if err != nil {
				log.Errorf("failed to fetch configmap %s from namespace %s. Setting the internal feature states to default values: %v. Error: %v",
					k8sOrchestratorInstance.internalFSS.configMapName, k8sOrchestratorInstance.internalFSS.configMapNamespace,
					k8sOrchestratorInstance.internalFSS.featureStates, err)
			} else {
				// Update values
				k8sOrchestratorInstance.internalFSS.featureStates = fssConfigMap.Data
				log.Infof("New internal feature states values stored successfully: %v", k8sOrchestratorInstance.internalFSS.featureStates)
			}
		}
	}

	// Set up kubernetes resource listeners for k8s orchestrator
	k8sOrchestratorInstance.informerManager = k8s.NewInformer(k8sClient)
	k8sOrchestratorInstance.informerManager.AddConfigMapListener(
		// Add
		func(obj interface{}) {
			configMapAdded(obj)
		}, // Update
		func(oldObj interface{}, newObj interface{}) {
			configMapUpdated(oldObj, newObj)
		}, // Delete
		func(obj interface{}) {
			configMapDeleted(obj)
		})
	k8sOrchestratorInstance.informerManager.Listen()
	return nil
}

// configMapAdded adds feature state switch values from configmap that has been created on K8s cluster
func configMapAdded(obj interface{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	fssConfigMap, ok := obj.(*v1.ConfigMap)
	if fssConfigMap == nil || !ok {
		log.Warnf("configMapAdded: unrecognized object %+v", obj)
		return
	}
	if fssConfigMap.Name == k8sOrchestratorInstance.supervisorFSS.configMapName &&
		fssConfigMap.Namespace == k8sOrchestratorInstance.supervisorFSS.configMapNamespace {
		k8sOrchestratorInstance.supervisorFSS.featureStates = fssConfigMap.Data
		log.Infof("New feature states values from %q stored successfully: %v", fssConfigMap.Name, k8sOrchestratorInstance.supervisorFSS.featureStates)
	} else if fssConfigMap.Name == k8sOrchestratorInstance.internalFSS.configMapName &&
		fssConfigMap.Namespace == k8sOrchestratorInstance.internalFSS.configMapNamespace {
		k8sOrchestratorInstance.internalFSS.featureStates = fssConfigMap.Data
		log.Infof("New feature states values from %q stored successfully: %v", fssConfigMap.Name, k8sOrchestratorInstance.internalFSS.featureStates)
	}
}

// configMapUpdated updates feature state switch values from configmap that has been created on K8s cluster
func configMapUpdated(oldObj, newObj interface{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	fssConfigMap, ok := newObj.(*v1.ConfigMap)
	if fssConfigMap == nil || !ok {
		log.Warnf("configMapUpdated: unrecognized new object %+v", newObj)
		return
	}
	if fssConfigMap.Name == k8sOrchestratorInstance.supervisorFSS.configMapName && fssConfigMap.Namespace == k8sOrchestratorInstance.supervisorFSS.configMapNamespace {
		k8sOrchestratorInstance.supervisorFSS.featureStates = fssConfigMap.Data
		log.Infof("New feature states values from %q stored successfully: %v", fssConfigMap.Name, k8sOrchestratorInstance.supervisorFSS.featureStates)
	} else if fssConfigMap.Name == k8sOrchestratorInstance.internalFSS.configMapName && fssConfigMap.Namespace == k8sOrchestratorInstance.internalFSS.configMapNamespace {
		k8sOrchestratorInstance.internalFSS.featureStates = fssConfigMap.Data
		log.Infof("New feature states values from %q stored successfully: %v", fssConfigMap.Name, k8sOrchestratorInstance.internalFSS.featureStates)
	}
}

// configMapDeleted clears the feature state switch values from the feature states map
func configMapDeleted(obj interface{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	fssConfigMap, ok := obj.(*v1.ConfigMap)
	if fssConfigMap == nil || !ok {
		log.Warnf("configMapDeleted: unrecognized object %+v", obj)
		return
	}
	// Supervisor FSS configmap
	if fssConfigMap.Name == k8sOrchestratorInstance.supervisorFSS.configMapName && fssConfigMap.Namespace == k8sOrchestratorInstance.supervisorFSS.configMapNamespace {
		for featureName := range k8sOrchestratorInstance.supervisorFSS.featureStates {
			k8sOrchestratorInstance.supervisorFSS.featureStates[featureName] = strconv.FormatBool(false)
		}
		log.Infof("configMapDeleted: %v deleted. Setting supervisor feature state values to false %v", fssConfigMap.Name,
			k8sOrchestratorInstance.supervisorFSS.featureStates)
	}
	// Internal FSS configmap
	if fssConfigMap.Name == k8sOrchestratorInstance.internalFSS.configMapName && fssConfigMap.Namespace == k8sOrchestratorInstance.internalFSS.configMapNamespace {
		for featureName := range k8sOrchestratorInstance.internalFSS.featureStates {
			k8sOrchestratorInstance.internalFSS.featureStates[featureName] = strconv.FormatBool(false)
		}
		log.Infof("configMapDeleted: %v deleted. Setting internal feature state values to false %v", fssConfigMap.Name,
			k8sOrchestratorInstance.internalFSS.featureStates)
	}
}

// IsFSSEnabled utilises the cluster flavor to check their corresponding FSS maps and returns
// if the feature state switch is enabled for the given feature indicated by featureName
func (c *K8sOrchestrator) IsFSSEnabled(ctx context.Context, featureName string) bool {
	log := logger.GetLogger(ctx)
	var (
		internalFeatureState   bool
		supervisorFeatureState bool
		err                    error
	)
	if c.clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		// Check internal FSS map
		if flag, ok := c.internalFSS.featureStates[featureName]; ok {
			internalFeatureState, err = strconv.ParseBool(flag)
			if err != nil {
				log.Errorf("Error while converting %v feature state value: %v to boolean. Setting the feature state to false", featureName, internalFeatureState)
				return false
			}
			return internalFeatureState
		}
		log.Debugf("Could not find the %s feature state in ConfigMap %s. Setting the feature state to false", featureName, c.internalFSS.configMapName)
		return false
	} else if c.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		// Check SV FSS map
		if flag, ok := c.supervisorFSS.featureStates[featureName]; ok {
			supervisorFeatureState, err = strconv.ParseBool(flag)
			if err != nil {
				log.Errorf("Error while converting %v feature state value: %v to boolean. Setting the feature state to false", featureName, supervisorFeatureState)
				return false
			}
			return supervisorFeatureState
		}
		log.Debugf("Could not find the %s feature state in ConfigMap %s. Setting the feature state to false", featureName, c.supervisorFSS.configMapName)
		return false
	} else if c.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Check internal FSS map
		if flag, ok := c.internalFSS.featureStates[featureName]; ok {
			internalFeatureState, err := strconv.ParseBool(flag)
			if err != nil {
				log.Errorf("Error while converting %v feature state value: %v to boolean. Setting the feature state to false", featureName, internalFeatureState)
				return false
			}
			if !internalFeatureState {
				// If FSS set to false, return
				log.Infof("%s feature state set to false in %s ConfigMap", featureName, c.internalFSS.configMapName)
				return internalFeatureState
			}
		} else {
			log.Debugf("Could not find the %s feature state in ConfigMap %s. Setting the feature state to false", featureName, c.internalFSS.configMapName)
			return false
		}
		// Check SV FSS map
		if flag, ok := c.supervisorFSS.featureStates[featureName]; ok {
			supervisorFeatureState, err := strconv.ParseBool(flag)
			if err != nil {
				log.Errorf("Error while converting %v feature state value: %v to boolean. Setting the feature state to false", featureName, supervisorFeatureState)
				return false
			}
			if !supervisorFeatureState {
				// If FSS set to false, return
				log.Infof("%s feature state set to false in %s ConfigMap", featureName, c.supervisorFSS.configMapName)
				return supervisorFeatureState
			}
		} else {
			log.Debugf("Could not find the %s feature state in ConfigMap %s. Setting the feature state to false", featureName, c.supervisorFSS.configMapName)
			return false
		}
		return true
	}
	log.Debugf("cluster flavor %q not recognised. Defaulting to false", c.clusterFlavor)
	return false
}
