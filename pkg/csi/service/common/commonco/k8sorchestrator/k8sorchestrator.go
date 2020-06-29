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

	"github.com/prometheus/common/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var (
	k8sOrchestratorInstance        *K8sOrchestrator
	onceFork8sOrchestratorInstance sync.Once
)

// K8sOrchestrator defines set of properties specific to K8s
type K8sOrchestrator struct {
	featureStates   map[string]string
	informerManager *k8s.InformerManager
}

// Newk8sOrchestrator instantiates K8sOrchestrator object and returns this object
func Newk8sOrchestrator() *K8sOrchestrator {
	onceFork8sOrchestratorInstance.Do(func() {
		log.Info("Initializing k8sOrchestratorInstance")
		k8sOrchestratorInstance = &K8sOrchestrator{}
		k8sOrchestratorInstance.featureStates = make(map[string]string)
		ctx, log := logger.GetNewContextWithLogger()
		k8sClient, _ := k8s.NewClient(ctx)
		fssConfigMap, err := k8sClient.CoreV1().ConfigMaps(common.CSINamespace).Get(common.CSIFeatureStatesConfigMapName, metav1.GetOptions{})
		if err != nil {
			errMsg := fmt.Errorf("failed to fetch configmap %s. Setting the feature states to default values: %v. Error: %v", common.CSIFeatureStatesConfigMapName, k8sOrchestratorInstance.featureStates, err)
			log.Debug(errMsg)
		} else {
			updateFSSValues(ctx, fssConfigMap, k8sOrchestratorInstance)
		}
		// Set up kubernetes resource listeners for metadata syncer
		k8sOrchestratorInstance.informerManager = k8s.NewInformer(k8sClient)
		k8sOrchestratorInstance.informerManager.AddConfigMapListener(
			func(obj interface{}) {
				configMapAdded(obj, k8sOrchestratorInstance)
			}, // Add
			func(oldObj interface{}, newObj interface{}) { // Update
				configMapUpdated(oldObj, newObj, k8sOrchestratorInstance)
			},
			func(obj interface{}) {
				configMapDeleted(obj, k8sOrchestratorInstance)
			}) // Delete
		log.Info("k8sOrchestratorInstance initialized")
		k8sOrchestratorInstance.informerManager.Listen()
	})
	return k8sOrchestratorInstance
}

// configMapAdded adds feature state switch values from configmap that has been created on K8s cluster
func configMapAdded(obj interface{}, c *K8sOrchestrator) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	fssConfigMap, ok := obj.(*v1.ConfigMap)
	if fssConfigMap == nil || !ok {
		log.Warnf("configMapAdded: unrecognized object %+v", obj)
		return
	}
	if fssConfigMap.Name == common.CSIFeatureStatesConfigMapName {
		updateFSSValues(ctx, fssConfigMap, c)
	}
}

// configMapUpdated updates feature state switch values from configmap that has been created on K8s cluster
func configMapUpdated(oldObj, newObj interface{}, c *K8sOrchestrator) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	fssConfigMap, ok := newObj.(*v1.ConfigMap)
	if fssConfigMap == nil || !ok {
		log.Warnf("configMapUpdated: unrecognized new object %+v", newObj)
		return
	}
	if fssConfigMap.Name == common.CSIFeatureStatesConfigMapName {
		updateFSSValues(ctx, fssConfigMap, c)
	}
}

// configMapDeleted clears the feature state switch values from the feature states map
func configMapDeleted(obj interface{}, c *K8sOrchestrator) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	fssConfigMap, ok := obj.(*v1.ConfigMap)
	if fssConfigMap == nil || !ok {
		log.Warnf("configMapDeleted: unrecognized object %+v", obj)
		return
	}
	if fssConfigMap.Name == common.CSIFeatureStatesConfigMapName {
		for featureName := range c.featureStates {
			c.featureStates[featureName] = strconv.FormatBool(false)
		}
	}
	log.Infof("configMapDeleted: %v deleted. Setting feature state values to false %v", fssConfigMap.Name, c.featureStates)
}

// updateFSSValues updates feature state switch values in the k8sorchestrator
func updateFSSValues(ctx context.Context, fssConfigMap *v1.ConfigMap, c *K8sOrchestrator) {
	log := logger.GetLogger(ctx)
	c.featureStates = fssConfigMap.Data
	log.Infof("New feature states values stored successfully: %v", c.featureStates)
}

// IsFSSEnabled checks if feature state switch is enabled for the given feature indicated by featureName
func (c *K8sOrchestrator) IsFSSEnabled(ctx context.Context, featureName string) bool {
	log := logger.GetLogger(ctx)
	var featureState bool
	var err error
	if flag, ok := c.featureStates[featureName]; ok {
		featureState, err = strconv.ParseBool(flag)
		if err != nil {
			log.Errorf("Error while converting %v feature state value: %v to boolean. Setting the feature state to false", featureName, featureState)
			return false
		}
		log.Debugf("Feature: %v state is set to %v", featureName, featureState)
		return featureState
	}
	log.Debugf("Could not find the feature state for : %v. Setting the feature state to %v", featureName, featureState)
	return false
}
