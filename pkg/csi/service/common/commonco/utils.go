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

package commonco

import (
	"context"
	"strings"

	cnstypes "github.com/vmware/govmomi/cns/types"

	csiconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco/k8sorchestrator"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// SetInitParams initializes the parameters required to create a container
// agnostic orchestrator instance.
func SetInitParams(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor, initParams *interface{},
	supervisorFSSName, supervisorFSSNamespace, internalFSSName, internalFSSNamespace, serviceMode string) {
	log := logger.GetLogger(ctx)
	// Set default values for FSS, if not given and initiate CO-agnostic init
	// params.
	switch clusterFlavor {
	case cnstypes.CnsClusterFlavorWorkload:
		if strings.TrimSpace(supervisorFSSName) == "" {
			log.Infof("Defaulting feature states configmap name to %q", csiconfig.DefaultSupervisorFSSConfigMapName)
			supervisorFSSName = csiconfig.DefaultSupervisorFSSConfigMapName
		}
		if strings.TrimSpace(supervisorFSSNamespace) == "" {
			log.Infof("Defaulting feature states configmap namespace to %q", csiconfig.DefaultCSINamespace)
			supervisorFSSNamespace = csiconfig.DefaultCSINamespace
		}
		*initParams = k8sorchestrator.K8sSupervisorInitParams{
			SupervisorFeatureStatesConfigInfo: csiconfig.FeatureStatesConfigInfo{
				Name:      supervisorFSSName,
				Namespace: supervisorFSSNamespace,
			},
			ServiceMode: serviceMode,
		}
	case cnstypes.CnsClusterFlavorVanilla:
		if strings.TrimSpace(internalFSSName) == "" {
			log.Infof("Defaulting feature states configmap name to %q", csiconfig.DefaultInternalFSSConfigMapName)
			internalFSSName = csiconfig.DefaultInternalFSSConfigMapName
		}
		if strings.TrimSpace(internalFSSNamespace) == "" {
			log.Infof("Defaulting feature states configmap namespace to %q", csiconfig.DefaultCSINamespace)
			internalFSSNamespace = csiconfig.DefaultCSINamespace
		}
		*initParams = k8sorchestrator.K8sVanillaInitParams{
			InternalFeatureStatesConfigInfo: csiconfig.FeatureStatesConfigInfo{
				Name:      internalFSSName,
				Namespace: internalFSSNamespace,
			},
			ServiceMode: serviceMode,
		}
	case cnstypes.CnsClusterFlavorGuest:
		if strings.TrimSpace(supervisorFSSName) == "" {
			log.Infof("Defaulting supervisor feature states configmap name to %q",
				csiconfig.DefaultSupervisorFSSConfigMapName)
			supervisorFSSName = csiconfig.DefaultSupervisorFSSConfigMapName
		}
		if strings.TrimSpace(supervisorFSSNamespace) == "" {
			log.Infof("Defaulting supervisor feature states configmap namespace to %q", csiconfig.DefaultCSINamespace)
			supervisorFSSNamespace = csiconfig.DefaultCSINamespace
		}
		if strings.TrimSpace(internalFSSName) == "" {
			log.Infof("Defaulting internal feature states configmap name to %q", csiconfig.DefaultInternalFSSConfigMapName)
			internalFSSName = csiconfig.DefaultInternalFSSConfigMapName
		}
		if strings.TrimSpace(internalFSSNamespace) == "" {
			log.Infof("Defaulting internal feature states configmap namespace to %q", csiconfig.DefaultCSINamespace)
			internalFSSNamespace = csiconfig.DefaultCSINamespace
		}
		*initParams = k8sorchestrator.K8sGuestInitParams{
			InternalFeatureStatesConfigInfo: csiconfig.FeatureStatesConfigInfo{
				Name:      internalFSSName,
				Namespace: internalFSSNamespace,
			},
			SupervisorFeatureStatesConfigInfo: csiconfig.FeatureStatesConfigInfo{
				Name:      supervisorFSSName,
				Namespace: supervisorFSSNamespace,
			},
			ServiceMode: serviceMode,
		}
	default:
		log.Fatalf("Unrecognised cluster flavor %q. Container orchestrator init params not initialized.", clusterFlavor)
	}
	log.Debugf("Container orchestrator init params: %+v", *initParams)
}
