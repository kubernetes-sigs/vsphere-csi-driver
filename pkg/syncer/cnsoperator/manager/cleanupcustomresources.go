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

package manager

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsunregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsunregistervolume/v1alpha1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// cleanUpCnsRegisterVolumeInstances cleans up successful CnsRegisterVolume instances
// whose creation time is past time specified in timeInMin
func cleanUpCnsRegisterVolumeInstances(ctx context.Context, restClientConfig *rest.Config, timeInMin int) {
	log := logger.GetLogger(ctx)
	log.Infof("cleanUpCnsRegisterVolumeInstances: start")
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("Failed to create CnsOperator client. Err: %+v", err)
		return
	}

	// Get list of CnsRegisterVolume instances from all namespaces
	cnsRegisterVolumesList := &cnsregistervolumev1alpha1.CnsRegisterVolumeList{}
	err = cnsOperatorClient.List(ctx, cnsRegisterVolumesList)
	if err != nil {
		log.Warnf("Failed to get CnsRegisterVolumes from supervisor cluster. Err: %+v", err)
		return
	}

	currentTime := time.Now()
	for _, cnsRegisterVolume := range cnsRegisterVolumesList.Items {
		var elapsedMinutes float64 = currentTime.Sub(cnsRegisterVolume.CreationTimestamp.Time).Minutes()
		if cnsRegisterVolume.Status.Registered && int(elapsedMinutes)-timeInMin >= 0 {
			err = cnsOperatorClient.Delete(ctx, &cnsregistervolumev1alpha1.CnsRegisterVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cnsRegisterVolume.Name,
					Namespace: cnsRegisterVolume.Namespace,
				},
			})
			if err != nil {
				log.Warnf("Failed to delete CnsRegisterVolume: %s on namespace: %s. Error: %v",
					cnsRegisterVolume.Name, cnsRegisterVolume.Namespace, err)
				continue
			}
			log.Infof("Successfully deleted CnsRegisterVolume: %s on namespace: %s",
				cnsRegisterVolume.Name, cnsRegisterVolume.Namespace)
		}
	}
}

// cleanUpCnsUnregisterVolumeInstances cleans up successful CnsUnregisterVolume instances
// whose creation time is past time specified in timeInMin
func cleanUpCnsUnregisterVolumeInstances(ctx context.Context, restClientConfig *rest.Config, timeInMin int) {
	log := logger.GetLogger(ctx)
	log.Infof("cleanUpCnsUnregisterVolumeInstances: start")
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("Failed to create CnsOperator client. Err: %+v", err)
		return
	}

	// Get list of CnsUnregisterVolume instances from all namespaces
	cnsUnregisterVolumesList := &cnsunregistervolumev1alpha1.CnsUnregisterVolumeList{}
	err = cnsOperatorClient.List(ctx, cnsUnregisterVolumesList)
	if err != nil {
		log.Warnf("Failed to get CnsUnregisterVolumes from supervisor cluster. Err: %+v", err)
		return
	}

	currentTime := time.Now()
	for _, cnsUnregisterVolume := range cnsUnregisterVolumesList.Items {
		var elapsedMinutes float64 = currentTime.Sub(cnsUnregisterVolume.CreationTimestamp.Time).Minutes()
		if cnsUnregisterVolume.Status.Unregistered && int(elapsedMinutes)-timeInMin >= 0 {
			err = cnsOperatorClient.Delete(ctx, &cnsunregistervolumev1alpha1.CnsUnregisterVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cnsUnregisterVolume.Name,
					Namespace: cnsUnregisterVolume.Namespace,
				},
			})
			if err != nil {
				log.Warnf("Failed to delete CnsUnregisterVolume: %s on namespace: %s. Error: %v",
					cnsUnregisterVolume.Name, cnsUnregisterVolume.Namespace, err)
				continue
			}
			log.Infof("Successfully deleted CnsUnregisterVolume: %s on namespace: %s",
				cnsUnregisterVolume.Name, cnsUnregisterVolume.Namespace)
		}
	}
}
