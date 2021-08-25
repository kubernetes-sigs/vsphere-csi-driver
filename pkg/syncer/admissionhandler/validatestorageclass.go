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

package admissionhandler

import (
	"context"
	"encoding/json"

	admissionv1 "k8s.io/api/admission/v1"
	stroagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

var (
	unSupportedParameters = parameterSet{
		common.CSIMigrationParams:                   struct{}{},
		common.DatastoreMigrationParam:              struct{}{},
		common.DiskFormatMigrationParam:             struct{}{},
		common.HostFailuresToTolerateMigrationParam: struct{}{},
		common.ForceProvisioningMigrationParam:      struct{}{},
		common.CacheReservationMigrationParam:       struct{}{},
		common.DiskstripesMigrationParam:            struct{}{},
		common.ObjectspacereservationMigrationParam: struct{}{},
		common.IopslimitMigrationParam:              struct{}{},
	}
)

const (
	volumeExpansionErrorMessage = "AllowVolumeExpansion can not be set to true on the in-tree vSphere StorageClass"
	migrationParamErrorMessage  = "Invalid StorageClass Parameters. " +
		"Migration specific parameters should not be used in the StorageClass"
)

// validateStorageClass helps validate AdmissionReview requests for StroageClass.
func validateStorageClass(ctx context.Context, ar *admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	if containerOrchestratorUtility != nil && !containerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
		// If CSI migration is disabled and webhook is running,
		// skip validation for StorageClass.
		return &admissionv1.AdmissionResponse{
			Allowed: true,
		}
	}
	log := logger.GetLogger(ctx)
	req := ar.Request
	var result *metav1.Status
	allowed := true

	switch req.Kind.Kind {
	case "StorageClass":
		sc := stroagev1.StorageClass{}
		log.Debugf("JSON req.Object.Raw: %v", string(req.Object.Raw))
		if err := json.Unmarshal(req.Object.Raw, &sc); err != nil {
			log.Error("error deserializing storage class")
			return &admissionv1.AdmissionResponse{
				Result: &metav1.Status{
					Message: err.Error(),
				},
			}
		}
		log.Infof("Validating StorageClass: %q", sc.Name)
		// AllowVolumeExpansion check for kubernetes.io/vsphere-volume provisioner.
		if sc.Provisioner == "kubernetes.io/vsphere-volume" {
			if sc.AllowVolumeExpansion != nil && *sc.AllowVolumeExpansion {
				allowed = false
				result = &metav1.Status{
					Reason: volumeExpansionErrorMessage,
				}
			}
		} else if sc.Provisioner == "csi.vsphere.vmware.com" {
			// Migration parameters check for csi.vsphere.vmware.com provisioner.
			for param := range sc.Parameters {
				if unSupportedParameters.Has(param) {
					allowed = false
					result = &metav1.Status{
						Reason: migrationParamErrorMessage,
					}
					break
				}
			}
		}
		if allowed {
			log.Infof("Validation of StorageClass: %q Passed", sc.Name)
		} else {
			log.Errorf("validation of StorageClass: %q Failed", sc.Name)
		}
	default:
		allowed = false
		log.Errorf("Can't validate resource kind: %q using validateStorageClass function", req.Kind.Kind)
	}
	// return AdmissionResponse result
	return &admissionv1.AdmissionResponse{
		Allowed: allowed,
		Result:  result,
	}
}
